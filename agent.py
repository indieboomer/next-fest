import time
import re
import sqlite3
import logging
import requests
from playwright.sync_api import sync_playwright
import schedule

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

DB_PATH = '/app/data/nextfest.db'


def init_db(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS games (
        appid INTEGER PRIMARY KEY,
        name TEXT,
        genres TEXT,
        tags TEXT,
        categories TEXT,
        has_ai_disclosure INTEGER DEFAULT 0,
        developers TEXT,
        publishers TEXT,
        release_date TEXT,
        supported_languages TEXT,
        price_initial INTEGER,
        price_final INTEGER,
        price_currency TEXT,
        first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        appid INTEGER NOT NULL,
        recommendations INTEGER,
        review_score INTEGER,
        review_score_desc TEXT,
        total_positive INTEGER,
        total_negative INTEGER,
        total_reviews INTEGER,
        player_count INTEGER,
        collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (appid) REFERENCES games (appid)
    )''')
    # Migrate existing snapshots table if columns are missing
    existing = {row[1] for row in c.execute("PRAGMA table_info(snapshots)")}
    for col, typedef in [
        ('review_score',      'INTEGER'),
        ('review_score_desc', 'TEXT'),
        ('total_positive',    'INTEGER'),
        ('total_negative',    'INTEGER'),
        ('total_reviews',     'INTEGER'),
        ('player_count',      'INTEGER'),
    ]:
        if col not in existing:
            c.execute(f'ALTER TABLE snapshots ADD COLUMN {col} {typedef}')
    conn.commit()


def scrape_appids():
    appids = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(args=['--no-sandbox', '--disable-dev-shm-usage'])
        context = browser.new_context()
        # Bypass age gate and GDPR consent popup
        context.add_cookies([
            {'name': 'birthtime',       'value': '631148401',  'domain': 'store.steampowered.com', 'path': '/'},
            {'name': 'lastagecheckage', 'value': '1-0-1990',   'domain': 'store.steampowered.com', 'path': '/'},
            {'name': 'mature_content',  'value': '1',          'domain': 'store.steampowered.com', 'path': '/'},
            {'name': 'cookiesettings',  'value': '{"version":1,"preference_cookies":true,"advertising_cookies":true,"analytics_cookies":true}',
             'domain': 'store.steampowered.com', 'path': '/'},
        ])
        page = context.new_page()

        page.goto("https://store.steampowered.com/sale/nextfest", wait_until='domcontentloaded')

        # Sections use id="SaleSection_XXXXX" — wait for at least one to appear
        try:
            page.wait_for_selector('[id^="SaleSection_"]', timeout=20000)
        except Exception:
            log.warning("SaleSection elements not found within timeout")

        time.sleep(3)

        # Scroll each section into view one by one — sections use Intersection Observer
        # and only fire their AJAX request when they actually enter the viewport
        sections = page.query_selector_all('[id^="SaleSection_"]')
        log.info("Found %d sections — scrolling each into view", len(sections))
        for i, section in enumerate(sections):
            section.scroll_into_view_if_needed()
            time.sleep(3)  # wait for section's AJAX to complete
            link_count = len(page.query_selector_all('a[href*="/app/"]'))
            log.info("Section %d/%d — cumulative app-links: %d", i + 1, len(sections), link_count)

        # Final pass: scroll back to top then to bottom for any missed late-loaders
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)

        log.info("Final count — sections: %d, app-links: %d, ds-appid: %d",
                 len(page.query_selector_all('[id^="SaleSection_"]')),
                 len(page.query_selector_all('a[href*="/app/"]')),
                 len(page.query_selector_all('[data-ds-appid]')))

        # Primary: data-ds-appid attributes on game cards
        for el in page.query_selector_all('[data-ds-appid]'):
            val = el.get_attribute('data-ds-appid') or ''
            for aid in val.split(','):
                aid = aid.strip()
                if aid.isdigit():
                    appids.add(int(aid))

        # Fallback: parse /app/ href links
        for link in page.query_selector_all('a[href*="/app/"]'):
            href = link.get_attribute('href') or ''
            m = re.search(r'/app/(\d+)', href)
            if m:
                appids.add(int(m.group(1)))

        browser.close()
    return appids


def fetch_game(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=english"
    response = requests.get(url, timeout=10)
    data = response.json()
    if not data[str(appid)]['success']:
        return None
    return data[str(appid)]['data']


def fetch_reviews(appid):
    """Returns review summary dict or empty dict on failure."""
    url = (f"https://store.steampowered.com/appreviews/{appid}"
           f"?json=1&language=all&num_per_page=0&filter=recent")
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get('success') == 1:
            return data.get('query_summary', {})
    except Exception as e:
        log.warning("Reviews fetch failed for %d: %s", appid, e)
    return {}


def fetch_player_count(appid):
    """Returns current concurrent player count or None on failure."""
    url = (f"https://api.steampowered.com/ISteamUserStats/"
           f"GetNumberOfCurrentPlayers/v1/?appid={appid}")
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data['response']['result'] == 1:
            return data['response']['player_count']
    except Exception as e:
        log.warning("Player count fetch failed for %d: %s", appid, e)
    return None


def collect():
    log.info("Starting data collection...")

    appids = scrape_appids()
    log.info("Found %d appids", len(appids))

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if not appids:
        log.info("No appids found — fest may not have started yet")
        conn.close()
        return

    c = conn.cursor()

    for appid in appids:
        try:
            game = fetch_game(appid)
            if game is None:
                log.warning("API returned success=false for appid %d", appid)
                continue

            name = game.get('name', '')
            genres = ', '.join([g['description'] for g in game.get('genres', [])])

            store_tags = game.get('tags', {})
            if isinstance(store_tags, dict):
                tags = ', '.join(store_tags.values())
            elif isinstance(store_tags, list):
                tags = ', '.join([t.get('description', '') for t in store_tags])
            else:
                tags = ''

            categories_list = game.get('categories', [])
            categories = ', '.join([cat['description'] for cat in categories_list])
            has_ai_disclosure = int(any(
                'AI' in cat['description'] or 'ai generated' in cat['description'].lower()
                for cat in categories_list
            ))

            developers = ', '.join(game.get('developers', []))
            publishers = ', '.join(game.get('publishers', []))
            release_date = game.get('release_date', {}).get('date', '')
            supported_languages = game.get('supported_languages', '')
            recommendations = game.get('recommendations', {}).get('total', 0)

            price_overview = game.get('price_overview', {})
            price_initial = price_overview.get('initial', 0)
            price_final = price_overview.get('final', 0)
            price_currency = price_overview.get('currency', '')

            # Reviews
            reviews = fetch_reviews(appid)
            review_score      = reviews.get('review_score')
            review_score_desc = reviews.get('review_score_desc')
            total_positive    = reviews.get('total_positive')
            total_negative    = reviews.get('total_negative')
            total_reviews     = reviews.get('total_reviews')

            # Live player count
            player_count = fetch_player_count(appid)

            c.execute('''INSERT OR REPLACE INTO games
                (appid, name, genres, tags, categories, has_ai_disclosure,
                 developers, publishers, release_date, supported_languages,
                 price_initial, price_final, price_currency,
                 first_seen, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        COALESCE((SELECT first_seen FROM games WHERE appid = ?), CURRENT_TIMESTAMP),
                        CURRENT_TIMESTAMP)''',
                (appid, name, genres, tags, categories, has_ai_disclosure,
                 developers, publishers, release_date, supported_languages,
                 price_initial, price_final, price_currency,
                 appid))

            c.execute('''INSERT INTO snapshots
                (appid, recommendations,
                 review_score, review_score_desc,
                 total_positive, total_negative, total_reviews,
                 player_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (appid, recommendations,
                 review_score, review_score_desc,
                 total_positive, total_negative, total_reviews,
                 player_count))

            log.info("Collected %s (%d) — reviews: %s, players: %s",
                     name, appid, review_score_desc or 'n/a', player_count or 'n/a')
        except Exception as e:
            log.error("Error fetching appid %d: %s", appid, e)
        time.sleep(1)

    conn.commit()
    conn.close()
    log.info("Data collection complete")


schedule.every().hour.do(collect)

collect()

while True:
    schedule.run_pending()
    time.sleep(60)

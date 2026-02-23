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
        collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (appid) REFERENCES games (appid)
    )''')
    conn.commit()


def scrape_appids():
    appids = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page()
        page.goto("https://store.steampowered.com/sale/nextfest")
        page.wait_for_load_state('networkidle')

        last_height = page.evaluate("document.body.scrollHeight")
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        links = page.query_selector_all('a[href*="/app/"]')
        for link in links:
            href = link.get_attribute('href')
            match = re.search(r'/app/(\d+)', href)
            if match:
                appids.add(int(match.group(1)))

        browser.close()
    return appids


def fetch_game(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=english"
    response = requests.get(url, timeout=10)
    data = response.json()
    if not data[str(appid)]['success']:
        return None
    return data[str(appid)]['data']


def collect():
    log.info("Starting data collection...")

    appids = scrape_appids()
    log.info("Found %d appids", len(appids))

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
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

            c.execute('INSERT INTO snapshots (appid, recommendations) VALUES (?, ?)',
                      (appid, recommendations))

            log.info("Collected %s (%d)", name, appid)
        except Exception as e:
            log.error("Error fetching appid %d: %s", appid, e)
        time.sleep(1)

    conn.commit()
    conn.close()
    log.info("Data collection complete")


schedule.every().day.at("00:00").do(collect)

collect()

while True:
    schedule.run_pending()
    time.sleep(60)

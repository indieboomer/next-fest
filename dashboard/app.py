import os
import sqlite3
import json
from flask import Flask, render_template, request, jsonify, send_file
from openai import OpenAI

app = Flask(__name__)
DB_PATH = '/app/data/nextfest.db'
client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])


def get_db():
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.OperationalError:
        return None


def build_ai_context(conn):
    """Build a compact dataset summary to send as GPT-4o context."""
    c = conn.cursor()
    lines = []

    total = c.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    ai_count = c.execute("SELECT COUNT(*) FROM games WHERE has_ai_disclosure=1").fetchone()[0]
    free_count = c.execute("SELECT COUNT(*) FROM games WHERE price_final=0").fetchone()[0]
    lines.append(f"Total games: {total}. AI-disclosed: {ai_count}. Free-to-play: {free_count}.")

    # Genre distribution
    rows = c.execute("""
        SELECT genres, COUNT(*) as cnt FROM games
        WHERE genres != '' GROUP BY genres ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    if rows:
        genre_summary = ', '.join(f"{r['genres']} ({r['cnt']})" for r in rows)
        lines.append(f"Top genres: {genre_summary}.")

    # Latest snapshot stats
    snap_date = c.execute("SELECT MAX(collected_at) FROM snapshots").fetchone()[0]
    if snap_date:
        lines.append(f"Latest snapshot: {snap_date}.")

        top_rec = c.execute("""
            SELECT g.name, s.recommendations, s.review_score_desc, s.player_count
            FROM snapshots s JOIN games g ON g.appid = s.appid
            WHERE s.collected_at = (SELECT MAX(collected_at) FROM snapshots WHERE appid = s.appid)
            ORDER BY s.recommendations DESC LIMIT 20
        """).fetchall()
        if top_rec:
            lines.append("Top 20 by recommendations:")
            for r in top_rec:
                lines.append(f"  - {r['name']}: {r['recommendations']} recs, "
                             f"{r['review_score_desc'] or 'no reviews'}, "
                             f"{r['player_count'] or 0} players")

        top_players = c.execute("""
            SELECT g.name, s.player_count, s.review_score_desc
            FROM snapshots s JOIN games g ON g.appid = s.appid
            WHERE s.collected_at = (SELECT MAX(collected_at) FROM snapshots WHERE appid = s.appid)
              AND s.player_count IS NOT NULL
            ORDER BY s.player_count DESC LIMIT 20
        """).fetchall()
        if top_players:
            lines.append("Top 20 by current players:")
            for r in top_players:
                lines.append(f"  - {r['name']}: {r['player_count']} players, "
                             f"{r['review_score_desc'] or 'no reviews'}")

    return '\n'.join(lines)


@app.route('/')
def index():
    conn = get_db()
    if conn is None:
        return render_template('index.html', stats=None)

    c = conn.cursor()
    stats = {}
    stats['total_games'] = c.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    stats['ai_games'] = c.execute("SELECT COUNT(*) FROM games WHERE has_ai_disclosure=1").fetchone()[0]
    stats['free_games'] = c.execute("SELECT COUNT(*) FROM games WHERE price_final=0").fetchone()[0]
    stats['snapshot_count'] = c.execute("SELECT COUNT(DISTINCT collected_at) FROM snapshots").fetchone()[0]
    stats['last_snapshot'] = c.execute("SELECT MAX(collected_at) FROM snapshots").fetchone()[0]

    top_genres = c.execute("""
        SELECT genres, COUNT(*) as cnt FROM games
        WHERE genres != '' GROUP BY genres ORDER BY cnt DESC LIMIT 8
    """).fetchall()
    stats['top_genres'] = [dict(r) for r in top_genres]

    top_games = c.execute("""
        SELECT g.name, g.appid, s.total_reviews, s.review_score_desc, s.player_count
        FROM snapshots s JOIN games g ON g.appid = s.appid
        WHERE s.collected_at = (SELECT MAX(collected_at) FROM snapshots WHERE appid = s.appid)
        ORDER BY s.total_reviews DESC NULLS LAST LIMIT 10
    """).fetchall()
    stats['top_games'] = [dict(r) for r in top_games]

    SPOTLIGHT_APPID = 3700780  # Wild West Pioneers Demo
    spotlight_row = c.execute("""
        SELECT g.name, g.appid, g.genres, g.review_score_desc AS g_review,
               s.player_count, s.total_reviews, s.review_score_desc,
               s.recommendations, s.total_positive, s.total_negative,
               s.collected_at
        FROM games g
        LEFT JOIN snapshots s ON s.appid = g.appid
            AND s.collected_at = (SELECT MAX(collected_at) FROM snapshots WHERE appid = g.appid)
        WHERE g.appid = ?
    """, (SPOTLIGHT_APPID,)).fetchone()
    spotlight = dict(spotlight_row) if spotlight_row else None

    conn.close()
    return render_template('index.html', stats=stats, spotlight=spotlight)


@app.route('/games')
def games():
    conn = get_db()
    if conn is None:
        return render_template('games.html', games=[], filters={})

    sort = request.args.get('sort', 'recommendations')
    ai_only = request.args.get('ai', '')
    genre_filter = request.args.get('genre', '')

    valid_sorts = {'recommendations', 'player_count', 'total_reviews', 'name'}
    if sort not in valid_sorts:
        sort = 'recommendations'

    where_clauses = []
    params = []
    if ai_only == '1':
        where_clauses.append("g.has_ai_disclosure = 1")
    if genre_filter:
        where_clauses.append("g.genres LIKE ?")
        params.append(f'%{genre_filter}%')

    where_sql = ('WHERE ' + ' AND '.join(where_clauses)) if where_clauses else ''

    order_col = f"s.{sort}" if sort != 'name' else "g.name"
    rows = conn.execute(f"""
        SELECT g.appid, g.name, g.genres, g.has_ai_disclosure, g.price_final, g.price_currency,
               s.recommendations, s.review_score_desc, s.player_count, s.total_reviews
        FROM games g
        LEFT JOIN snapshots s ON s.appid = g.appid
            AND s.collected_at = (SELECT MAX(collected_at) FROM snapshots WHERE appid = g.appid)
        {where_sql}
        ORDER BY {order_col} DESC NULLS LAST
    """, params).fetchall()

    all_genres = conn.execute("""
        SELECT DISTINCT genres FROM games WHERE genres != '' ORDER BY genres
    """).fetchall()

    conn.close()
    return render_template('games.html',
                           games=[dict(r) for r in rows],
                           filters={'sort': sort, 'ai': ai_only, 'genre': genre_filter},
                           all_genres=[r['genres'] for r in all_genres])


@app.route('/games/<int:appid>')
def game_detail(appid):
    conn = get_db()
    if conn is None:
        return "Database not available", 503

    game = conn.execute("SELECT * FROM games WHERE appid = ?", (appid,)).fetchone()
    if game is None:
        return "Game not found", 404

    snapshots = conn.execute("""
        SELECT collected_at, recommendations, player_count,
               total_positive, total_negative, review_score_desc
        FROM snapshots WHERE appid = ? ORDER BY collected_at ASC
    """, (appid,)).fetchall()

    conn.close()

    chart_labels = [s['collected_at'] for s in snapshots]
    chart_recs   = [s['recommendations'] for s in snapshots]
    chart_players = [s['player_count'] for s in snapshots]

    return render_template('game_detail.html',
                           game=dict(game),
                           snapshots=[dict(s) for s in snapshots],
                           chart_labels=json.dumps(chart_labels),
                           chart_recs=json.dumps(chart_recs),
                           chart_players=json.dumps(chart_players))


@app.route('/chat', methods=['GET', 'POST'])
def chat():
    if request.method == 'GET':
        return render_template('chat.html')

    data = request.get_json()
    messages = data.get('messages', [])
    if not messages:
        return jsonify({'reply': 'No message received.'}), 400

    conn = get_db()
    context = build_ai_context(conn) if conn else "No data collected yet."
    if conn:
        conn.close()

    system_prompt = (
        "You are a Steam Next Fest data analyst. You have access to the following "
        "dataset collected by an automated agent during the festival:\n\n"
        f"{context}\n\n"
        "Answer the user's questions concisely and analytically. "
        "When you don't have specific data, say so clearly."
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": system_prompt}] + messages,
        max_tokens=1024,
    )
    reply = response.choices[0].message.content
    return jsonify({'reply': reply})


@app.route('/download-db')
def download_db():
    if not os.path.exists(DB_PATH):
        return "Database not available yet.", 503
    return send_file(DB_PATH, as_attachment=True, download_name='nextfest.db',
                     mimetype='application/x-sqlite3')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)

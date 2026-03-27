from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import sqlite3, os, json, threading, time, logging, queue, requests
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

BASE_DIR = os.environ.get('STORAGE_DIR', '/tmp/tiklib')
DB_PATH  = os.path.join(BASE_DIR, 'library.db')
os.makedirs(BASE_DIR, exist_ok=True)

RAPIDAPI_KEY  = os.environ.get('RAPIDAPI_KEY', '')
RAPIDAPI_HOST = 'tiktok-scraper7.p.rapidapi.com'
MAX_SECS      = 120  # تخطي الفيديوهات اللي فوق دقيقتين

# ── SSE ───────────────────────────────────────────────────────────────────────
sse_clients = []
sse_lock = threading.Lock()

def push(data):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_lock:
        for q in list(sse_clients):
            try: q.put_nowait(msg)
            except: sse_clients.remove(q)

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS profiles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT UNIQUE NOT NULL,
                url         TEXT NOT NULL,
                added_at    TEXT DEFAULT (datetime('now')),
                last_synced TEXT,
                video_count INTEGER DEFAULT 0,
                status      TEXT DEFAULT 'pending'
            );
            CREATE TABLE IF NOT EXISTS videos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id   INTEGER NOT NULL,
                video_id     TEXT UNIQUE NOT NULL,
                title        TEXT,
                play_url     TEXT,
                nowm_url     TEXT,
                thumbnail    TEXT,
                duration     INTEGER DEFAULT 0,
                view_count   INTEGER DEFAULT 0,
                like_count   INTEGER DEFAULT 0,
                added_at     TEXT DEFAULT (datetime('now')),
                published_at TEXT,
                author       TEXT,
                FOREIGN KEY (profile_id) REFERENCES profiles(id)
            );
        ''')

init_db()

# ── RapidAPI calls ────────────────────────────────────────────────────────────
def rapidapi_get(endpoint, params):
    headers = {
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST
    }
    url = f"https://{RAPIDAPI_HOST}{endpoint}"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_user_videos(username, count=30, cursor=0):
    """جلب فيديوهات البروفايل"""
    try:
        clean = username.lstrip('@')
        data = rapidapi_get('/user/posts', {
            'unique_id': clean,
            'count': str(count),
            'cursor': str(cursor)
        })
        return data
    except Exception as e:
        log.error(f"RapidAPI error for {username}: {e}")
        return None

def get_video_nowm(video_url):
    """جلب رابط الفيديو بدون علامة مائية"""
    try:
        data = rapidapi_get('/video/info', {'url': video_url})
        # محاولة جلب nowatermark url
        if data and data.get('data'):
            d = data['data']
            return (
                d.get('play') or
                d.get('wmplay') or
                d.get('video', {}).get('play_addr', {}).get('url_list', [''])[0]
            )
    except Exception as e:
        log.error(f"nowm error: {e}")
    return None

# ── Sync profile ──────────────────────────────────────────────────────────────
def sync_profile(profile_id, username, new_only=False):
    db = get_db()
    try:
        db.execute("UPDATE profiles SET status='downloading' WHERE id=?", (profile_id,))
        db.commit()

        count_fetch = 15 if new_only else 50
        data = get_user_videos(username, count=count_fetch)

        if not data or not data.get('data'):
            log.warning(f"[{username}] No data returned from API")
            db.execute("UPDATE profiles SET status='error' WHERE id=?", (profile_id,))
            db.commit()
            return

        items = data['data'].get('videos', []) or data['data'].get('aweme_list', []) or []
        if not items and isinstance(data.get('data'), list):
            items = data['data']

        log.info(f"[{username}] Got {len(items)} videos from API")

        new_count = 0
        skipped   = 0

        for item in items:
            # استخرج البيانات من الـ response
            vid_id   = str(item.get('video_id') or item.get('aweme_id') or item.get('id') or '')
            duration = int(item.get('duration') or item.get('video', {}).get('duration', 0) or 0)
            # duration أحياناً بالميلي ثانية
            if duration > 10000:
                duration = duration // 1000

            if duration > MAX_SECS:
                skipped += 1
                continue

            if not vid_id:
                continue

            if db.execute("SELECT id FROM videos WHERE video_id=?", (vid_id,)).fetchone():
                continue

            # روابط الفيديو
            play_url = (
                item.get('play') or
                item.get('wmplay') or
                item.get('video', {}).get('play_addr', {}).get('url_list', [''])[0] or ''
            )
            nowm_url = (
                item.get('play') or
                item.get('hdplay') or
                play_url
            )

            # الصورة المصغرة
            thumbnail = (
                item.get('cover') or
                item.get('origin_cover') or
                item.get('dynamic_cover') or
                item.get('video', {}).get('cover', {}).get('url_list', [''])[0] or ''
            )

            title = item.get('title') or item.get('desc') or ''
            views = int(item.get('play_count') or item.get('statistics', {}).get('play_count', 0) or 0)
            likes = int(item.get('digg_count') or item.get('statistics', {}).get('digg_count', 0) or 0)
            pub   = str(item.get('create_time') or '')

            db.execute('''
                INSERT OR IGNORE INTO videos
                (profile_id, video_id, title, play_url, nowm_url, thumbnail,
                 duration, view_count, like_count, published_at, author)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ''', (profile_id, vid_id, title, play_url, nowm_url, thumbnail,
                  duration, views, likes, pub, username))
            new_count += 1

            if new_count > 0:
                push({'type': 'new_video', 'username': username,
                      'title': title, 'thumbnail': thumbnail})

        total = db.execute("SELECT COUNT(*) FROM videos WHERE profile_id=?", (profile_id,)).fetchone()[0]
        db.execute(
            "UPDATE profiles SET last_synced=?, video_count=?, status='active' WHERE id=?",
            (datetime.now().isoformat(), total, profile_id)
        )
        db.commit()
        push({'type': 'sync_done', 'username': username, 'new': new_count})
        log.info(f"[{username}] Done: +{new_count} new, {skipped} skipped (>{MAX_SECS}s)")

    except Exception as e:
        log.error(f"[{username}] sync error: {e}")
        db.execute("UPDATE profiles SET status='error' WHERE id=?", (profile_id,))
        db.commit()
    finally:
        db.close()

# ── Cleanup old videos ────────────────────────────────────────────────────────
def delete_old():
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    db = get_db()
    old = db.execute("SELECT id, profile_id FROM videos WHERE added_at < ?", (cutoff,)).fetchall()
    if old:
        ids = [v['id'] for v in old]
        db.execute(f"DELETE FROM videos WHERE id IN ({','.join('?'*len(ids))})", ids)
        db.execute('''UPDATE profiles SET video_count=(
            SELECT COUNT(*) FROM videos WHERE profile_id=profiles.id)''')
        db.commit()
        log.info(f"Deleted {len(ids)} old videos")
    db.close()

# ── Scheduler ─────────────────────────────────────────────────────────────────
def scheduled_sync():
    db = get_db()
    profiles = db.execute(
        "SELECT * FROM profiles WHERE status NOT IN ('paused','downloading')"
    ).fetchall()
    db.close()
    for p in profiles:
        threading.Thread(
            target=sync_profile,
            args=(p['id'], p['username'], True),
            daemon=True
        ).start()
        time.sleep(2)
    delete_old()

scheduler = BackgroundScheduler(timezone='UTC')
scheduler.add_job(scheduled_sync, 'interval', hours=6,  id='sync')
scheduler.add_job(delete_old,     'cron',     hour=3,   id='cleanup')
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

# ── Helpers ───────────────────────────────────────────────────────────────────
def extract_username(url):
    url = url.strip().rstrip('/')
    if not url.startswith('http'):
        return '@' + url.lstrip('@')
    for part in url.split('/'):
        if part.startswith('@'):
            return part
    return '@' + url.split('/')[-1].lstrip('@')

# ── API Routes ────────────────────────────────────────────────────────────────
@app.route('/api/profiles', methods=['GET'])
def get_profiles():
    db = get_db()
    rows = db.execute("SELECT * FROM profiles ORDER BY added_at DESC").fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/profiles', methods=['POST'])
def add_profile():
    data = request.json or {}
    url  = data.get('url', '').strip()
    if not url: return jsonify({'error': 'URL مطلوب'}), 400

    username = extract_username(url)
    db = get_db()
    try:
        pid = db.execute(
            "INSERT INTO profiles (username, url, status) VALUES (?,?,'pending')",
            (username, url)
        ).lastrowid
        db.commit()
        threading.Thread(
            target=sync_profile, args=(pid, username, False), daemon=True
        ).start()
        return jsonify({'id': pid, 'username': username, 'status': 'downloading'})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'البروفايل موجود بالفعل'}), 409
    finally:
        db.close()

@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
def delete_profile(pid):
    db = get_db()
    db.execute("DELETE FROM videos WHERE profile_id=?", (pid,))
    db.execute("DELETE FROM profiles WHERE id=?", (pid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})

@app.route('/api/profiles/<int:pid>/sync', methods=['POST'])
def do_sync(pid):
    db = get_db()
    p = db.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
    db.close()
    if not p: return jsonify({'error': 'مش موجود'}), 404
    if p['status'] == 'downloading': return jsonify({'status': 'already_running'})
    threading.Thread(
        target=sync_profile, args=(p['id'], p['username'], False), daemon=True
    ).start()
    return jsonify({'status': 'syncing'})

@app.route('/api/videos')
def get_videos():
    pid = request.args.get('profile_id')
    q   = request.args.get('q', '')
    db  = get_db()
    base = "SELECT v.*, p.username FROM videos v JOIN profiles p ON v.profile_id=p.id"
    if pid:
        rows = db.execute(f"{base} WHERE v.profile_id=? ORDER BY v.added_at DESC LIMIT 200", (pid,)).fetchall()
    elif q:
        rows = db.execute(f"{base} WHERE v.title LIKE ? ORDER BY v.added_at DESC LIMIT 100", (f'%{q}%',)).fetchall()
    else:
        rows = db.execute(f"{base} ORDER BY v.added_at DESC LIMIT 200").fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/stats')
def stats():
    db = get_db()
    p = db.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
    v = db.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    db.close()
    return jsonify({'profiles': p, 'videos': v})

@app.route('/api/config')
def config():
    return jsonify({'has_key': bool(RAPIDAPI_KEY)})

@app.route('/api/events')
def sse_stream():
    q = queue.Queue(maxsize=50)
    with sse_lock:
        sse_clients.append(q)
    def gen():
        try:
            yield 'data: {"type":"connected"}\n\n'
            while True:
                try:
                    yield q.get(timeout=25)
                except:
                    yield ': ping\n\n'
        finally:
            with sse_lock:
                if q in sse_clients: sse_clients.remove(q)
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/favicon.ico')
def favicon(): return '', 204

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, threaded=True)

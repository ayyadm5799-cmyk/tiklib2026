from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import sqlite3, os, json, threading, time, logging, queue, requests, hashlib
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

BASE_DIR      = os.environ.get('STORAGE_DIR', '/tmp/tiklib')
DB_PATH       = os.path.join(BASE_DIR, 'library.db')
RAPIDAPI_KEY  = os.environ.get('RAPIDAPI_KEY', '')
MAX_SECS      = 120

os.makedirs(BASE_DIR, exist_ok=True)

# ── SSE ───────────────────────────────────────────────────────────────────────
sse_clients, sse_lock = [], threading.Lock()

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
                platform    TEXT DEFAULT 'tiktok',
                added_at    TEXT DEFAULT (datetime('now')),
                last_synced TEXT,
                video_count INTEGER DEFAULT 0,
                status      TEXT DEFAULT 'pending',
                avatar      TEXT DEFAULT ''
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
                platform     TEXT DEFAULT 'tiktok',
                favorited    INTEGER DEFAULT 0,
                note         TEXT DEFAULT '',
                tags         TEXT DEFAULT '',
                watch_count  INTEGER DEFAULT 0,
                FOREIGN KEY (profile_id) REFERENCES profiles(id)
            );
            CREATE TABLE IF NOT EXISTS tags (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                color TEXT DEFAULT '#25f4ee'
            );
        ''')

init_db()

# ── RapidAPI helpers ──────────────────────────────────────────────────────────
HEADERS_TIKTOK = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": "tiktok-scraper7.p.rapidapi.com"
}
HEADERS_INSTA = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
}
HEADERS_YT = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": "youtube-media-downloader.p.rapidapi.com"
}

def api_get(url, headers, params={}, timeout=30):
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ── Platform: TikTok ──────────────────────────────────────────────────────────
def fetch_tiktok(username, count=30, cursor=0):
    clean = username.lstrip('@')
    data = api_get(
        'https://tiktok-scraper7.p.rapidapi.com/user/posts',
        HEADERS_TIKTOK,
        {'unique_id': clean, 'count': str(count), 'cursor': str(cursor)}
    )
    items = []
    if data and data.get('data'):
        d = data['data']
        items = d.get('videos') or d.get('aweme_list') or (d if isinstance(d, list) else [])
    results = []
    for item in items:
        vid_id   = str(item.get('video_id') or item.get('aweme_id') or item.get('id') or '')
        duration = int(item.get('duration') or item.get('video', {}).get('duration', 0) or 0)
        if duration > 10000: duration //= 1000
        if not vid_id or duration > MAX_SECS: continue
        results.append({
            'video_id':    vid_id,
            'title':       item.get('title') or item.get('desc') or '',
            'play_url':    item.get('play') or item.get('wmplay') or '',
            'nowm_url':    item.get('play') or item.get('hdplay') or item.get('wmplay') or '',
            'thumbnail':   item.get('cover') or item.get('origin_cover') or '',
            'duration':    duration,
            'view_count':  int(item.get('play_count') or 0),
            'like_count':  int(item.get('digg_count') or 0),
            'published_at':str(item.get('create_time') or ''),
            'platform':    'tiktok'
        })
    return results

# ── Platform: Instagram ───────────────────────────────────────────────────────
def fetch_instagram(username, count=20):
    clean = username.lstrip('@')
    try:
        data = api_get(
            'https://instagram-scraper-api2.p.rapidapi.com/v1/posts',
            HEADERS_INSTA,
            {'username_or_id_or_url': clean}
        )
        items = data.get('data', {}).get('items', [])
        results = []
        for item in items[:count]:
            # Reels only (video)
            if item.get('media_type') not in (2, 'VIDEO'): continue
            vid_id   = str(item.get('id') or item.get('pk') or '')
            duration = int(item.get('video_duration') or 0)
            if duration > MAX_SECS: continue
            versions = item.get('video_versions') or []
            play_url = versions[0].get('url', '') if versions else ''
            thumb    = ''
            img_v    = item.get('image_versions2', {}).get('candidates', [])
            if img_v: thumb = img_v[0].get('url', '')
            results.append({
                'video_id':    vid_id,
                'title':       item.get('caption', {}).get('text', '') if item.get('caption') else '',
                'play_url':    play_url,
                'nowm_url':    play_url,
                'thumbnail':   thumb,
                'duration':    duration,
                'view_count':  int(item.get('view_count') or item.get('play_count') or 0),
                'like_count':  int(item.get('like_count') or 0),
                'published_at':str(item.get('taken_at') or ''),
                'platform':    'instagram'
            })
        return results
    except Exception as e:
        log.error(f"Instagram fetch error: {e}")
        return []

# ── Platform: YouTube (Shorts) ────────────────────────────────────────────────
def fetch_youtube(channel_url, count=20):
    try:
        data = api_get(
            'https://youtube-media-downloader.p.rapidapi.com/v2/channel/shorts',
            HEADERS_YT,
            {'channelUrl': channel_url, 'nextToken': ''}
        )
        items = data.get('shorts') or data.get('items') or []
        results = []
        for item in items[:count]:
            vid_id   = str(item.get('id') or item.get('videoId') or '')
            duration = int(item.get('duration') or item.get('lengthSeconds') or 0)
            if not vid_id: continue
            # YouTube Shorts كلها أقل من دقيقة
            thumb = ''
            thumbs = item.get('thumbnails') or []
            if thumbs: thumb = thumbs[-1].get('url', '')
            play_url = f"https://www.youtube.com/shorts/{vid_id}"
            results.append({
                'video_id':    vid_id,
                'title':       item.get('title') or item.get('name') or '',
                'play_url':    play_url,
                'nowm_url':    play_url,
                'thumbnail':   thumb,
                'duration':    duration,
                'view_count':  int(item.get('viewCount') or item.get('views') or 0),
                'like_count':  0,
                'published_at':str(item.get('publishedAt') or ''),
                'platform':    'youtube'
            })
        return results
    except Exception as e:
        log.error(f"YouTube fetch error: {e}")
        return []

# ── Sync ──────────────────────────────────────────────────────────────────────
def sync_profile(profile_id, username, url, platform, new_only=False):
    db = get_db()
    try:
        db.execute("UPDATE profiles SET status='downloading' WHERE id=?", (profile_id,))
        db.commit()

        count = 15 if new_only else 50

        if platform == 'tiktok':
            items = fetch_tiktok(username, count=count)
        elif platform == 'instagram':
            items = fetch_instagram(username, count=count)
        elif platform == 'youtube':
            items = fetch_youtube(url, count=count)
        else:
            items = []

        log.info(f"[{username}] Got {len(items)} videos")

        new_count = 0
        for v in items:
            if db.execute("SELECT id FROM videos WHERE video_id=?", (v['video_id'],)).fetchone():
                continue
            db.execute('''
                INSERT OR IGNORE INTO videos
                (profile_id,video_id,title,play_url,nowm_url,thumbnail,
                 duration,view_count,like_count,published_at,author,platform)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (profile_id, v['video_id'], v['title'], v['play_url'], v['nowm_url'],
                  v['thumbnail'], v['duration'], v['view_count'], v['like_count'],
                  v['published_at'], username, v['platform']))
            new_count += 1
            if new_count <= 3:
                push({'type':'new_video','username':username,
                      'title':v['title'],'thumbnail':v['thumbnail'],'platform':platform})

        total = db.execute("SELECT COUNT(*) FROM videos WHERE profile_id=?", (profile_id,)).fetchone()[0]
        db.execute("UPDATE profiles SET last_synced=?,video_count=?,status='active' WHERE id=?",
                   (datetime.now().isoformat(), total, profile_id))
        db.commit()
        push({'type':'sync_done','username':username,'new':new_count,'platform':platform})
        log.info(f"[{username}] +{new_count} new")

    except Exception as e:
        log.error(f"[{username}] sync error: {e}")
        db.execute("UPDATE profiles SET status='error' WHERE id=?", (profile_id,))
        db.commit()
    finally:
        db.close()

def delete_old():
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    db = get_db()
    old = db.execute(
        "SELECT id FROM videos WHERE added_at < ? AND favorited=0", (cutoff,)
    ).fetchall()
    if old:
        ids = [v['id'] for v in old]
        db.execute(f"DELETE FROM videos WHERE id IN ({','.join('?'*len(ids))})", ids)
        db.execute('UPDATE profiles SET video_count=(SELECT COUNT(*) FROM videos WHERE profile_id=profiles.id)')
        db.commit()
        log.info(f"Deleted {len(ids)} old videos (kept favorites)")
    db.close()

def scheduled_sync():
    db = get_db()
    profiles = db.execute("SELECT * FROM profiles WHERE status NOT IN ('paused','downloading')").fetchall()
    db.close()
    for p in profiles:
        threading.Thread(target=sync_profile,
            args=(p['id'], p['username'], p['url'], p['platform'], True), daemon=True).start()
        time.sleep(2)
    delete_old()

scheduler = BackgroundScheduler(timezone='UTC')
scheduler.add_job(scheduled_sync, 'interval', hours=6, id='sync')
scheduler.add_job(delete_old, 'cron', hour=3, id='cleanup')
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

# ── URL → platform detector ───────────────────────────────────────────────────
def detect_platform(url):
    url = url.lower()
    if 'instagram.com' in url: return 'instagram'
    if 'youtube.com' in url or 'youtu.be' in url: return 'youtube'
    return 'tiktok'

def extract_username(url, platform):
    url = url.strip().rstrip('/')
    if not url.startswith('http'):
        return '@' + url.lstrip('@')
    for part in url.split('/'):
        if part.startswith('@'): return part
    last = url.split('/')[-1].lstrip('@')
    return '@' + last

# ── API ───────────────────────────────────────────────────────────────────────
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

    platform = detect_platform(url)
    if not url.startswith('http'):
        if platform == 'tiktok':
            url = 'https://www.tiktok.com/' + (url if url.startswith('@') else '@'+url)
        elif platform == 'instagram':
            url = 'https://www.instagram.com/' + url.lstrip('@') + '/'

    username = extract_username(url, platform)
    db = get_db()
    try:
        pid = db.execute(
            "INSERT INTO profiles (username,url,platform,status) VALUES (?,?,?,'pending')",
            (username, url, platform)
        ).lastrowid
        db.commit()
        threading.Thread(target=sync_profile,
            args=(pid, username, url, platform, False), daemon=True).start()
        return jsonify({'id': pid, 'username': username, 'platform': platform})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'البروفايل موجود بالفعل'}), 409
    finally:
        db.close()

@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
def delete_profile(pid):
    db = get_db()
    db.execute("DELETE FROM videos WHERE profile_id=?", (pid,))
    db.execute("DELETE FROM profiles WHERE id=?", (pid,))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/api/profiles/<int:pid>/sync', methods=['POST'])
def do_sync(pid):
    db = get_db()
    p = db.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
    db.close()
    if not p: return jsonify({'error': 'مش موجود'}), 404
    if p['status'] == 'downloading': return jsonify({'status': 'already_running'})
    threading.Thread(target=sync_profile,
        args=(p['id'], p['username'], p['url'], p['platform'], False), daemon=True).start()
    return jsonify({'status': 'syncing'})

@app.route('/api/videos')
def get_videos():
    pid      = request.args.get('profile_id')
    q        = request.args.get('q', '')
    platform = request.args.get('platform', '')
    favs     = request.args.get('favorites', '')
    tag      = request.args.get('tag', '')
    sort     = request.args.get('sort', 'recent')  # recent | popular | duration

    db   = get_db()
    base = "SELECT v.*, p.username, p.platform as p_platform FROM videos v JOIN profiles p ON v.profile_id=p.id"
    conds, params = [], []

    if pid:      conds.append("v.profile_id=?");  params.append(pid)
    if q:        conds.append("v.title LIKE ?");   params.append(f'%{q}%')
    if platform: conds.append("v.platform=?");     params.append(platform)
    if favs:     conds.append("v.favorited=1")
    if tag:      conds.append("v.tags LIKE ?");    params.append(f'%{tag}%')

    where = f"WHERE {' AND '.join(conds)}" if conds else ''

    order = {
        'popular':  'v.view_count DESC',
        'duration': 'v.duration DESC',
        'recent':   'v.added_at DESC'
    }.get(sort, 'v.added_at DESC')

    rows = db.execute(f"{base} {where} ORDER BY {order} LIMIT 200", params).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/videos/<int:vid>/favorite', methods=['POST'])
def toggle_fav(vid):
    db = get_db()
    cur = db.execute("SELECT favorited FROM videos WHERE id=?", (vid,)).fetchone()
    if not cur: return jsonify({'error': 'مش موجود'}), 404
    new_val = 0 if cur['favorited'] else 1
    db.execute("UPDATE videos SET favorited=? WHERE id=?", (new_val, vid))
    db.commit(); db.close()
    return jsonify({'favorited': new_val})

@app.route('/api/videos/<int:vid>/note', methods=['POST'])
def save_note(vid):
    note = (request.json or {}).get('note', '')
    db = get_db()
    db.execute("UPDATE videos SET note=? WHERE id=?", (note, vid))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/api/videos/<int:vid>/tags', methods=['POST'])
def save_tags(vid):
    tags = (request.json or {}).get('tags', '')
    db = get_db()
    db.execute("UPDATE videos SET tags=? WHERE id=?", (tags, vid))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/api/videos/<int:vid>/watch', methods=['POST'])
def inc_watch(vid):
    db = get_db()
    db.execute("UPDATE videos SET watch_count=watch_count+1 WHERE id=?", (vid,))
    db.commit(); db.close()
    return jsonify({'ok': True})

@app.route('/api/tags')
def get_tags():
    db = get_db()
    # جلب كل التاجات الموجودة في الفيديوهات
    rows = db.execute("SELECT DISTINCT tags FROM videos WHERE tags != ''").fetchall()
    db.close()
    all_tags = set()
    for r in rows:
        for t in r['tags'].split(','):
            t = t.strip()
            if t: all_tags.add(t)
    return jsonify(sorted(list(all_tags)))

@app.route('/api/stats')
def stats():
    db = get_db()
    p     = db.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
    v     = db.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    favs  = db.execute("SELECT COUNT(*) FROM videos WHERE favorited=1").fetchone()[0]
    tiktok  = db.execute("SELECT COUNT(*) FROM videos WHERE platform='tiktok'").fetchone()[0]
    insta   = db.execute("SELECT COUNT(*) FROM videos WHERE platform='instagram'").fetchone()[0]
    yt      = db.execute("SELECT COUNT(*) FROM videos WHERE platform='youtube'").fetchone()[0]
    db.close()
    return jsonify({'profiles':p,'videos':v,'favorites':favs,
                    'tiktok':tiktok,'instagram':insta,'youtube':yt})

@app.route('/api/config')
def config():
    return jsonify({'has_key': bool(RAPIDAPI_KEY)})

@app.route('/api/events')
def sse_stream():
    q = queue.Queue(maxsize=50)
    with sse_lock: sse_clients.append(q)
    def gen():
        try:
            yield 'data: {"type":"connected"}\n\n'
            while True:
                try: yield q.get(timeout=25)
                except: yield ': ping\n\n'
        finally:
            with sse_lock:
                if q in sse_clients: sse_clients.remove(q)
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

@app.route('/favicon.ico')
def favicon(): return '', 204

@app.route('/')
def index(): return send_from_directory('static', 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, threaded=True)

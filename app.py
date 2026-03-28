from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import os, json, threading, time, logging, queue, requests
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')  # Supabase connection string
MAX_SECS     = 120

# ── SSE ───────────────────────────────────────────────────────────────────────
sse_clients, sse_lock = [], threading.Lock()

def push(data):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_lock:
        for q in list(sse_clients):
            try: q.put_nowait(msg)
            except: pass

# ── DB (PostgreSQL) ───────────────────────────────────────────────────────────
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn

def init_db():
    db = get_db()
    cur = db.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS profiles (
            id          SERIAL PRIMARY KEY,
            username    TEXT UNIQUE NOT NULL,
            url         TEXT NOT NULL,
            platform    TEXT DEFAULT 'tiktok',
            added_at    TIMESTAMP DEFAULT NOW(),
            last_synced TIMESTAMP,
            video_count INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS videos (
            id           SERIAL PRIMARY KEY,
            profile_id   INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
            video_id     TEXT UNIQUE NOT NULL,
            title        TEXT DEFAULT '',
            play_url     TEXT DEFAULT '',
            nowm_url     TEXT DEFAULT '',
            thumbnail    TEXT DEFAULT '',
            duration     INTEGER DEFAULT 0,
            view_count   INTEGER DEFAULT 0,
            like_count   INTEGER DEFAULT 0,
            added_at     TIMESTAMP DEFAULT NOW(),
            published_at TEXT DEFAULT '',
            author       TEXT DEFAULT '',
            platform     TEXT DEFAULT 'tiktok',
            favorited    BOOLEAN DEFAULT FALSE,
            note         TEXT DEFAULT '',
            tags         TEXT DEFAULT ''
        );
    ''')
    db.commit()
    cur.close()
    db.close()
    log.info("DB initialized")

try:
    init_db()
except Exception as e:
    log.error(f"DB init error: {e}")

# ── RapidAPI ──────────────────────────────────────────────────────────────────
def api_get(url, host, params={}, timeout=30):
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": host}
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ── TikTok (كل الفيديوهات بـ pagination) ─────────────────────────────────────
def fetch_tiktok(username, fetch_all=False):
    clean     = username.lstrip('@')
    results   = []
    cursor    = 0
    page      = 0
    max_pages = 50

    while True:
        try:
            data = api_get(
                'https://tiktok-scraper7.p.rapidapi.com/user/posts',
                'tiktok-scraper7.p.rapidapi.com',
                {'unique_id': clean, 'count': '30', 'cursor': str(cursor)}
            )
        except Exception as e:
            log.error(f"TikTok page {page} error: {e}")
            break

        if not data or not data.get('data'):
            break

        d     = data['data']
        items = d.get('videos') or d.get('aweme_list') or (d if isinstance(d, list) else [])
        if not items:
            break

        for item in items:
            vid_id   = str(item.get('video_id') or item.get('aweme_id') or item.get('id') or '')
            duration = int(item.get('duration') or 0)
            if duration > 10000: duration //= 1000
            if not vid_id or duration > MAX_SECS:
                continue
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

        page += 1
        if not fetch_all:
            break

        has_more = d.get('hasMore') or d.get('has_more') or False
        cursor   = d.get('cursor') or d.get('nextCursor') or 0
        if not has_more or not cursor or page >= max_pages:
            break
        time.sleep(0.5)

    log.info(f"[{username}] TikTok: {len(results)} videos in {page} pages")
    return results

# ── Instagram ─────────────────────────────────────────────────────────────────
def fetch_instagram(username, count=20):
    try:
        clean = username.lstrip('@')
        data  = api_get(
            'https://instagram-scraper-api2.p.rapidapi.com/v1/posts',
            'instagram-scraper-api2.p.rapidapi.com',
            {'username_or_id_or_url': clean}
        )
        items   = data.get('data', {}).get('items', [])
        results = []
        for item in items[:count]:
            if item.get('media_type') not in (2, 'VIDEO'): continue
            vid_id   = str(item.get('id') or item.get('pk') or '')
            duration = int(item.get('video_duration') or 0)
            if duration > MAX_SECS: continue
            versions = item.get('video_versions') or []
            play_url = versions[0].get('url', '') if versions else ''
            img_v    = item.get('image_versions2', {}).get('candidates', [])
            thumb    = img_v[0].get('url', '') if img_v else ''
            cap      = item.get('caption') or {}
            results.append({
                'video_id': vid_id,
                'title': cap.get('text','') if isinstance(cap,dict) else '',
                'play_url': play_url, 'nowm_url': play_url, 'thumbnail': thumb,
                'duration': duration, 'view_count': int(item.get('view_count') or 0),
                'like_count': int(item.get('like_count') or 0),
                'published_at': str(item.get('taken_at') or ''), 'platform': 'instagram'
            })
        return results
    except Exception as e:
        log.error(f"Instagram error: {e}"); return []

# ── YouTube Shorts ────────────────────────────────────────────────────────────
def fetch_youtube(channel_url, count=20):
    try:
        data  = api_get(
            'https://youtube-media-downloader.p.rapidapi.com/v2/channel/shorts',
            'youtube-media-downloader.p.rapidapi.com',
            {'channelUrl': channel_url}
        )
        items   = data.get('shorts') or data.get('items') or []
        results = []
        for item in items[:count]:
            vid_id = str(item.get('id') or item.get('videoId') or '')
            if not vid_id: continue
            thumbs = item.get('thumbnails') or []
            thumb  = thumbs[-1].get('url','') if thumbs else ''
            results.append({
                'video_id': vid_id,
                'title': item.get('title') or '',
                'play_url': f'https://www.youtube.com/shorts/{vid_id}',
                'nowm_url': f'https://www.youtube.com/shorts/{vid_id}',
                'thumbnail': thumb,
                'duration': int(item.get('duration') or 0),
                'view_count': int(item.get('viewCount') or 0),
                'like_count': 0, 'published_at': '', 'platform': 'youtube'
            })
        return results
    except Exception as e:
        log.error(f"YouTube error: {e}"); return []

# ── Sync profile ──────────────────────────────────────────────────────────────
def sync_profile(profile_id, username, url, platform, new_only=False):
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("UPDATE profiles SET status='downloading' WHERE id=%s", (profile_id,))
        db.commit()

        if platform == 'tiktok':
            items = fetch_tiktok(username, fetch_all=not new_only)
        elif platform == 'instagram':
            items = fetch_instagram(username)
        elif platform == 'youtube':
            items = fetch_youtube(url)
        else:
            items = []

        log.info(f"[{username}] {len(items)} videos fetched")
        new_count = 0

        for v in items:
            cur.execute("SELECT id FROM videos WHERE video_id=%s", (v['video_id'],))
            if cur.fetchone():
                continue
            cur.execute('''
                INSERT INTO videos
                (profile_id,video_id,title,play_url,nowm_url,thumbnail,
                 duration,view_count,like_count,published_at,author,platform)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (video_id) DO NOTHING
            ''', (profile_id, v['video_id'], v['title'], v['play_url'], v['nowm_url'],
                  v['thumbnail'], v['duration'], v['view_count'], v['like_count'],
                  v['published_at'], username, v['platform']))
            new_count += 1
            if new_count <= 3:
                push({'type':'new_video','username':username,
                      'title':v['title'],'thumbnail':v['thumbnail'],'platform':platform})

        cur.execute("SELECT COUNT(*) as cnt FROM videos WHERE profile_id=%s", (profile_id,))
        total = cur.fetchone()['cnt']
        cur.execute(
            "UPDATE profiles SET last_synced=NOW(), video_count=%s, status='active' WHERE id=%s",
            (total, profile_id)
        )
        db.commit()
        push({'type':'sync_done','username':username,'new':new_count})
        log.info(f"[{username}] done +{new_count}")

    except Exception as e:
        log.error(f"[{username}] sync error: {e}")
        try:
            db.rollback()
            cur.execute("UPDATE profiles SET status='error' WHERE id=%s", (profile_id,))
            db.commit()
        except: pass
    finally:
        cur.close()
        db.close()

# ── حذف الفيديوهات القديمة (أكتر من 30 يوم، مش في المفضلة) ───────────────────
def delete_old():
    try:
        db  = get_db()
        cur = db.cursor()
        cur.execute('''
            DELETE FROM videos
            WHERE added_at < NOW() - INTERVAL '30 days'
            AND favorited = FALSE
        ''')
        deleted = cur.rowcount
        # تحديث عدد الفيديوهات في كل بروفايل
        cur.execute('''
            UPDATE profiles SET video_count = (
                SELECT COUNT(*) FROM videos WHERE profile_id = profiles.id
            )
        ''')
        db.commit()
        cur.close()
        db.close()
        if deleted > 0:
            log.info(f"Cleaned {deleted} old videos")
    except Exception as e:
        log.error(f"delete_old error: {e}")

# ── Background loop: تحديث كل 6 ساعات ───────────────────────────────────────
def bg_loop():
    while True:
        time.sleep(6 * 3600)
        try:
            db  = get_db()
            cur = db.cursor()
            cur.execute("SELECT * FROM profiles WHERE status NOT IN ('paused','downloading')")
            profiles = cur.fetchall()
            cur.close()
            db.close()
            for p in profiles:
                threading.Thread(
                    target=sync_profile,
                    args=(p['id'], p['username'], p['url'], p['platform'], True),
                    daemon=True
                ).start()
                time.sleep(3)
            delete_old()
        except Exception as e:
            log.error(f"bg_loop error: {e}")

threading.Thread(target=bg_loop, daemon=True).start()

# ── Helpers ───────────────────────────────────────────────────────────────────
def detect_platform(url):
    u = url.lower()
    if 'instagram.com' in u: return 'instagram'
    if 'youtube.com' in u or 'youtu.be' in u: return 'youtube'
    return 'tiktok'

def extract_username(url):
    url = url.strip().rstrip('/')
    if not url.startswith('http'):
        return '@' + url.lstrip('@')
    for part in url.split('/'):
        if part.startswith('@'): return part
    return '@' + url.split('/')[-1].lstrip('@')

def row_to_dict(row):
    if row is None: return None
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d

# ── API Routes ────────────────────────────────────────────────────────────────
@app.route('/api/profiles', methods=['GET'])
def get_profiles():
    db  = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM profiles ORDER BY added_at DESC")
    rows = [row_to_dict(r) for r in cur.fetchall()]
    cur.close(); db.close()
    return jsonify(rows)

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
    username = extract_username(url)
    db  = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            "INSERT INTO profiles (username,url,platform,status) VALUES (%s,%s,%s,'pending') RETURNING id",
            (username, url, platform)
        )
        pid = cur.fetchone()['id']
        db.commit()
        threading.Thread(
            target=sync_profile,
            args=(pid, username, url, platform, False),
            daemon=True
        ).start()
        return jsonify({'id': pid, 'username': username, 'platform': platform})
    except Exception as e:
        db.rollback()
        if 'unique' in str(e).lower():
            return jsonify({'error': 'البروفايل موجود بالفعل'}), 409
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close(); db.close()

@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
def delete_profile(pid):
    db  = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM videos WHERE profile_id=%s", (pid,))
    cur.execute("DELETE FROM profiles WHERE id=%s", (pid,))
    db.commit(); cur.close(); db.close()
    return jsonify({'ok': True})

@app.route('/api/profiles/<int:pid>/sync', methods=['POST'])
def do_sync(pid):
    db  = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM profiles WHERE id=%s", (pid,))
    p = cur.fetchone()
    cur.close(); db.close()
    if not p: return jsonify({'error': 'مش موجود'}), 404
    if p['status'] == 'downloading': return jsonify({'status': 'already_running'})
    threading.Thread(
        target=sync_profile,
        args=(p['id'], p['username'], p['url'], p['platform'], False),
        daemon=True
    ).start()
    return jsonify({'status': 'syncing'})

@app.route('/api/videos')
def get_videos():
    pid      = request.args.get('profile_id')
    q        = request.args.get('q','')
    platform = request.args.get('platform','')
    favs     = request.args.get('favorites','')
    tag      = request.args.get('tag','')
    sort     = request.args.get('sort','recent')

    db  = get_db()
    cur = db.cursor()

    base   = "SELECT v.*, p.username FROM videos v JOIN profiles p ON v.profile_id=p.id"
    conds  = []
    params = []

    if pid:      conds.append("v.profile_id=%s");  params.append(pid)
    if q:        conds.append("v.title ILIKE %s");  params.append(f'%{q}%')
    if platform: conds.append("v.platform=%s");     params.append(platform)
    if favs:     conds.append("v.favorited=TRUE")
    if tag:      conds.append("v.tags ILIKE %s");   params.append(f'%{tag}%')

    where = f"WHERE {' AND '.join(conds)}" if conds else ''
    order = {
        'popular':  'v.view_count DESC',
        'duration': 'v.duration DESC'
    }.get(sort, 'v.added_at DESC')

    cur.execute(f"{base} {where} ORDER BY {order} LIMIT 200", params)
    rows = [row_to_dict(r) for r in cur.fetchall()]
    cur.close(); db.close()
    return jsonify(rows)

@app.route('/api/videos/<int:vid>/favorite', methods=['POST'])
def toggle_fav(vid):
    db  = get_db()
    cur = db.cursor()
    cur.execute("SELECT favorited FROM videos WHERE id=%s", (vid,))
    row = cur.fetchone()
    if not row: return jsonify({'error': 'مش موجود'}), 404
    new_val = not row['favorited']
    cur.execute("UPDATE videos SET favorited=%s WHERE id=%s", (new_val, vid))
    db.commit(); cur.close(); db.close()
    return jsonify({'favorited': new_val})

@app.route('/api/videos/<int:vid>/note', methods=['POST'])
def save_note(vid):
    note = (request.json or {}).get('note', '')
    db   = get_db()
    cur  = db.cursor()
    cur.execute("UPDATE videos SET note=%s WHERE id=%s", (note, vid))
    db.commit(); cur.close(); db.close()
    return jsonify({'ok': True})

@app.route('/api/videos/<int:vid>/tags', methods=['POST'])
def save_tags(vid):
    tags = (request.json or {}).get('tags', '')
    db   = get_db()
    cur  = db.cursor()
    cur.execute("UPDATE videos SET tags=%s WHERE id=%s", (tags, vid))
    db.commit(); cur.close(); db.close()
    return jsonify({'ok': True})

@app.route('/api/tags')
def get_tags():
    db  = get_db()
    cur = db.cursor()
    cur.execute("SELECT DISTINCT tags FROM videos WHERE tags != ''")
    rows = cur.fetchall()
    cur.close(); db.close()
    all_tags = set()
    for r in rows:
        for t in (r['tags'] or '').split(','):
            t = t.strip()
            if t: all_tags.add(t)
    return jsonify(sorted(list(all_tags)))

@app.route('/api/stats')
def stats():
    db  = get_db()
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as c FROM profiles"); p = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) as c FROM videos");   v = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) as c FROM videos WHERE favorited=TRUE"); f = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) as c FROM videos WHERE platform='tiktok'"); tt = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) as c FROM videos WHERE platform='instagram'"); ig = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) as c FROM videos WHERE platform='youtube'"); yt = cur.fetchone()['c']
    cur.close(); db.close()
    return jsonify({'profiles':p,'videos':v,'favorites':f,'tiktok':tt,'instagram':ig,'youtube':yt})

@app.route('/api/config')
def config():
    return jsonify({'has_key': bool(RAPIDAPI_KEY), 'has_db': bool(DATABASE_URL)})

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
    log.info(f"Starting on port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)

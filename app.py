from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import os, json, threading, time, logging, queue, requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

RAPIDAPI_KEY  = os.environ.get('RAPIDAPI_KEY', '')
SUPABASE_URL  = os.environ.get('SUPABASE_URL', '')   # https://xxx.supabase.co
DROPBOX_TOKEN = os.environ.get('DROPBOX_TOKEN', '')  # Dropbox access token
SUPABASE_KEY  = os.environ.get('SUPABASE_KEY', '')   # anon/service key
MAX_SECS      = 120

# ── SSE ───────────────────────────────────────────────────────────────────────
sse_clients, sse_lock = [], threading.Lock()

def push(data):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_lock:
        for q in list(sse_clients):
            try: q.put_nowait(msg)
            except: pass

# ── Supabase REST helpers ─────────────────────────────────────────────────────
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

def sb_get(table, params={}):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                     headers=sb_headers(), params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def sb_post(table, data):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",
                      headers=sb_headers(), json=data, timeout=15)
    r.raise_for_status()
    return r.json()

def sb_patch(table, filters, data):
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}",
                       headers={**sb_headers(), "Prefer": "return=representation"},
                       params=filters, json=data, timeout=15)
    r.raise_for_status()
    return r.json()

def sb_delete(table, filters):
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}",
                        headers=sb_headers(), params=filters, timeout=15)
    r.raise_for_status()

def init_db():
    """إنشاء الجداول عبر Supabase SQL endpoint"""
    sql = """
    CREATE TABLE IF NOT EXISTS profiles (
        id SERIAL PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        url TEXT NOT NULL,
        platform TEXT DEFAULT 'tiktok',
        added_at TIMESTAMPTZ DEFAULT NOW(),
        last_synced TIMESTAMPTZ,
        video_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending'
    );
    CREATE TABLE IF NOT EXISTS videos (
        id SERIAL PRIMARY KEY,
        profile_id INTEGER NOT NULL,
        video_id TEXT UNIQUE NOT NULL,
        title TEXT DEFAULT '',
        play_url TEXT DEFAULT '',
        nowm_url TEXT DEFAULT '',
        thumbnail TEXT DEFAULT '',
        duration INTEGER DEFAULT 0,
        view_count INTEGER DEFAULT 0,
        like_count INTEGER DEFAULT 0,
        added_at TIMESTAMPTZ DEFAULT NOW(),
        published_at TEXT DEFAULT '',
        author TEXT DEFAULT '',
        platform TEXT DEFAULT 'tiktok',
        favorited BOOLEAN DEFAULT FALSE,
        note TEXT DEFAULT '',
        tags TEXT DEFAULT ''
    );
    """
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
            headers=sb_headers(),
            json={"sql": sql},
            timeout=15
        )
        log.info("DB init attempted")
    except Exception as e:
        log.warning(f"DB init via RPC failed (tables may already exist): {e}")

# ── RapidAPI ──────────────────────────────────────────────────────────────────
# ── مفاتيح RapidAPI المتناوبة (10 مفاتيح = 5000 request/شهر مجاناً) ──────────
RAPIDAPI_KEYS = [
    os.environ.get('RAPIDAPI_KEY1',  'bdc10d9259msheaf058c993f20ebp16e506jsn2a8817aa02ce'),
    os.environ.get('RAPIDAPI_KEY2',  '302bf2cb16msha864e114fb18807p1e4cc2jsnedbde253dd65'),
    os.environ.get('RAPIDAPI_KEY3',  '14c47f2386mshbf5a6c5b8e99aa8p17a5c3jsn9bf4400a9e55'),
    os.environ.get('RAPIDAPI_KEY4',  'c26f16f928msh8d5cc5aeeb75591p119d40jsnd30521857091'),
    os.environ.get('RAPIDAPI_KEY5',  'e93e9b192emshc75a1e42940a030p10f13ejsn077c893de4e9'),
    os.environ.get('RAPIDAPI_KEY6',  '32f1336919msh3cd23cfd527c2f1p1aba87jsna4c1777797c8'),
    os.environ.get('RAPIDAPI_KEY7',  'c31801407cmshe87419cd85e0d2cp1b60ffjsn73dba0733d09'),
    os.environ.get('RAPIDAPI_KEY8',  'b2ba7eed62msh561bef662e1989ep1db77fjsn8ebe58bab9a9'),
    os.environ.get('RAPIDAPI_KEY9',  '77aa495508msh7551299092bdb37p103884jsn4c9e6747ea36'),
    os.environ.get('RAPIDAPI_KEY10', 'c1bf960ae1msh06f344c296608c0p1d8feejsnd2ac9c756d86'),
]
_key_index = 0
_key_lock  = threading.Lock()

def get_next_key():
    """يرجع المفتاح الحالي ويستعد للتالي لو فيه خطأ"""
    global _key_index
    with _key_lock:
        return RAPIDAPI_KEYS[_key_index % len(RAPIDAPI_KEYS)]

def rotate_key():
    """ينتقل للمفتاح التالي"""
    global _key_index
    with _key_lock:
        _key_index = (_key_index + 1) % len(RAPIDAPI_KEYS)
        log.info(f"Rotated to API key #{_key_index + 1}")

RAPIDAPI_KEY2 = RAPIDAPI_KEYS[0]  # للتوافق مع الكود القديم
DROPBOX_REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', '_MLs8NC9BVoAAAAAAAAAAQO-uJzWOpraUKTIFECOFo6ZhNAVYtIkakFT39qkEHyW')
DROPBOX_CLIENT_ID     = os.environ.get('DROPBOX_CLIENT_ID', 'jyu5031v2rkzfr4')
DROPBOX_CLIENT_SECRET = os.environ.get('DROPBOX_CLIENT_SECRET', 'ni1uvt1lfe2z3cw')
DROPBOX_TOKEN         = os.environ.get('DROPBOX_TOKEN', '')  # للتوافق

_dropbox_access_token = None
_dropbox_token_lock   = threading.Lock()

def get_dropbox_token():
    """يجيب Access Token صالح — يجدده تلقائياً لو انتهى"""
    global _dropbox_access_token
    with _dropbox_token_lock:
        if not _dropbox_access_token:
            _dropbox_access_token = refresh_dropbox_token()
        return _dropbox_access_token

def refresh_dropbox_token():
    """يجدد الـ Access Token باستخدام الـ Refresh Token الدائم"""
    try:
        r = requests.post(
            "https://api.dropbox.com/oauth2/token",
            data={
                "grant_type":    "refresh_token",
                "refresh_token": DROPBOX_REFRESH_TOKEN,
                "client_id":     DROPBOX_CLIENT_ID,
                "client_secret": DROPBOX_CLIENT_SECRET,
            },
            timeout=15
        )
        r.raise_for_status()
        token = r.json().get('access_token')
        log.info("Dropbox token refreshed successfully")
        return token
    except Exception as e:
        log.error(f"Dropbox token refresh error: {e}")
        return None

# ── Dropbox Upload ────────────────────────────────────────────────────────────
def upload_to_dropbox(video_url, username, video_id, title=''):
    """تحميل الفيديو ورفعه على Dropbox/videos/"""
    global _dropbox_access_token
    try:
        # جيب الفيديو
        r = requests.get(video_url, timeout=60, stream=True)
        r.raise_for_status()
        video_data = r.content

        dropbox_path = f"/videos/{video_id}.mp4"

        # جرب برفع — لو فشل بـ 401 جدد التوكن وجرب تاني
        for attempt in range(2):
            token = get_dropbox_token()
            if not token:
                log.error("No Dropbox token available")
                return None
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream",
                "Dropbox-API-Arg": json.dumps({
                    "path": dropbox_path,
                    "mode": "overwrite",
                    "autorename": False
                })
            }
            resp = requests.post(
                "https://content.dropboxapi.com/2/files/upload",
                headers=headers,
                data=video_data,
                timeout=120
            )
            if resp.status_code == 401 and attempt == 0:
                # التوكن انتهى — جدده وجرب تاني
                log.warning("Dropbox token expired, refreshing...")
                with _dropbox_token_lock:
                    _dropbox_access_token = refresh_dropbox_token()
                continue
            resp.raise_for_status()
            log.info(f"Dropbox upload success: {dropbox_path}")
            return dropbox_path

    except Exception as e:
        log.error(f"Dropbox upload error: {e}")
        return None

def api_get(url, host, params={}, timeout=30, key=None):
    k = key or RAPIDAPI_KEY
    headers = {"x-rapidapi-key": k, "x-rapidapi-host": host}
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ── TikTok (API2 الجديد 500 req/شهر، API1 احتياطي) ───────────────────────────
def fetch_tiktok(username, fetch_all=False):
    clean, results, cursor, page = username.lstrip('@'), [], 0, 0
    while True:
        data = None
        # نجرب المفاتيح بالتناوب
        tried = 0
        while tried < len(RAPIDAPI_KEYS):
            current_key = get_next_key()
            try:
                data = api_get(
                    'https://tiktok-video-no-watermark2.p.rapidapi.com/user/posts',
                    'tiktok-video-no-watermark2.p.rapidapi.com',
                    {'unique_id': clean, 'count': '20', 'cursor': str(cursor)},
                    key=current_key
                )
                break  # نجح!
            except Exception as e:
                log.warning(f"Key #{_key_index+1} failed: {e}")
                rotate_key()
                tried += 1
                time.sleep(0.5)
        if not data:
            log.error(f"All {len(RAPIDAPI_KEYS)} keys failed for {username}")
            break

        if not data: break
        d = data.get('data') or data
        items = (d.get('videos') or d.get('aweme_list') or
                 d.get('itemList') or (d if isinstance(d, list) else []))
        if not items: break
        for item in items:
            vid_id   = str(item.get('video_id') or item.get('aweme_id') or
                          item.get('id') or item.get('videoId') or '')
            duration = int(item.get('duration') or
                          item.get('video', {}).get('duration', 0) or 0)
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
        page += 1
        if not fetch_all: break
        has_more = d.get('hasMore') or d.get('has_more') or False
        cursor   = d.get('cursor') or d.get('nextCursor') or 0
        if not has_more or not cursor or page >= 50: break
        time.sleep(0.5)
    log.info(f"[{username}] fetched {len(results)} videos")
    return results

def fetch_instagram(username, count=20):
    try:
        data  = api_get('https://instagram-scraper-api2.p.rapidapi.com/v1/posts',
                        'instagram-scraper-api2.p.rapidapi.com',
                        {'username_or_id_or_url': username.lstrip('@')})
        items = data.get('data', {}).get('items', [])
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
            results.append({'video_id': vid_id,
                'title': cap.get('text','') if isinstance(cap,dict) else '',
                'play_url': play_url, 'nowm_url': play_url, 'thumbnail': thumb,
                'duration': duration, 'view_count': int(item.get('view_count') or 0),
                'like_count': int(item.get('like_count') or 0),
                'published_at': str(item.get('taken_at') or ''), 'platform': 'instagram'})
        return results
    except Exception as e:
        log.error(f"Instagram error: {e}"); return []

def fetch_youtube(channel_url, count=20):
    try:
        data  = api_get('https://youtube-media-downloader.p.rapidapi.com/v2/channel/shorts',
                        'youtube-media-downloader.p.rapidapi.com',
                        {'channelUrl': channel_url})
        items = data.get('shorts') or data.get('items') or []
        results = []
        for item in items[:count]:
            vid_id = str(item.get('id') or item.get('videoId') or '')
            if not vid_id: continue
            thumbs = item.get('thumbnails') or []
            results.append({'video_id': vid_id, 'title': item.get('title') or '',
                'play_url': f'https://www.youtube.com/shorts/{vid_id}',
                'nowm_url': f'https://www.youtube.com/shorts/{vid_id}',
                'thumbnail': thumbs[-1].get('url','') if thumbs else '',
                'duration': int(item.get('duration') or 0),
                'view_count': int(item.get('viewCount') or 0),
                'like_count': 0, 'published_at': '', 'platform': 'youtube'})
        return results
    except Exception as e:
        log.error(f"YouTube error: {e}"); return []

# ── Sync ──────────────────────────────────────────────────────────────────────
def sync_profile(profile_id, username, url, platform, new_only=False):
    try:
        sb_patch('profiles', {'id': f'eq.{profile_id}'}, {'status': 'downloading'})
        if platform == 'tiktok':      items = fetch_tiktok(username, fetch_all=not new_only)
        elif platform == 'instagram': items = fetch_instagram(username)
        elif platform == 'youtube':   items = fetch_youtube(url)
        else:                         items = []

        # جيب الـ video_ids الموجودة
        existing = set()
        try:
            rows = sb_get('videos', {'profile_id': f'eq.{profile_id}', 'select': 'video_id'})
            existing = {r['video_id'] for r in rows}
        except: pass

        new_count = 0
        _dropbox_queue = []
        for v in items:
            if v['video_id'] in existing: continue
            try:
                sb_post('videos', {
                    'profile_id':   profile_id,
                    'video_id':     v['video_id'],
                    'title':        v['title'],
                    'play_url':     v['play_url'],
                    'nowm_url':     v['nowm_url'],
                    'thumbnail':    v['thumbnail'],
                    'duration':     v['duration'],
                    'view_count':   v['view_count'],
                    'like_count':   v['like_count'],
                    'published_at': v['published_at'],
                    'author':       username,
                    'platform':     v['platform']
                })
                new_count += 1
                existing.add(v['video_id'])
                if new_count <= 3:
                    push({'type':'new_video','username':username,
                          'title':v['title'],'thumbnail':v['thumbnail'],'platform':platform})
                # تجميع للرفع على Dropbox لاحقاً
                _dropbox_queue.append({
                    'url': v.get('nowm_url') or v.get('play_url',''),
                    'username': username,
                    'video_id': v['video_id'],
                    'title': v.get('title','')
                })
            except Exception as e:
                if 'duplicate' not in str(e).lower():
                    log.error(f"Insert error: {e}")

        total = len(existing)
        sb_patch('profiles', {'id': f'eq.{profile_id}'},
                 {'status': 'active', 'video_count': total,
                  'last_synced': datetime.utcnow().isoformat()})
        push({'type':'sync_done','username':username,'new':new_count})
        log.info(f"[{username}] done +{new_count}")

        # رفع على Dropbox في الخلفية بعد انتهاء الـ sync
        if DROPBOX_TOKEN and _dropbox_queue:
            def _batch_upload(queue):
                for item in queue:
                    if item['url']:
                        upload_to_dropbox(item['url'], item['username'],
                                         item['video_id'], item['title'])
                        time.sleep(1)  # delay بسيط بين كل فيديو
            threading.Thread(target=_batch_upload, args=(_dropbox_queue,), daemon=True).start()
            log.info(f"[{username}] قائمة Dropbox: {len(_dropbox_queue)} فيديو")
    except Exception as e:
        log.error(f"[{username}] sync error: {e}")
        try: sb_patch('profiles', {'id': f'eq.{profile_id}'}, {'status': 'error'})
        except: pass

def delete_old():
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        sb_delete('videos', {'added_at': f'lt.{cutoff}', 'favorited': 'eq.false'})
        log.info("Old videos cleaned")
    except Exception as e:
        log.error(f"delete_old error: {e}")

# ── Dropbox Upload ───────────────────────────────────────────────────────────
def upload_to_dropbox(video_url, username, title, video_id):
    """رفع فيديو على Dropbox تلقائياً"""
    if not DROPBOX_TOKEN:
        return False
    try:
        import re
        clean_title = re.sub(r'[<>:"/\\|?*\n\r\t]', '', title or video_id)[:50]
        clean_user  = username.lstrip('@')
        dropbox_path = f"/TikLib/{clean_user}/{clean_title}_{video_id[:8]}.mp4"

        # تحميل الفيديو أولاً في الذاكرة
        r = requests.get(video_url, timeout=60,
                        headers={'User-Agent': 'Mozilla/5.0'},
                        stream=True)
        r.raise_for_status()
        video_data = r.content

        # رفع على Dropbox
        headers = {
            'Authorization': f'Bearer {DROPBOX_TOKEN}',
            'Content-Type': 'application/octet-stream',
            'Dropbox-API-Arg': json.dumps({
                'path': dropbox_path,
                'mode': 'add',
                'autorename': True,
                'mute': False
            })
        }
        up = requests.post(
            'https://content.dropboxapi.com/2/files/upload',
            headers=headers,
            data=video_data,
            timeout=120
        )
        up.raise_for_status()
        result = up.json()
        log.info(f"Dropbox ✅ {dropbox_path}")
        return result.get('path_display', dropbox_path)
    except Exception as e:
        log.error(f"Dropbox upload error: {e}")
        return False

def bg_loop():
    while True:
        time.sleep(6 * 3600)
        try:
            profiles = sb_get('profiles', {'status': 'neq.paused', 'select': '*'})
            for p in profiles:
                threading.Thread(target=sync_profile,
                    args=(p['id'],p['username'],p['url'],p['platform'],True),
                    daemon=True).start()
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
    if not url.startswith('http'): return '@' + url.lstrip('@')
    for part in url.split('/'):
        if part.startswith('@'): return part
    return '@' + url.split('/')[-1].lstrip('@')

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/api/profiles', methods=['GET'])
def get_profiles():
    return jsonify(sb_get('profiles', {'order': 'added_at.desc'}))

@app.route('/api/profiles', methods=['POST'])
def add_profile():
    data = request.json or {}
    url  = data.get('url','').strip()
    if not url: return jsonify({'error':'URL مطلوب'}), 400
    platform = detect_platform(url)
    if not url.startswith('http'):
        url = 'https://www.tiktok.com/' + (url if url.startswith('@') else '@'+url)
    username = extract_username(url)
    try:
        rows = sb_post('profiles', {'username':username,'url':url,'platform':platform,'status':'pending'})
        pid  = rows[0]['id'] if rows else None
        if pid:
            threading.Thread(target=sync_profile,
                args=(pid,username,url,platform,False), daemon=True).start()
        return jsonify({'id':pid,'username':username,'platform':platform})
    except Exception as e:
        if 'duplicate' in str(e).lower():
            return jsonify({'error':'البروفايل موجود بالفعل'}), 409
        return jsonify({'error': str(e)}), 500

@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
def delete_profile(pid):
    sb_delete('videos',   {'profile_id': f'eq.{pid}'})
    sb_delete('profiles', {'id': f'eq.{pid}'})
    return jsonify({'ok': True})

@app.route('/api/profiles/<int:pid>/sync', methods=['POST'])
def do_sync(pid):
    rows = sb_get('profiles', {'id': f'eq.{pid}', 'select': '*'})
    if not rows: return jsonify({'error':'مش موجود'}), 404
    p = rows[0]
    if p['status'] == 'downloading': return jsonify({'status':'already_running'})
    threading.Thread(target=sync_profile,
        args=(p['id'],p['username'],p['url'],p['platform'],False), daemon=True).start()
    return jsonify({'status':'syncing'})

@app.route('/api/videos')
def get_videos():
    params = {'order': 'added_at.desc', 'limit': '200', 'select': '*,profiles(username)'}
    if request.args.get('profile_id'):
        params['profile_id'] = f"eq.{request.args['profile_id']}"
    if request.args.get('platform'):
        params['platform'] = f"eq.{request.args['platform']}"
    if request.args.get('favorites'):
        params['favorited'] = 'eq.true'
    if request.args.get('q'):
        params['title'] = f"ilike.*{request.args['q']}*"
    sort = request.args.get('sort','recent')
    if sort == 'popular':  params['order'] = 'view_count.desc'
    if sort == 'duration': params['order'] = 'duration.desc'

    rows = sb_get('videos', params)
    # flatten username from join
    for r in rows:
        if isinstance(r.get('profiles'), dict):
            r['username'] = r['profiles'].get('username','')
        r.pop('profiles', None)
    return jsonify(rows)

@app.route('/api/videos/<int:vid>/favorite', methods=['POST'])
def toggle_fav(vid):
    rows = sb_get('videos', {'id': f'eq.{vid}', 'select': 'favorited'})
    if not rows: return jsonify({'error':'مش موجود'}), 404
    new_val = not rows[0]['favorited']
    sb_patch('videos', {'id': f'eq.{vid}'}, {'favorited': new_val})
    return jsonify({'favorited': new_val})

@app.route('/api/videos/<int:vid>/note', methods=['POST'])
def save_note(vid):
    sb_patch('videos', {'id': f'eq.{vid}'}, {'note': (request.json or {}).get('note','')})
    return jsonify({'ok': True})

@app.route('/api/videos/<int:vid>/tags', methods=['POST'])
def save_tags(vid):
    sb_patch('videos', {'id': f'eq.{vid}'}, {'tags': (request.json or {}).get('tags','')})
    return jsonify({'ok': True})

@app.route('/api/tags')
def get_tags():
    rows = sb_get('videos', {'select': 'tags', 'tags': 'neq.'})
    all_tags = set()
    for r in rows:
        for t in (r.get('tags') or '').split(','):
            t = t.strip()
            if t: all_tags.add(t)
    return jsonify(sorted(list(all_tags)))

@app.route('/api/stats')
def stats():
    try:
        p    = len(sb_get('profiles', {'select': 'id'}))
        v    = len(sb_get('videos',   {'select': 'id'}))
        favs = len(sb_get('videos',   {'select': 'id', 'favorited': 'eq.true'}))
        tt   = len(sb_get('videos',   {'select': 'id', 'platform':  'eq.tiktok'}))
        ig   = len(sb_get('videos',   {'select': 'id', 'platform':  'eq.instagram'}))
        yt   = len(sb_get('videos',   {'select': 'id', 'platform':  'eq.youtube'}))
        return jsonify({'profiles':p,'videos':v,'favorites':favs,'tiktok':tt,'instagram':ig,'youtube':yt})
    except Exception as e:
        log.error(f"stats error: {e}")
        return jsonify({'profiles':0,'videos':0,'favorites':0,'tiktok':0,'instagram':0,'youtube':0})

@app.route('/api/config')
def config():
    return jsonify({'has_key': bool(RAPIDAPI_KEY), 'has_db': bool(SUPABASE_URL and SUPABASE_KEY)})

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

# ── Publisher API (للربط مع موقع النشر) ──────────────────────────────────────
# تتبع آخر فيديو تم نشره لكل صفحة
published_tracker = {}
published_lock = threading.Lock()

@app.route('/api/publisher/next')
def publisher_next():
    """يجيب فيديوهات جاهزة للنشر على فيسبوك"""
    count   = int(request.args.get('count', 1))
    page_id = request.args.get('page_id', 'default')
    platform = request.args.get('platform', 'tiktok')

    try:
        params = {
            'order': 'added_at.desc',
            'limit': '50',
            'select': '*,profiles(username)',
            'favorited': 'neq.true'  # تجنب إعادة نشر المفضلة
        }
        if platform != 'all':
            params['platform'] = f'eq.{platform}'

        rows = sb_get('videos', params)

        # flatten username
        for r in rows:
            if isinstance(r.get('profiles'), dict):
                r['username'] = r['profiles'].get('username', '')
            r.pop('profiles', None)

        if not rows:
            return jsonify({'success': False, 'error': 'لا توجد فيديوهات في المكتبة', 'videos': []})

        # تجنب إعادة نشر نفس الفيديو للصفحة دي
        import random
        with published_lock:
            if page_id not in published_tracker:
                published_tracker[page_id] = set()
            published = published_tracker[page_id]
            available = [v for v in rows if v.get('video_id') not in published]
            if not available:
                published_tracker[page_id] = set()
                available = rows
            selected = random.sample(available, min(count, len(available)))
            for v in selected:
                published_tracker[page_id].add(v.get('video_id',''))

        # رجّع بالشكل اللي موقع النشر بيحتاجه
        result = []
        for v in selected:
            result.append({
                'video_id':   v['video_id'],
                'title':      v.get('title', ''),
                'url':        v.get('nowm_url') or v.get('play_url', ''),
                'thumbnail':  v.get('thumbnail', ''),
                'duration':   v.get('duration', 0),
                'view_count': v.get('view_count', 0),
                'author':     v.get('author') or v.get('username', ''),
                'platform':   v.get('platform', 'tiktok'),
            })

        return jsonify({'success': True, 'videos': result, 'count': len(result)})

    except Exception as e:
        log.error(f"publisher/next error: {e}")
        return jsonify({'success': False, 'error': str(e), 'videos': []})

@app.route('/api/publisher/stats')
def publisher_stats():
    """إحصائيات المكتبة للنشر"""
    try:
        total = len(sb_get('videos', {'select': 'id'}))
        tiktok = len(sb_get('videos', {'select': 'id', 'platform': 'eq.tiktok'}))
        return jsonify({
            'total_videos': total,
            'tiktok': tiktok,
            'ready': total > 0,
            'library_url': 'https://tiklib2026.up.railway.app'
        })
    except Exception as e:
        return jsonify({'total_videos': 0, 'ready': False, 'error': str(e)})


# ── Settings Storage ──────────────────────────────────────────────────────────
SETTINGS_FILE = os.path.join(os.environ.get('STORAGE_DIR', '/tmp/tiklib'), 'settings.json')

def load_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f:
                return json.load(f)
    except: pass
    return {}

def save_settings(data):
    try:
        os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(data, f)
        return True
    except Exception as e:
        log.error(f"Save settings error: {e}")
        return False

def apply_settings():
    """تطبيق الإعدادات المحفوظة على المتغيرات العامة"""
    global RAPIDAPI_KEY, RAPIDAPI_KEYS, DROPBOX_REFRESH_TOKEN
    global DROPBOX_CLIENT_ID, DROPBOX_CLIENT_SECRET, MAX_SECS
    global _dropbox_access_token
    s = load_settings()
    if s.get('rapidapi_key'):
        RAPIDAPI_KEY = s['rapidapi_key']
        RAPIDAPI_KEYS[0] = s['rapidapi_key']
    if s.get('rapidapi_keys'):
        keys = [k.strip() for k in s['rapidapi_keys'].split('\n') if k.strip()]
        if keys:
            RAPIDAPI_KEYS = keys
    if s.get('dropbox_refresh_token'):
        DROPBOX_REFRESH_TOKEN = s['dropbox_refresh_token']
        _dropbox_access_token = None  # force refresh
    if s.get('dropbox_client_id'):
        DROPBOX_CLIENT_ID = s['dropbox_client_id']
    if s.get('dropbox_client_secret'):
        DROPBOX_CLIENT_SECRET = s['dropbox_client_secret']
    if s.get('max_duration'):
        try: MAX_SECS = int(s['max_duration'])
        except: pass

# Apply saved settings on startup
apply_settings()

@app.route('/api/settings', methods=['GET'])
def get_settings():
    s = load_settings()
    # إخفاء المفاتيح جزئياً للأمان
    def mask(v):
        if not v: return ''
        return v[:8] + '***' + v[-4:] if len(v) > 12 else '***'
    return jsonify({
        'rapidapi_key':           mask(s.get('rapidapi_key','')),
        'rapidapi_keys':          s.get('rapidapi_keys',''),
        'dropbox_refresh_token':  mask(s.get('dropbox_refresh_token','')),
        'dropbox_client_id':      s.get('dropbox_client_id',''),
        'dropbox_client_secret':  mask(s.get('dropbox_client_secret','')),
        'max_duration':           s.get('max_duration', 120),
        'has_rapidapi':           bool(s.get('rapidapi_key') or RAPIDAPI_KEY),
        'has_dropbox':            bool(s.get('dropbox_refresh_token') or DROPBOX_REFRESH_TOKEN),
        'dropbox_folder':         s.get('dropbox_folder', '/videos'),
        'keys_count':             len([k for k in (s.get('rapidapi_keys','') or '').split('\n') if k.strip()]) or len(RAPIDAPI_KEYS),
    })

@app.route('/api/settings', methods=['POST'])
def update_settings():
    data = request.json or {}
    s = load_settings()
    # حفظ القيم غير الفارغة فقط
    fields = ['rapidapi_key','rapidapi_keys','dropbox_refresh_token',
              'dropbox_client_id','dropbox_client_secret','max_duration','dropbox_folder']
    for f in fields:
        if f in data and data[f] != '' and not str(data[f]).endswith('***'):
            s[f] = data[f]
    if save_settings(s):
        apply_settings()
        return jsonify({'ok': True, 'msg': 'تم حفظ الإعدادات ✅'})
    return jsonify({'ok': False, 'msg': 'خطأ في الحفظ'}), 500

@app.route('/api/settings/test_dropbox', methods=['POST'])
def test_dropbox():
    """اختبار اتصال Dropbox"""
    try:
        token = get_dropbox_token()
        if not token:
            return jsonify({'ok': False, 'msg': 'فشل الحصول على Token'})
        r = requests.post(
            'https://api.dropboxapi.com/2/users/get_current_account',
            headers={'Authorization': f'Bearer {token}'},
            timeout=10
        )
        if r.status_code == 200:
            d = r.json()
            return jsonify({'ok': True, 'msg': f'✅ متصل - {d.get("name",{}).get("display_name","")}'})
        return jsonify({'ok': False, 'msg': f'خطأ {r.status_code}'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/settings/test_rapidapi', methods=['POST'])
def test_rapidapi():
    """اختبار مفتاح RapidAPI"""
    try:
        key = get_next_key()
        r = requests.get(
            'https://tiktok-video-no-watermark2.p.rapidapi.com/user/info',
            headers={'x-rapidapi-key': key, 'x-rapidapi-host': 'tiktok-video-no-watermark2.p.rapidapi.com'},
            params={'unique_id': 'tiktok'},
            timeout=10
        )
        if r.status_code == 200:
            return jsonify({'ok': True, 'msg': f'✅ المفتاح شغال - {len(RAPIDAPI_KEYS)} مفتاح'})
        elif r.status_code == 429:
            rotate_key()
            return jsonify({'ok': False, 'msg': '⚠️ هذا المفتاح وصل الحد - جرب التالي'})
        return jsonify({'ok': False, 'msg': f'خطأ {r.status_code}'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/settings')
def settings(): return send_from_directory('static', 'settings.html')

@app.route('/')
def index(): return send_from_directory('static', 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info(f"TikLib starting on port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)

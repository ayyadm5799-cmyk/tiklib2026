from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import os, json, threading, time, logging, queue, requests, random
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY  = os.environ.get('SUPABASE_KEY', '')
MAX_SECS      = int(os.environ.get('MAX_SECS', 120))
SETTINGS_FILE = os.path.join(os.environ.get('STORAGE_DIR', '/tmp/tiklib'), 'settings.json')
os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)

# ── SSE ───────────────────────────────────────────────────────────────────────
sse_clients, sse_lock = [], threading.Lock()
def push(data):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_lock:
        for q in list(sse_clients):
            try: q.put_nowait(msg)
            except: pass

# ── Settings ──────────────────────────────────────────────────────────────────
def load_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f: return json.load(f)
    except: pass
    return {}

def save_settings_file(data):
    try:
        with open(SETTINGS_FILE, 'w') as f: json.dump(data, f)
        return True
    except: return False

def get_keys(name):
    """يجيب قائمة مفاتيح من الإعدادات"""
    s = load_settings()
    raw = s.get(name, '')
    return [k.strip() for k in raw.split('\n') if k.strip()]

# ── Supabase ──────────────────────────────────────────────────────────────────
def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "return=representation"}

def sb_get(table, params={}):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=sb_headers(), params=params, timeout=15)
    r.raise_for_status(); return r.json()

def sb_post(table, data):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=sb_headers(), json=data, timeout=15)
    r.raise_for_status(); return r.json()

def sb_patch(table, filters, data):
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(), "Prefer": "return=representation"},
        params=filters, json=data, timeout=15)
    r.raise_for_status(); return r.json()

def sb_delete(table, filters):
    r = requests.delete(f"{SUPABASE_URL}/rest/v1/{table}", headers=sb_headers(), params=filters, timeout=15)
    r.raise_for_status()

def init_db():
    sql = """
    CREATE TABLE IF NOT EXISTS profiles (
        id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, url TEXT NOT NULL,
        platform TEXT DEFAULT 'tiktok', added_at TIMESTAMPTZ DEFAULT NOW(),
        last_synced TIMESTAMPTZ, video_count INTEGER DEFAULT 0, status TEXT DEFAULT 'pending'
    );
    CREATE TABLE IF NOT EXISTS videos (
        id SERIAL PRIMARY KEY, profile_id INTEGER NOT NULL, video_id TEXT UNIQUE NOT NULL,
        title TEXT DEFAULT '', play_url TEXT DEFAULT '', nowm_url TEXT DEFAULT '',
        thumbnail TEXT DEFAULT '', duration INTEGER DEFAULT 0, view_count INTEGER DEFAULT 0,
        like_count INTEGER DEFAULT 0, added_at TIMESTAMPTZ DEFAULT NOW(),
        published_at TEXT DEFAULT '', author TEXT DEFAULT '', platform TEXT DEFAULT 'tiktok',
        favorited BOOLEAN DEFAULT FALSE, note TEXT DEFAULT '', tags TEXT DEFAULT ''
    );"""
    try:
        requests.post(f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
            headers=sb_headers(), json={"sql": sql}, timeout=15)
    except: pass

try: init_db()
except: pass

# ── Dropbox ───────────────────────────────────────────────────────────────────
_dropbox_token = None
_dropbox_lock  = threading.Lock()

def get_dropbox_token():
    global _dropbox_token
    with _dropbox_lock:
        if not _dropbox_token:
            s = load_settings()
            rt = s.get('dropbox_refresh_token','')
            ci = s.get('dropbox_client_id','')
            cs = s.get('dropbox_client_secret','')
            if rt and ci and cs:
                try:
                    r = requests.post("https://api.dropbox.com/oauth2/token",
                        data={"grant_type":"refresh_token","refresh_token":rt,
                              "client_id":ci,"client_secret":cs}, timeout=15)
                    _dropbox_token = r.json().get('access_token')
                except Exception as e:
                    log.error(f"Dropbox token error: {e}")
        return _dropbox_token

def upload_dropbox(video_url, video_id):
    global _dropbox_token
    s = load_settings()
    folder = s.get('dropbox_folder', '/videos')
    try:
        video_data = requests.get(video_url, timeout=60).content
        path = f"{folder}/{video_id}.mp4"
        for attempt in range(2):
            token = get_dropbox_token()
            if not token: return None
            resp = requests.post("https://content.dropboxapi.com/2/files/upload",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/octet-stream",
                         "Dropbox-API-Arg": json.dumps({"path": path, "mode": "overwrite"})},
                data=video_data, timeout=120)
            if resp.status_code == 401 and attempt == 0:
                with _dropbox_lock: _dropbox_token = None
                continue
            resp.raise_for_status()
            log.info(f"Dropbox ✅ {path}")
            return path
    except Exception as e:
        log.error(f"Dropbox error: {e}")
    return None

# ── TikTok Sources (بالترتيب — يجرب واحدة واحدة) ─────────────────────────────

MOBILE_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15'

def _parse_items(items_raw, platform='tiktok'):
    results = []
    for item in items_raw:
        vid_id   = str(item.get('id') or item.get('video_id') or item.get('aweme_id') or '')
        duration = int(item.get('duration') or item.get('video',{}).get('duration',0) or 0)
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
            'platform':    platform
        })
    return results

# Source 1: tikwm.com
def source_tikwm(username, cursor=0):
    clean = username.lstrip('@')
    endpoints = [
        ('POST', 'https://www.tikwm.com/api/user/posts',
         {'unique_id': clean, 'count': 20, 'cursor': cursor, 'hd': 1}),
        ('POST', 'https://tikwm.com/api/user/posts',
         {'unique_id': clean, 'count': 20, 'cursor': cursor, 'hd': 1}),
    ]
    headers = {'User-Agent': MOBILE_UA, 'Referer': 'https://tikwm.com/', 'Origin': 'https://tikwm.com'}
    for method, url, data in endpoints:
        try:
            r = requests.post(url, data=data, headers=headers, timeout=20) if method=='POST' \
                else requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                j = r.json()
                if j.get('code') == 0 and j.get('data'):
                    d = j['data']
                    items = _parse_items(d.get('videos', []))
                    return items, d.get('hasMore', False), d.get('cursor', 0)
        except Exception as e:
            log.warning(f"tikwm {url}: {e}")
    return None, False, 0

# Source 2: RapidAPI (مفاتيح من الإعدادات)
_rapid_idx = 0
_rapid_lock = threading.Lock()

def source_rapidapi(username, cursor=0):
    clean = username.lstrip('@')
    keys = get_keys('rapidapi_keys')
    if not keys: return None, False, 0
    global _rapid_idx
    tried = 0
    while tried < len(keys):
        with _rapid_lock:
            key = keys[_rapid_idx % len(keys)]
            _rapid_idx = (_rapid_idx + 1) % len(keys)
        try:
            r = requests.get(
                'https://tiktok-video-no-watermark2.p.rapidapi.com/user/posts',
                headers={'x-rapidapi-key': key, 'x-rapidapi-host': 'tiktok-video-no-watermark2.p.rapidapi.com'},
                params={'unique_id': clean, 'count': '20', 'cursor': str(cursor)},
                timeout=20)
            if r.status_code == 200:
                j = r.json()
                d = j.get('data') or {}
                items = _parse_items(d.get('videos') or d.get('aweme_list') or [])
                return items, d.get('hasMore', False), d.get('cursor', 0)
            elif r.status_code in (429, 403):
                log.warning(f"RapidAPI key failed ({r.status_code}), rotating...")
        except Exception as e:
            log.warning(f"RapidAPI: {e}")
        tried += 1
        time.sleep(0.3)
    return None, False, 0

# Source 3: ScraperAPI proxy → tikwm
def source_scraperapi(username, cursor=0):
    keys = get_keys('scraperapi_keys')
    if not keys: return None, False, 0
    clean = username.lstrip('@')
    for key in keys:
        try:
            r = requests.post(
                'http://api.scraperapi.com/',
                params={'api_key': key, 'url': 'https://www.tikwm.com/api/user/posts', 'render': 'false'},
                data={'unique_id': clean, 'count': 20, 'cursor': cursor, 'hd': 1},
                timeout=40)
            if r.status_code == 200:
                j = r.json()
                if j.get('code') == 0 and j.get('data'):
                    d = j['data']
                    items = _parse_items(d.get('videos', []))
                    return items, d.get('hasMore', False), d.get('cursor', 0)
        except Exception as e:
            log.warning(f"ScraperAPI: {e}")
    return None, False, 0

# Source 4: Apify TikTok scraper
def source_apify(username):
    keys = get_keys('apify_keys')
    if not keys: return None
    clean = username.lstrip('@')
    for key in keys:
        try:
            # تشغيل Apify actor
            run_r = requests.post(
                'https://api.apify.com/v2/acts/clockworks~free-tiktok-scraper/runs',
                headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
                json={'profiles': [f'https://www.tiktok.com/@{clean}'], 'resultsPerPage': 20,
                      'maxProfilesPerQuery': 1, 'shouldDownloadVideos': False},
                timeout=30)
            if run_r.status_code in (200, 201):
                run_id = run_r.json().get('data', {}).get('id')
                if not run_id: continue
                # انتظر النتيجة (max 60 ثانية)
                for _ in range(12):
                    time.sleep(5)
                    res_r = requests.get(
                        f'https://api.apify.com/v2/acts/clockworks~free-tiktok-scraper/runs/{run_id}',
                        headers={'Authorization': f'Bearer {key}'}, timeout=15)
                    if res_r.json().get('data', {}).get('status') == 'SUCCEEDED':
                        items_r = requests.get(
                            f'https://api.apify.com/v2/acts/clockworks~free-tiktok-scraper/runs/{run_id}/dataset/items',
                            headers={'Authorization': f'Bearer {key}'}, timeout=15)
                        raw = items_r.json()
                        if raw:
                            results = []
                            for item in raw:
                                vid_id = str(item.get('id') or '')
                                dur = int(item.get('videoMeta', {}).get('duration', 0))
                                if not vid_id or dur > MAX_SECS: continue
                                results.append({
                                    'video_id': vid_id,
                                    'title': item.get('text', ''),
                                    'play_url': item.get('videoUrl', ''),
                                    'nowm_url': item.get('videoUrl', ''),
                                    'thumbnail': item.get('covers', [None])[0] or '',
                                    'duration': dur,
                                    'view_count': int(item.get('playCount', 0)),
                                    'like_count': int(item.get('diggCount', 0)),
                                    'published_at': str(item.get('createTime', '')),
                                    'platform': 'tiktok'
                                })
                            return results
                        break
        except Exception as e:
            log.warning(f"Apify: {e}")
    return None

# Source 5: Bright Data
def source_brightdata(username):
    keys = get_keys('brightdata_keys')
    if not keys: return None
    clean = username.lstrip('@')
    for key in keys:
        try:
            r = requests.get(
                f'https://api.brightdata.com/datasets/v3/trigger',
                headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
                params={'dataset_id': 'gd_lu702nij2f790tmv9h', 'format': 'json'},
                json=[{'url': f'https://www.tiktok.com/@{clean}'}],
                timeout=30)
            if r.status_code in (200, 201):
                data = r.json()
                items = data if isinstance(data, list) else data.get('data', [])
                results = []
                for item in items:
                    vid_id = str(item.get('id') or item.get('video_id') or '')
                    dur = int(item.get('duration') or 0)
                    if not vid_id or dur > MAX_SECS: continue
                    results.append({
                        'video_id': vid_id, 'title': item.get('title',''),
                        'play_url': item.get('video_url',''), 'nowm_url': item.get('video_url',''),
                        'thumbnail': item.get('cover',''), 'duration': dur,
                        'view_count': int(item.get('plays',0)), 'like_count': int(item.get('likes',0)),
                        'published_at': str(item.get('create_time','')), 'platform': 'tiktok'
                    })
                if results: return results
        except Exception as e:
            log.warning(f"BrightData: {e}")
    return None

# ── Main TikTok fetcher (يجرب المصادر بالترتيب) ──────────────────────────────
def fetch_tiktok(username, fetch_all=False):
    results, cursor, page = [], 0, 0
    sources_tried = set()

    while True:
        items = None
        has_more = False
        next_cursor = 0

        # ترتيب المصادر
        for source_name, source_fn in [
            ('tikwm',      lambda: source_tikwm(username, cursor)),
            ('rapidapi',   lambda: source_rapidapi(username, cursor)),
            ('scraperapi', lambda: source_scraperapi(username, cursor)),
        ]:
            if source_name in sources_tried and page > 0:
                continue
            result = source_fn()
            if result[0] is not None:
                items, has_more, next_cursor = result
                log.info(f"[{username}] Source '{source_name}' got {len(items)} items (page {page})")
                break
            else:
                sources_tried.add(source_name)
                log.warning(f"[{username}] Source '{source_name}' failed")

        # لو كل المصادر العادية فشلت، جرب Apify و BrightData (بياخدوا وقت أطول)
        if items is None and page == 0:
            log.info(f"[{username}] Trying async sources (Apify/BrightData)...")
            apify_res = source_apify(username)
            if apify_res:
                results.extend(apify_res)
                log.info(f"[{username}] Apify got {len(apify_res)} items")
                break
            bd_res = source_brightdata(username)
            if bd_res:
                results.extend(bd_res)
                log.info(f"[{username}] BrightData got {len(bd_res)} items")
                break
            log.error(f"[{username}] All sources failed!")
            break

        if items is None: break
        results.extend(items)
        page += 1
        if not fetch_all or not has_more or not next_cursor or page >= 50: break
        cursor = next_cursor
        time.sleep(0.8)

    log.info(f"[{username}] Total: {len(results)} videos")
    return results

# ── Sync ──────────────────────────────────────────────────────────────────────
def sync_profile(profile_id, username, url, platform, new_only=False):
    try:
        sb_patch('profiles', {'id': f'eq.{profile_id}'}, {'status': 'downloading'})
        if platform == 'tiktok': items = fetch_tiktok(username, fetch_all=not new_only)
        else: items = []

        existing = set()
        try:
            rows = sb_get('videos', {'profile_id': f'eq.{profile_id}', 'select': 'video_id'})
            existing = {r['video_id'] for r in rows}
        except: pass

        new_count, dropbox_queue = 0, []
        for v in items:
            if v['video_id'] in existing: continue
            try:
                sb_post('videos', {
                    'profile_id': profile_id, 'video_id': v['video_id'],
                    'title': v['title'], 'play_url': v['play_url'], 'nowm_url': v['nowm_url'],
                    'thumbnail': v['thumbnail'], 'duration': v['duration'],
                    'view_count': v['view_count'], 'like_count': v['like_count'],
                    'published_at': v['published_at'], 'author': username, 'platform': v['platform']
                })
                new_count += 1
                existing.add(v['video_id'])
                if new_count <= 3:
                    push({'type':'new_video','username':username,'title':v['title'],
                          'thumbnail':v['thumbnail'],'platform':platform})
                if v.get('nowm_url') or v.get('play_url'):
                    dropbox_queue.append({'url': v.get('nowm_url') or v['play_url'],
                                         'video_id': v['video_id']})
            except Exception as e:
                if 'duplicate' not in str(e).lower(): log.error(f"Insert: {e}")

        total = len(existing)
        sb_patch('profiles', {'id': f'eq.{profile_id}'},
                 {'status':'active','video_count':total,'last_synced':datetime.utcnow().isoformat()})
        push({'type':'sync_done','username':username,'new':new_count})
        log.info(f"[{username}] done +{new_count}")

        # رفع Dropbox في الخلفية
        if dropbox_queue and load_settings().get('dropbox_refresh_token'):
            def _upload_batch(q):
                for item in q:
                    upload_dropbox(item['url'], item['video_id'])
                    time.sleep(1)
            threading.Thread(target=_upload_batch, args=(dropbox_queue,), daemon=True).start()

    except Exception as e:
        log.error(f"[{username}] sync error: {e}")
        try: sb_patch('profiles', {'id': f'eq.{profile_id}'}, {'status': 'error'})
        except: pass

def delete_old():
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        sb_delete('videos', {'added_at': f'lt.{cutoff}', 'favorited': 'eq.false'})
        log.info("Old videos cleaned")
    except Exception as e: log.error(f"delete_old: {e}")

def bg_loop():
    while True:
        time.sleep(6 * 3600)
        try:
            profiles = sb_get('profiles', {'status': 'neq.paused', 'select': '*'})
            for p in profiles:
                threading.Thread(target=sync_profile,
                    args=(p['id'],p['username'],p['url'],p['platform'],True), daemon=True).start()
                time.sleep(3)
            delete_old()
        except Exception as e: log.error(f"bg: {e}")

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

def row_to_dict(row):
    d = dict(row)
    for k,v in d.items():
        if isinstance(v, datetime): d[k] = v.isoformat()
    return d

# ── Settings API ──────────────────────────────────────────────────────────────
def mask(v):
    if not v: return ''
    return v[:6]+'***'+v[-3:] if len(v) > 9 else '***'

@app.route('/api/settings', methods=['GET'])
def get_settings():
    s = load_settings()
    return jsonify({
        'rapidapi_keys':          s.get('rapidapi_keys',''),
        'scraperapi_keys':        s.get('scraperapi_keys',''),
        'apify_keys':             s.get('apify_keys',''),
        'brightdata_keys':        s.get('brightdata_keys',''),
        'dropbox_refresh_token':  mask(s.get('dropbox_refresh_token','')),
        'dropbox_client_id':      s.get('dropbox_client_id',''),
        'dropbox_client_secret':  mask(s.get('dropbox_client_secret','')),
        'dropbox_folder':         s.get('dropbox_folder','/videos'),
        'max_duration':           s.get('max_duration', 120),
        'has_rapidapi':   bool(get_keys('rapidapi_keys')),
        'has_scraperapi': bool(get_keys('scraperapi_keys')),
        'has_apify':      bool(get_keys('apify_keys')),
        'has_brightdata': bool(get_keys('brightdata_keys')),
        'has_dropbox':    bool(s.get('dropbox_refresh_token')),
        'rapidapi_count':   len(get_keys('rapidapi_keys')),
        'scraperapi_count': len(get_keys('scraperapi_keys')),
        'apify_count':      len(get_keys('apify_keys')),
        'brightdata_count': len(get_keys('brightdata_keys')),
    })

@app.route('/api/settings', methods=['POST'])
def update_settings():
    global MAX_SECS, _dropbox_token
    data = request.json or {}
    s = load_settings()
    fields = ['rapidapi_keys','scraperapi_keys','apify_keys','brightdata_keys',
              'dropbox_refresh_token','dropbox_client_id','dropbox_client_secret',
              'dropbox_folder','max_duration']
    for f in fields:
        if f in data and data[f] not in ('', None) and '***' not in str(data[f]):
            s[f] = data[f]
    if save_settings_file(s):
        if s.get('max_duration'):
            try: MAX_SECS = int(s['max_duration'])
            except: pass
        _dropbox_token = None  # force refresh
        return jsonify({'ok': True, 'msg': '✅ تم حفظ الإعدادات'})
    return jsonify({'ok': False, 'msg': 'خطأ في الحفظ'}), 500

@app.route('/api/settings/test_dropbox', methods=['POST'])
def test_dropbox():
    try:
        token = get_dropbox_token()
        if not token: return jsonify({'ok': False, 'msg': 'تعذر الحصول على Token — تحقق من الإعدادات'})
        r = requests.post('https://api.dropboxapi.com/2/users/get_current_account',
            headers={'Authorization': f'Bearer {token}'}, timeout=10)
        if r.status_code == 200:
            name = r.json().get('name', {}).get('display_name', '')
            return jsonify({'ok': True, 'msg': f'✅ متصل — {name}'})
        return jsonify({'ok': False, 'msg': f'خطأ {r.status_code}'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})

@app.route('/api/settings/test_rapidapi', methods=['POST'])
def test_rapidapi():
    keys = get_keys('rapidapi_keys')
    if not keys: return jsonify({'ok': False, 'msg': 'مفيش مفاتيح RapidAPI'})
    working = 0
    for key in keys:
        try:
            r = requests.get('https://tiktok-video-no-watermark2.p.rapidapi.com/user/info',
                headers={'x-rapidapi-key': key, 'x-rapidapi-host': 'tiktok-video-no-watermark2.p.rapidapi.com'},
                params={'unique_id': 'tiktok'}, timeout=10)
            if r.status_code == 200: working += 1
        except: pass
    if working:
        return jsonify({'ok': True, 'msg': f'✅ {working}/{len(keys)} مفتاح شغال'})
    return jsonify({'ok': False, 'msg': f'❌ كل المفاتيح ({len(keys)}) فاشلة'})

@app.route('/api/settings/test_source', methods=['POST'])
def test_source():
    """اختبار مصدر معين"""
    source = (request.json or {}).get('source', 'tikwm')
    test_user = 'tiktok'
    try:
        if source == 'tikwm':
            items, _, _ = source_tikwm(test_user)
            ok = items is not None
            return jsonify({'ok': ok, 'msg': f'✅ tikwm شغال' if ok else '❌ tikwm فاشل'})
        elif source == 'scraperapi':
            keys = get_keys('scraperapi_keys')
            if not keys: return jsonify({'ok': False, 'msg': 'مفيش مفاتيح ScraperAPI'})
            items, _, _ = source_scraperapi(test_user)
            ok = items is not None
            return jsonify({'ok': ok, 'msg': f'✅ ScraperAPI شغال' if ok else '❌ ScraperAPI فاشل'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)})
    return jsonify({'ok': False, 'msg': 'مصدر غير معروف'})

# ── Profiles API ──────────────────────────────────────────────────────────────
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
            threading.Thread(target=sync_profile, args=(pid,username,url,platform,False), daemon=True).start()
        return jsonify({'id':pid,'username':username,'platform':platform})
    except Exception as e:
        if 'duplicate' in str(e).lower(): return jsonify({'error':'البروفايل موجود'}), 409
        return jsonify({'error': str(e)}), 500

@app.route('/api/profiles/<int:pid>', methods=['DELETE'])
def delete_profile(pid):
    sb_delete('videos', {'profile_id': f'eq.{pid}'})
    sb_delete('profiles', {'id': f'eq.{pid}'})
    return jsonify({'ok': True})

@app.route('/api/profiles/<int:pid>/sync', methods=['POST'])
def do_sync(pid):
    rows = sb_get('profiles', {'id': f'eq.{pid}', 'select': '*'})
    if not rows: return jsonify({'error':'مش موجود'}), 404
    p = rows[0]
    if p['status'] == 'downloading': return jsonify({'status':'already_running'})
    threading.Thread(target=sync_profile, args=(p['id'],p['username'],p['url'],p['platform'],False), daemon=True).start()
    return jsonify({'status':'syncing'})

# ── Videos API ────────────────────────────────────────────────────────────────
@app.route('/api/videos')
def get_videos():
    pid=request.args.get('profile_id'); q=request.args.get('q','')
    platform=request.args.get('platform',''); favs=request.args.get('favorites','')
    sort=request.args.get('sort','recent')
    base="SELECT v.*, p.username FROM videos v JOIN profiles p ON v.profile_id=p.id"
    conds,params=[],[]
    if pid:      conds.append("v.profile_id=%s"); params.append(pid)
    if q:        conds.append("v.title ILIKE %s"); params.append(f'%{q}%')
    if platform: conds.append("v.platform=%s"); params.append(platform)
    if favs:     conds.append("v.favorited=TRUE")
    where=f"WHERE {' AND '.join(conds)}" if conds else ''
    order={'popular':'v.view_count DESC','duration':'v.duration DESC'}.get(sort,'v.added_at DESC')
    rows=sb_get('videos', {
        'order': order.replace('v.','').replace(' DESC','.desc').replace(' ASC','.asc'),
        'limit': '200', 'select': '*,profiles(username)',
        **({'profile_id': f'eq.{pid}'} if pid else {}),
        **({'platform': f'eq.{platform}'} if platform else {}),
        **({'favorited': 'eq.true'} if favs else {}),
        **({'title': f'ilike.*{q}*'} if q else {}),
    })
    for r in rows:
        if isinstance(r.get('profiles'), dict):
            r['username'] = r['profiles'].get('username','')
        r.pop('profiles', None)
    return jsonify(rows)

@app.route('/api/videos/<int:vid>/favorite', methods=['POST'])
def toggle_fav(vid):
    rows=sb_get('videos',{'id':f'eq.{vid}','select':'favorited'})
    if not rows: return jsonify({'error':'مش موجود'}), 404
    new_val=not rows[0]['favorited']
    sb_patch('videos',{'id':f'eq.{vid}'},{'favorited':new_val})
    return jsonify({'favorited':new_val})

@app.route('/api/videos/<int:vid>/note', methods=['POST'])
def save_note(vid):
    sb_patch('videos',{'id':f'eq.{vid}'},{'note':(request.json or {}).get('note','')})
    return jsonify({'ok':True})

@app.route('/api/videos/<int:vid>/tags', methods=['POST'])
def save_tags(vid):
    sb_patch('videos',{'id':f'eq.{vid}'},{'tags':(request.json or {}).get('tags','')})
    return jsonify({'ok':True})

@app.route('/api/tags')
def get_tags():
    rows=sb_get('videos',{'select':'tags','tags':'neq.'})
    all_tags=set()
    for r in rows:
        for t in (r.get('tags') or '').split(','):
            t=t.strip()
            if t: all_tags.add(t)
    return jsonify(sorted(list(all_tags)))

@app.route('/api/stats')
def stats():
    try:
        p=len(sb_get('profiles',{'select':'id'}))
        v=len(sb_get('videos',{'select':'id'}))
        f=len(sb_get('videos',{'select':'id','favorited':'eq.true'}))
        s=load_settings()
        return jsonify({
            'profiles':p,'videos':v,'favorites':f,
            'sources': {
                'tikwm':      '✅ متاح دائماً',
                'rapidapi':   f"{'✅' if get_keys('rapidapi_keys') else '❌'} {len(get_keys('rapidapi_keys'))} مفتاح",
                'scraperapi': f"{'✅' if get_keys('scraperapi_keys') else '❌'} {len(get_keys('scraperapi_keys'))} مفتاح",
                'apify':      f"{'✅' if get_keys('apify_keys') else '❌'} {len(get_keys('apify_keys'))} مفتاح",
                'brightdata': f"{'✅' if get_keys('brightdata_keys') else '❌'} {len(get_keys('brightdata_keys'))} مفتاح",
                'dropbox':    '✅ متصل' if s.get('dropbox_refresh_token') else '❌ غير مفعل',
            }
        })
    except Exception as e:
        return jsonify({'profiles':0,'videos':0,'favorites':0,'sources':{}})

@app.route('/api/config')
def config():
    s=load_settings()
    return jsonify({'has_key':bool(get_keys('rapidapi_keys')),'has_db':bool(SUPABASE_URL and SUPABASE_KEY)})

@app.route('/api/publisher/next')
def publisher_next():
    count=int(request.args.get('count',1))
    page_id=request.args.get('page_id','default')
    try:
        rows=sb_get('videos',{'order':'added_at.desc','limit':'100','select':'*,profiles(username)'})
        for r in rows:
            if isinstance(r.get('profiles'),dict): r['username']=r['profiles'].get('username','')
            r.pop('profiles',None)
        if not rows: return jsonify({'success':False,'error':'لا توجد فيديوهات','videos':[]})
        selected=random.sample(rows,min(count,len(rows)))
        result=[{'video_id':v['video_id'],'title':v.get('title',''),
                 'url':v.get('nowm_url') or v.get('play_url',''),
                 'thumbnail':v.get('thumbnail',''),'duration':v.get('duration',0),
                 'author':v.get('username',''),'platform':v.get('platform','tiktok')} for v in selected]
        return jsonify({'success':True,'videos':result,'count':len(result)})
    except Exception as e:
        return jsonify({'success':False,'error':str(e),'videos':[]})

@app.route('/api/publisher/stats')
def publisher_stats():
    try:
        total=len(sb_get('videos',{'select':'id'}))
        return jsonify({'total_videos':total,'ready':total>0,'library_url':'https://tiklib2026.up.railway.app'})
    except: return jsonify({'total_videos':0,'ready':False})

@app.route('/api/events')
def sse_stream():
    q=queue.Queue(maxsize=50)
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

@app.route('/settings')
def settings_page(): return send_from_directory('static', 'settings.html')

@app.route('/')
def index(): return send_from_directory('static', 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info(f"TikLib starting on port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)

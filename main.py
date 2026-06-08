import os, re, json, asyncio, logging, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import OrderedDict

# Pyrogram imports (Safe handling)
try:
    from pyrogram.errors import ListenerTimeout
except ImportError:
    class ListenerTimeout(Exception): pass

# ───────────────── 🛠️ RAILWAY CONFIGURATION & LOGGING ─────────────────
# Railway logs ko clean rakhne ke liye logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("GuidelyCLI")

# Environment Variables (Inhe Railway Dashboard mein add karna hai)
BASE_URL = os.getenv("GUIDELY_BASE_URL", "https://mobapi.guidely.in")
DRM_BASE_URL = os.getenv("GUIDELY_DRM_URL", "https://guidely.in/blog/drmplayer")
API_KEY = os.getenv("GUIDELY_API_KEY", "85a1364c-0419-42d5-b4c9-5dbe71549743")
AUTH_TOKEN = os.getenv("GUIDELY_AUTH_TOKEN", "384561026d1529e5a9fbc852f029fe160457c89b12a4c32afb568aee2a86d145ce48b14c1b5b4d9ac9a9d2942423d9ebf08a4bb3ab53687008f6137f5aa62df8")
DEVICE_ID = os.getenv("GUIDELY_DEVICE_ID", "PQ3B.190801.04221524")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))

# 🚨 RAILWAY SPECIAL: Hamesha /tmp mein files save karein, warna disk full ho jayegi
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/tmp/guidely_exports") 
THUMB_PATH = os.getenv("THUMB_PATH", "Modules/Tools/Thumbnail/Extractor.jpg")

# ───────────────── 🌐 GLOBAL SESSION (Memory Leak Fix) ─────────────────
# Railway ki RAM bachane ke liye hum ek global session use karenge 
# jo TCP connections ko reuse karega (Connection Pooling).
_guidely_session = None

def get_session():
    global _guidely_session
    if _guidely_session is None:
        _guidely_session = requests.Session()
        # Retry mechanism agar aapki API ek second ke liye down ho
        retry_strategy = Retry(
            total=3, 
            backoff_factor=0.5, 
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        # Connection pooling for fast API responses
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry_strategy)
        _guidely_session.mount("http://", adapter)
        _guidely_session.mount("https://", adapter)
        
        _guidely_session.headers.update({
            "User-Agent": "Dart/3.5 (dart:io)",
            "platform": "Android",
            "api-key": API_KEY,
            "auth": AUTH_TOKEN,
            "Content-Type": "application/json; charset=utf-8",
            "accept-encoding": "gzip",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        })
    return _guidely_session

_guidely_store = {}

# ───────────────── 🛠️ UTILS ─────────────────
def _safe_filename(name):
    safe = "".join(c if c.isalnum() or c in " _-." else "_" for c in name)
    return safe[:80].strip() + ".txt"

def _extract_token(iframe):
    if not iframe or not isinstance(iframe, str): return None
    m = re.search(r'(?:access_token|token|id)=([a-zA-Z0-9\-_.]+)', iframe)
    return m.group(1) if m else None

# ───────────────── 🎥 DRM ENDPOINTS (M3U8 FETCHER) ─────────────────
def _get_m3u8(token, session=None):
    if not token: return None
    sess = session or get_session()
    
    # Aapki 3 DRM APIs
    endpoints = [
        {"url": f"{DRM_BASE_URL}/new-main.php", "params": {"token": token, "device_id": DEVICE_ID}, "headers": {"User-Agent": "Dart/3.5 (dart:io)", "Content-Type": "application/json"}},
        {"url": f"{DRM_BASE_URL}/tpstream-player.php", "params": {"token": token}, "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://guidely.in/"}},
        {"url": f"{DRM_BASE_URL}/player.php", "params": {"token": token, "device_id": DEVICE_ID}, "headers": {"User-Agent": "Dart/3.5 (dart:io)"}}
    ]

    for ep in endpoints:
        try:
            r = sess.get(ep["url"], params=ep["params"], headers=ep["headers"], timeout=(5, 10))
            if r.status_code == 200:
                try:
                    data = r.json()
                    if data.get("status") and isinstance(data.get("data"), dict):
                        file_url = data["data"].get("file_url")
                        if file_url: return file_url
                except json.JSONDecodeError:
                    continue
        except requests.exceptions.RequestException as e:
            logger.warning(f"DRM API Error: {e}")
            continue
    return None

def _process_session(s, session=None):
    if not isinstance(s, dict): return None
    cat = (s.get("catgname") or "General").strip()
    title = (s.get("name") or "No Title").strip()
    
    pdf = s.get("urlpdf") if isinstance(s.get("urlpdf"), str) else None
    
    vid_raw = None
    vld = s.get("video_link_data")
    if isinstance(vld, dict): vid_raw = vld.get("video_id")
    if not vid_raw: vid_raw = _extract_token(s.get("iframe"))
    if not vid_raw and s.get("video_link"):
        m = re.search(r'/(\d+)$', s.get("video_link", ""))
        if m: vid_raw = m.group(1)
    
    m3u8 = _get_m3u8(vid_raw, session) if vid_raw else None
    if not m3u8 and not pdf: return None
        
    return {"cat": cat, "title": title, "video": m3u8, "pdf": pdf}

# ───────────────── ⚙️ EXTRACTION CORE ─────────────────
def _extract_batch_sync(bid, output_file_path):
    stats = {}
    session = get_session()
    
    try:
        url = f"{BASE_URL}/live-video-class-new/{bid}"
        res = session.get(url, timeout=20)
        if res.status_code != 200:
            logger.error(f"Batch API Error: {res.status_code} for ID {bid}")
            return False, {"total": 0, "video": 0, "pdf": 0, "subjects": {}}
        
        data = res.json()
        if not isinstance(data, dict): return False, {"total": 0, "video": 0, "pdf": 0, "subjects": {}}
        
        items = []
        for key in ["live", "upcmg", "prevs", "recorded", "classes", "sessions"]:
            lst = data.get("data", {}).get(key) if isinstance(data.get("data"), dict) else data.get(key)
            if isinstance(lst, list): items.extend(lst)
        
        if not items: return False, {"total": 0, "video": 0, "pdf": 0, "subjects": {}}
        
        total = len(items)
        results = [None] * total
        
        # Multi-threading for fast extraction
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_process_session, items[i], session): i for i in range(total)}
            for future in as_completed(futures):
                idx = futures[future]
                try: results[idx] = future.result()
                except Exception as e: logger.warning(f"Item {idx} error: {e}")
        
        valid = [r for r in results if r]
        if not valid: return False, {"total": 0, "video": 0, "pdf": 0, "subjects": {}}
        
        grouped = OrderedDict()
        for r in valid: grouped.setdefault(r["cat"], []).append(r)
        
        # 🚨 RAILWAY SPECIAL: File ko /tmp mein save karna
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, "w", encoding="utf-8", buffering=65536) as f:
            f.write(f"# GUIDELY Export\n# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-" * 80 + "\n")
            
            v_total, p_total = 0, 0
            for cat, sessions in grouped.items():
                if cat not in stats: stats[cat] = {"video": 0, "pdf": 0}
                for s in sessions:
                    if s["video"]:
                        f.write(f"[{cat}] | {s['title']} | 🎥 {s['video']}\n")
                        stats[cat]["video"] += 1; v_total += 1
                    if s["pdf"]:
                        f.write(f"[{cat}] | {s['title']} | 📄 {s['pdf']}\n")
                        stats[cat]["pdf"] += 1; p_total += 1
                f.flush()
        
        return True, {"total": v_total + p_total, "video": v_total, "pdf": p_total, "subjects": stats}
        
    except Exception as e:
        logger.error(f"GUIDELY extract error for batch {bid}: {e}")
        return False, {"total": 0, "video": 0, "pdf": 0, "subjects": {}}

# ───────────────── 📝 CAPTION GENERATOR ─────────────────
def generate_guidely_caption(result_file, batch_name, batch_id, user_mention, price="Free"):
    try:
        with open(result_file, "r", encoding="utf-8") as f: content = f.read()
    except Exception: content = ""
    
    lines = [l.strip() for l in content.splitlines() if l.strip() and " | " in l]
    pdf_lines = [l for l in lines if "📄" in l or ".pdf" in l.lower()]
    video_lines = [l for l in lines if "🎥" in l]
    total_urls, total_pdf, total_vid = len(lines), len(pdf_lines), len(video_lines)
    
    subjects = {}
    for l in lines:
        match = re.match(r"^\[([^\]]+)\]", l)
        if match: subjects[match.group(1)] = subjects.get(match.group(1), 0) + 1
    
    subj_text = "\n".join([f"  • {s}: {c}" for s, c in list(subjects.items())[:5]])
    if len(subjects) > 5: subj_text += f"\n  • ...and {len(subjects)-5} more"
    
    return (
        f"<blockquote><b>⚜️ App: GUIDELY</b>\n<b>🔗 Batch:</b> {batch_name}</blockquote>\n\n"
        f"<b>======= BATCH DETAILS =======</b>\n"
        f"<blockquote expandable>🌟 <b>Batch Name :</b> {batch_name}\n"
        f"🪪 <b>Batch ID :</b> {batch_id}\n"
        f"💰 <b>Price :</b> ₹{price}</blockquote>\n\n"
        f"<b>======= LINK SUMMARY =======</b>\n"
        f"<blockquote expandable>🔢 <b>Total Links :</b> {total_urls}\n"
        f"┠🎥 <b>Videos :</b> {total_vid}\n"
        f"┠📄 <b>PDF Notes :</b> {total_pdf}\n"
        f"┠📚 <b>Subjects :</b> {len(subjects)}\n{subj_text}</blockquote>\n\n"
        f"<b>🧑‍🏫 Generated By :</b> {user_mention}\n"
        f"<b>📅 Generated On :</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}"
    )

# ───────────────── 🤖 BOT HANDLERS ─────────────────
def fetch_batches_sync():
    try:
        session = get_session()
        res = session.get(f"{BASE_URL}/video-class", timeout=10)
        if res.status_code != 200: return []
        
        data = res.json()
        if not isinstance(data, dict): return []
        
        products = data.get("data", {}).get("products", []) if isinstance(data.get("data"), dict) else data.get("products", [])
        if not isinstance(products, list): return []
        
        return [{"id": str(b.get("id")), "title": b.get("title", "Unknown"), "price": b.get("price", "0"), "image": b.get("image", "")} for b in products if isinstance(b, dict)]
    except Exception as e:
        logger.error(f"GUIDELY fetch batches error: {e}")
        return []

async def guidely_start_handler(bot, message, user_id):
    chat_id = message.chat.id
    prog = await bot.send_message(chat_id, "🔄 Fetching GUIDELY batches...")
    
    try:
        loop = asyncio.get_event_loop()
        batches = await loop.run_in_executor(None, fetch_batches_sync)
        
        if not batches: return await prog.edit_text("❌ No batches found or API error!")
        
        batch_text = "<blockquote><b>⚜️ GUIDELY Available Batches</b></blockquote>\n\n"
        batch_map = {}
        for i, b in enumerate(batches, 1):
            title = b['title'][:55].replace('\n', ' ').strip()
            batch_text += f"{i}] {title} | ₹{b.get('price', '0')}\n"
            batch_map[i] = b
        
        _guidely_store[user_id] = {"batch_map": batch_map, "batches": batches}
        await prog.delete()
        
        # 🚨 RAILWAY SPECIAL: Temp file ko /tmp mein banakar turant delete karna
        txt_file = "/tmp/GUIDELY_Batches.txt"
        with open(txt_file, "w", encoding="utf-8") as f: f.write(batch_text)
        
        thumb = THUMB_PATH if os.path.exists(THUMB_PATH) else None
        await bot.send_document(chat_id, txt_file, caption="📋 **GUIDELY Batches**\n\n👉 Send batch number (e.g., `5`)", thumb=thumb)
        if os.path.exists(txt_file): os.remove(txt_file) # Delete immediately
        
        try:
            inp = await bot.ask(chat_id, "", timeout=60)
            await inp.delete()
            cmd = inp.text.strip()
            
            if cmd.lower() == "cancel":
                _guidely_store.pop(user_id, None)
                return await bot.send_message(chat_id, "🔙 Cancelled.")
            if not cmd.isdigit():
                return await bot.send_message(chat_id, "❌ Send valid batch number!")
            
            batch_num = int(cmd)
            if batch_num not in batch_map:
                return await bot.send_message(chat_id, f"❌ Invalid! Choose 1-{len(batch_map)}")
            
            selected = batch_map[batch_num]
            await guidely_batch_selected(bot, message, user_id, selected, show_progress=True)
            
        except ListenerTimeout:
            await bot.send_message(chat_id, "⏰ Timeout! Start again.")
        finally:
            _guidely_store.pop(user_id, None)
            
    except Exception as e:
        logger.error(f"GUIDELY start error: {e}")
        await prog.edit_text(f"❌ Error: {str(e)[:200]}")

async def guidely_batch_selected(bot, message, user_id, batch, show_progress=True):
    chat_id = message.chat.id
    bid, bname, price = batch["id"], batch["title"], batch.get("price", "0")
    
    prog = await message.reply(f"⚡ **Extracting:** {bname[:40]}...\n\n🚀 Fast mode enabled!") if show_progress else None
    
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        fname = _safe_filename(bname)
        fpath = os.path.join(OUTPUT_DIR, fname)
        
        loop = asyncio.get_event_loop()
        success, stats = await loop.run_in_executor(None, _extract_batch_sync, bid, fpath)
        
        if not success or not os.path.exists(fpath) or stats["total"] == 0:
            msg = "❌ Extraction failed or empty batch!"
            if prog: await prog.edit_text(msg)
            else: await bot.send_message(chat_id, f"❌ Failed: {bname[:30]}")
            return False
        
        try:
            user = await bot.get_users(chat_id)
            user_mention = user.mention
        except Exception: user_mention = f"User#{chat_id}"
        
        caption = generate_guidely_caption(fpath, bname, bid, user_mention, price)
        thumb = THUMB_PATH if os.path.exists(THUMB_PATH) else None
        await bot.send_document(chat_id, fpath, caption=caption, thumb=thumb)
        
        if prog:
            try:
                elapsed = time.time() - (prog.date.timestamp() if prog.date else time.time())
                await prog.edit_text(f"✅ **Done in {elapsed:.1f}s!**\n📦 {bname[:30]}... | 📊 {stats['total']} links\n🎥{stats['video']} videos + 📄{stats['pdf']} PDFs")
            except: pass
        
        # 🚨 RAILWAY SPECIAL: File ko Telegram par bhejne ke turant baad delete karna
        if os.path.exists(fpath): os.remove(fpath)
        return True
        
    except Exception as e:
        logger.error(f"GUIDELY extract error: {e}")
        msg = f"❌ Error: {str(e)[:200]}"
        if prog: await prog.edit_text(msg)
        else: await bot.send_message(chat_id, f"❌ Error: {bname[:30]}")
        return False
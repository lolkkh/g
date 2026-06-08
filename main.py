import os
import re
import logging
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
import uvicorn

# ───────────────── 🛠️ LOGGING & CONFIGURATION ─────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("GuidelyAPI")

BASE_URL = os.getenv("GUIDELY_BASE_URL", "https://mobapi.guidely.in")
DRM_BASE_URL = os.getenv("GUIDELY_DRM_URL", "https://guidely.in/blog/drmplayer")
API_KEY = os.getenv("GUIDELY_API_KEY", "85a1364c-0419-42d5-b4c9-5dbe71549743")
AUTH_TOKEN = os.getenv("GUIDELY_AUTH_TOKEN", "384561026d1529e5a9fbc852f029fe160457c89b12a4c32afb568aee2a86d145ce48b14c1b5b4d9ac9a9d2942423d9ebf08a4bb3ab53687008f6137f5aa62df8")
DEVICE_ID = os.getenv("GUIDELY_DEVICE_ID", "PQ3B.190801.04221524")

# ───────────────── 🌐 GLOBAL ASYNC HTTP CLIENT (Super Fast) ─────────────────
# httpx is non-blocking and handles thousands of concurrent requests easily
client = httpx.AsyncClient(
    timeout=httpx.Timeout(10.0, connect=5.0),
    limits=httpx.Limits(max_keepalive_connections=50, max_connections=100),
    headers={
        "User-Agent": "Dart/3.5 (dart:io)",
        "platform": "Android",
        "api-key": API_KEY,
        "auth": AUTH_TOKEN,
        "Content-Type": "application/json; charset=utf-8",
        "accept-encoding": "gzip",
    }
)

# ───────────────── 🛠️ CORE EXTRACTION LOGIC (ASYNC) ─────────────────
def _extract_token(iframe):
    if not iframe or not isinstance(iframe, str): return None
    m = re.search(r'(?:access_token|token|id)=([a-zA-Z0-9\-_.]+)', iframe)
    return m.group(1) if m else None

async def _fetch_drm_url(ep):
    """Tries a single DRM endpoint"""
    try:
        r = await client.get(ep["url"], params=ep["params"], headers=ep["headers"])
        if r.status_code == 200:
            data = r.json()
            if data.get("status") and isinstance(data.get("data"), dict):
                return data["data"].get("file_url")
    except Exception:
        pass
    return None

async def _get_m3u8(token):
    """🔥 CONCURRENT FALLBACK: Tries all 3 endpoints simultaneously!"""
    if not token: return None
    
    endpoints = [
        {"url": f"{DRM_BASE_URL}/new-main.php", "params": {"token": token, "device_id": DEVICE_ID}, "headers": {"User-Agent": "Dart/3.5 (dart:io)", "Content-Type": "application/json"}},
        {"url": f"{DRM_BASE_URL}/tpstream-player.php", "params": {"token": token}, "headers": {"User-Agent": "Mozilla/5.0", "Referer": "https://guidely.in/"}},
        {"url": f"{DRM_BASE_URL}/player.php", "params": {"token": token, "device_id": DEVICE_ID}, "headers": {"User-Agent": "Dart/3.5 (dart:io)"}}
    ]
    
    # Create tasks for all 3 endpoints
    tasks = [_fetch_drm_url(ep) for ep in endpoints]
    
    # Wait for the FIRST task to complete successfully
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            if result:
                return result # Return immediately, ignoring the slow ones!
        except Exception:
            continue
            
    return None

async def _process_session(s):
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
        
    m3u8 = await _get_m3u8(vid_raw) if vid_raw else None
    if not m3u8 and not pdf: return None
    return {"cat": cat, "title": title, "video": m3u8, "pdf": pdf}

async def _fetch_batches_sync():
    res = await client.get(f"{BASE_URL}/video-class")
    if res.status_code != 200: return []
    data = res.json()
    products = data.get("data", {}).get("products", []) if isinstance(data.get("data"), dict) else data.get("products", [])
    if not isinstance(products, list): return []
    return [{"id": str(b.get("id")), "title": b.get("title", "Unknown"), "price": b.get("price", "0"), "image": b.get("image", "")} for b in products if isinstance(b, dict)]

async def _extract_batch_sync(bid):
    res = await client.get(f"{BASE_URL}/live-video-class-new/{bid}")
    if res.status_code != 200: raise Exception(f"Batch API Error: {res.status_code}")
    data = res.json()
    
    items = []
    for key in ["live", "upcmg", "prevs", "recorded", "classes", "sessions"]:
        lst = data.get("data", {}).get(key) if isinstance(data.get("data"), dict) else data.get(key)
        if isinstance(lst, list): items.extend(lst)
    if not items: raise Exception("No items found in this batch")
    
    # 🔥 Process ALL items concurrently using asyncio.gather
    tasks = [_process_session(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    valid = [r for r in results if isinstance(r, dict)]
    
    # Group by subject
    grouped = {}
    for r in valid:
        cat = r["cat"]
        if cat not in grouped: grouped[cat] = []
        grouped[cat].append({"title": r["title"], "video": r["video"], "pdf": r["pdf"]})
        
    return {
        "total": len(valid),
        "video": sum(1 for r in valid if r["video"]),
        "pdf": sum(1 for r in valid if r["pdf"]),
        "subjects": grouped
    }

# ───────────────── 🚀 FASTAPI ROUTES (ENDPOINTS) ─────────────────
app = FastAPI(title="Guidely Extractor API")

@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()

@app.get("/")
def root():
    return {"status": "running", "message": "Guidely API is ready!", "endpoints": ["/allbatch", "/batch/{batch_id}", "/drm/{token}"]}

@app.get("/allbatch")
async def all_batch():
    try:
        batches = await _fetch_batches_sync()
        return {"success": True, "count": len(batches), "data": batches}
    except Exception as e:
        logger.error(f"Error in /allbatch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/batch/{batch_id}")
async def get_batch(batch_id: str):
    try:
        result = await _extract_batch_sync(batch_id)
        return {"success": True, "batch_id": batch_id, "data": result}
    except Exception as e:
        logger.error(f"Error in /batch/{batch_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/drm/{token}")
async def get_drm(token: str):
    try:
        url = await _get_m3u8(token)
        if not url:
            return {"success": False, "message": "No DRM link found for this token"}
        return {"success": True, "data": {"file_url": url}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logger.info(f"🚀 Starting Guidely API server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)

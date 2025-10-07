# -*- coding: utf-8 -*-
"""
Two-Phase PropertyGuru Runner: ADLIST (SRP) -> ADVIEW (Detail)
- Discord dashboard with ADLIST / ADVIEW sections + live phase
- ADLIST: same retry/backoff/final-sweep model as ADVIEW
- End of ADLIST: build CSV (with listing_id), compress+upload, then auto-start ADVIEW
- ADVIEW: rich field extraction (phone, developer, title, bumi, psf, etc.)
- FINAL: ADVIEW CSV merged with ADLIST times & agent_id/listing_id
- Threaded, per-thread MV3 proxy extensions, staggered launches
"""

import os, re, io, time, json, math, gzip, zipfile, random, queue, heapq, threading, logging, shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import requests
except Exception:
    requests = None

# ====================== USER CONFIG ======================
ADLIST_THREADS = 5
ADVIEW_THREADS = 5
VERSION_MAIN = 139
PAGELOAD_TIMEOUT = 45
WAIT_NEXTDATA = 25
THREAD_LAUNCH_DELAY_STEP = 2  # T0=2s, T1=4s, ...

# Category page caps (ADLIST)
CATEGORIES = [
    {"intent": "sale", "segment": "commercial",  "is_commercial": True,  "pages": 200},
    {"intent": "sale", "segment": "residential", "is_commercial": False, "pages": 1000},
    {"intent": "rent", "segment": "commercial",  "is_commercial": True,  "pages": 200},
    {"intent": "rent", "segment": "residential", "is_commercial": False, "pages": 600},
]

# Discord webhooks (reuse your existing 4)
DASHBOARD_WEBHOOK = "https://discord.com/api/webhooks/1405420190652567682/qOKf09vjntEdCRth8A6D9AkUsfPN_oWx5Yjbtz43QCqcZnzARrx_EX_qSwJosc9lhQ-y"
RETRY_WEBHOOK     = "https://discord.com/api/webhooks/1405420193756217394/LvtHVEmX4GjQQrQ_8W0O7MFSoAeaevTPJ0yScmMF4tScfAmrBM3dotWgUZdnjUTl0HFs"
EXHAUSTED_WEBHOOK = "https://discord.com/api/webhooks/1405545971122966549/DxH-c9pKo2J4BJka0FulzB55IdNIFPOQRatAKmg-CL6Il5UUM_xJfoPQafn4-zK_MQ4D"
CSV_WEBHOOK       = "https://discord.com/api/webhooks/1405554758726717573/akd645rjb2bS-GhUbTJei-GMVsWmd9c7FPb-aTL852s9Cc-Zx9Q3SDEdiPLidxyOQH4x"

DASHBOARD_UPDATE_MIN = 10
DASHBOARD_UPDATE_MAX = 20
DASHBOARD_BAR_WIDTH  = 16
# Proxy auth mode: "whitelist" (IP whitelisted → no creds) or "userpass" (use MV3 extension)
PROXY_MODE = "whitelist"   # or "userpass"
SYS_IP_OVERRIDE = "161.142.139.172"


# Proxy pool (old 10 + new 10)
proxies = [
    # --- OLD 10 ---
    # {"server": "isp.decodo.com:10001", "username": "user-spkttgw7rl-ip-92.113.225.103", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10002", "username": "user-spkttgw7rl-ip-92.113.225.238", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10003", "username": "user-spkttgw7rl-ip-92.113.164.122", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10004", "username": "user-spkttgw7rl-ip-92.113.225.147", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10005", "username": "user-spkttgw7rl-ip-92.113.225.123", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10006", "username": "user-spkttgw7rl-ip-92.113.164.255", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10007", "username": "user-spkttgw7rl-ip-92.113.225.56",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10008", "username": "user-spkttgw7rl-ip-92.113.164.39",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10009", "username": "user-spkttgw7rl-ip-92.113.164.98",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10010", "username": "user-spkttgw7rl-ip-92.113.164.195", "password": "718IbwlQ5b~ckmPlqE"},
    # --- NEW 10 (corrected) ---
    {"server": "isp.decodo.com:10001", "username": "user-spqfamqoeg-ip-92.113.225.14",  "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10002", "username": "user-spqfamqoeg-ip-92.113.164.242","password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10003", "username": "user-spqfamqoeg-ip-92.113.164.179","password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10004", "username": "user-spqfamqoeg-ip-92.113.225.53", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10005", "username": "user-spqfamqoeg-ip-92.113.164.79", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10006", "username": "user-spqfamqoeg-ip-92.113.164.163","password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10007", "username": "user-spqfamqoeg-ip-92.113.225.182","password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10008", "username": "user-spqfamqoeg-ip-92.113.164.66", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10009", "username": "user-spqfamqoeg-ip-92.113.164.119","password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10010", "username": "user-spqfamqoeg-ip-92.113.225.175","password": "8e2~wuIbCEshvtu18K"},
]

# ====== Paths & Globals ======
TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
BASE_DIR = os.path.abspath(".")

ADLIST_DIR = os.path.join(BASE_DIR, f"adlist_propertyguru_{TS}")
ADVIEW_DIR = os.path.join(BASE_DIR, f"adview_propertyguru_{TS}")
LOG_DIR    = os.path.join(BASE_DIR, f"logs_{TS}")
EXT_DIR    = os.path.join(LOG_DIR, "proxy_exts")
os.makedirs(ADLIST_DIR, exist_ok=True)
os.makedirs(ADVIEW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(EXT_DIR, exist_ok=True)

AUDIT_DIR  = os.path.join(ADVIEW_DIR, "audit")
os.makedirs(AUDIT_DIR, exist_ok=True)

ADLIST_EXT_ROOT = os.path.join(EXT_DIR, "adlist")
ADVIEW_EXT_ROOT = os.path.join(EXT_DIR, "adview")
os.makedirs(ADLIST_EXT_ROOT, exist_ok=True)
os.makedirs(ADVIEW_EXT_ROOT, exist_ok=True)

# ====== Logging ======
def setup_logging():
    def _prep(name,fname):
        lg = logging.getLogger(name); lg.setLevel(logging.INFO)
        fh = logging.FileHandler(os.path.join(LOG_DIR,fname), encoding="utf-8")
        fh.setFormatter(logging.Formatter('%(asctime)s - Thread%(thread_id)s - %(message)s'))
        lg.addHandler(fh); lg.propagate = False
        return lg
    return (_prep("performance","performance.log"),
            _prep("detection","detection.log"),
            _prep("errors","errors.log"))

perf_logger, detection_logger, error_logger = setup_logging()

# --- IP probes ---
def system_public_ipv4():
    ov = (globals().get("SYS_IP_OVERRIDE") or "").strip()
    if ov:
        return ov
    try:
        if requests:
            return requests.get("https://ipv4.api.ipify.org?format=json", timeout=10).json()["ip"]
    except Exception:
        pass
    return "unknown"

def _browser_ipv4_newtab(driver):
    """Open a temporary tab to read IPv4 (bypasses site CSP)."""
    cur = driver.current_window_handle
    driver.switch_to.new_window('tab')
    try:
        driver.get("https://ipv4.icanhazip.com/")
        ip = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
    finally:
        driver.close()
        driver.switch_to.window(cur)
    return ip

def verify_proxy(driver, label="PG", sys_ip=None, thread_id=0):

    """Return True if browser IP != system IP. Uses CSP-safe fallback."""
    sys_ip = (sys_ip or system_public_ipv4()).strip()
    try:
        # try fast in-page fetch first (works on most neutral pages)
        script = """
        const cb = arguments[0];
        fetch('https://ipv4.api.ipify.org?format=json', {cache:'no-store'})
          .then(r=>r.json()).then(j=>cb({ok:true, ip:j.ip}))
          .catch(e=>cb({ok:false, err:String(e)}));
        """
        res = driver.execute_async_script(script)
        pg_ip = res.get("ip") if res and res.get("ok") else None
    except Exception:
        pg_ip = None

    # Fallback if blocked by CSP or fetch failed
    if not pg_ip:
        pg_ip = _browser_ipv4_newtab(driver)

    msg = f"[IP] {label}: system={sys_ip}  browser={pg_ip}"
    print(msg)
    try: detection_logger.info(msg, extra={'thread_id': thread_id})

    except: pass

    return bool(pg_ip) and (not str(pg_ip).startswith("error")) and (sys_ip != pg_ip)

# ====== Discord Client ======
class DiscordClient:
    def __init__(self, webhook_url:str):
        self.enabled = (requests is not None) and bool(webhook_url)
        self.webhook = webhook_url
        self.msg_id = None
        self.queue = queue.Queue()
        self.sender_thread = None
        self.session = requests.Session() if (self.enabled and requests) else None
        self._stop = threading.Event()

    def start(self):
        if not self.enabled: return
        self.sender_thread = threading.Thread(target=self._run_sender, name="discord_sender", daemon=True)
        self.sender_thread.start()

    def stop(self):
        if not self.enabled: return
        self._stop.set()
        try: self.queue.put_nowait(("__STOP__", None))
        except: pass
        if self.sender_thread: self.sender_thread.join(timeout=5)

    def send_event(self, content:str):
        if not self.enabled: return
        self.queue.put(("event", {"content": content}))

    def send_file(self, file_path: str, content: str = None):
        if not self.enabled:
            print(f"[WEBHOOK] enqueue skipped (disabled): {file_path}")
            return
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
        except Exception:
            size_mb = -1
        print(f"[WEBHOOK] enqueue file: {os.path.basename(file_path)} ({size_mb:.2f} MB); q={self.queue.qsize()+1}")
        self.queue.put(("file", {"path": file_path, "content": content or ""}))


    def set_dashboard(self, content:str):
        if not self.enabled: return
        if self.msg_id is None:
            self.queue.put(("create", {"content": content}))
        else:
            self.queue.put(("edit", {"content": content}))

    def _run_sender(self):
        while not self._stop.is_set():
            try:
                kind, payload = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "__STOP__": break
            try:
                if kind == "create":
                    url = self.webhook + "?wait=true"
                    r = self.session.post(url, json=payload, timeout=15)
                    if r.status_code == 200:
                        data = r.json()
                        self.msg_id = data.get("id")
                elif kind == "edit" and self.msg_id:
                    edit_url = self.webhook + f"/messages/{self.msg_id}"
                    self.session.patch(edit_url, json=payload, timeout=15)
                elif kind == "event":
                    self.session.post(self.webhook, json=payload, timeout=15)
                elif kind == "file":
                    path = payload["path"]; text = payload.get("content", "")
                    try:
                        size = os.path.getsize(path)
                        if size >= 10 * 1024 * 1024:
                            self.session.post(self.webhook, json={"content": f"⚠️ File too large to upload ({size/1024/1024:.2f} MB): {os.path.basename(path)}"}, timeout=15)
                        else:
                            upload_url = self.webhook + "?wait=true"  # ask Discord to return JSON
                            with open(path, "rb") as f:
                                files = {"file": (os.path.basename(path), f, "application/octet-stream")}
                                data = {"content": text or ""}
                                r = self.session.post(upload_url, data=data, files=files, timeout=60)
                            
                            # <-- Add these lines to see what Discord returns
                            print(f"[WEBHOOK] status={r.status_code}")
                            body = r.text[:400] if r.text else ""
                            if r.status_code >= 300:
                                # error path: show the first part of the response body (Discord explains why)
                                print(f"[WEBHOOK] ERROR body: {body}")
                            else:
                                ct = r.headers.get("Content-Type", "")
                                if "application/json" in ct:
                                    try:
                                        msg = r.json()
                                        atts = msg.get("attachments", [])
                                        first = atts[0] if atts else {}
                                        print(
                                            f"[WEBHOOK] OK id={msg.get('id')} "
                                            f"att={len(atts)} "
                                            f"file={first.get('filename','-')} "
                                            f"size={first.get('size','-')}"
                                        )
                                    except Exception as e:
                                        print(f"[WEBHOOK] OK (JSON parse issue): {type(e).__name__}: {e} | body: {body}")
                                else:
                                    print("[WEBHOOK] OK (no JSON body)")

                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                time.sleep(0.3)

# ====== Utilities ======
def text_bar(pct: float, width: int = DASHBOARD_BAR_WIDTH) -> str:
    pct = max(0.0, min(1.0, pct))
    filled = int(round(pct * width))
    return "█" * filled + "░" * (width - filled)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
]
def pick_ua(): return random.choice(user_agents)

def mask_ip(ip:str)->str:
    try:
        parts = ip.split("."); parts[-1] = "xxx"; return ".".join(parts)
    except: return ip

def only_digits(value) -> str:
    if value is None: return ""
    if isinstance(value, (int, float)):
        try: return str(int(value))
        except Exception: return re.sub(r"\D+", "", str(value))
    return re.sub(r"\D+", "", str(value))

def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", str(s)).strip("-")[:120]

# ====== Proxy + Driver ======
# --- IP probes (unified) ---

def build_proxy_ext(proxy_cfg:dict, thread_id:int, root:str)->str:
    host, port_str = proxy_cfg["server"].split(":")
    port = int(port_str)
    ext_dir = os.path.join(root, f"t{thread_id}_{host}_{port}")
    os.makedirs(ext_dir, exist_ok=True)

    manifest = {
        "name": f"ProxyAuth t{thread_id}",
        "version": "1.0",
        "manifest_version": 3,
        "permissions": ["proxy", "webRequest", "webRequestBlocking", "storage"],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"}
    }
    bg_js = f"""
const cfg = {{
  host: "{host}",
  port: {port},
  username: "{proxy_cfg.get('username','')}",
  password: "{proxy_cfg.get('password','')}"
}};

// Set fixed proxy (redundant with CLI, but fine; both point to same server)
function setProxy() {{
  chrome.proxy.settings.set({{
    value: {{
      mode: "fixed_servers",
      rules: {{
        singleProxy: {{ scheme: "http", host: cfg.host, port: cfg.port }},
        bypassList: ["<local>"]
      }}
    }},
    scope: "regular"
  }});
}}
chrome.runtime.onInstalled.addListener(setProxy);
chrome.runtime.onStartup.addListener(setProxy);

// IMPORTANT: MV3 uses asyncBlocking; only answer PROXY auth
chrome.webRequest.onAuthRequired.addListener(
  (details, callback) => {{
    if (details.isProxy && cfg.username && cfg.password) {{
      callback({{ authCredentials: {{ username: cfg.username, password: cfg.password }} }});
    }} else {{
      callback();
    }}
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
"""
    with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as f:
        f.write(bg_js)
    return ext_dir

def start_driver(user_agent:str, proxy_cfg:dict, thread_id:int, ext_root:str):
    opts = uc.ChromeOptions()
    # Force proxy at process start (critical)
    opts.add_argument(f"--proxy-server=http://{proxy_cfg['server']}")
    opts.add_argument("--proxy-bypass-list=<-loopback>")

    # Hardening / perf
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-background-networking")
    opts.page_load_strategy = "eager"
    opts.add_argument(f"user-agent={user_agent}")

    if PROXY_MODE == "userpass":
        ext_path = build_proxy_ext(proxy_cfg, thread_id, ext_root)
        opts.add_argument(f"--disable-extensions-except={ext_path}")
        opts.add_argument(f"--load-extension={ext_path}")
    # else: whitelist mode → no extension needed

    driver = uc.Chrome(options=opts, version_main=VERSION_MAIN)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    return driver


def get_next_data(driver):
    try:
        el = WebDriverWait(driver, WAIT_NEXTDATA).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="__NEXT_DATA__"]'))
        )
        txt = el.get_attribute("innerText") or ""
        return txt if txt.strip().startswith("{") else None
    except TimeoutException:
        return None

def get_proxy_ip(idx:int)->str:
    u = proxies[idx]["username"]
    return u.split("-ip-")[1] if "-ip-" in u else f"proxy_{idx}"

# ====== Stage Container ======
class Stage:
    def __init__(self, name:str, threads:int, ext_root:str):
        self.name = name
        self.threads = threads
        self.ext_root = ext_root

        self.ready_q = queue.Queue()
        self.deferred_q = queue.Queue()
        self.delayed_heap = []
        self.delayed_lock = threading.Lock()
        self.delayed_seq = 0

        self.state_lock = threading.Lock()
        self.in_flight = set()
        self.done_set = set()
        self.deferred_set = set()

        self.used_proxies = set()
        self.assigned_proxy = {}
        self.initial_proxy_indices = set()

        self.thread_stats = {}
        self.thread_stats_lock = threading.Lock()

        self.metrics = {"total": 0, "completed": 0, "ok": 0, "retried": 0, "deferred": 0, "final_exhausted": 0, "start_ts": time.time()}
        self.overall_bar = None
        self.thread_bars = {}

    def assign_initial_proxy(self, thread_id:int, exclude:set|None=None)->int:
        preferred = thread_id % len(proxies)
        candidates = list(range(len(proxies)))
        if exclude:
            candidates = [i for i in candidates if i not in exclude] or list(range(len(proxies)))
        with self.state_lock:
            idx = None
            if preferred in candidates and preferred not in self.used_proxies:
                idx = preferred
            else:
                for i in candidates:
                    if i not in self.used_proxies:
                        idx = i; break
                if idx is None:
                    idx = preferred
            self.used_proxies.add(idx)
            self.assigned_proxy[thread_id] = idx
            self.initial_proxy_indices.add(idx)
            return idx

    def rotate_proxy_for_thread(self, thread_id:int, current_idx:int)->int:
        with self.state_lock:
            self.used_proxies.discard(current_idx)
            idx = None
            for i in range(len(proxies)):
                if i != current_idx and i not in self.used_proxies:
                    idx = i; break
            if idx is None:
                idx = (current_idx + 1) % len(proxies)
                if idx == current_idx: idx = (idx + 1) % len(proxies)
            self.used_proxies.add(idx)
            self.assigned_proxy[thread_id] = idx
            return idx

    def release_proxy(self, idx:int):
        with self.state_lock:
            self.used_proxies.discard(idx)

    def schedule_retry(self, task:dict, seconds:int):
        ready_at = time.time() + seconds
        with self.delayed_lock:
            self.delayed_seq += 1
            heapq.heappush(self.delayed_heap, (ready_at, self.delayed_seq, task))

    def pop_due_delayed(self, to_ready_max:int=100):
        moved = 0
        now = time.time()
        while True:
            with self.delayed_lock:
                if self.delayed_heap and self.delayed_heap[0][0] <= now and moved < to_ready_max:
                    _, _, task = heapq.heappop(self.delayed_heap)
                else:
                    break
            self.ready_q.put(task); moved += 1
        return moved

# ====== ADLIST specifics ======
def build_adlist_url(intent:str, is_commercial:bool, page:int)->str:
    base = "https://www.propertyguru.com.my/property-for-sale" if intent == "sale" else "https://www.propertyguru.com.my/property-for-rent"
    return f"{base}?isCommercial={'true' if is_commercial else 'false'}&sort=date&order=desc&page={page}"

def extract_adlist_rows_from_nextdata(text:str, intent:str, segment:str, page_no:int):
    rows = []
    try:
        data = json.loads(text)
        listings = data["props"]["pageProps"]["pageData"]["data"]["listingsData"]
    except Exception:
        return rows
    for item in listings:
        ld = (item.get("listingData") or {}) if isinstance(item, dict) else {}
        url = ld.get("url") or ""
        title = ld.get("localizedTitle") or (ld.get("property", {}) or {}).get("typeText") or ""
        posted = ld.get("postedOn") or item.get("postedOn") or {}
        listed_unix = None
        if isinstance(posted, dict):
            try: listed_unix = int(posted.get("unix"))
            except Exception: listed_unix = None
        agent = ld.get("agent") or {}
        agent_name = agent.get("name") if isinstance(agent, dict) else None
        agent_id   = agent.get("id")   if isinstance(agent, dict) else None
        ad_id = ld.get("id") or ld.get("listingId") or item.get("id") or None
        rows.append({
            "intent": intent, "segment": segment, "url": url, "title": title,
            "listed_unix": listed_unix, "agent_name": agent_name, "agent_id": agent_id,
            "ad_id": ad_id, "page_no": page_no
        })
    return rows

# ====== ADVIEW rich extraction ======
DOMAIN = "https://www.propertyguru.com.my"

def get_data_root(j):
    return j.get("props",{}).get("pageProps",{}).get("pageData",{}).get("data",{})

def get_by_path(d, dotted):
    cur = d
    for tok in dotted.split("."):
        if isinstance(cur, dict) and tok in cur:
            cur = cur[tok]
        elif isinstance(cur, list) and tok.isdigit():
            i = int(tok)
            if 0 <= i < len(cur):
                cur = cur[i]
            else:
                return None
        else:
            return None
    return cur

def pick_first(d, paths):
    for p in paths:
        v = get_by_path(d, p)
        if v not in (None, "", []):
            return v
    return ""

def digits_only(x):
    if x in (None, "", []): return ""
    return "".join(re.findall(r"\d+", str(x)))

def make_abs(u):
    if not isinstance(u, str) or not u:
        return ""
    return u if u.startswith("http") else (DOMAIN + u)

def last_token(address):
    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    return parts[-1] if parts else ""

def build_amenities(property_info):
    am = (property_info or {}).get("amenities", [])
    if isinstance(am, list) and am:
        out = []
        for item in am:
            if not isinstance(item, dict): 
                continue
            unit = str(item.get("unit","")).strip()
            val  = str(item.get("value","")).strip()
            if unit and val:
                if unit.lower() in ("sqft","sf"):
                    out.append(f"{val} {unit}")
                else:
                    out.append(f"{unit} {val}")
        return "; ".join(out)
    return ""

def build_facilities(data):
    fac = (data or {}).get("facilitiesData", {})
    if isinstance(fac, dict):
        items = fac.get("data", [])
        if isinstance(items, list):
            texts = [x.get("text","").strip() for x in items if isinstance(x, dict) and x.get("text")]
            return ", ".join([t for t in texts if t])
    return ""

def map_tenure(code):
    if not code: return ""
    up = str(code).strip().upper()
    return {"F":"Freehold","L":"Leasehold"}.get(up, str(code))

# Regexes for details scan
R_BUMI        = re.compile(r"\b(?:Not\s+)?Bumi\s+Lot\b", re.I)
R_TITLE       = re.compile(r"\b(Individual|Strata|Master)\s+title\b", re.I)
R_DEV         = re.compile(r"^Developed by\s+(.+)$", re.I)
R_COMPLETE_YR = re.compile(r"\b(Completed|Completion)\s+in\s+(\d{4})\b", re.I)
R_FLOOR       = re.compile(r"([\d,\.]+)\s*(sqft|sf)\s*floor\s*area\b", re.I)
R_LAND        = re.compile(r"([\d,\.]+)\s*(sqft|sf)\s*land\s*area\b", re.I)
R_PSF         = re.compile(r"\bRM\s*([\d\.,]+)\s*psf\b", re.I)
R_TENURE_TXT  = re.compile(r"\b(Freehold|Leasehold)\s+tenure\b", re.I)

# ---- Furnishing (STRICT, structural-only) ----
FURNISH_PATHS_STRICT = [
    "propertyOverviewData.propertyInfo.furnishing",
    "listingData.property.furnishing",
    "listingData.furnishing",
    "listingDetail.attributes.furnishing",  # React-Flight
]

def normalize_furnishing(s: str) -> str:
    if not isinstance(s, str): return ""
    t = s.strip().lower()
    # strong negatives / bare
    if t in {"bare", "unfurnished", "not furnished", "non furnished", "no furnishing"}:
        return "Unfurnished"
    if t in {"partly furnished", "partially furnished", "semi furnished", "semi-furnished"}:
        return "Partially Furnished"
    if t in {"fully furnished", "furnished"}:
        return "Fully Furnished"
    return ""  # unknown → reject, don't guess

def furnishing_from_metatable(dd: dict) -> str:
    meta = (dd.get("detailsData") or {}).get("metatable") or {}
    for it in (meta.get("items") or []):
        if isinstance(it, dict) and it.get("icon") == "furnished-o":
            return normalize_furnishing(it.get("value",""))
    return ""

def furnishing_from_labeled_items(dd: dict) -> str:
    # Scan ONLY structured 'details' style tables for a label starting with "Furnish"
    def iter_items(node):
        if isinstance(node, dict):
            items = node.get("items")
            if isinstance(items, list):
                for x in items:
                    if isinstance(x, dict): yield x
            for v in node.values():
                yield from iter_items(v)
        elif isinstance(node, list):
            for v in node:
                yield from iter_items(v)

    scope = (dd.get("detailsData") or {})  # keep scope tight
    for it in iter_items(scope):
        label = str(it.get("label") or it.get("name") or it.get("title") or "").strip()
        val   = str(it.get("value") or it.get("text") or "").strip()
        if label and val and label.lower().startswith("furnish"):
            v = normalize_furnishing(val)
            if v: return v
    return ""

def extract_furnishing(dd: dict) -> tuple[str, str]:
    # 1) metatable icon
    v = furnishing_from_metatable(dd)
    if v: return v, "detailsData.metatable(icon=furnished-o)"
    # 2) strict key paths
    for p in FURNISH_PATHS_STRICT:
        raw = get_by_path(dd, p) if isinstance(dd, dict) else None
        v = normalize_furnishing(raw if isinstance(raw, str) else "")
        if v: return v, p
    # 3) labeled items in details only
    v = furnishing_from_labeled_items(dd)
    if v: return v, "detailsData.labeled"
    # 4) give up
    return "", ""


def iter_detail_strings(node):
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, dict) and "items" in v and isinstance(v["items"], list):
                for it in v["items"]:
                    if isinstance(it, dict):
                        for key in ("value","text","label","name"):
                            s = it.get(key)
                            if isinstance(s, str) and s.strip():
                                yield s.strip()
            elif isinstance(v, list) and ("detail" in k.lower() or "item" in k.lower()):
                for it in v:
                    if isinstance(it, dict):
                        for key in ("value","text","label","name"):
                            s = it.get(key)
                            if isinstance(s, str) and s.strip():
                                yield s.strip()
            yield from iter_detail_strings(v)
    elif isinstance(node, list):
        for it in node: yield from iter_detail_strings(it)

def fill_from_details(strings, seed):
    for v in strings:
        # if not seed["furnishing"] and R_FURN.search(v): seed["furnishing"] = v.strip().title()
        if not seed["property_title"] and (m:=R_TITLE.search(v)): seed["property_title"] = m.group(0).title()
        if not seed["bumi_lot"] and (m:=R_BUMI.search(v)): seed["bumi_lot"] = "Not Bumi Lot" if "Not" in m.group(0) else "Bumi Lot"
        if not seed["developer"] and (m:=R_DEV.search(v)): seed["developer"] = m.group(1).strip()
        if not seed["completion_year"] and (m:=R_COMPLETE_YR.search(v)): seed["completion_year"] = m.group(2)
        if not seed["build_up"] and (m:=R_FLOOR.search(v)): seed["build_up"] = digits_only(m.group(1))
        if not seed["land_area"] and (m:=R_LAND.search(v)): seed["land_area"] = digits_only(m.group(1))
        if not seed["price_per_square_feet"] and (m:=R_PSF.search(v)): seed["price_per_square_feet"] = digits_only(m.group(1))
        if not seed["tenure"] and (m:=R_TENURE_TXT.search(v)): seed["tenure"] = m.group(1).title()
    return seed

# ---- Money & State helpers ----
MALAYSIAN_STATES = {
    "Johor","Kedah","Kelantan","Melaka","Negeri Sembilan","Pahang","Perak","Perlis",
    "Pulau Pinang","Penang","Sabah","Sarawak","Selangor","Terengganu",
    "Kuala Lumpur","W.P. Kuala Lumpur","Putrajaya","Labuan"
}
STATE_SYNONYMS = {
    "Penang": "Pulau Pinang",
    "W.P. Kuala Lumpur": "Kuala Lumpur",
}

def parse_money_value(v) -> str:
    """
    Robust money parser that preserves decimals correctly, but outputs an integer MYR string.
    Prefers numeric inputs; only parses strings as a fallback.
    """
    if v in (None, "", "-"):
        return ""
    # numeric straight-through
    if isinstance(v, (int, float)):
        return str(int(round(float(v))))
    # string fallback: "RM 1,234,567.00" / "1,234,567.50"
    s = str(v)
    m = re.search(r'(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d+))?', s)
    if not m:
        return ""
    whole = m.group(1).replace(",", "")
    dec   = m.group(2) or ""
    if dec:
        return str(int(round(float(f"{whole}.{dec}"))))
    return whole

def find_state_in_address(address: str) -> str:
    """
    Pick the first known Malaysian state appearing anywhere in the address.
    Normalizes synonyms (e.g., Penang -> Pulau Pinang).
    """
    if not isinstance(address, str) or not address.strip():
        return ""
    # match full state names as whole words, case-insensitive
    for st in MALAYSIAN_STATES:
        if re.search(rf'\b{re.escape(st)}\b', address, re.I):
            canon = STATE_SYNONYMS.get(st, st)
            return canon
    # also try synonyms themselves if the canonical form wasn’t present
    for syn, canon in STATE_SYNONYMS.items():
        if re.search(rf'\b{re.escape(syn)}\b', address, re.I):
            return canon
    return ""


# Candidate paths
URL_PATHS   = ["listingData.url"]
TITLE_PATHS = ["listingData.localizedTitle", "listingData.title"]
PROPERTY_TYPE_PATHS = ["propertyOverviewData.propertyInfo.propertyType","listingData.propertyType","listingData.property.typeText","listingData.property.type"]
ADDRESS_PATHS = ["propertyOverviewData.propertyInfo.fullAddress","listingData.displayAddress","listingData.address","listingData.property.addressText"]
STATE_PATHS   = ["propertyOverviewData.propertyInfo.stateName","listingData.property.stateName","listingData.stateName"]
DISTRICT_PATHS= ["propertyOverviewData.propertyInfo.districtName","listingData.property.districtName","listingData.districtName","listingData.districtText"]
SUBAREA_PATHS = ["propertyOverviewData.propertyInfo.areaName","listingData.property.areaName","listingData.areaName","listingData.areaText"]
LISTER_NAME_PATHS = ["contactAgentData.contactAgentCard.agentInfoProps.agent.name","listingData.agent.name"]
LISTER_URL_PATHS  = ["contactAgentData.contactAgentCard.agentInfoProps.agent.profileUrl","listingData.agent.profileUrl","listingData.agent.url"]
PHONE_PATHS = ["contactAgentData.contactAgentCard.agentInfoProps.agent.mobile","listingData.agent.contactNumbers.0.number","listingData.agent.contactNumbers.0.displayNumber","listingData.agent.phoneNumber","listingData.agent.mobile","listingData.agent.contactNumber"]
AGENCY_NAME_PATHS = ["contactAgentData.contactAgentCard.agency.name","listingData.agent.agency.name","listingData.agent.agencyName"]
AGENCY_REG_PATHS  = ["contactAgentData.contactAgentCard.agency.registrationNumber","contactAgentData.contactAgentCard.agency.licenseNo","listingData.agent.agency.registrationNumber","listingData.agent.agency.registrationNo","listingData.agent.agency.regNo"]
REN_PATHS = ["listingData.agent.licenseNumber","listingData.agent.renNo","listingData.agent.registrationNo","listingData.agent.ren","contactAgentData.contactAgentCard.agentInfoProps.agent.licenseNumber"]
PRICE_PATHS = ["propertyOverviewData.propertyInfo.price.amount","listingData.priceValue","listingData.pricePretty","listingData.price"]
ROOMS_PATHS = ["propertyOverviewData.propertyInfo.bedrooms","listingData.property.bedrooms","listingData.bedrooms"]
TOILETS_PATHS = ["propertyOverviewData.propertyInfo.bathrooms","listingData.property.bathrooms","listingData.bathrooms"]
PSF_PATHS = ["propertyOverviewData.propertyInfo.price.perSqft","propertyOverviewData.propertyInfo.pricePerSqft","listingData.floorAreaPsf"]
# FURNISHING_PATHS = ["propertyOverviewData.propertyInfo.furnishing","listingData.property.furnishing","listingData.furnishing"]
FLOOR_AREA_PATHS = ["propertyOverviewData.propertyInfo.builtUp.size","propertyOverviewData.propertyInfo.builtUpSqft","listingData.floorArea","listingData.property.builtUpArea"]
LAND_AREA_PATHS  = ["propertyOverviewData.propertyInfo.landArea.size","propertyOverviewData.propertyInfo.landAreaSqft","listingData.landArea","listingData.property.landArea"]
TENURE_PATHS = ["propertyOverviewData.propertyInfo.tenure","listingData.property.tenure","listingData.tenure"]
PROPERTY_TITLE_PATHS = ["propertyOverviewData.propertyInfo.titleType","listingData.property.titleType","listingData.property.title"]
BUMI_PATHS = ["propertyOverviewData.propertyInfo.bumiLot","listingData.property.bumiLot"]
TOTAL_UNITS_PATHS = ["propertyOverviewData.propertyInfo.totalUnits","listingData.property.totalUnits"]
COMPLETION_YEAR_PATHS = ["propertyOverviewData.propertyInfo.completedYear","propertyOverviewData.propertyInfo.completionYear","listingData.property.completedYear","listingData.property.yearBuilt"]
DEVELOPER_PATHS = ["propertyOverviewData.propertyInfo.developer","listingData.property.developer"]

# ====== Dashboard Builder ======
def build_dashboard_text(adlist: Stage, adview: Stage, phase:str) -> str:
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M') + " MYT"

    adlist_total = adlist.metrics["total"]; adview_total = adview.metrics["total"]
    adlist_done  = adlist.metrics["completed"]; adview_done = adview.metrics["completed"]

    if adview_total > 0:
        overall_total = adlist_total + adview_total
        overall_done  = adlist_done + adview_done
    else:
        overall_total = max(1, adlist_total)
        overall_done  = adlist_done
    overall_pct = overall_done / overall_total if overall_total else 0.0
    overall_bar = text_bar(overall_pct, DASHBOARD_BAR_WIDTH)

    def sec_fmt(sec:float)->str:
        if sec <= 0: return "--"
        return f"{int(sec//3600)}h{int((sec%3600)//60):02d}m"

    def stage_line(stage: Stage, label:str):
        total = stage.metrics["total"]; comp  = stage.metrics["completed"]
        ok    = stage.metrics["ok"];    retr  = stage.metrics["retried"]
        deff  = stage.metrics["deferred"]; errs  = stage.metrics["final_exhausted"]
        elapsed = max(1.0, time.time() - stage.metrics["start_ts"])
        rate = comp / elapsed
        remaining = max(0, total - comp)
        eta = sec_fmt(remaining / rate if rate > 0 else 0)
        pct = (comp / total) if total else 0.0
        bar = text_bar(pct, DASHBOARD_BAR_WIDTH)
        return f"{label}  [{bar}] {int(pct*100)}% • {comp:,}/{total:,} • ok={ok:,} • retried={retr:,} • deferred={deff:,} • errors={errs:,} • {rate:.2f} u/s • ETA {eta}"

    lines = []
    lines.append(f"🏗️ PropertyGuru Multi-Phase — Run {now_str}")
    lines.append("")
    lines.append(f"Overall [{overall_bar}] {int(overall_pct*100)}% • {overall_done:,}/{overall_total:,}")
    lines.append(f"Phase: {phase}")
    lines.append("")
    lines.append(stage_line(adlist, "ADLIST"))
    lines.append(stage_line(adview, "ADVIEW" if adview.metrics["total"] > 0 else "ADVIEW (Pending)"))
    lines.append("")
    active = adlist if phase == "ADLIST" else adview
    fair = math.ceil(max(1, active.metrics["total"]) / max(1, active.threads))
    with active.thread_stats_lock:
        for tid in range(active.threads):
            st = active.thread_stats.get(tid, {"done":0,"state":"init","proxy":"-"})
            pct = (st["done"] / fair) if fair else 0.0
            tbar = text_bar(pct, 12)
            prefix = "L" if active.name.startswith("ADLIST") else "V"
            lines.append(f"{prefix}{tid}  [{tbar}] {int(pct*100)}% • {st['done']:,}/{fair:,} • {st.get('state','')} • {st.get('proxy','-')}")
    return "\n".join(lines)

# ====== Background loops ======
def dispatcher_loop(stage: Stage, stop_event: threading.Event):
    while not stop_event.is_set():
        stage.pop_due_delayed(100)
        time.sleep(0.5)

def dashboard_loop(stop_event: threading.Event, dashboard_bot: DiscordClient, adlist: Stage, adview: Stage, get_phase):
    last_pct = -1
    while not stop_event.is_set():
        try:
            phase = get_phase()
            text = build_dashboard_text(adlist, adview, phase)
            adlist_pct = (adlist.metrics["completed"] / adlist.metrics["total"]) if adlist.metrics["total"] else 0.0
            adview_pct = (adview.metrics["completed"] / adview.metrics["total"]) if adview.metrics["total"] else 0.0
            overall_pct = (adlist_pct + adview_pct) / (1 if adview.metrics["total"] == 0 else 2)
            significant = (int(overall_pct*100) != int(last_pct*100))
            if significant and dashboard_bot.enabled:
                dashboard_bot.set_dashboard(text)
                last_pct = overall_pct
            time.sleep(random.uniform(DASHBOARD_UPDATE_MIN, DASHBOARD_UPDATE_MAX))
            if dashboard_bot.enabled:
                dashboard_bot.set_dashboard(text)
        except Exception:
            time.sleep(2)

# ====== ADLIST Worker ======
def adlist_worker(thread_id:int, stage: Stage, retry_bot: DiscordClient, exhausted_bot: DiscordClient):
    time.sleep((thread_id + 1) * THREAD_LAUNCH_DELAY_STEP)

    ua = pick_ua()
    proxy_idx = stage.assign_initial_proxy(thread_id, exclude=None)
    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
    
    # right after: driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
    sys_ip = system_public_ipv4()
    print(f"[IP] system IPv4      : {sys_ip}")
    
    # check via a neutral page (optional)
    driver.get("https://ipv4.icanhazip.com")
    time.sleep(1.2)
    print(f"[IP] browser IPv4 (nav): {(driver.find_element(By.TAG_NAME,'body').text or '').strip()}")
    prox_ok = verify_proxy(driver, label=f"ADLIST T{thread_id}", sys_ip=sys_ip, thread_id=thread_id)
    if not prox_ok:
        print(f"[IP] T{thread_id} ❌ Proxy not in effect, rotating…")
        try:
            driver.quit()
        except Exception:
            pass
        old_idx = proxy_idx
        proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
        ua = pick_ua()
        driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
        # re-check once after restart
        verify_proxy(driver, label=f"ADLIST T{thread_id} (recheck)", sys_ip=sys_ip, thread_id=thread_id)

    # now confirm **while on PG**
    pg_url = "https://www.propertyguru.com.my/property-for-sale?isCommercial=false&sort=date&order=desc&page=1"
    driver.get(pg_url)
    
    # replace the old pg_ip / quick verdict block with this:
    pg_ok = verify_proxy(driver, label=f"{stage.name} T{thread_id} (PG)", sys_ip=sys_ip)
    print("[IP] ✅ Proxy in effect for this PG page." if pg_ok else
          "[IP] ❌ Looks like direct IP (or probe failed). Check whitelisting / --proxy-server.")

    proxy_ip = mask_ip(get_proxy_ip(proxy_idx))

    with stage.thread_stats_lock:
        stage.thread_stats[thread_id] = {"done": 0, "state": "OK", "proxy": proxy_ip}

    try:
        while True:
            try:
                task = stage.ready_q.get(timeout=1.0)
            except queue.Empty:
                with stage.state_lock, stage.delayed_lock:
                    nothing_left = stage.ready_q.qsize() == 0 and len(stage.delayed_heap) == 0 and len(stage.in_flight) == 0
                    deferred_empty = stage.deferred_q.qsize() == 0
                if nothing_left and deferred_empty:
                    break
                else:
                    if nothing_left and not deferred_empty:
                        try:
                            while True:
                                t2 = stage.deferred_q.get_nowait()
                                stage.ready_q.put(t2)
                        except queue.Empty:
                            pass
                    continue

            intent  = task["intent"]; segment = task["segment"]; is_com  = task["is_commercial"]
            page_no = task["page"];   attempt = task.get("attempt", 1)
            url = build_adlist_url(intent, is_com, page_no)

            with stage.state_lock:
                key = (intent, segment, page_no)
                if key in stage.done_set or key in stage.in_flight:
                    continue
                stage.in_flight.add(key)

            try:
                driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
                driver.get(url)
                text = get_next_data(driver)
                if not text:
                    detection_logger.info(f"[ADLIST] NEXT_DATA missing {url}", extra={'thread_id': thread_id})
                    raise TimeoutException("NEXT_DATA missing")

                out_name = f"{intent}_{segment}_page_{page_no}.json"
                out_path = os.path.join(ADLIST_DIR, out_name)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(text)

                rows = extract_adlist_rows_from_nextdata(text, intent, segment, page_no)
                scrape_unix = int(time.time())
                for r in rows:
                    r["scrape_unix"] = scrape_unix
                if not hasattr(stage, "adlist_rows"):
                    stage.adlist_rows = []; stage.adlist_rows_lock = threading.Lock()
                with stage.adlist_rows_lock:
                    stage.adlist_rows.extend(rows)

                with stage.state_lock:
                    stage.metrics["ok"] += 1; stage.metrics["completed"] += 1; stage.done_set.add(key)
                with stage.thread_stats_lock:
                    st = stage.thread_stats[thread_id]; st["done"] += 1; st["state"] = "OK"

                if stage.overall_bar is not None: stage.overall_bar.update(1)
                if stage.thread_bars.get(thread_id): stage.thread_bars[thread_id].update(1)
                time.sleep(random.uniform(1.6, 3.2))

            except Exception as e:
                err_msg = str(e)[:180]
                error_logger.error(f"[ADLIST] {url} err: {err_msg}", extra={'thread_id': thread_id})

                old_mask = proxy_ip
                try:
                    try: driver.quit()
                    except: pass
                    proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
                    proxy_ip  = mask_ip(get_proxy_ip(proxy_idx))
                    ua = pick_ua()
                    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["state"] = "Restarted"; stage.thread_stats[thread_id]["proxy"] = proxy_ip
                except Exception:
                    pass

                if attempt == 1:
                    backoff = int(random.uniform(60, 180))
                    with stage.state_lock: stage.metrics["retried"] += 1
                    if retry_bot.enabled:
                        retry_bot.send_event(
                            f"🔁 Retry A • ADLIST • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                            f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (2/3)"
                        )
                    task["attempt"] = 2; stage.schedule_retry(task, backoff)
                elif attempt == 2:
                    backoff = int(random.uniform(600, 780))
                    with stage.state_lock: stage.metrics["retried"] += 1
                    if retry_bot.enabled:
                        retry_bot.send_event(
                            f"🔁 Retry B • ADLIST • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                            f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (3/3)"
                        )
                    task["attempt"] = 3; stage.schedule_retry(task, backoff)
                else:
                    with stage.state_lock: stage.metrics["deferred"] += 1
                    if (intent,segment,page_no) not in stage.deferred_set:
                        stage.deferred_set.add((intent,segment,page_no))
                        task2 = dict(task); stage.deferred_q.put(task2)

            finally:
                with stage.state_lock:
                    key2 = (intent, segment, page_no)
                    if key2 in stage.in_flight: stage.in_flight.discard(key2)

    finally:
        try: driver.quit()
        except: pass
        stage.release_proxy(proxy_idx)
        with stage.thread_stats_lock:
            stage.thread_stats[thread_id]["state"] = "finished"

# ====== ADVIEW Worker ======
SUCCESS_F = os.path.join(AUDIT_DIR, "successes.ndjson")
DEFER_F   = os.path.join(AUDIT_DIR, "deferred.ndjson")
FAIL_F    = os.path.join(AUDIT_DIR, "failures_exhausted.ndjson")
audit_lock = threading.Lock()

def audit_append(path:str, obj:dict):
    try:
        with audit_lock, open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass

def adview_worker(thread_id:int, stage: Stage, retry_bot: DiscordClient, exhausted_bot: DiscordClient):
    time.sleep((thread_id + 1) * THREAD_LAUNCH_DELAY_STEP)

    ua = pick_ua()
    proxy_idx = stage.assign_initial_proxy(thread_id, exclude=None)
    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADVIEW_EXT_ROOT)
    # right after: driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
    sys_ip = system_public_ipv4()
    print(f"[IP] system IPv4      : {sys_ip}")
    
    # check via a neutral page (optional)
    driver.get("https://ipv4.icanhazip.com")
    time.sleep(1.2)
    print(f"[IP] browser IPv4 (nav): {(driver.find_element(By.TAG_NAME,'body').text or '').strip()}")
    
    prox_ok = verify_proxy(driver, label=f"ADVIEW T{thread_id}", sys_ip=sys_ip, thread_id=thread_id)
    if not prox_ok:
        print(f"[IP] T{thread_id} ❌ Proxy not in effect, rotating…")
        try:
            driver.quit()
        except Exception:
            pass
        old_idx = proxy_idx
        proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
        ua = pick_ua()
        driver = start_driver(ua, proxies[proxy_idx], thread_id, ADVIEW_EXT_ROOT)
        # re-check once after restart
        verify_proxy(driver, label=f"ADVIEW T{thread_id} (recheck)", sys_ip=sys_ip, thread_id=thread_id)
    
    # now confirm **while on PG**
    # now confirm **while on PG**
    pg_url = "https://www.propertyguru.com.my/property-for-sale?isCommercial=false&sort=date&order=desc&page=1"
    driver.get(pg_url)
    
    # replace the old pg_ip / quick verdict block with this:
    pg_ok = verify_proxy(driver, label=f"{stage.name} T{thread_id} (PG)", sys_ip=sys_ip, thread_id=thread_id)

    print("[IP] ✅ Proxy in effect for this PG page." if pg_ok else
          "[IP] ❌ Looks like direct IP (or probe failed). Check whitelisting / --proxy-server.")

    proxy_ip = mask_ip(get_proxy_ip(proxy_idx))

    with stage.thread_stats_lock:
        stage.thread_stats[thread_id] = {"done": 0, "state": "OK", "proxy": proxy_ip}

    try:
        while True:
            try:
                task = stage.ready_q.get(timeout=1.0)
            except queue.Empty:
                with stage.state_lock, stage.delayed_lock:
                    nothing_left = stage.ready_q.qsize() == 0 and len(stage.delayed_heap) == 0 and len(stage.in_flight) == 0
                    deferred_empty = stage.deferred_q.qsize() == 0
                if nothing_left and deferred_empty:
                    break
                else:
                    if nothing_left and not deferred_empty:
                        try:
                            while True:
                                t2 = stage.deferred_q.get_nowait()
                                stage.ready_q.put(t2)
                        except queue.Empty:
                            pass
                    continue

            url      = task["url"]
            intent   = task.get("intent","unknown")
            segment  = task.get("segment","unknown")
            ad_id_in = task.get("ad_id")
            attempt  = task.get("attempt", 1)
            in_final = task.get("phase") == "Final"

            with stage.state_lock:
                if url in stage.done_set or url in stage.in_flight:
                    continue
                stage.in_flight.add(url)

            try:
                driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
                driver.get(url)
                text = get_next_data(driver)
                if not text:
                    detection_logger.info(f"[ADVIEW] NEXT_DATA missing {url}", extra={'thread_id': thread_id})
                    raise TimeoutException("NEXT_DATA missing")

                # Save raw JSON
                try:
                    data = json.loads(text)
                except Exception:
                    data = {}
                dd = get_data_root(data)
                ad_id = (dd.get("listingData") or {}).get("id") or (dd.get("listingData") or {}).get("listingId") or ad_id_in
                raw_name = f"adview_{safe_name(intent)}_{safe_name(segment)}_{safe_name(ad_id or url)}.json"
                with open(os.path.join(ADVIEW_DIR, raw_name), "w", encoding="utf-8") as f:
                    f.write(text)

                # ---- Rich field extraction ----
                ld = dd.get("listingData", {}) or {}
                property_info = (dd.get("propertyOverviewData", {}) or {}).get("propertyInfo", {}) or {}
                row = {}
                row["url"]   = make_abs(pick_first(dd, URL_PATHS)) or url
                row["ad_id"] = ad_id
                row["title"] = (pick_first(dd, TITLE_PATHS) or (ld.get("property") or {}).get("typeText") or "")

                address = pick_first(dd, ADDRESS_PATHS)
                state   = pick_first(dd, STATE_PATHS)
                if not state:
                    state = find_state_in_address(address)  # safer than last_token()
                district= pick_first(dd, DISTRICT_PATHS)
                subarea = pick_first(dd, SUBAREA_PATHS)

                row["property_type"] = pick_first(dd, PROPERTY_TYPE_PATHS) or ""
                row["address"] = address or ""
                row["state"], row["subregion"], row["subarea"] = state or "", district or "", subarea or ""

                # Location (robust, combined)
                if address and state and district:
                    row["location"] = f"{subarea+', ' if subarea else ''}{district}, {state}"
                else:
                    parts = [p for p in [subarea, district, state] if p]
                    row["location"] = ", ".join(parts) if parts else address or ""

                # Lister & agency
                row["lister"] = pick_first(dd, LISTER_NAME_PATHS) or ""
                row["lister_url"]  = make_abs(pick_first(dd, LISTER_URL_PATHS)) or ""
                row["phone_number"]= str(pick_first(dd, PHONE_PATHS) or "")
                row["agency"]      = pick_first(dd, AGENCY_NAME_PATHS) or ""
                row["agency_registration_number"] = pick_first(dd, AGENCY_REG_PATHS) or ""
                row["ren"]  = str(pick_first(dd, REN_PATHS) or "")

                # Price & core numbers
                # Prefer numeric fields first; only parse pretty as a fallback
                price_raw = pick_first(dd, ["listingData.price", "propertyOverviewData.propertyInfo.price.amount"])
                if price_raw in (None, "", "-"):
                    price_raw = pick_first(dd, ["listingData.priceValue"])  # sometimes numeric
                if price_raw in (None, "", "-"):
                    price_raw = pick_first(dd, ["listingData.pricePretty", "listingData.price"])  # pretty text fallback
                
                row["price"] = parse_money_value(price_raw)

                row["rooms"] = pick_first(dd, ROOMS_PATHS) or ""
                row["toilets"] = pick_first(dd, TOILETS_PATHS) or ""
                psf_raw = pick_first(dd, PSF_PATHS)
                row["price_per_square_feet"] = digits_only(psf_raw) if psf_raw != "" else ""

                row["furnishing"], furn_src = extract_furnishing(dd)
                row["build_up"] = digits_only(pick_first(dd, FLOOR_AREA_PATHS))
                row["land_area"]  = digits_only(pick_first(dd, LAND_AREA_PATHS))
                row["tenure"] = map_tenure(pick_first(dd, TENURE_PATHS))
                row["property_title"] = pick_first(dd, PROPERTY_TITLE_PATHS) or ""
                row["bumi_lot"] = pick_first(dd, BUMI_PATHS) or ""
                row["total_units"] = pick_first(dd, TOTAL_UNITS_PATHS) or ""
                row["completion_year"] = pick_first(dd, COMPLETION_YEAR_PATHS) or ""
                row["developer"] = pick_first(dd, DEVELOPER_PATHS) or ""

                # Fill blanks from modal-style details
                seed = {
                    "property_title": row["property_title"], "bumi_lot": row["bumi_lot"],
                    "developer": row["developer"], "completion_year": digits_only(row["completion_year"]),
                    "build_up": row["build_up"], "land_area": row["land_area"],
                    "price_per_square_feet": row["price_per_square_feet"], "tenure": row["tenure"],
                    "furnishing": row.get("furnishing", "")
                }
                seed = fill_from_details(iter_detail_strings(dd), seed)
                row["furnishing"] = seed["furnishing"] or row["furnishing"]
                row["property_title"] = seed["property_title"] or row["property_title"]
                row["bumi_lot"] = seed["bumi_lot"] or row["bumi_lot"]
                row["developer"] = seed["developer"] or row["developer"]
                row["completion_year"] = seed["completion_year"] or digits_only(row["completion_year"])
                row["build_up"] = seed["build_up"] or row["build_up"]
                row["land_area"]  = seed["land_area"] or row["land_area"]
                row["price_per_square_feet"] = seed["price_per_square_feet"] or row["price_per_square_feet"]
                row["tenure"] = seed["tenure"] or row["tenure"]
                
                row["amenities"]  = build_amenities(property_info)
                row["facilities"] = build_facilities(dd)

                row["scrape_unix"] = int(time.time())

                if not hasattr(stage, "adview_rows"):
                    stage.adview_rows = []; stage.adview_rows_lock = threading.Lock()
                with stage.adview_rows_lock:
                    stage.adview_rows.append(row)

                perf_logger.info(f"[ADVIEW] OK {url}", extra={'thread_id': thread_id})

                with stage.state_lock:
                    stage.metrics["ok"] += 1; stage.metrics["completed"] += 1; stage.done_set.add(url)
                with stage.thread_stats_lock:
                    st = stage.thread_stats[thread_id]; st["done"] += 1; st["state"] = "OK"

                if stage.overall_bar is not None: stage.overall_bar.update(1)
                if stage.thread_bars.get(thread_id): stage.thread_bars[thread_id].update(1)
                time.sleep(random.uniform(1.6, 3.2))

            except Exception as e:
                err_msg = str(e)[:180]
                error_logger.error(f"[ADVIEW] {url} err: {err_msg}", extra={'thread_id': thread_id})

                old_mask = proxy_ip
                try:
                    try: driver.quit()
                    except: pass
                    proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
                    proxy_ip  = mask_ip(get_proxy_ip(proxy_idx))
                    ua = pick_ua()
                    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADVIEW_EXT_ROOT)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["state"] = "Restarted"; stage.thread_stats[thread_id]["proxy"] = proxy_ip
                except Exception:
                    pass

                if in_final:
                    with stage.state_lock:
                        stage.metrics["final_exhausted"] += 1; stage.metrics["completed"] += 1; stage.done_set.add(url)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["done"] += 1; stage.thread_stats[thread_id]["state"] = "Final Exhausted"

                    audit_append(FAIL_F, {
                        "url": url, "attempts": attempt, "why": f"{type(e).__name__}: {err_msg}",
                        "thread_id": thread_id, "proxy": proxy_ip, "ua_label": f"Chrome/{VERSION_MAIN}",
                        "first_failure_unix": int(time.time()), "last_attempt_unix": int(time.time())
                    })
                else:
                    if attempt == 1:
                        backoff = int(random.uniform(60, 180))
                        with stage.state_lock: stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry A • ADVIEW • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (2/3)"
                            )
                        task["attempt"] = 2; stage.schedule_retry(task, backoff)
                    elif attempt == 2:
                        backoff = int(random.uniform(600, 780))
                        with stage.state_lock: stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry B • ADVIEW • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (3/3)"
                            )
                        task["attempt"] = 3; stage.schedule_retry(task, backoff)
                    else:
                        with stage.state_lock: stage.metrics["deferred"] += 1
                        if url not in stage.deferred_set:
                            stage.deferred_set.add(url)
                            task2 = dict(task); task2["phase"] = "Final"
                            stage.deferred_q.put(task2)
                            audit_append(DEFER_F, {
                                "url": url, "attempts": attempt, "why": f"{type(e).__name__}: {err_msg}",
                                "thread_id": thread_id, "proxy": proxy_ip, "ua_label": f"Chrome/{VERSION_MAIN}",
                                "deferred_unix": int(time.time())
                            })
            finally:
                with stage.state_lock:
                    if url in stage.in_flight: stage.in_flight.discard(url)

    finally:
        try: driver.quit()
        except: pass
        stage.release_proxy(proxy_idx)
        with stage.thread_stats_lock:
            stage.thread_stats[thread_id]["state"] = "finished"

# ====== Compression + Upload ======
def compress_and_upload(csv_path:str, csv_bot: DiscordClient, label:str):
    try:
        zip_path = os.path.splitext(csv_path)[0] + ".zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(csv_path, arcname=os.path.basename(csv_path))
        zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        
        if csv_bot.enabled and zip_size_mb < 9.8:
            csv_bot.send_file(zip_path, content=f"📦 {label} ZIP • {zip_size_mb:.2f} MB")
            return
        gz_path = csv_path + ".gz"
        with open(csv_path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        gz_size_mb = os.path.getsize(gz_path) / (1024 * 1024)
        if csv_bot.enabled and gz_size_mb < 9.8:
            csv_bot.send_file(gz_path, content=f"🗜️ PropertyGuru {label} GZIP • {gz_size_mb:.2f} MB")
        else:
            if csv_bot.enabled:
                csv_bot.send_event(f"⚠️ {label} CSV too large to send (zip={zip_size_mb:.2f} MB, gzip={gz_size_mb:.2f} MB). Saved locally at: {csv_path}")
    except Exception:
        pass

# ====== MAIN ======
if __name__ == "__main__":
    print(f"📁 ADLIST → {ADLIST_DIR}")
    print(f"📁 ADVIEW → {ADVIEW_DIR}")
    print(f"📋 Logs   → {LOG_DIR}")

    dashboard_bot = DiscordClient(DASHBOARD_WEBHOOK)
    retry_bot     = DiscordClient(RETRY_WEBHOOK)
    exhausted_bot = DiscordClient(EXHAUSTED_WEBHOOK)
    csv_bot       = DiscordClient(CSV_WEBHOOK)
    for bot in (dashboard_bot, retry_bot, exhausted_bot, csv_bot): bot.start()

    adlist = Stage("ADLIST", ADLIST_THREADS, ADLIST_EXT_ROOT)
    adview = Stage("ADVIEW", ADVIEW_THREADS, ADVIEW_EXT_ROOT)

    # Seed ADLIST tasks
    total_pages = 0
    for cfg in CATEGORIES:
        for p in range(1, cfg["pages"] + 1):
            adlist.ready_q.put({"intent": cfg["intent"], "segment": cfg["segment"], "is_commercial": cfg["is_commercial"], "page": p, "attempt": 1})
            total_pages += 1
    adlist.metrics["total"] = total_pages

    current_phase = {"phase": "ADLIST"}
    stop_event = threading.Event()
    dash_thr = threading.Thread(target=dashboard_loop, args=(stop_event, dashboard_bot, adlist, adview, lambda: current_phase["phase"]), daemon=True)
    dash_thr.start()

    # Progress bars (optional)
    if tqdm is not None:
        adlist.overall_bar = tqdm(total=adlist.metrics["total"], desc="ADLIST Overall", position=0, dynamic_ncols=True)
        fair = math.ceil(adlist.metrics["total"] / max(1, adlist.threads))
        for t in range(adlist.threads):
            adlist.thread_bars[t] = tqdm(total=fair, desc=f"A{t}", position=t+1, dynamic_ncols=True)

    disp_list = threading.Thread(target=dispatcher_loop, args=(adlist, stop_event), daemon=True, name="adlist_dispatcher")
    disp_list.start()

    # Run ADLIST
    with ThreadPoolExecutor(max_workers=adlist.threads) as ex:
        _ = [ex.submit(adlist_worker, i, adlist, retry_bot, exhausted_bot) for i in range(adlist.threads)]
        while True:
            time.sleep(1)
            with adlist.state_lock, adlist.delayed_lock:
                main_done = adlist.ready_q.qsize() == 0 and len(adlist.delayed_heap) == 0 and len(adlist.in_flight) == 0
                deferred_empty = adlist.deferred_q.qsize() == 0
            if main_done and deferred_empty:
                break

    if tqdm is not None:
        if adlist.overall_bar: adlist.overall_bar.close()
        for t in list(adlist.thread_bars.keys()):
            try: adlist.thread_bars[t].close()
            except Exception: pass

    # Build ADLIST CSV
    adlist_csv_path = os.path.join(ADLIST_DIR, f"PG_adlist_{TS}.csv")
    total_rows = 0
    if hasattr(adlist, "adlist_rows") and adlist.adlist_rows:
        df = pd.DataFrame(adlist.adlist_rows)
        if set(["url","intent","segment"]).issubset(df.columns):
            df = df.drop_duplicates(subset=["url","intent","segment"])
        listed_dt_local = pd.to_datetime(df.get("listed_unix"), unit="s", utc=True, errors="coerce") + pd.Timedelta(hours=8)
        scrape_dt_local = pd.to_datetime(df.get("scrape_unix"), unit="s", utc=True, errors="coerce") + pd.Timedelta(hours=8)
        df["updated_date"]     = listed_dt_local.dt.strftime("%Y-%m-%d")
        df["listed_time"]     = listed_dt_local.dt.strftime("%H:%M:%S")
        df["scrape_date"] = scrape_dt_local.dt.strftime("%Y-%m-%d %H:%M:%S")
        cols = ["intent","segment","url","title","updated_date","listed_time","scrape_date","agent_name","agent_id","ad_id"]
        for c in cols:
            if c not in df.columns: df[c] = None
        df_final = df[cols]
        df_final.to_csv(adlist_csv_path, index=False, encoding="utf-8-sig")
        total_rows = len(df_final)
    else:
        pd.DataFrame(columns=["intent","segment","url","title","updated_date","listed_time","scrape_date","agent_name","agent_id","ad_id"]).to_csv(adlist_csv_path, index=False, encoding="utf-8-sig")
    print(f"📄 ADLIST CSV written: {adlist_csv_path} (rows: {total_rows})")
    compress_and_upload(adlist_csv_path, csv_bot, label="ADLIST")
    
    t_end = time.time() + 15  # wait up to 15s for the sender thread to drain
    while hasattr(csv_bot, "queue") and not csv_bot.queue.empty() and time.time() < t_end:
        time.sleep(0.2)

    # ====== Start ADVIEW ======
    current_phase["phase"] = "ADVIEW"

    # Prefer fresh initial proxies for ADVIEW
    adview_initial_exclude = set(adlist.initial_proxy_indices) if adlist.initial_proxy_indices else set()
    original_assign = adview.assign_initial_proxy
    def assign_with_exclude(thread_id:int, exclude:set|None=None):
        ex = adview_initial_exclude if exclude is None else exclude
        return Stage.assign_initial_proxy(adview, thread_id, exclude=ex)
    adview.assign_initial_proxy = assign_with_exclude

    # Queue ADVIEW URLs from ADLIST CSV
    df_in = pd.read_csv(adlist_csv_path)
    adview_urls = 0
    for _, row in df_in.iterrows():
        url = str(row.get("url","")).strip()
        if not url or url == "nan": continue
        task = {
            "url": url,
            "intent": row.get("intent","unknown"),
            "segment": row.get("segment","unknown"),
            "ad_id": row.get("ad_id") if "ad_id" in row else None,
            "attempt": 1
        }
        adview.ready_q.put(task); adview_urls += 1
    adview.metrics["total"] = adview_urls
    print(f"🧾 ADVIEW URLs queued: {adview_urls}")

    if tqdm is not None:
        adview.overall_bar = tqdm(total=adview.metrics["total"], desc="ADVIEW Overall", position=0, dynamic_ncols=True)
        fair = math.ceil(max(1, adview.metrics["total"]) / max(1, adview.threads))
        for t in range(adview.threads):
            adview.thread_bars[t] = tqdm(total=fair, desc=f"V{t}", position=t+1, dynamic_ncols=True)

    disp_view = threading.Thread(target=dispatcher_loop, args=(adview, stop_event), daemon=True, name="adview_dispatcher")
    disp_view.start()

    with ThreadPoolExecutor(max_workers=adview.threads) as ex:
        _ = [ex.submit(adview_worker, i, adview, retry_bot, exhausted_bot) for i in range(adview.threads)]
        while True:
            time.sleep(1)
            with adview.state_lock, adview.delayed_lock:
                main_done = adview.ready_q.qsize() == 0 and len(adview.delayed_heap) == 0 and len(adview.in_flight) == 0
                deferred_empty = adview.deferred_q.qsize() == 0
            if main_done and deferred_empty:
                break

    if tqdm is not None:
        if adview.overall_bar: adview.overall_bar.close()
        for t in list(adview.thread_bars.keys()):
            try: adview.thread_bars[t].close()
            except Exception: pass

    # Stop dashboard
    if dashboard_bot.enabled:
        dashboard_bot.set_dashboard(build_dashboard_text(adlist, adview, current_phase["phase"]))
    stop_event.set()
    try: dash_thr.join(timeout=3)
    except Exception: pass

    # ====== Build FINAL ADVIEW CSV (rich ADVIEW + ADLIST timing/agent) ======
    adview_csv_path = os.path.join(ADVIEW_DIR, f"PG_adview_{TS}.csv")
    total_rows_view = 0

    # Build adview DF
    if hasattr(adview, "adview_rows") and adview.adview_rows:
        df_view = pd.DataFrame(adview.adview_rows).drop_duplicates(subset=["url"])

        # Adlist slice for merge
        df_adlist = pd.read_csv(adlist_csv_path)[["url","updated_date","listed_time","scrape_date","agent_id","ad_id"]]

        # Merge on URL, prefer ADVIEW ad_id if present, else fill from ADLIST
        df_merged = df_view.merge(df_adlist, on="url", how="left", suffixes=("", "_adlist"))
        df_merged["ad_id"] = df_merged["ad_id"].fillna(df_merged.get("ad_id_adlist"))
        if "ad_id_adlist" in df_merged.columns: df_merged.drop(columns=["ad_id_adlist"], inplace=True)

        # To MYT for scrape_unix if you ever want; but final timing comes from ADLIST
        final_cols = [
            "url","ad_id","title","property_type","state","subregion","subarea","location","address",
            "price","price_per_square_feet","rooms","toilets","furnishing","build_up","land_area",
            "tenure","property_title","bumi_lot","total_units","completion_year","developer",
            "lister","lister_url","phone_number","agency","agency_registration_number","ren",
            "amenities","facilities",
            # from ADLIST:
            "updated_date","listed_time","scrape_date","agent_id"
        ]
        for c in final_cols:
            if c not in df_merged.columns: df_merged[c] = None
        df_final = df_merged[final_cols]
        df_final.to_csv(adview_csv_path, index=False, encoding="utf-8-sig")
        total_rows_view = len(df_final)
    else:
        pd.DataFrame(columns=[
            "url","ad_id","title","property_type","state","subregion","subarea","location","address",
            "price","price_per_square_feet","rooms","toilets","furnishing","build_up","land_area",
            "tenure","property_title","bumi_lot","total_units","completion_year","developer",
            "lister","lister_url","phone_number","agency","agency_registration_number","ren",
            "amenities","facilities","updated_date","listed_time","scrape_date","agent_id"
        ]).to_csv(adview_csv_path, index=False, encoding="utf-8-sig")

    print(f"📄 ADVIEW CSV written: {adview_csv_path} (rows: {total_rows_view})")
    
    # --- Sanity check the path and size ---
    try:
        if not os.path.isfile(adview_csv_path):
            error_logger.error(f"[ADVIEW] CSV not found at {adview_csv_path}", extra={'thread_id': 0})
            print(f"[CHECK] MISSING: {adview_csv_path}")
        else:
            size_bytes = os.path.getsize(adview_csv_path)
            print(f"[CHECK] EXISTS: {adview_csv_path}  ({size_bytes:,} bytes)")
    except Exception as _e:
        error_logger.error(f"[ADVIEW] CSV path check failed: {type(_e).__name__}: {_e}", extra={'thread_id': 0})
        
    # --- Just-in-time restart of CSV webhook BEFORE uploading ADVIEW ---
    try:
        csv_bot.stop()   # safe even if it's not running
    except Exception:
        pass
    
    csv_bot = DiscordClient(CSV_WEBHOOK)
    csv_bot.start()
    print(f"[DEBUG] csv_bot restarted before ADVIEW upload; enabled={csv_bot.enabled}")
    
    # (optional) quick ping to confirm webhook reachability
    # if requests and CSV_WEBHOOK:
    #     try:
    #         # r = requests.post(CSV_WEBHOOK + "?wait=true",
    #         #                   json={"content": "[ping] starting ADVIEW upload"},
    #                           timeout=15)
    #         # print(f"[WEBHOOK] ping status={r.status_code}")
    #     except Exception as e:
    #         print(f"[WEBHOOK] ping failed: {type(e).__name__}: {e}")
    # --- end restart block ---
        
    compress_and_upload(adview_csv_path, csv_bot, label="ADVIEW")

    # Close Discord bots
    if dashboard_bot.enabled:
        dashboard_bot.set_dashboard(build_dashboard_text(adlist, adview, current_phase["phase"]))
        time.sleep(0.5)
    dashboard_bot.stop(); retry_bot.stop(); exhausted_bot.stop(); csv_bot.stop()


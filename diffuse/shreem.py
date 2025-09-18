# ============================ PRODUCTION BUILDER (MAINTENANCE-AWARE) ============================
# Polygon minute → daily intraday metrics + tomorrow targets + Planetary features + Shadbala + Catch22(z50)
# + TA (no TA-Lib dependency), VWAP-directional winrate, restartable writes (UPSERT),
# single-writer DB discipline, multi-core HTTP, optional multi-core Catch22, and
# robust MERGE-style UPSERT that preserves existing columns.
#
# Author: Rajdeep + ChatGPT (hardened as requested)
# ================================================================================================

import os, time, math, pytz, warnings, threading
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text

# --------------------------------- CONFIG ---------------------------------
# I/O
DB_URL                = "sqlite:///db/intraday_panel.db"
DB_TABLE              = "intraday_panel"
ASTRO_TABLE           = "astro_daily"
UNIVERSE_TABLE        = "universe_monthly"   # (month_start, ticker) rows

# Polygon
POLYGON_API_KEY       = "H0hS0iR4p96FFolZUAysetlg_eK6gFgZ"
_BASE_V2              = "https://api.polygon.io/v2"
_BASE_V3              = "https://api.polygon.io/v3"

# Build window (upper bound only; initial build limited by MAX_BUILD_DAYS below)
START_DATE            = "2016-07-01"     # inclusive (America/New_York)
END_DATE              = "2065-09-10"     # inclusive (America/New_York)

# Universe / liquidity
ADV_LOOKBACK_DAYS     = 10
MAX_TICKERS           = 1500

# ---- UNIVERSE STABILITY KNOBS ----
UNIVERSE_POLICY       = "stable_fullrange"  # "stable_fullrange" or "monthly_top_liquid"
COVERAGE_TOLERANCE    = 0.98                # require >=98% of trading days to have minute bars across range

# Runtime / concurrency
TIMEZONE              = "America/New_York"
RTH_ONLY              = True
MAX_WORKERS_HTTP      = min(64, (os.cpu_count() or 8) * 4)
REQUESTS_PER_SECOND   = 40.0
MAX_RETRIES           = 6
BACKOFF_BASE          = 0.6
TIMEOUT_SEC           = 45
Z50_WINDOW            = 50
C22_MIN_WIN           = 16                 # minimum days before computing Catch22 (expanding fallback)

# ---- Warm buffer + NO full history ----
HISTORY_WARM_DAYS     = 100                # pull up to last 100 trading days from DB as context
MAX_BUILD_DAYS        = 100000                # build at most the last 100 calendar days on initial run

DEBUG                 = 1
warnings.simplefilter("ignore", FutureWarning)

# ---- TQDM progress plumbing ----
from tqdm.auto import tqdm

BAR_FMT = "{desc:<34} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"

class TQDM:
    POS_MASTER   = 0  # months / dates
    POS_UNIVERSE = 1  # minute probe / ADV selection
    POS_TICKERS  = 2  # per-ticker build for a month or a day
    _print_lock  = threading.Lock()

    @classmethod
    def bar(cls, *, total, desc, unit="", position=0, leave=True, mininterval=0.25):
        return tqdm(total=total, desc=desc, unit=unit, position=position, leave=leave,
                    dynamic_ncols=True, mininterval=mininterval, bar_format=BAR_FMT)

    @classmethod
    def write(cls, msg: str):
        with cls._print_lock:
            tqdm.write(str(msg))

pd.options.mode.copy_on_write = True

# --------------------------------- TIMEZONE ---------------------------------
_tz_et = pytz.timezone(TIMEZONE)

def today_et() -> date:
    return datetime.now(_tz_et).date()

def _next_weekday_date(d: date) -> date:
    """
    Next Mon–Fri date (ignores US holidays; good enough for astro/calendar fill).
    """
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:  # 5=Sat, 6=Sun
        nd += timedelta(days=1)
    return nd
# --------------------------------- ENGINE & WAL ---------------------------------
from sqlalchemy.engine.url import make_url

def _sqlite_file_path(db_url: str) -> Optional[str]:
    try:
        url = make_url(db_url)
    except Exception:
        return None
    if url.get_backend_name() != "sqlite":
        return None
    if not url.database or url.database == ":memory:":
        return None
    return url.database

def _ensure_sqlite_dir(db_url: str) -> Optional[str]:
    path = _sqlite_file_path(db_url)
    if path:
        abs_path = os.path.abspath(path)
        parent = os.path.dirname(abs_path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        return abs_path
    return None

def get_engine():
    abs_path = _ensure_sqlite_dir(DB_URL)
    connect_args = {"check_same_thread": False} if DB_URL.startswith("sqlite") else {}
    eng = create_engine(DB_URL if abs_path is None else f"sqlite:///{abs_path}",
                        future=True, connect_args=connect_args)
    if DB_URL.startswith("sqlite"):
        with eng.begin() as con:
            con.exec_driver_sql("PRAGMA journal_mode=WAL;")
            con.exec_driver_sql("PRAGMA synchronous=NORMAL;")
            con.exec_driver_sql("PRAGMA temp_store=MEMORY;")
            con.exec_driver_sql("PRAGMA cache_size=-80000;")
    return eng

# --------------------------------- HTTP w/ SESSION + R/L ---------------------------------
_threadlocal = threading.local()

def _get_session() -> requests.Session:
    s = getattr(_threadlocal, "session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS_HTTP,
                                                pool_maxsize=MAX_WORKERS_HTTP)
        s.mount("https://", adapter); s.mount("http://", adapter)
        _threadlocal.session = s
    return s

class RateLimiter:
    def __init__(self, rps: float):
        self.capacity = max(1.0, float(rps))
        self.tokens   = self.capacity
        self.fill     = self.capacity
        self.ts       = time.perf_counter()
        self._lock    = threading.Lock()
    def wait(self):
        with self._lock:
            now = time.perf_counter()
            self.tokens = min(self.capacity, self.tokens + (now - self.ts) * self.fill)
            self.ts = now
            if self.tokens < 1.0:
                sleep_for = (1.0 - self.tokens) / self.fill
                time.sleep(max(0.0, sleep_for))
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

_rate = RateLimiter(REQUESTS_PER_SECOND)

def _backoff(attempt:int):
    time.sleep((BACKOFF_BASE ** attempt) + np.random.uniform(0,0.25))

def _fetch_v2_aggs_all(url: str, params: dict) -> list:
    rows = []
    u, p = url, dict(params) if params else {}
    while True:
        data = _req("GET", u, p)
        res = data.get("results", []) or []
        if res:
            rows.extend(res)
        nxt = data.get("next_url") or data.get("next_page_url") or None
        if not nxt:
            break
        # next_url already includes the cursor; pass empty params so _req will just add apiKey
        u, p = nxt, {}
    return rows

def _req(method:str, url:str, params:Optional[dict]=None):
    if params is None: params = {}
    params["apiKey"] = POLYGON_API_KEY
    sess = _get_session()
    for attempt in range(MAX_RETRIES):
        _rate.wait()
        try:
            r = sess.request(method, url, params=params, timeout=TIMEOUT_SEC)
            sc = r.status_code
            if sc == 429 or (500 <= sc < 600): _backoff(attempt+1); continue
            if sc != 200: _backoff(attempt+1); continue
            return r.json()
        except Exception:
            _backoff(attempt+1)
            continue
    r = _get_session().request(method, url, params=params, timeout=TIMEOUT_SEC*2)
    r.raise_for_status()
    return r.json()

# --------------------------------- POLYGON HELPERS ---------------------------------
def fetch_all_common_stock_tickers(max_pages:Optional[int]=None) -> List[str]:
    tickers = []
    base = f"{_BASE_V3}/reference/tickers"
    url, params = base, dict(market="stocks", active="true", type="CS", limit=1000, sort="ticker")
    while True:
        data = _req("GET", url, params)
        results = data.get("results", [])
        for r in results:
            t = r.get("ticker")
            if t: tickers.append(t)
        next_url = data.get("next_url") or data.get("next_page_url")
        cursor   = data.get("next_page_token") or data.get("next_cursor") or data.get("cursor")
        if next_url:
            url, params = next_url, {}
        elif cursor:
            url, params = base, dict(market="stocks", active="true", type="CS", limit=1000, sort="ticker", cursor=cursor)
        else:
            break
        if max_pages and len(tickers) >= max_pages*1000: break
    return tickers

def fetch_daily_bars(ticker:str, start:str, end:str) -> pd.DataFrame:
    url = f"{_BASE_V2}/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    data = _req("GET", url, dict(adjusted="true", sort="asc", limit=50000))
    res = data.get("results", [])
    if not res:
        return pd.DataFrame(columns=["date_et","open","high","low","close","vwap","volume"])
    df = pd.DataFrame(res)
    df["ts_utc"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df["date_et"] = df["ts_utc"].dt.tz_convert(_tz_et).dt.date
    df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume","vw":"vwap"}, inplace=True)
    return df[["date_et","open","high","low","close","vwap","volume"]].sort_values("date_et")

def fetch_minute_bars(ticker:str, start:str, end:str) -> pd.DataFrame:
    url = f"{_BASE_V2}/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
    params = dict(adjusted="true", sort="asc", limit=50000)
    res = _fetch_v2_aggs_all(url, params)
    if not res:
        return pd.DataFrame(columns=["ts_et","open","high","low","close","vwap","volume"])
    df = pd.DataFrame(res)
    df["ts_utc"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df["ts_et"]  = df["ts_utc"].dt.tz_convert(_tz_et)
    df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume","vw":"vwap"}, inplace=True)
    return df[["ts_et","open","high","low","close","vwap","volume"]]


def has_minute_window(ticker:str, start:str, end:str) -> bool:
    try:
        url = f"{_BASE_V2}/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
        data = _req("GET", url, dict(adjusted="true", sort="asc", limit=1))
        return len(data.get("results", [])) > 0
    except Exception:
        return False

# --------------------------------- RTH FILTER ---------------------------------
def filter_rth(df_min: pd.DataFrame) -> pd.DataFrame:
    if df_min.empty or not RTH_ONLY:
        return df_min
    t = df_min["ts_et"].dt.time
    return df_min[(t >= pd.Timestamp("09:30").time()) & (t < pd.Timestamp("16:00").time())]

# --------------------------------- INTRADAY METRICS ---------------------------------
def _hurst_dfa(r: np.ndarray) -> float:
    try:
        x = np.asarray(r, dtype=float)
        x = x[np.isfinite(x)]
        n = x.size
        if n < 64: return np.nan
        std = float(np.nanstd(x))
        if not np.isfinite(std) or std < 1e-12:
            scale = float(np.nanmedian(np.abs(x))) or 1.0
            x = x + (np.arange(n) - n/2.0) * (1e-10 * scale)
        y = np.cumsum(x - np.nanmean(x))
        s_min, s_max = 8, max(9, n // 4)
        if s_max <= s_min: return np.nan
        log_scales = np.unique(np.floor(np.logspace(np.log10(s_min), np.log10(s_max), num=10)).astype(int))
        pow2 = 2 ** np.arange(3, int(np.floor(np.log2(s_max))) + 1)
        scales = np.unique(np.concatenate([log_scales, pow2[pow2 >= s_min]]))
        Ss, Fs = [], []
        for s in scales:
            nseg = n // s
            if nseg < 2: continue
            segF = []
            for i in range(nseg):
                seg = y[i*s:(i+1)*s]
                t = np.arange(s, dtype=float)
                A = np.vstack([t, np.ones_like(t)]).T
                m, c = np.linalg.lstsq(A, seg, rcond=None)[0]
                detr = seg - (m*t + c)
                f2 = float(np.mean(detr**2))
                if np.isfinite(f2): segF.append(max(f2, 1e-18))
            if segF:
                F = float(np.sqrt(np.mean(segF)))
                if np.isfinite(F) and F > 0:
                    Ss.append(float(s)); Fs.append(F)
        if len(Fs) < 3: return np.nan
        slope, _ = np.polyfit(np.log(np.array(Ss)), np.log(np.array(Fs)), 1)
        return float(np.clip(slope, 0.0, 1.0)) if np.isfinite(slope) else np.nan
    except Exception:
        return np.nan

def _area_over_under_ratio(prices: np.ndarray) -> float:
    if prices.size < 3: return np.nan
    p0, p1 = float(prices[0]), float(prices[-1])
    n = prices.size
    t = np.arange(n, dtype=float)
    line = p0 + (p1 - p0) * (t / (n - 1))
    diff = prices - line
    area_over  = float(np.sum(np.clip(diff,  0, None)))
    area_under = float(np.sum(np.clip(-diff, 0, None)))
    eps = 1e-12
    return (area_over / max(area_under, eps)) if (p1 - p0) >= 0 else (area_under / max(area_over, eps))

# -------- TA helpers (no TA-Lib dependency) --------
def _ema(x, p):
    x = np.asarray(x, float)
    if x.size == 0: return np.array([])
    a = 2/(p+1)
    out = np.empty_like(x); out[:] = np.nan
    for i, v in enumerate(x):
        if np.isfinite(v):
            if not np.isfinite(out[i-1]) if i>0 else True:
                out[i]=v
            else:
                out[i] = out[i-1] + a*(v - out[i-1])
    return out

def _rsi(close, p=14):
    c = np.asarray(close, float)
    if c.size < p+1: return np.full_like(c, np.nan)
    d = np.diff(c)
    up = np.clip(d, 0, None)
    dn = np.clip(-d, 0, None)
    a = 1.0/p
    rs = np.full(c.shape, np.nan)
    au = 0.0; ad = 0.0; init = False
    for i in range(1, len(c)):
        if not init and i >= p:
            au = np.mean(up[i-p:i]); ad = np.mean(dn[i-p:i]); init = True
        elif init:
            au = (1-a)*au + a*up[i-1]
            ad = (1-a)*ad + a*dn[i-1]
        if init and ad > 0:
            rs[i] = 100 - 100/(1 + (au/ad))
        elif init and ad == 0:
            rs[i] = 100.0
    return rs

def _macd(close, fast=12, slow=26, sig=9):
    c = np.asarray(close, float)
    ema_f = _ema(c, fast)
    ema_s = _ema(c, slow)
    macd = ema_f - ema_s
    sigl = _ema(macd, sig)
    hist = macd - sigl
    return macd, sigl, hist

def _true_range(h,l,c):
    tr = np.empty_like(c); tr[:] = np.nan
    prev_c = c[0] if len(c)>0 else np.nan
    for i in range(len(c)):
        if i==0:
            tr[i] = h[i]-l[i]
        else:
            tr[i] = max(h[i]-l[i], abs(h[i]-prev_c), abs(l[i]-prev_c))
        prev_c = c[i]
    return tr

def _adx(h,l,c, p=14):
    h = np.asarray(h,float); l=np.asarray(l,float); c=np.asarray(c,float)
    if len(c) < p+2:
        return np.full_like(c, np.nan)
    dm_plus = np.zeros_like(c); dm_minus = np.zeros_like(c)
    for i in range(1,len(c)):
        up = h[i]-h[i-1]; dn = l[i-1]-l[i]
        dm_plus[i]  = up  if (up > dn and up>0) else 0.0
        dm_minus[i] = dn  if (dn > up and dn>0) else 0.0
    tr = _true_range(h,l,c)
    # Wilder smoothing
    def _wilder(x):
        out = np.full_like(x, np.nan)
        av = np.nan
        for i in range(len(x)):
            if i == p:
                av = np.nanmean(x[1:p+1])
                out[i] = av
            elif i > p:
                av = (av*(p-1) + x[i]) / p
                out[i] = av
        return out
    atr = _wilder(tr)
    dip = 100.0 * (_wilder(dm_plus) / (atr + 1e-12))
    dim = 100.0 * (_wilder(dm_minus)/ (atr + 1e-12))
    dx  = 100.0 * (np.abs(dip - dim) / (dip + dim + 1e-12))
    adx = _wilder(dx)
    return adx

def _bb_width(close, p=20, k=2.0):
    c = pd.Series(close, dtype=float)
    ma = c.rolling(p, min_periods=p).mean()
    sd = c.rolling(p, min_periods=p).std(ddof=1)
    upper = ma + k*sd
    lower = ma - k*sd
    return (upper - lower) / (ma + 1e-12)

def _obv(close, vol):
    c = np.asarray(close,float); v = np.asarray(vol,float)
    obv = np.zeros_like(c)
    for i in range(1,len(c)):
        if c[i] > c[i-1]: obv[i] = obv[i-1] + v[i]
        elif c[i] < c[i-1]: obv[i] = obv[i-1] - v[i]
        else: obv[i] = obv[i-1]
    return obv

def _mfi(h,l,c,v, p=14):
    tp = (np.asarray(h)+np.asarray(l)+np.asarray(c))/3.0
    rmf = tp * np.asarray(v)
    pos = np.zeros_like(tp); neg = np.zeros_like(tp)
    for i in range(1,len(tp)):
        if tp[i] > tp[i-1]: pos[i] = rmf[i]
        elif tp[i] < tp[i-1]: neg[i] = rmf[i]
    mfi = np.full_like(tp, np.nan)
    for i in range(p, len(tp)):
        pm = np.sum(pos[i-p+1:i+1]); nm = np.sum(neg[i-p+1:i+1])
        if (pm+nm) > 0:
            mfr = pm / (nm + 1e-12)
            mfi[i] = 100 - 100/(1+mfr)
    return mfi

def _cmf(h,l,c,v, p=None):
    h=np.asarray(h,float); l=np.asarray(l,float); c=np.asarray(c,float); v=np.asarray(v,float)
    mf = ((c - l) - (h - c)) / (np.maximum(h - l, 1e-12))
    num = np.nansum(mf * v); den = np.nansum(v) + 1e-12
    return num/den

# -------- Intraday daily metrics --------
def compute_intraday_metrics_for_day(min_df: pd.DataFrame) -> Optional[dict]:
    df = filter_rth(min_df) if RTH_ONLY else min_df
    if df.empty: return None
    if len(df) < 8: return None

    p_close = df["close"].astype(float).values
    p_high  = df["high"].astype(float).values
    p_low   = df["low"].astype(float).values
    p_vwap  = df["vwap"].astype(float).values
    vol     = df["volume"].astype(float).values

    # --- run-up + *signed* velocity/acceleration from minute VWAP ---
    vw = np.asarray(p_vwap, dtype=float)
    vw = vw[np.isfinite(vw)]
    if vw.size >= 4:
        # Max run-up (drawup) using minute VWAP (unchanged)
        cummin_vw = np.minimum.accumulate(vw)
        with np.errstate(divide='ignore', invalid='ignore'):
            runup_series = (vw / np.clip(cummin_vw, 1e-12, None)) - 1.0
        max_run_up = float(np.nanmax(runup_series)) if np.isfinite(runup_series).any() else np.nan
    
        # Signed per-minute VWAP return ("velocity")
        rv = np.diff(vw) / np.clip(vw[:-1], 1e-12, None)
        avg_vwap_velocity = float(np.nanmean(rv)) if rv.size else np.nan
    
        # Signed acceleration = mean change in minute returns
        acc = np.diff(rv)
        avg_vwap_acceleration = float(np.nanmean(acc)) if acc.size else np.nan
    else:
        max_run_up = np.nan
        avg_vwap_velocity = np.nan
        avg_vwap_acceleration = np.nan
    
    logp = np.log(np.clip(p_close, 1e-12, None))
    r = np.diff(logp)
    sharpe = np.nan if (np.allclose(np.nanstd(r, ddof=1), 0.0) or len(r)<2) else (np.nanmean(r) / np.nanstd(r, ddof=1)) * math.sqrt(max(1,len(r)))

    vdiff = np.diff(p_vwap)
    up = float(np.sum(vdiff > 0)); dn = float(np.sum(vdiff < 0))
    winrate_ratio = (up + 0.5) / (dn + 0.5)

    cummax = np.maximum.accumulate(p_close)
    dd = (p_close / cummax) - 1.0
    max_drawdown = float(np.nanmin(dd)) if dd.size else np.nan
    slope_open_to_close_pct = (p_close[-1] - p_close[0]) / (p_close[0] + 1e-12)

    hurst_whole_day = _hurst_dfa(r)
    area_over_under_ratio = _area_over_under_ratio(p_close)

    rsi_last = float(pd.Series(_rsi(p_close, 14)).iloc[-1])
    macd, sig, hist = _macd(p_close, 12, 26, 9)
    macd_last, macd_sig_last, macd_hist_last = float(macd[-1]), float(sig[-1]), float(hist[-1])
    adx_last = float(pd.Series(_adx(p_high, p_low, p_close, 14)).iloc[-1])
    bbw_last = float(pd.Series(_bb_width(p_close, 20, 2.0)).iloc[-1])
    obv_end  = float(_obv(p_close, vol)[-1])
    mfi_last = float(pd.Series(_mfi(p_high, p_low, p_close, vol, 14)).iloc[-1])
    cmf_day  = float(_cmf(p_high, p_low, p_close, vol))

    return dict(
        intraday_sharpe=sharpe,
        winrate_ratio=winrate_ratio,
        max_drawdown=max_drawdown,
        slope_open_to_close_pct=slope_open_to_close_pct,
        hurst_whole_day=hurst_whole_day,
        area_over_under_ratio=area_over_under_ratio,
        ta_rsi_last_14=rsi_last,
        ta_macd_last=macd_last,
        ta_macd_signal_last=macd_sig_last,
        ta_macd_hist_last=macd_hist_last,
        ta_adx_last_14=adx_last,
        ta_bb_width_last_20=bbw_last,
        ta_obv_end=obv_end,
        ta_mfi_last_14=mfi_last,
        ta_cmf_day=cmf_day,
        max_run_up=max_run_up,
        avg_vwap_velocity=avg_vwap_velocity,
        avg_vwap_acceleration=avg_vwap_acceleration,
    )

# --------------------------------- JYOTISH ---------------------------------
import swisseph as swe

SIDEREAL_MODE = swe.SIDM_LAHIRI
NODE_TYPE     = "TRUE"

NYC_LAT, NYC_LON   = 40.7128, -74.0060
DAILY_HH, DAILY_MM = 9, 30

PLANETS_ASTRO = [
    ("Sun",     swe.SUN),
    ("Moon",    swe.MOON),
    ("Mars",    swe.MARS),
    ("Mercury", swe.MERCURY),
    ("Jupiter", swe.JUPITER),
    ("Venus",   swe.VENUS),
    ("Saturn",  swe.SATURN),
    ("Rahu",    swe.TRUE_NODE if NODE_TYPE.upper()=="TRUE" else swe.MEAN_NODE),
    ("Ketu",    None),
]

SIGNS = ["Aries","Taurus","Gemini","Cancer","Leo","Virgo","Libra","Scorpio","Sagittarius","Capricorn","Aquarius","Pisces"]
RASHI_DEG = 30.0; NK_SIZE = 360.0/27.0; PADA_SIZE = NK_SIZE/4.0

_JYOTISH_YUTI_SOFT_ORB = 12.0
_JYOTISH_STRENGTH_BASE = 100.0

EXALT_SIGN = {"Sun":0,"Moon":1,"Mars":10,"Mercury":5,"Jupiter":3,"Venus":11,"Saturn":6,"Rahu":1,"Ketu":7}
DEBIL_SIGN = {"Sun":6,"Moon":7,"Mars":4, "Mercury":11,"Jupiter":9,"Venus":5, "Saturn":0,"Rahu":7,"Ketu":1}
OWN_SIGNS  = {"Sun":[4],"Moon":[3],"Mars":[0,7],"Mercury":[2,5],"Jupiter":[8,11],
              "Venus":[1,6],"Saturn":[9,10],"Rahu":[2,10],"Ketu":[7,8,11]}

FRIENDS = {
    "Sun":["Moon","Mars","Jupiter"],
    "Moon":["Sun","Mercury"],
    "Mars":["Sun","Moon","Jupiter"],
    "Mercury":["Sun","Venus"],
    "Jupiter":["Sun","Moon","Mars"],
    "Venus":["Mercury","Saturn"],
    "Saturn":["Mercury","Venus"],
    "Rahu":["Venus","Saturn","Mercury"],
    "Ketu":["Mars","Jupiter"]
}
ENEMIES = {
    "Sun":["Venus","Saturn"],
    "Moon":[],
    "Mars":["Mercury"],
    "Mercury":["Moon"],
    "Jupiter":["Venus","Mercury"],
    "Venus":["Sun","Moon"],
    "Saturn":["Sun","Moon"],
    "Rahu":["Sun","Moon"],
    "Ketu":["Sun","Moon","Mercury"]
}

NAISARGIKA_RAW = {"Sun":60,"Moon":51,"Venus":43,"Jupiter":34,"Mercury":26,"Mars":17,"Saturn":9,"Rahu":20,"Ketu":20}
BENEFICS = set(["Jupiter","Venus","Mercury"])
MALEFICS = set(["Saturn","Mars","Rahu","Ketu","Sun"])

# ---- NEW: Rashi rulers & Nakshatra lords (Lahiri) ----
RULER_BY_SIGN = [
    "Mars","Venus","Mercury","Moon","Sun","Mercury","Venus","Mars","Jupiter","Saturn","Saturn","Jupiter"
]  # 0..11 = Aries..Pisces

# Ashwini→Revati: Ketu, Venus, Sun, Moon, Mars, Rahu, Jupiter, Saturn, Mercury (×3)
NAK_LORDS = [
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury",
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury",
    "Ketu","Venus","Sun","Moon","Mars","Rahu","Jupiter","Saturn","Mercury"
]  # len=27

def _nak_lord_by_index(nk_idx: int) -> str:
    return NAK_LORDS[int(nk_idx) % 27]

def _rashi_lord_by_sign_index(sign_idx: int) -> str:
    return RULER_BY_SIGN[int(sign_idx) % 12]

def _safe_total(shad: Dict[str, Dict[str, float]], name: str) -> float:
    return float(shad.get(name, {}).get("total", 0.0))

def planet_recursive_bala_map(
    pos_by_name: Dict[str, Tuple[float, float]],  # {"Planet": (lon_deg_sidereal, speed_deg/day)}
    shad: Dict[str, Dict[str, float]],            # {"Planet": {"total": ...}, ...}
) -> Dict[str, float]:
    """
    For each planet P:
      S0 = total(P) + total(nakshatra_lord(P))
      Then recurse via rashi lord chain:
         for Q in chain_of_rashi_lords(P):
             S *= ( total(Q) + total(nakshatra_lord(Q)) )
         Stop if Q is in own sign (its rashi lord == Q), on repeats, or after touching up to 9 bodies.
    Returns: {"Planet": value}
    """
    out: Dict[str, float] = {}
    for root in pos_by_name.keys():
        # root placement
        try:
            lon_root = float(pos_by_name[root][0])
        except Exception:
            continue
        nk_root, _ = _nak_index_pada(lon_root)
        sign_root  = _rashi_index(lon_root)
        root_nak_lord = _nak_lord_by_index(nk_root)
        S = _safe_total(shad, root) + _safe_total(shad, root_nak_lord)

        visited = {root, root_nak_lord}
        curr = _rashi_lord_by_sign_index(sign_root)
        steps = 0

        while curr in pos_by_name and curr not in visited and steps < 9:
            lon_c = float(pos_by_name[curr][0])
            nk_c, _ = _nak_index_pada(lon_c)
            term = _safe_total(shad, curr) + _safe_total(shad, _nak_lord_by_index(nk_c))
            S *= term

            visited.update([curr, _nak_lord_by_index(nk_c)])
            # stop on own-sign (ruler points to itself) or trivial self-loop
            sign_c = _rashi_index(lon_c)
            next_ruler = _rashi_lord_by_sign_index(sign_c)
            if next_ruler == curr:
                break
            curr = next_ruler
            steps += 1

        out[root] = float(S)
    return out

def _tz_to_julday(local_dt):
    tz = _tz_et
    if local_dt.tzinfo is None:
        local_dt = tz.localize(local_dt, is_dst=None)
    else:
        local_dt = local_dt.astimezone(tz)
    dt_utc = local_dt.astimezone(pytz.utc)
    return swe.julday(dt_utc.year, dt_utc.month, dt_utc.day,
                      dt_utc.hour + dt_utc.minute/60.0 + dt_utc.second/3600.0)

def _ayanamsha_deg(jd_ut) -> float:
    swe.set_sid_mode(SIDEREAL_MODE, 0, 0)
    return float(swe.get_ayanamsa_ut(jd_ut))

def _lon_spd_sidereal(jd_ut, body):
    flags = swe.FLG_SWIEPH | swe.FLG_SPEED | swe.FLG_SIDEREAL
    pos, ret = swe.calc_ut(jd_ut, body, flags)
    lon = float(pos[0]) % 360.0
    spd = float(pos[3]) if len(pos) >= 4 else np.nan
    return lon, spd

def _nak_index_pada(lon):
    lon = lon % 360.0
    nk_idx = int(lon // NK_SIZE)
    within = lon - nk_idx * NK_SIZE
    pada = int(within // PADA_SIZE) + 1
    return nk_idx, pada

def _rashi_index(lon):  # 0..11
    return int((lon % 360.0) // RASHI_DEG)

def _deg_sep(a, b):
    d = abs((b - a) % 360.0)
    return d if d <= 180.0 else 360.0 - d

def _ascendant_sidereal(jd_ut, lat=40.7128, lon=-74.0060):
    hsys = b'P'
    cusps, ascmc = swe.houses_ex(jd_ut, lat, lon, hsys)
    asc_trop = float(ascmc[0]) % 360.0
    ay = _ayanamsha_deg(jd_ut)
    return (asc_trop - ay) % 360.0

def _house_index(asc_deg, body_lon):  # 1..12
    diff = (body_lon - asc_deg) % 360.0
    return int(diff // 30.0) + 1

def _jyotish_targets_for(planet_name, lon):
    s = int((lon % 360)//30)
    targets = {(s + 6) % 12}
    if planet_name == "Mars":    targets |= {(s + 3) % 12, (s + 7) % 12}
    elif planet_name == "Jupiter": targets |= {(s + 4) % 12, (s + 8) % 12}
    elif planet_name == "Saturn":  targets |= {(s + 2) % 12, (s + 9) % 12}
    elif planet_name in ("Rahu","Ketu"): targets |= {(s + 4) % 12, (s + 8) % 12}
    return targets

def aspect_strength(p1_name, lon1, p2_name, lon2):
    s2 = int((lon2 % 360)//30)
    targets = _jyotish_targets_for(p1_name, lon1)
    if s2 not in targets: return 0.0
    deg_in_sign = (lon1 % 30.0)
    center = (s2 * 30.0 + deg_in_sign) % 360.0
    delta = _deg_sep(lon2, center)
    sigma = _JYOTISH_YUTI_SOFT_ORB / 2.0
    return float(_JYOTISH_STRENGTH_BASE * np.exp(-0.5 * (delta / sigma) ** 2))

def conj_strength(p1_name, lon1, p2_name, lon2):
    if int((lon1%360)//30) != int((lon2%360)//30): return 0.0
    deg1 = (lon1 % 30.0); deg2 = (lon2 % 30.0)
    delta = abs(deg1 - deg2)
    sigma = _JYOTISH_YUTI_SOFT_ORB / 2.0
    return float(_JYOTISH_STRENGTH_BASE * np.exp(-0.5 * (delta / sigma) ** 2))

def _sthana_bala(name, sign_idx):
    if name not in EXALT_SIGN: return 0.5
    if sign_idx == EXALT_SIGN[name]: return 1.0
    if sign_idx == DEBIL_SIGN[name]: return 0.0
    if sign_idx in OWN_SIGNS.get(name, []): return 0.8
    ruler_by_sign = ["Mars","Venus","Mercury","Moon","Sun","Mercury","Venus","Mars","Jupiter","Saturn","Saturn","Jupiter"]
    ruler = ruler_by_sign[sign_idx]
    if ruler in FRIENDS.get(name, []): return 0.7
    if ruler in ENEMIES.get(name, []): return 0.3
    return 0.5

def _dig_bala(name, house_idx):
    best = {"Sun":10,"Mars":10,"Moon":4,"Venus":4,"Jupiter":1,"Mercury":1,"Saturn":7,"Rahu":7,"Ketu":1}.get(name, 1)
    d = min((house_idx - best) % 12, (best - house_idx) % 12)
    return max(0.0, 1.0 - (d / 6.0))

def _kala_bala(name: str, dayfrac: float) -> float:
    dayfrac = float(np.clip(dayfrac, 0.0, 1.0))
    if name in ("Sun", "Jupiter", "Saturn"):
        # Stronger during daytime
        return 0.3 + 0.7 * dayfrac
    if name in ("Moon", "Venus", "Mars"):
        # Stronger during nighttime
        return 1.0 - 0.7 * dayfrac
    if name == "Mercury":
        # Close to neutral, tiny day bias
        return 0.45 + 0.1 * dayfrac
    # Nodes: slight night bias but very mild
    if name in ("Rahu", "Ketu"):
        return 0.55 - 0.1 * dayfrac
    # Fallback
    return 0.5

def _daylight_fraction_for_date(d: date, lat_deg=NYC_LAT, lon_deg=NYC_LON) -> float:
    local_noon = datetime(d.year, d.month, d.day, 12, 0, 0)
    jd_ut = _tz_to_julday(local_noon)
    pos, ret = swe.calc_ut(jd_ut, swe.SUN, swe.FLG_SWIEPH | swe.FLG_EQUATORIAL)
    dec = float(pos[1])  # declination (deg)
    dec = np.deg2rad(dec)
    phi = np.deg2rad(lat_deg)
    h0 = np.deg2rad(-0.833)
    cosH0 = (np.sin(h0) - np.sin(phi)*np.sin(dec)) / (np.cos(phi)*np.cos(dec) + 1e-12)
    cosH0 = np.clip(cosH0, -1.0, 1.0)
    H0 = np.arccos(cosH0)
    day_hours = (2.0 * H0) * (180.0/np.pi) / 15.0
    return float(np.clip(day_hours / 24.0, 0.0, 1.0))

def _naisargika_bala(name):
    raw = NAISARGIKA_RAW.get(name, 20.0)
    mn, mx = min(NAISARGIKA_RAW.values()), max(NAISARGIKA_RAW.values())
    return (raw - mn) / (mx - mn + 1e-9)

def _cheshta_bala(name, speed_deg_per_day):
    typical = {"Sun":0.9856,"Moon":13.176,"Mars":0.524,"Mercury":4.092,"Jupiter":0.0831,"Venus":1.602,"Saturn":0.0335,"Rahu":0.053,"Ketu":0.053}
    s = abs(float(speed_deg_per_day)) if np.isfinite(speed_deg_per_day) else 0.0
    base = s / max(typical.get(name, s if s>0 else 1.0), 1e-6)
    base = min(base, 2.0) / 2.0
    retro = 0.2 if (float(speed_deg_per_day) < 0) else 0.0
    return min(1.0, base + retro)

def _drik_bala(name, longs_by_name):
    if name not in longs_by_name: return 0.5
    lon_target = longs_by_name[name]
    pos = 0.0; neg = 0.0
    for other, lon_other in longs_by_name.items():
        if other == name: continue
        s = aspect_strength(other, lon_other, name, lon_target)
        if other in BENEFICS: pos += s
        elif other in MALEFICS: neg += s
    net = pos - neg
    return max(0.0, min(1.0, 0.5 + net / 1200.0))

def _tithi_yoga_karana(jd_ut):
    sun_lon, _ = _lon_spd_sidereal(jd_ut, swe.SUN)
    moon_lon, _= _lon_spd_sidereal(jd_ut, swe.MOON)
    diff = (moon_lon - sun_lon) % 360.0
    tithi_idx = int(diff // 12.0)
    yoga_angle = (sun_lon + moon_lon) % 360.0
    yoga_idx  = int(yoga_angle // (360.0/27.0))
    karana_idx = int(((diff % 360.0) // 6.0) % 11)
    return tithi_idx, yoga_idx, karana_idx

def _shadbala_proxy_components(jd_ut, d: date):
    swe.set_sid_mode(SIDEREAL_MODE, 0, 0)
    longs = {}; speeds = {}
    for nm, body in PLANETS_ASTRO:
        if nm == "Ketu": continue
        lon, spd = _lon_spd_sidereal(jd_ut, body)
        longs[nm] = lon; speeds[nm] = spd
    rahu_lon = longs["Rahu"]; rahu_spd = speeds["Rahu"]
    longs["Ketu"] = (rahu_lon + 180.0) % 360.0
    speeds["Ketu"] = rahu_spd
    asc = _ascendant_sidereal(jd_ut, NYC_LAT, NYC_LON)
    dayfrac = _daylight_fraction_for_date(d, NYC_LAT, NYC_LON)
    out = {}
    for nm, lon in longs.items():
        sign_idx  = _rashi_index(lon)
        house_idx = _house_index(asc, lon)
        sthana = _sthana_bala(nm, sign_idx)
        dig    = _dig_bala(nm, house_idx)
        kala   = _kala_bala(nm, dayfrac)
        cheshta= _cheshta_bala(nm, speeds[nm])
        nais   = _naisargika_bala(nm)
        drik   = _drik_bala(nm, longs)
        total  = np.nanmean([sthana, dig, kala, cheshta, nais, drik])
        out[nm] = {"sthana":sthana,"dig":dig,"kala":kala,"cheshta":cheshta,"naisargika":nais,"drik":drik,"total":float(total)}
    return out, longs

def astro_daily_df(start_date: str, end_date: str) -> pd.DataFrame:
    sd = pd.to_datetime(start_date).date()
    ed = pd.to_datetime(end_date).date()
    rows = []
    d = sd
    while d <= ed:
        local_dt = datetime(d.year, d.month, d.day, DAILY_HH, DAILY_MM, 0)
        jd_ut = _tz_to_julday(local_dt)

        pos = {}
        for nm, body in PLANETS_ASTRO:
            if nm == "Ketu": continue
            lon, spd = _lon_spd_sidereal(jd_ut, body)
            pos[nm] = (lon, spd)
        rahu_lon, rahu_spd = pos["Rahu"]
        pos["Ketu"] = ((rahu_lon + 180.0) % 360.0, rahu_spd)

        shad, longs = _shadbala_proxy_components(jd_ut, d)
        longs_by_name = {nm: v[0] for nm, v in pos.items()}

        base = {"date": d}


        # --- NEW: planet_recursive_bala per planet ---
        prb_map = planet_recursive_bala_map(
            {nm: pos[nm] for nm in pos.keys()},  # lon/speed for each planet (incl. nodes)
            shad                                   # shadbala proxy components (totals already in here)
        )
        for nm, val in prb_map.items():
            base[f"astro_recursive_bala__{nm}"] = float(val)

        for nm, (lon, spd) in pos.items():
            nk_idx, pada = _nak_index_pada(lon)
            base[f"astro__{nm}_nak_index"] = int(nk_idx)
            base[f"astro__{nm}_pada"]      = int(pada)
            base[f"astro__{nm}_retro"]     = 1 if spd < 0 else 0
            base[f"astro__{nm}_vel"]       = float(spd)
            sign_idx = _rashi_index(lon)
            base[f"astro__{nm}_sign_idx"]  = int(sign_idx)

        names = list(longs_by_name.keys())
        for p1 in names:
            lon1 = longs_by_name[p1]
            for p2 in names:
                if p1 == p2: continue
                lon2 = longs_by_name[p2]
                base[f"astro__aspect_{p1}_{p2}"] = float(aspect_strength(p1, lon1, p2, lon2))

        for i in range(len(names)):
            p1 = names[i]; lon1 = longs_by_name[p1]
            for j in range(i+1, len(names)):
                p2 = names[j]; lon2 = longs_by_name[p2]
                c = float(conj_strength(p1, lon1, p2, lon2))
                base[f"astro__conj_{p1}_{p2}"] = c
                base[f"astro__conj_{p2}_{p1}"] = c

        for nm, comps in shad.items():
            for key in ["sthana","dig","kala","cheshta","naisargika","drik","total"]:
                base[f"shadbala__{nm}_{key}"] = float(comps[key])

        tithi_idx, yoga_idx, karana_idx = _tithi_yoga_karana(jd_ut)
        base["astro__tithi_index"]  = int(tithi_idx)
        base["astro__yoga_index"]   = int(yoga_idx)
        base["astro__karana_index"] = int(karana_idx)

        moon_sign = _rashi_index(longs_by_name["Moon"])
        kset = {0, 3, 6, 9}
        kcount = 0
        for nm in names:
            if nm == "Moon": continue
            psign = _rashi_index(longs_by_name[nm])
            flag = 1 if ((psign - moon_sign) % 12) in kset else 0
            base[f"astro__kendra_to_moon__{nm}"] = int(flag)
            kcount += flag
        base["astro__kendra_to_moon_count"] = int(kcount)

        rows.append(base)
        d += timedelta(days=1)

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    return df

# --------------------------------- CATCH22 (50-day z with expanding fallback) ---------------------------------
CATCH22_BASE_COLS = [
    "intraday_sharpe","winrate_ratio","max_drawdown","slope_open_to_close_pct",
    "hurst_whole_day","area_over_under_ratio","open","high","low","close","vwap","volume",
    "ta_rsi_last_14","ta_macd_last","ta_macd_signal_last","ta_macd_hist_last",
    "ta_adx_last_14","ta_bb_width_last_20","ta_obv_end","ta_mfi_last_14","ta_cmf_day",
    "max_run_up","avg_vwap_velocity","avg_vwap_acceleration",
]

def _init_catch22():
    mod=None; func=None; names=None
    try:
        import catch22 as mod
    except Exception:
        try:
            import pycatch22 as mod
        except Exception:
            raise ImportError("catch22 is required. Install with: pip install catch22")
    if hasattr(mod, "catch22_all"): func = mod.catch22_all
    elif hasattr(mod, "catch22"):   func = mod.catch22
    else: raise AttributeError("catch22 function not found.")
    if hasattr(mod,"feature_names"): names=list(getattr(mod,"feature_names"))
    elif hasattr(mod,"catch22_names"): names=list(getattr(mod,"catch22_names"))
    else:
        test = np.linspace(0,1,256); r = func(test)
        names = list(r.get("names")) if isinstance(r, dict) and "names" in r else [f"c22_{i:02d}" for i in range(1,23)]
    if len(set(names)) != len(names): raise ValueError("Duplicate Catch22 names internal.")
    def compute(arr: np.ndarray):
        r = func(arr)
        vals = r.get("values") if isinstance(r, dict) else r
        vals = np.asarray(vals, float).ravel()
        if vals.shape[0] != 22: vals = vals[:22]
        return vals.tolist()
    return compute, names

def compute_catch22_z50_per_ticker(gdf: pd.DataFrame,
                                   window: int = Z50_WINDOW,
                                   min_win: int = C22_MIN_WIN) -> pd.DataFrame:
    if gdf.empty:
        return gdf

    compute_c22, names = _init_catch22()
    gdf = gdf.sort_values("date_et").reset_index(drop=True)
    n = len(gdf)
    blocks = []
    EPS_STD = 1e-9
    cols_present = [c for c in CATCH22_BASE_COLS if c in gdf.columns]

    for col in cols_present:
        s = pd.to_numeric(gdf[col], errors="coerce").astype(float).values
        raw = np.full((n, len(names)), np.nan, dtype=float)
        for i in range(n):
            lw = min(window, i + 1)
            if lw < min_win:
                continue
            seg = s[i - lw + 1 : i + 1]
            if not np.isfinite(seg).all():
                continue
            try:
                vals = compute_c22(seg)
                raw[i, :] = np.array(vals, dtype=float)
            except Exception:
                pass
        blocks.append((raw, [f"{col}__{nm}" for nm in names]))

    if not blocks:
        return gdf

    raw_all = np.concatenate([blk for blk, _ in blocks], axis=1)
    colnames = sum([nm for _, nm in blocks], [])
    feat_df = pd.DataFrame(raw_all, columns=colnames)

    roll_mean = feat_df.rolling(window=window, min_periods=window).mean()
    roll_std  = feat_df.rolling(window=window, min_periods=window).std(ddof=1).clip(lower=EPS_STD)
    z50 = (feat_df - roll_mean) / roll_std

    exp_mean = feat_df.expanding(min_periods=min_win).mean()
    exp_std  = feat_df.expanding(min_periods=min_win).std(ddof=1).clip(lower=EPS_STD)
    zexp = (feat_df - exp_mean) / exp_std

    z_final = z50.where(np.isfinite(z50), zexp)
    z_final = z_final.fillna(0.0)
    z_final = z_final.add_prefix("z50__").astype("float32")

    out = pd.concat([gdf.drop(columns=[c for c in gdf.columns if c.startswith("z50__")], errors="ignore"),
                     z_final], axis=1)
    return out

# --------------------------------- SCHEMA / UPSERT ---------------------------------
_SCHEMA_COLS = None
_write_lock  = threading.Lock()

def _sqlite_table_exists(con, name: str) -> bool:
    row = con.execute(text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"), {"n": name}).fetchone()
    return row is not None

def ensure_indices(engine):
    with engine.begin() as con:
        con.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{UNIVERSE_TABLE}" (
                "month_start" DATE NOT NULL,
                "ticker"      TEXT NOT NULL,
                PRIMARY KEY ("month_start","ticker")
            )
        '''))

        if _sqlite_table_exists(con, DB_TABLE):
            con.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_ticker_date ON "{DB_TABLE}"(ticker, date_et)'))
            con.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS uq_{DB_TABLE}_ticker_date ON "{DB_TABLE}"(ticker, date_et)'))

        if _sqlite_table_exists(con, ASTRO_TABLE):
            con.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS uq_{ASTRO_TABLE}_date ON "{ASTRO_TABLE}"(date)'))

        if _sqlite_table_exists(con, UNIVERSE_TABLE):
            con.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS uq_{UNIVERSE_TABLE}_month_ticker ON "{UNIVERSE_TABLE}"(month_start, ticker)'))

def precreate_schema(engine, astro_df: pd.DataFrame):
    global _SCHEMA_COLS
    base_today = [
        "ticker","date_et",
        "intraday_sharpe","winrate_ratio","max_drawdown","slope_open_to_close_pct",
        "hurst_whole_day","area_over_under_ratio",
        "max_run_up","avg_vwap_velocity","avg_vwap_acceleration",
        "ta_rsi_last_14","ta_macd_last","ta_macd_signal_last","ta_macd_hist_last",
        "ta_adx_last_14","ta_bb_width_last_20","ta_obv_end","ta_mfi_last_14","ta_cmf_day",
        "open","high","low","close","vwap","volume",
    ]
    tomo_cols = [f"tomo__{c}" for c in [
        "intraday_sharpe","winrate_ratio","max_drawdown","slope_open_to_close_pct",
        "hurst_whole_day","area_over_under_ratio",
        "open","high","low","close","vwap","volume",
        "max_run_up","avg_vwap_velocity","avg_vwap_acceleration",
        # --- NEW tomo targets ---
        "oc_pct",              # C(+1)/O(+1)-1
        "o_over_c_today_pct",  # O(+1)/C(0)-1
        "oo_plus1_pct",        # O(+2)/O(+1)-1
        "co_plus1_pct",        # O(+2)/C(+1)-1
        "consec_run_len",      # signed int as float
        "consec_run_total_pct" # (last_node/O(+1))-1 over that run
    ]]
    tomo_raw_cols = [f"tomo_raw__{c}" for c in ["open","high","low","close","vwap","volume"]]

    calendar_cols = ["next__gap_days","next__weekday","next__month","next__date_et"]
    astro_cols = [c for c in astro_df.columns if c != "date"]

    _, c22_names = _init_catch22()
    z50_cols = []
    for base in CATCH22_BASE_COLS:
        for nm in c22_names:
            z50_cols.append(f"z50__{base}__{nm}")

    all_cols = list(dict.fromkeys(base_today + tomo_cols + tomo_raw_cols + calendar_cols + astro_cols + z50_cols))

    numeric_like = [c for c in all_cols if c not in ("ticker","next__weekday","next__month","date_et","next__date_et")]
    schema_df = pd.DataFrame({c: pd.Series(dtype="float64") for c in numeric_like})
    for c in ("ticker","next__weekday","next__month"): schema_df[c] = pd.Series(dtype="object")
    schema_df["date_et"] = pd.Series(dtype="object")
    schema_df["next__date_et"] = pd.Series(dtype="object")
    schema_df = schema_df[all_cols]

    with engine.begin() as con:
        schema_df.head(0).to_sql(DB_TABLE, con, if_exists="append", index=False)
    _SCHEMA_COLS = list(schema_df.columns)
    ensure_indices(engine)

def _align_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    global _SCHEMA_COLS
    if _SCHEMA_COLS is None: return df
    out = df.copy()
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated(keep="last")]
    out = out.reindex(columns=_SCHEMA_COLS)
    for dc in ("date_et","next__date_et"):
        if dc in out.columns:
            out[dc] = pd.to_datetime(out[dc], errors="coerce").dt.date
    protect = {"ticker","next__weekday","next__month","date_et","next__date_et"}
    num_cols = [c for c in out.columns if c not in protect]
    if num_cols:
        out[num_cols] = out[num_cols].apply(pd.to_numeric, errors="coerce")
    return out

def upsert_df(engine,
              df: pd.DataFrame,
              table: str = DB_TABLE,
              pk=("ticker","date_et"),
              allow_insert: bool = True):
    """
    MERGE-style upsert, but SAFE:
      - UPDATE: only overwrite a column when the incoming value is NOT NULL and NOT NaN
      - INSERT: optional (allow_insert); when False we will not create new rows
    """
    if df is None or df.empty:
        return

    df = _align_to_schema(df)

    with _write_lock:
        with engine.begin() as con:
            # Ensure table exists
            df.head(0).to_sql(table, con, if_exists="append", index=False)

            temp = f'temp_{table}_{int(time.time()*1e6)}'
            df.to_sql(temp, con, if_exists="replace", index=False)

            cols = list(df.columns)
            update_cols = [c for c in cols if c not in pk]

            if update_cols:
                sub_where = " AND ".join([f'"{temp}"."{k}" = "{table}"."{k}"' for k in pk])
                # Overwrite only with real values (not NULL, not NaN). The "x = x" check weeds out NaNs.
                set_list = [
                    f'''"{c}" = (
                        SELECT CASE
                                 WHEN "{temp}"."{c}" IS NOT NULL AND "{temp}"."{c}" = "{temp}"."{c}"
                                   THEN "{temp}"."{c}"
                                 ELSE "{table}"."{c}"
                               END
                          FROM "{temp}" WHERE {sub_where}
                    )'''
                    for c in update_cols
                ]
                exists_where = sub_where
                sql_update = f'''
                    UPDATE "{table}"
                       SET {", ".join(set_list)}
                     WHERE EXISTS (SELECT 1 FROM "{temp}" WHERE {exists_where})
                '''
                con.exec_driver_sql(sql_update)

            if allow_insert:
                col_list = ", ".join([f'"{c}"' for c in cols])
                not_exists = " AND ".join([f'"{table}"."{k}" = "{temp}"."{k}"' for k in pk])
                sql_insert = f'''
                    INSERT INTO "{table}" ({col_list})
                    SELECT {col_list}
                      FROM "{temp}"
                     WHERE NOT EXISTS (SELECT 1 FROM "{table}" WHERE {not_exists})
                '''
                con.exec_driver_sql(sql_insert)

            con.exec_driver_sql(f'DROP TABLE IF EXISTS "{temp}"')

def upsert_universe(engine, month_start: date, tickers: List[str]):
    """
    Dialect-agnostic INSERT-if-missing for the UNIVERSE table.
    (No ON CONFLICT; uses WHERE NOT EXISTS.)
    """
    if not tickers:
        return

    df = pd.DataFrame({
        "month_start": [month_start]*len(tickers),
        "ticker": tickers
    })

    with engine.begin() as con:
        df.head(0).to_sql(UNIVERSE_TABLE, con, if_exists="append", index=False)
        temp = f'temp_uni_{int(time.time()*1e6)}'
        df.to_sql(temp, con, if_exists="replace", index=False)

        col_list = '"month_start","ticker"'
        not_exists = (
            f'"{UNIVERSE_TABLE}"."month_start" = "{temp}"."month_start" '
            f'AND "{UNIVERSE_TABLE}"."ticker" = "{temp}"."ticker"'
        )
        sql_insert = f'''
            INSERT INTO "{UNIVERSE_TABLE}" ({col_list})
            SELECT {col_list} FROM "{temp}"
             WHERE NOT EXISTS (SELECT 1 FROM "{UNIVERSE_TABLE}" WHERE {not_exists})
        '''
        con.exec_driver_sql(sql_insert)

        con.exec_driver_sql(f'DROP TABLE IF EXISTS "{temp}"')

    ensure_indices(engine)

def upsert_astro(engine, astro: pd.DataFrame):
    """
    Dialect-agnostic UPSERT for ASTRO_TABLE:
    - UPDATE existing rows for matching date (LHS not qualified)
    - INSERT rows that don't exist
    """
    if astro is None or astro.empty:
        return

    with engine.begin() as con:
        astro.head(0).to_sql(ASTRO_TABLE, con, if_exists="append", index=False)
        temp = f'temp_astro_{int(time.time()*1e6)}'
        astro.to_sql(temp, con, if_exists="replace", index=False)

        cols = list(astro.columns)
        if "date" not in cols:
            raise ValueError(f'"date" column missing in astro DataFrame')

        # UPDATE existing rows (LHS must not be table-qualified in SQLite)
        update_cols = [c for c in cols if c != "date"]
        if update_cols:
            sub_where = f'"{temp}"."date" = "{ASTRO_TABLE}"."date"'
            set_list = [
                f'"{c}" = (SELECT "{temp}"."{c}" FROM "{temp}" WHERE {sub_where})'
                for c in update_cols
            ]
            sql_update = f'''
                UPDATE "{ASTRO_TABLE}"
                   SET {", ".join(set_list)}
                 WHERE EXISTS (SELECT 1 FROM "{temp}" WHERE {sub_where})
            '''
            con.exec_driver_sql(sql_update)

        # INSERT rows that don't exist
        col_list = ", ".join([f'"{c}"' for c in cols])
        not_exists = f'"{ASTRO_TABLE}"."date" = "{temp}"."date"'
        sql_insert = f'''
            INSERT INTO "{ASTRO_TABLE}" ({col_list})
            SELECT {col_list} FROM "{temp}"
             WHERE NOT EXISTS (SELECT 1 FROM "{ASTRO_TABLE}" WHERE {not_exists})
        '''
        con.exec_driver_sql(sql_insert)

        con.exec_driver_sql(f'DROP TABLE IF EXISTS "{temp}"')

# --------------------------------- UNIVERSE HELPERS ---------------------------------
def month_firsts(start: date, end: date) -> List[date]:
    cur = date(start.year, start.month, 1)
    out = []
    while cur <= end:
        out.append(cur)
        cur = date(cur.year+1, 1, 1) if cur.month == 12 else date(cur.year, cur.month+1, 1)
    return out

def compute_universe_for_month(month_start: date, max_tickers:int, adv_lookback_days:int) -> List[str]:
    label = month_start.strftime("%Y-%m")
    probe_start = (month_start - timedelta(days=3)).strftime("%Y-%m-%d")
    probe_end   = (month_start + timedelta(days=3)).strftime("%Y-%m-%d")

    all_tickers = fetch_all_common_stock_tickers()

    keep = []
    p_probe = TQDM.bar(total=len(all_tickers), desc=f"[{label}] minute probe", unit="stk",
                       position=TQDM.POS_UNIVERSE, leave=False)
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_HTTP) as ex:
            futs = {ex.submit(has_minute_window, t, probe_start, probe_end): t for t in all_tickers}
            for fut in as_completed(futs):
                t = futs[fut]
                try:
                    if fut.result():
                        keep.append(t)
                except Exception:
                    pass
                p_probe.update(1)
    finally:
        p_probe.close()

    lookback_start = (month_start - timedelta(days=adv_lookback_days*6)).strftime("%Y-%m-%d")
    end_prev       = (month_start - timedelta(days=1)).strftime("%Y-%m-%d")
    adv_map: Dict[str,float] = {}

    def _pull_adv(t: str) -> Tuple[str,float]:
        dfd = fetch_daily_bars(t, lookback_start, end_prev)
        if dfd.empty: return t, 0.0
        dfd = dfd[dfd["date_et"] <= pd.to_datetime(end_prev).date()].tail(adv_lookback_days)
        if dfd.empty: return t, 0.0
        adv = float(np.mean(dfd["close"].values * dfd["volume"].values))
        return t, (adv if np.isfinite(adv) else 0.0)

    p_adv = TQDM.bar(total=len(keep), desc=f"[{label}] ADV calc", unit="stk",
                     position=TQDM.POS_UNIVERSE, leave=False)
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_HTTP) as ex:
            futs = {ex.submit(_pull_adv, t): t for t in keep}
            for fut in as_completed(futs):
                try:
                    tt, adv = fut.result()
                    adv_map[tt] = adv
                except Exception:
                    pass
                p_adv.update(1)
    finally:
        p_adv.close()

    ranked = sorted(adv_map.items(), key=lambda kv: kv[1], reverse=True)
    universe = [t for t, _ in ranked[:max_tickers]]
    return universe

def compute_stable_universe_for_range(start_d: date, end_d: date,
                                      max_tickers: int,
                                      coverage_tol: float = COVERAGE_TOLERANCE) -> List[str]:
    start_str = start_d.strftime("%Y-%m-%d")
    end_str   = end_d.strftime("%Y-%m-%d")

    s0 = (start_d - timedelta(days=3)).strftime("%Y-%m-%d")
    s1 = (start_d + timedelta(days=3)).strftime("%Y-%m-%d")
    e0 = (end_d   - timedelta(days=3)).strftime("%Y-%m-%d")
    e1 = (end_d   + timedelta(days=3)).strftime("%Y-%m-%d")

    all_tickers = fetch_all_common_stock_tickers()
    keep_boundary = []
    p_bound = TQDM.bar(total=len(all_tickers), desc="[UNI] boundary probe", unit="stk",
                       position=TQDM.POS_UNIVERSE, leave=False)
    try:
        def _probe(t):
            a = has_minute_window(t, s0, s1)
            b = has_minute_window(t, e0, e1)
            return t, (a and b)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_HTTP) as ex:
            futs = {ex.submit(_probe, t): t for t in all_tickers}
            for fut in as_completed(futs):
                t, ok = fut.result()
                if ok:
                    keep_boundary.append(t)
                p_bound.update(1)
    finally:
        p_bound.close()

    if not keep_boundary:
        return []

    survivors: Dict[str, float] = {}
    p_cov = TQDM.bar(total=len(keep_boundary), desc="[UNI] coverage+ADV", unit="stk",
                     position=TQDM.POS_UNIVERSE, leave=False)

    def _cov_adv(t):
        dfd = fetch_daily_bars(t, start_str, end_str)
        if dfd.empty:
            return t, 0.0, 0.0
        trade_days = set(dfd["date_et"])
        dfm = fetch_minute_bars(t, start_str, end_str)
        if dfm.empty:
            return t, 0.0, 0.0
        min_days = set(dfm["ts_et"].dt.date)
        cov = len(trade_days & min_days) / (len(trade_days) or 1)
        adv = float(np.mean((dfd["close"].astype(float) * dfd["volume"].astype(float)).values))
        return t, cov, adv

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_HTTP) as ex:
            futs = {ex.submit(_cov_adv, t): t for t in keep_boundary}
            for fut in as_completed(futs):
                t, cov, adv = fut.result()
                if np.isfinite(cov) and cov >= coverage_tol and np.isfinite(adv) and adv > 0:
                    survivors[t] = adv
                p_cov.update(1)
    finally:
        p_cov.close()

    ranked = sorted(survivors.items(), key=lambda kv: kv[1], reverse=True)
    return [t for t, _ in ranked[:max_tickers]]

def upsert_universe_all_months(engine, start_d: date, end_d: date, tickers: List[str]):
    for m in month_firsts(start_d, end_d):
        upsert_universe(engine, m, tickers)

def latest_universe_in_db(engine) -> List[str]:
    with engine.begin() as con:
        row = con.execute(text(f'SELECT MAX(month_start) FROM "{UNIVERSE_TABLE}"')).fetchone()
    if not row or not row[0]:
        return []
    ms = pd.to_datetime(row[0]).date()
    return month_universe_in_db(engine, ms)

def month_universe_in_db(engine, mstart: date) -> List[str]:
    with engine.begin() as con:
        rows = con.execute(text(f'SELECT ticker FROM "{UNIVERSE_TABLE}" WHERE month_start = :m'),
                           {"m": mstart}).fetchall()
    return [r[0] for r in rows]

# --------------------------------- PANEL BUILD (PER TICKER) ---------------------------------
def month_ranges(start: date, end: date) -> List[Tuple[str, str]]:
    out = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        nxt = date(cur.year+1, 1, 1) if cur.month==12 else date(cur.year, cur.month+1, 1)
        last = min(end, nxt - timedelta(days=1))
        out.append((cur.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")))
        cur = nxt
    return out

def _compute_tomo_chain_features_for_dates(daily_all: pd.DataFrame,
                                           base_dates: List[date],
                                           horizon_moves: int = 10) -> pd.DataFrame:
    """
    For each base 'today' date D, compute tomorrow-based targets using the raw daily series:
      oc_pct           = (C[D+1] / O[D+1]) - 1
      oo_plus1_pct     = (O[D+2] / O[D+1]) - 1
      co_plus1_pct     = (O[D+2] / C[D+1]) - 1
      consec_run_len   = signed length of first monotone run across: O[D+1], C[D+1], O[D+2], C[D+2], ...
      consec_run_total = (last_node / O[D+1]) - 1  over that same run.
    Notes:
      - Stops on first sign flip, 0-change, NaN, or when data ends. Max horizon given by 'horizon_moves'.
      - Returns a DataFrame indexed by base date.
    """
    if daily_all.empty or not base_dates:
        return pd.DataFrame(index=pd.Index(base_dates, name="date_et"))

    df = daily_all.sort_values("date_et").reset_index(drop=True)
    dates = df["date_et"].tolist()
    idx_map = {d: i for i, d in enumerate(dates)}

    O = df["open"].astype(float).values
    C = df["close"].astype(float).values

    out = {
        "tomo__oc_pct": [],
        "tomo__o_over_c_today_pct": [],
        "tomo__oo_plus1_pct": [],
        "tomo__co_plus1_pct": [],
        "tomo__consec_run_len": [],
        "tomo__consec_run_total_pct": [],
    }

    def _safe_div(a, b):
        if a is None or b is None: return np.nan
        if not np.isfinite(a) or not np.isfinite(b) or b == 0: return np.nan
        return (a / b) - 1.0

    for d in base_dates:
        i = idx_map.get(d, None)
        # we need i+1 (tomorrow), i+2 (day after), etc.
        oc_pct = np.nan
        oo1_pct = np.nan
        co1_pct = np.nan
        run_len = 0.0
        run_total = 0.0

        if i is not None and (i + 1) < len(df):
            o1 = float(O[i+1]); c1 = float(C[i+1])  # tomorrow
            oc_pct = _safe_div(c1, o1)

            c0 = float(C[i])
            o_over_c_today_pct = _safe_div(o1, c0)

            o2 = float(O[i+2]) if (i + 2) < len(df) else np.nan
            if np.isfinite(o2):
                oo1_pct = _safe_div(o2, o1)
                co1_pct = _safe_div(o2, c1)

            # Build node chain: O[i+1], C[i+1], O[i+2], C[i+2], O[i+3], C[i+3], ...
            nodes = []
            k = i + 1
            while k < len(df):
                nodes.append(float(O[k]))
                nodes.append(float(C[k]))
                k += 1
                if len(nodes) >= 2 * (horizon_moves // 2 + 1):  # enough to cover horizon_moves
                    break
            # The first element must be O[i+1]
            if nodes:
                # Compute diffs between consecutive nodes
                diffs = []
                for j in range(1, len(nodes)):
                    a, b = nodes[j-1], nodes[j]
                    if not (np.isfinite(a) and np.isfinite(b)):
                        diffs.append(np.nan)
                    else:
                        diffs.append(b - a)

                # Determine run: first non-zero, non-NaN sets the sign
                sign = 0
                for dval in diffs:
                    if not np.isfinite(dval) or dval == 0:
                        sign = 0
                        break
                    sign = 1 if dval > 0 else -1
                    break

                if sign != 0:
                    # Count consecutive moves matching 'sign'
                    cnt = 0
                    last_node_idx = 0  # index in nodes
                    for j, dval in enumerate(diffs):
                        if not np.isfinite(dval) or dval == 0:
                            break
                        sgn = 1 if dval > 0 else -1
                        if sgn != sign:
                            break
                        cnt += 1
                        last_node_idx = j + 1  # node index after applying this move
                        if cnt >= horizon_moves:
                            break
                    run_len = float(sign * cnt)
                    # Total pct over the run from O[i+1] (nodes[0]) to nodes[last_node_idx]
                    if cnt > 0 and last_node_idx < len(nodes) and np.isfinite(nodes[0]) and nodes[0] != 0:
                        run_total = (nodes[last_node_idx] / nodes[0]) - 1.0
                    else:
                        run_total = 0.0
                else:
                    run_len = 0.0
                    run_total = 0.0

        out["tomo__oc_pct"].append(oc_pct)
        out["tomo__oo_plus1_pct"].append(oo1_pct)
        out["tomo__co_plus1_pct"].append(co1_pct)
        out["tomo__consec_run_len"].append(run_len)
        out["tomo__consec_run_total_pct"].append(run_total)
        out["tomo__o_over_c_today_pct"].append(o_over_c_today_pct)

    x = pd.DataFrame(out)
    x.index = pd.Index(base_dates, name="date_et")
    return x

def apply_neutral_imputations(df: pd.DataFrame) -> pd.DataFrame:
    neutrals = {
        "intraday_sharpe": 0.0,
        "winrate_ratio": 1.0,
        "max_drawdown": 0.0,
        "slope_open_to_close_pct": 0.0,
        "hurst_whole_day": 0.5,
        "area_over_under_ratio": 1.0,
        "max_run_up": 0.0,
        "avg_vwap_velocity": 0.0,
        "avg_vwap_acceleration": 0.0,
        "ta_rsi_last_14": 50.0,
        "ta_macd_last": 0.0,
        "ta_macd_signal_last": 0.0,
        "ta_macd_hist_last": 0.0,
        "ta_adx_last_14": 0.0,
        "ta_bb_width_last_20": 0.0,
        "ta_obv_end": 0.0,
        "ta_mfi_last_14": 50.0,
        "ta_cmf_day": 0.0,
        "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "vwap": 0.0, "volume": 0.0,
    }
    for k, v in neutrals.items():
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce").fillna(v)
    if "next__weekday" in df.columns:
        df["next__weekday"] = df["next__weekday"].fillna("None")
    if "next__month" in df.columns:
        df["next__month"] = df["next__month"].fillna("None")
    return df

# ---- DB helpers for warm history ----
def _table_columns(engine, table: str) -> List[str]:
    with engine.begin() as con:
        rows = con.exec_driver_sql(f'PRAGMA table_info("{table}")').fetchall()
    return [r[1] for r in rows]

def load_warm_history(engine, ticker: str, start_day: date, days: int = HISTORY_WARM_DAYS) -> pd.DataFrame:
    cols_in_db = set(_table_columns(engine, DB_TABLE))
    need_cols = {"ticker", "date_et"} | set(CATCH22_BASE_COLS)
    select_cols = list(need_cols & cols_in_db)

    if not select_cols:
        return pd.DataFrame()

    col_list = ", ".join([f'"{c}"' for c in select_cols])
    sql = f'''
        SELECT {col_list}
        FROM "{DB_TABLE}"
        WHERE ticker = :t
          AND date_et < :start_day
        ORDER BY date_et DESC
        LIMIT :lim
    '''
    with engine.begin() as con:
        df = pd.read_sql(sql, con, params={"t": ticker, "start_day": start_day, "lim": int(days)})

    if df.empty:
        return df

    df["date_et"] = pd.to_datetime(df["date_et"]).dt.date
    df = df.sort_values("date_et").reset_index(drop=True)
    df = apply_neutral_imputations(df)
    return df

def build_ticker_panel(ticker: str, start_date: str, end_date: str,
                       astro_df: pd.DataFrame,
                       warm_df: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    start_dt = pd.to_datetime(start_date).date()
    end_dt   = pd.to_datetime(end_date).date()
    eff_end_dt = end_dt + timedelta(days=7)
    eff_end_str = eff_end_dt.strftime("%Y-%m-%d")

    # --- NEW: pull extra days BEFORE start to get a real previous trading day ---
    daily_pad_days = 10  # spans weekends/holidays
    daily_all = fetch_daily_bars(
        ticker,
        (start_dt - timedelta(days=daily_pad_days)).strftime("%Y-%m-%d"),
        eff_end_str
    )
    if daily_all.empty:
        return None
    daily_all = daily_all.sort_values("date_et").reset_index(drop=True)

    # Compute d/d-1 % changes on the RAW daily series
    daily_norm = daily_all.copy()
    for col in ["open","high","low","close","vwap","volume"]:
        daily_norm[col] = pd.to_numeric(daily_norm[col], errors="coerce").pct_change()

    # Keep only the build span for joining with intraday metrics
    daily = daily_all[daily_all["date_et"].between(start_dt, eff_end_dt)].copy()

    # ----- intraday metrics for each day in [start_dt..eff_end_dt] -----
    rows = []
    for ms, me in month_ranges(start_dt, eff_end_dt):
        dfm = fetch_minute_bars(ticker, ms, me)
        if dfm.empty:
            continue
        dfm["date_et"] = dfm["ts_et"].dt.date
        for d, day_df in dfm.groupby("date_et"):
            metrics = compute_intraday_metrics_for_day(day_df)
            if metrics is None:
                continue
            rows.append({"ticker": ticker, "date_et": d, **metrics})

    if not rows:
        return None

    chunk  = pd.DataFrame(rows)
    merged = pd.merge(chunk, daily, on="date_et", how="inner").sort_values("date_et").reset_index(drop=True)

    # Tomorrow targets from raw daily fields
    metric_cols = [
        "intraday_sharpe","winrate_ratio","max_drawdown","slope_open_to_close_pct",
        "hurst_whole_day","area_over_under_ratio",
        "open","high","low","close","vwap","volume",
        "max_run_up","avg_vwap_velocity","avg_vwap_acceleration",
    ]
    for col in metric_cols:
        merged[f"tomo__{col}"] = merged[col].shift(-1)
    
    # SNAPSHOT RAW TOMORROW OHLCV BEFORE ANY %/NORMALIZATION
    for col in ["open","high","low","close","vwap","volume"]:
        merged[f"tomo_raw__{col}"] = merged[f"tomo__{col}"]
    
    # Now convert tomo__volume to % change vs today's RAW volume
    with np.errstate(divide='ignore', invalid='ignore'):
        merged["tomo__volume"] = np.where(
            (merged["volume"].notna()) & (merged["volume"] > 0),
            (merged["tomo__volume"] / merged["volume"]) - 1.0,
            np.nan
        )
    

    # --- NEW: map normalized day-over-day % changes from daily_norm ---
    norm_cols = ["open","high","low","close","vwap","volume"]
    norm_map = (daily_norm.set_index("date_et")[norm_cols]
                .rename(columns={c: f"{c}_norm" for c in norm_cols}))
    merged = merged.merge(norm_map, left_on="date_et", right_index=True, how="left")
    for c in norm_cols:
        merged[c] = merged[f"{c}_norm"]
    merged.drop(columns=[f"{c}_norm" for c in norm_cols], inplace=True)
    merged[norm_cols] = merged[norm_cols].replace([np.inf, -np.inf], np.nan)

    # Calendar "next__" (unchanged)
    next_dates = merged["date_et"].shift(-1)
    mask_na = next_dates.isna()
    if mask_na.any():
        fallback = merged.loc[mask_na, "date_et"].apply(_next_weekday_date)
        next_dates = next_dates.copy()
        next_dates.loc[mask_na] = pd.to_datetime(fallback)
    today_ts = pd.to_datetime(merged["date_et"])
    next_ts  = pd.to_datetime(next_dates)
    gap      = (next_ts - today_ts).dt.days.astype("float") - 1.0
    merged["next__gap_days"] = gap.clip(lower=0)
    merged["next__weekday"]  = next_ts.dt.day_name()
    merged["next__month"]    = next_ts.dt.month_name()
    merged["next__date_et"]  = next_ts.dt.normalize()

    # Astro join on next__date_et (unchanged)
    astro = astro_df.rename(columns={"date":"next__date_et"})
    merged = pd.merge(merged, astro, how="left", on="next__date_et")

    # Normalize tomo OHLCV by tomorrow's open (unchanged)
    with np.errstate(divide='ignore', invalid='ignore'):
        op = pd.to_numeric(merged["tomo__open"], errors="coerce")
        ok = op.notna() & (op != 0)
        for col in ["high","low","close","open","vwap"]:
            merged.loc[ok, f"tomo__{col}"] = (
                pd.to_numeric(merged.loc[ok, f"tomo__{col}"], errors="coerce") / op.loc[ok]
            )
        merged.loc[ok, "tomo__open"] = 1.0

    # Use the same 'daily_all' we already fetched (raw OHLCV incl. build window + pad)
    ext = _compute_tomo_chain_features_for_dates(
        daily_all=daily_all[["date_et","open","close"]],  # keep it lean
        base_dates=merged["date_et"].tolist(),
        horizon_moves=10
    )
    merged = merged.merge(ext, left_on="date_et", right_index=True, how="left")
    
    merged = apply_neutral_imputations(merged)

    # Warm-history path
    if warm_df is not None and not warm_df.empty:
        need_cols = ["ticker","date_et"] + CATCH22_BASE_COLS
        for col in need_cols:
            if col not in warm_df.columns and col not in ("ticker","date_et"):
                warm_df[col] = 0.0
        warm_small = warm_df[["ticker","date_et"] + [c for c in CATCH22_BASE_COLS if c in warm_df.columns]].copy()
        warm_small["ticker"]  = str(ticker)
        warm_small["date_et"] = pd.to_datetime(warm_small["date_et"]).dt.date
        combo = pd.concat([warm_small, merged], ignore_index=True, sort=False)
        combo = compute_catch22_z50_per_ticker(combo)
        out = combo[(combo["date_et"] >= start_dt) & (combo["date_et"] <= end_dt)].reset_index(drop=True)
        return out

    # No-warm path
    merged = compute_catch22_z50_per_ticker(merged)
    merged = merged[(merged["date_et"] >= start_dt) & (merged["date_et"] <= end_dt)].reset_index(drop=True)
    return merged


# --------------------------------- MAINTENANCE OPS ---------------------------------
def last_db_date(engine) -> Optional[date]:
    with engine.begin() as con:
        row = con.execute(text(f'SELECT MAX(date_et) FROM "{DB_TABLE}"')).fetchone()
    if not row or not row[0]:
        return None
    return pd.to_datetime(row[0]).date()

def tickers_on_date(engine, d: date) -> List[str]:
    with engine.begin() as con:
        rows = con.execute(text(f'SELECT DISTINCT ticker FROM "{DB_TABLE}" WHERE date_et = :d'), {"d": d}).fetchall()
    return [r[0] for r in rows]

def month_universe_in_db(engine, mstart: date) -> List[str]:
    with engine.begin() as con:
        rows = con.execute(text(f'SELECT ticker FROM "{UNIVERSE_TABLE}" WHERE month_start = :m'), {"m": mstart}).fetchall()
    return [r[0] for r in rows]

def update_prev_day_tomo_and_astro(engine, tickers: List[str], prev_day: date, curr_day: date, astro_df: pd.DataFrame):
    """
    Update-only writer:
      - Fills tomo__{open..vwap,volume}, tomo__intraday_* (from today's computed intraday),
        calendar next__*, and astro@curr_day for prev_day.
      - Never INSERTS prev_day rows. If prev_day row doesn't exist, we skip it.
      - Preserves existing today-metrics via upsert_df's non-null guard.
    """
    if not tickers:
        return

    # 1) Pull today's intraday metrics from DB (these were just computed & upserted)
    intraday_cols = [
        "intraday_sharpe","winrate_ratio","max_drawdown","slope_open_to_close_pct",
        "hurst_whole_day","area_over_under_ratio",
        "max_run_up","avg_vwap_velocity","avg_vwap_acceleration",
    ]
    placeholders = ",".join([f":t{i}" for i in range(len(tickers))])
    params = {"d": curr_day}
    params.update({f"t{i}": t for i, t in enumerate(tickers)})

    with engine.begin() as con:
        sql_intr = f'''
            SELECT ticker, {", ".join(intraday_cols)}
              FROM "{DB_TABLE}"
             WHERE date_et = :d
               AND ticker IN ({placeholders})
        '''
        today_intr = pd.read_sql(sql_intr, con, params=params) if tickers else pd.DataFrame(columns=["ticker"]+intraday_cols)

    intr_map = {row["ticker"]: {c: row[c] for c in intraday_cols} for _, row in today_intr.iterrows()}

    # 2) Pull prev-day and today's daily OHLCV to compute % change for tomo__volume
    start = prev_day.strftime("%Y-%m-%d")
    end   = (curr_day + timedelta(days=10)).strftime("%Y-%m-%d")  # allow O(+2) if available

    daily_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_HTTP) as ex:
        futs = {ex.submit(fetch_daily_bars, t, start, end): t for t in tickers}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                dfd = fut.result()
                if not dfd.empty:
                    dfd = dfd.sort_values("date_et").reset_index(drop=True)
                    # indices: we need rows for curr_day (i1) and possibly curr_day+1 (i2)
                    dates_list = dfd["date_et"].tolist()
                    idx_map = {dd: ii for ii, dd in enumerate(dates_list)}
                
                    i1 = idx_map.get(curr_day, None)   # tomorrow relative to prev_day
                    if i1 is None:
                        continue
                
                    rc = dfd.iloc[i1]
                    prev_row = dfd[dfd["date_et"] == prev_day].tail(1)
                
                    prev_vol = float(prev_row.iloc[0]["volume"]) if not prev_row.empty else np.nan
                    cur_vol  = float(rc["volume"])
                    vol_pct  = ((cur_vol / prev_vol) - 1.0) if (np.isfinite(prev_vol) and prev_vol > 0) else np.nan
                
                    # NEW: oc_pct (uses curr_day open/close)
                    o1 = float(rc["open"]) if pd.notna(rc["open"]) else np.nan
                    c1 = float(rc["close"]) if pd.notna(rc["close"]) else np.nan
                    oc_pct = ((c1 / o1) - 1.0) if (np.isfinite(o1) and o1 != 0 and np.isfinite(c1)) else np.nan

                    # NEW: today close → tomorrow open
                    o_over_c_today_pct = np.nan
                    prev_close = float(prev_row.iloc[0]["close"]) if not prev_row.empty else np.nan
                    if np.isfinite(prev_close) and prev_close != 0 and np.isfinite(o1):
                        o_over_c_today_pct = (o1 / prev_close) - 1.0
                    
                                    
                    # Try to get O(+2) for oo/co+1 and run stats
                    oo1_pct = np.nan
                    co1_pct = np.nan
                    run_len = 0.0
                    run_total = 0.0
                
                    i2 = i1 + 1
                    if i2 < len(dfd):
                        o2 = float(dfd.iloc[i2]["open"])
                        if np.isfinite(o2) and np.isfinite(o1) and o1 != 0:
                            oo1_pct = (o2 / o1) - 1.0
                        if np.isfinite(o2) and np.isfinite(c1) and c1 != 0:
                            co1_pct = (o2 / c1) - 1.0
                
                        # Build nodes from O(i1), C(i1), O(i2), C(i2), ...
                        Oser = dfd["open"].astype(float).values
                        Cser = dfd["close"].astype(float).values
                        nodes = []
                        k = i1
                        max_moves = 10
                        while k < len(dfd) and len(nodes) < 2 * (max_moves // 2 + 1):
                            nodes.append(float(Oser[k]))
                            nodes.append(float(Cser[k]))
                            k += 1
                
                        if nodes:
                            diffs = []
                            for j in range(1, len(nodes)):
                                a, b = nodes[j-1], nodes[j]
                                diffs.append(b - a if np.isfinite(a) and np.isfinite(b) else np.nan)
                            sign = 0
                            for dv in diffs:
                                if not np.isfinite(dv) or dv == 0:
                                    sign = 0
                                    break
                                sign = 1 if dv > 0 else -1
                                break
                            if sign != 0:
                                cnt = 0
                                last_node_idx = 0
                                for j, dv in enumerate(diffs):
                                    if not np.isfinite(dv) or dv == 0:
                                        break
                                    sgn = 1 if dv > 0 else -1
                                    if sgn != sign:
                                        break
                                    cnt += 1
                                    last_node_idx = j + 1
                                    if cnt >= max_moves:
                                        break
                                run_len = float(sign * cnt)
                                if cnt > 0 and last_node_idx < len(nodes) and np.isfinite(nodes[0]) and nodes[0] != 0:
                                    run_total = (nodes[last_node_idx] / nodes[0]) - 1.0
                                else:
                                    run_total = 0.0
                
                    daily_map[t] = dict(
                        open=float(rc["open"]), high=float(rc["high"]), low=float(rc["low"]),
                        close=float(rc["close"]), vwap=float(rc["vwap"]), volume=cur_vol,
                        volume_pct=vol_pct,
                        oc_pct=oc_pct,
                        o_over_c_today_pct=o_over_c_today_pct,
                        oo1_pct=oo1_pct,
                        co1_pct=co1_pct,
                        run_len=run_len,
                        run_total=run_total,
                    )

            except Exception:
                pass
    if not daily_map:
        return


    # 3) Calendar & astro @ curr_day
    gap_days     = max(0, (curr_day - prev_day).days - 1)
    next_weekday = pd.Timestamp(curr_day).day_name()
    next_month   = pd.Timestamp(curr_day).month_name()

    astro_next = astro_df.rename(columns={"date":"next__date_et"})
    astro_cols = [c for c in astro_next.columns if c != "next__date_et"]  # <-- define here
    curr_ts    = pd.Timestamp(curr_day)
    astro_row  = astro_next[astro_next["next__date_et"] == curr_ts]

    # 4) Build update records (NO INSERTS)
    recs = []
    for t, x in daily_map.items():
        o = x["open"]
        if o and np.isfinite(o) and abs(o) > 0:
            base = dict(
                ticker=t, date_et=prev_day,
                # Normalized by tomorrow open
                tomo__open=1.0,
                tomo__high=x["high"]/o, tomo__low=x["low"]/o,
                tomo__close=x["close"]/o, tomo__vwap=x["vwap"]/o,
                # CHANGED: use % change vs prev-day
                tomo__volume=x.get("volume_pct", np.nan),

                # Raw tomorrow OHLCV
                tomo_raw__open=o,
                tomo_raw__high=x["high"],
                tomo_raw__low=x["low"],
                tomo_raw__close=x["close"],
                tomo_raw__vwap=x["vwap"],
                tomo_raw__volume=x["volume"],
                    
                # Calendar
                next__date_et=curr_day,
                next__gap_days=float(gap_days),
                next__weekday=next_weekday,
                next__month=next_month,
            )

            base["tomo__oc_pct"]               = x.get("oc_pct", np.nan)
            base["tomo__oo_plus1_pct"]         = x.get("oo1_pct", np.nan)
            base["tomo__co_plus1_pct"]         = x.get("co1_pct", np.nan)
            base["tomo__consec_run_len"]       = x.get("run_len", 0.0)
            base["tomo__consec_run_total_pct"] = x.get("run_total", 0.0)
            base["tomo__o_over_c_today_pct"]   = x.get("o_over_c_today_pct", np.nan)  # NEW


            # Inject tomo__intraday_* from today's intraday metrics if present
            if t in intr_map:
                for c in intraday_cols:
                    base[f"tomo__{c}"] = intr_map[t].get(c, np.nan)

            # Attach astro@curr_day
            if not astro_row.empty:
                for c in astro_cols:
                    val = astro_row.iloc[0][c]
                    is_num = pd.api.types.is_numeric_dtype(astro_row[c].dtype)
                    base[c] = float(val) if is_num and pd.notna(val) else val

            recs.append(base)

    if recs:
        df = pd.DataFrame(recs)
        # Update only (no insert)
        upsert_df(engine, df, DB_TABLE, allow_insert=False)


# --------------------------------- RUN MODES ---------------------------------
def _limit_to_last_n_days(s: date, e: date, n_days: int) -> Tuple[date, date]:
    today = today_et()
    e2 = min(e, today)
    s2 = max(s, e2 - timedelta(days=max(1, n_days-1)))
    return s2, e2

def initial_full_build():
    engine = get_engine()

    # Limit to last MAX_BUILD_DAYS by default
    sdt_raw = pd.to_datetime(START_DATE).date()
    edt_raw = pd.to_datetime(END_DATE).date()
    sdt, edt = _limit_to_last_n_days(sdt_raw, edt_raw, MAX_BUILD_DAYS)

    astro = astro_daily_df(sdt.strftime("%Y-%m-%d"), (edt + pd.Timedelta(days=14)).strftime("%Y-%m-%d"))
    upsert_astro(engine, astro)
    precreate_schema(engine, astro)

    months = month_firsts(sdt, edt)

    if UNIVERSE_POLICY == "stable_fullrange":
        if DEBUG:
            TQDM.write(f"[Universe] STABLE universe {sdt} → {edt} (top-{MAX_TICKERS})")
        stable_uni = compute_stable_universe_for_range(sdt, edt, MAX_TICKERS, COVERAGE_TOLERANCE)
        upsert_universe_all_months(engine, sdt, edt, stable_uni)

        p_months = TQDM.bar(total=len(months), desc="Months", unit="mo",
                            position=TQDM.POS_MASTER, leave=True)
        try:
            for m in months:
                m_start = m
                m_end = (date(m.year, 12, 31) if m.month == 12 else (date(m.year, m.month+1, 1) - timedelta(days=1)))
                m_end = min(m_end, edt)

                ok_cnt = 0; warn_cnt = 0
                def _work(t):
                    nonlocal ok_cnt, warn_cnt
                    try:
                        warm = load_warm_history(engine, t, m_start, HISTORY_WARM_DAYS)
                        df = build_ticker_panel(t, m_start.strftime("%Y-%m-%d"), m_end.strftime("%Y-%m-%d"), astro, warm_df=warm)
                        if df is not None and not df.empty:
                            upsert_df(engine, df, DB_TABLE)
                            ok_cnt += 1
                            return True, t, len(df)
                        else:
                            warn_cnt += 1
                            return False, t, "empty"
                    except Exception as e:
                        warn_cnt += 1
                        return False, t, str(e)

                p_tickers = TQDM.bar(total=len(stable_uni), desc=f"[{m:%Y-%m}] build", unit="stk",
                                     position=TQDM.POS_TICKERS, leave=False)
                try:
                    with ThreadPoolExecutor(max_workers=min(8, (os.cpu_count() or 8)//2)) as ex:
                        futs = [ex.submit(_work, t) for t in stable_uni]
                        for fut in as_completed(futs):
                            ok, t, info = fut.result()
                            if DEBUG and not ok:
                                TQDM.write(f"[WARN] {t}: {info}")
                            p_tickers.update(1)
                            p_tickers.set_postfix(ok=ok_cnt, warn=warn_cnt, refresh=False)
                finally:
                    p_tickers.close()
                p_months.update(1)
        finally:
            p_months.close()
    else:
        p_months = TQDM.bar(total=len(months), desc="Months", unit="mo",
                            position=TQDM.POS_MASTER, leave=True)
        try:
            for m in months:
                if DEBUG:
                    TQDM.write(f"[Universe] Monthly top-{MAX_TICKERS} for {m}")
                uni = compute_universe_for_month(m, MAX_TICKERS, ADV_LOOKBACK_DAYS)
                upsert_universe(engine, m, uni)

                m_start = m
                m_end = (date(m.year, 12, 31) if m.month == 12 else (date(m.year, m.month+1, 1) - timedelta(days=1)))
                m_end = min(m_end, edt)

                ok_cnt = 0; warn_cnt = 0
                def _work(t):
                    nonlocal ok_cnt, warn_cnt
                    try:
                        warm = load_warm_history(engine, t, m_start, HISTORY_WARM_DAYS)
                        df = build_ticker_panel(t, m_start.strftime("%Y-%m-%d"), m_end.strftime("%Y-%m-%d"), astro, warm_df=warm)
                        if df is not None and not df.empty:
                            upsert_df(engine, df, DB_TABLE)
                            ok_cnt += 1
                            return True, t, len(df)
                        else:
                            warn_cnt += 1
                            return False, t, "empty"
                    except Exception as e:
                        warn_cnt += 1
                        return False, t, str(e)

                p_tickers = TQDM.bar(total=len(uni), desc=f"[{m:%Y-%m}] build", unit="stk",
                                     position=TQDM.POS_TICKERS, leave=False)
                try:
                    with ThreadPoolExecutor(max_workers=min(8, (os.cpu_count() or 8)//2)) as ex:
                        futs = [ex.submit(_work, t) for t in uni]
                        for fut in as_completed(futs):
                            ok, t, info = fut.result()
                            if DEBUG and not ok:
                                TQDM.write(f"[WARN] {t}: {info}")
                            p_tickers.update(1)
                            p_tickers.set_postfix(ok=ok_cnt, warn=warn_cnt, refresh=False)
                finally:
                    p_tickers.close()
                p_months.update(1)
        finally:
            p_months.close()

    ensure_indices(engine)
    if DEBUG: TQDM.write("[Build] Initial full build complete.")

# --- TRADING DAY HELPERS (use SPY daily bars as the calendar) ---
def trading_days_between(start_d: date, end_d: date, ref_ticker: str = "SPY") -> List[date]:
    if start_d > end_d:
        return []
    days = set()

    # Daily (may lag)
    dfd = fetch_daily_bars(ref_ticker, start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))
    if not dfd.empty:
        days.update(pd.to_datetime(dfd["date_et"]).dt.date.tolist())

    # Minute (present as soon as the session starts; definitely present after the close)
    dfm = fetch_minute_bars(ref_ticker, start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))
    if not dfm.empty:
        days.update(dfm["ts_et"].dt.date.tolist())

    return sorted(d for d in days if start_d <= d <= end_d)

def previous_trading_day_map(days: List[date]) -> Dict[date, date]:
    return {days[i]: days[i-1] for i in range(1, len(days))}

# ============================ MAINTENANCE REDESIGN ============================
# New knobs
MAINT_BACKFILL_TRADING_DAYS = 10
MAINT_TEMP_DB_DIR           = "db/maint_runs"  # new DBs will be created here

# --- generic engine factory (like get_engine, but for any URL) ---
def get_engine_for(db_url: str):
    abs_path = _ensure_sqlite_dir(db_url)
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    eng = create_engine(db_url if abs_path is None else f"sqlite:///{abs_path}",
                        future=True, connect_args=connect_args)
    if db_url.startswith("sqlite"):
        with eng.begin() as con:
            con.exec_driver_sql("PRAGMA journal_mode=WAL;")
            con.exec_driver_sql("PRAGMA synchronous=NORMAL;")
            con.exec_driver_sql("PRAGMA temp_store=MEMORY;")
            con.exec_driver_sql("PRAGMA cache_size=-80000;")
    return eng

def _temp_db_url_for_run(run_day: Optional[date] = None) -> str:
    """Create a unique sqlite URL for this maintenance run."""
    rd = run_day or today_et()
    ts = int(time.time())
    os.makedirs(MAINT_TEMP_DB_DIR, exist_ok=True)
    fname = f"intraday_panel_maint_{rd:%Y%m%d}_{ts}.db"
    return f"sqlite:///{os.path.abspath(os.path.join(MAINT_TEMP_DB_DIR, fname))}"

def _gather_universe_from_db(engine, start_d: date, end_d: date) -> List[str]:
    """Union of tickers stored in UNIVERSE_TABLE for months covering [start_d, end_d]."""
    months = month_firsts(start_d, end_d)
    uni_set = set()
    for m in months:
        uni_set.update(month_universe_in_db(engine, m))
    if not uni_set:
        # fallbacks: latest month’s stored universe, then tickers observed in the DB on the last built day
        last_list = latest_universe_in_db(engine)
        if last_list:
            return last_list
        ld = last_db_date(engine)
        if ld:
            return tickers_on_date(engine, ld)
    return sorted(uni_set)

def _recent_trading_window(run_day: date, n_trading_days: int) -> Tuple[date, date, List[date]]:
    """Return (start, end, list_of_recent_trading_days) ensuring we include 'today' if it’s a trading day."""
    # pad calendar range to be safe (weekends/holidays)
    cal_start = run_day - timedelta(days=max(14, n_trading_days * 3))
    days = trading_days_between(cal_start, run_day)
    if not days:
        return run_day, run_day, []
    recent = days[-n_trading_days:] if len(days) >= n_trading_days else days
    return recent[0], days[-1], recent

def _delete_master_dates(engine, dates: List[date]):
    """Hard delete specific dates from the master so they can be fully replaced."""
    if not dates:
        return
    with engine.begin() as con:
        # fast path: contiguous? delete a range; else delete IN list
        dates_sorted = sorted(dates)
        rangeable = (dates_sorted == [dates_sorted[0] + timedelta(days=i) for i in range((dates_sorted[-1] - dates_sorted[0]).days + 1)])
        if rangeable:
            con.exec_driver_sql(
                f'DELETE FROM "{DB_TABLE}" WHERE date_et >= :a AND date_et <= :b',
                {"a": dates_sorted[0], "b": dates_sorted[-1]}
            )
        else:
            # break into chunks to avoid too many parameters
            CH = 900
            for i in range(0, len(dates_sorted), CH):
                chunk = dates_sorted[i:i+CH]
                placeholders = ", ".join([f":d{i}" for i in range(len(chunk))])
                params = {f"d{i}": d for i, d in enumerate(chunk)}
                con.exec_driver_sql(f'DELETE FROM "{DB_TABLE}" WHERE date_et IN ({placeholders})', params)

def _read_temp_slice(temp_engine, start_d: date, end_d: date) -> pd.DataFrame:
    """Pull the built slice from temp DB for merge back to master."""
    with temp_engine.begin() as con:
        df = pd.read_sql(
            f'SELECT * FROM "{DB_TABLE}" WHERE date_et >= :a AND date_et <= :b',
            con,
            params={"a": start_d, "b": end_d}
        )
    if not df.empty:
        df["date_et"] = pd.to_datetime(df["date_et"]).dt.date
    return df

# ---------------------- NEW MAINTENANCE (drop-in replacement) ----------------------
def maintenance_run(run_day: Optional[date] = None,
                    backfill_trading_days: int = MAINT_BACKFILL_TRADING_DAYS,
                    temp_db_url: Optional[str] = None):
    """
    Maintenance = "mini initial build" over the last N trading days, using only DB-stored universes.
    Steps
      1) Decide maintenance window = last N trading days up to run_day (incl. today if trading)
      2) Create a fresh TEMP DB; precreate schema there (same columns as master)
      3) Build panels per month/per ticker (like initial build, stable branch), writing ONLY to TEMP
         - Universe = union of UNIVERSE_TABLE rows for months in range; no refetch
         - Warm history pulled from MASTER to keep z50 happy
      4) Merge TEMP → MASTER for [window], after:
         - Deleting MASTER's rows for those dates (so the "last 10" are hard-replaced)
         - Upserting the rebuilt slice (also fills any missing)
    """
    # --- resolve day/window ---
    run_day = run_day or today_et()
    master_engine = get_engine()
    start_d, end_d, recent_days = _recent_trading_window(run_day, backfill_trading_days)

    if not recent_days:
        if DEBUG: TQDM.write(f"[Maintenance] No trading days found up to {run_day}.")
        return

    # --- universe ONLY from DB ---
    universe = _gather_universe_from_db(master_engine, start_d, end_d)
    if not universe:
        if DEBUG: TQDM.write("[Maintenance] No stored universe found in DB; skipping.")
        return

    # --- astro for the window (pad for tomo/chain/astro joins) ---
    astro_start = (start_d - timedelta(days=1)).strftime("%Y-%m-%d")
    astro_end   = (end_d + timedelta(days=14)).strftime("%Y-%m-%d")
    astro = astro_daily_df(astro_start, astro_end)

    # --- TEMP DB setup ---
    temp_db_url = temp_db_url or _temp_db_url_for_run(run_day)
    temp_engine = get_engine_for(temp_db_url)
    upsert_astro(temp_engine, astro)           # keep a copy in temp too (useful for audits)
    precreate_schema(temp_engine, astro)       # sets _SCHEMA_COLS to the full schema

    # --- (Optional) ensure master schema is also current (in case new columns landed) ---
    upsert_astro(master_engine, astro)
    precreate_schema(master_engine, astro)

    # --- build like initial stable branch, but just over [start_d..end_d] ---
    months = month_firsts(start_d, end_d)
    p_months = TQDM.bar(total=len(months), desc="Maint months", unit="mo",
                        position=TQDM.POS_MASTER, leave=True)
    try:
        for m in months:
            m_start = m
            m_end = (date(m.year, 12, 31) if m.month == 12 else (date(m.year, m.month + 1, 1) - timedelta(days=1)))
            # clamp to maintenance window
            m_start = max(m_start, start_d)
            m_end   = min(m_end,   end_d)

            ok_cnt = 0; warn_cnt = 0

            def _work(t):
                nonlocal ok_cnt, warn_cnt
                try:
                    warm = load_warm_history(master_engine, t, m_start, HISTORY_WARM_DAYS)
                    df = build_ticker_panel(t,
                                            m_start.strftime("%Y-%m-%d"),
                                            m_end.strftime("%Y-%m-%d"),
                                            astro,
                                            warm_df=warm)
                    if df is not None and not df.empty:
                        upsert_df(temp_engine, df, DB_TABLE)  # write to TEMP
                        ok_cnt += 1
                        return True, t, len(df)
                    else:
                        warn_cnt += 1
                        return False, t, "empty"
                except Exception as e:
                    warn_cnt += 1
                    return False, t, str(e)

            p_tickers = TQDM.bar(total=len(universe), desc=f"[{m:%Y-%m}] maint build", unit="stk",
                                 position=TQDM.POS_TICKERS, leave=False)
            try:
                with ThreadPoolExecutor(max_workers=min(8, (os.cpu_count() or 8)//2)) as ex:
                    futs = [ex.submit(_work, t) for t in universe]
                    for fut in as_completed(futs):
                        ok, t, info = fut.result()
                        if DEBUG and not ok:
                            TQDM.write(f"[WARN] {t}@{m:%Y-%m}: {info}")
                        p_tickers.update(1)
                        p_tickers.set_postfix(ok=ok_cnt, warn=warn_cnt, refresh=False)
            finally:
                p_tickers.close()

            p_months.update(1)
    finally:
        p_months.close()

    ensure_indices(temp_engine)

    # --- MERGE: replace master's last N trading days, and backfill any missing in [start_d..end_d] ---
    # 1) hard delete the master's recent dates
    _delete_master_dates(master_engine, recent_days)

    # 2) read rebuilt slice from temp and upsert into master
    rebuilt_slice = _read_temp_slice(temp_engine, start_d, end_d)
    if rebuilt_slice is not None and not rebuilt_slice.empty:
        upsert_df(master_engine, rebuilt_slice, DB_TABLE, allow_insert=True)
        ensure_indices(master_engine)
        if DEBUG:
            TQDM.write(f"[Maintenance] Temp DB: {temp_db_url}")
            TQDM.write(f"[Maintenance] Replaced {len(recent_days)} trading days and merged slice {start_d} → {end_d}.")
    else:
        if DEBUG: TQDM.write("[Maintenance] No rebuilt rows found in temp DB; nothing merged.")
# ========================== END MAINTENANCE REDESIGN ==========================

# --- small change to entrypoint to keep compatibility ---
def run_builder():
    path = _sqlite_file_path(DB_URL)
    initial_needed = True if (path is None) else (not os.path.exists(path))
    if initial_needed:
        TQDM.write("[Mode] Initial full build.")
        initial_full_build()
    else:
        TQDM.write("[Mode] Maintenance mode.")
        maintenance_run()  # now uses the redesigned flow above


# ============================ RUN ============================================
run_builder()
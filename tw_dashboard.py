#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股觀察儀表板(本機版 v4)
============================
執行方式:  python tw_dashboard_v4.py
瀏覽器會自動開啟 http://localhost:8765

v4 新增:
1. 自動更新間隔新增 30 秒 / 1 分鐘 / 5 分鐘。
2. 接上證交所 MIS 即時報價端點(約 5 秒延遲,證交所官網報價同源):
   盤中會顯示即時成交價與漲跌(自選股與加權指數),
   收盤後自動等同收盤價。若即時來源失敗,自動退回日資料,不影響使用。
   流量說明:每次更新對即時端點僅發 1 個請求(整個清單一次帶入),
   日資料另有 9 分鐘快取,即使 30 秒更新一次,流量仍極小。

沿用 v2/v3:詳細錯誤回報、SSL 降級重試、rwd 備援端點、持股損益、投組總覽。
僅使用 Python 標準函式庫。
"""

import json
import os
import sys
import re
import ssl
import time
import threading
import traceback
import webbrowser
import http.cookiejar
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

PORT = 8765
APP_VERSION = 30                 # 自動更新用的版本號(整數)
CACHE_SECONDS = 540      # 日資料快取
RT_CACHE_SECONDS = 15    # 即時報價快取
TW_TZ = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.twse.com.tw/",
}

OPENAPI = {
    "daily":     "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
    "index":     "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX",
    "valuation": "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL",
}

MIS_INDEX_PAGE = "https://mis.twse.com.tw/stock/index.jsp"
MIS_API = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

_cache = {}
_lock = threading.Lock()
_ssl_fallback_warned = False

_cookiejar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookiejar))
_opener_unverified = None
_mis_primed = False


def log(msg):
    line = f"[{datetime.now(TW_TZ).strftime('%m-%d %H:%M:%S')}] {msg}"
    if sys.stdout is not None:
        try:
            print(line, flush=True)
            return
        except Exception:
            pass
    try:    # 背景執行(pythonw)沒有主控台,改寫入程式資料夾的記錄檔
        with open(os.path.join(_app_dir(), "tw_dashboard.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _unverified_opener():
    global _opener_unverified
    if _opener_unverified is None:
        ctx = ssl._create_unverified_context()
        _opener_unverified = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx),
            urllib.request.HTTPCookieProcessor(_cookiejar))
    return _opener_unverified


def _open(req, timeout=30):
    """統一的開啟函式:帶 cookie、SSL 驗證失敗時自動降級重試。"""
    global _ssl_fallback_warned
    try:
        with _opener.open(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.URLError as e:
        if "CERTIFICATE_VERIFY_FAILED" in str(e):
            if not _ssl_fallback_warned:
                log("警告:SSL 憑證驗證失敗,改用不驗證模式重試")
                _ssl_fallback_warned = True
            with _unverified_opener().open(req, timeout=timeout) as resp:
                return resp.read()
        raise


def _dtimer(sec, fn, args=None):
    """背景計時器:不會拖住程式結束(程式要重新啟動或結束時,連接埠才能立即釋放)。"""
    t = threading.Timer(sec, fn, args=args or [])
    t.daemon = True
    return t


def _port_free(port):
    """確認連接埠真的可以使用(舊程式可能已停止回應,但還沒釋放連接埠)。"""
    import socket
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if os.name != "nt":
            sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sk.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sk.close()


def http_get(url, timeout=30, extra_headers=None):
    h = dict(HEADERS)
    if extra_headers:
        h.update(extra_headers)
    req = urllib.request.Request(url, headers=h)
    for attempt in (1, 2):
        try:
            return _open(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if attempt == 2 or e.code < 500:
                raise  # 4xx 為確定性錯誤,重試無意義
            log(f"連線暫時失敗(HTTP {e.code}),1 秒後自動重試:{url.split('/')[2]}")
            time.sleep(1)
        except Exception as e:
            if attempt == 2:
                raise
            log(f"連線暫時失敗({type(e).__name__}),1 秒後自動重試:{url.split('/')[2]}")
            time.sleep(1)


def strip_tags(s):
    return re.sub(r"<[^>]*>", "", str(s or "")).strip()


def parse_sign(cell):
    t = strip_tags(cell)
    if "-" in t:
        return "-"
    if "+" in t:
        return "+"
    return ""


# ── 日資料:OpenAPI 與 rwd 備援 ────────────────────────────
def from_openapi(kind):
    data = json.loads(http_get(OPENAPI[kind]).decode("utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError(f"OpenAPI {kind} 回傳格式異常")
    return data


def rwd_mi_index_raw():
    last_err = None
    for delta in range(7):
        d = (datetime.now(TW_TZ) - timedelta(days=delta)).strftime("%Y%m%d")
        url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
               f"?date={d}&type=ALLBUT0999&response=json")
        try:
            j = json.loads(http_get(url).decode("utf-8"))
            if j.get("stat") == "OK" and j.get("tables"):
                log(f"備援來源:使用 rwd MI_INDEX,交易日 {d}")
                return j
            last_err = ValueError(f"{d}: {j.get('stat')}")
        except Exception as e:
            last_err = e
        time.sleep(0.4)
    raise last_err or ValueError("rwd 端點無資料")


def from_rwd():
    j = rwd_mi_index_raw()
    index_rows, daily_rows = [], []

    for table in j.get("tables", []):
        fields = table.get("fields") or []
        rows = table.get("data") or []
        if not fields or not rows:
            continue

        if "指數" in fields and "收盤指數" in fields and not index_rows:
            fi = {f: i for i, f in enumerate(fields)}
            for r in rows:
                index_rows.append({
                    "指數": strip_tags(r[fi["指數"]]),
                    "收盤指數": strip_tags(r[fi["收盤指數"]]),
                    "漲跌(+/-)": parse_sign(r[fi.get("漲跌(+/-)", 2)]),
                    "漲跌點數": strip_tags(r[fi.get("漲跌點數", 3)]),
                    "漲跌百分比": strip_tags(r[fi.get("漲跌百分比", 4)]),
                })

        elif "證券代號" in fields and "收盤價" in fields:
            fi = {f: i for i, f in enumerate(fields)}
            for r in rows:
                sign = parse_sign(r[fi.get("漲跌(+/-)", 9)]) if "漲跌(+/-)" in fi else ""
                diff = strip_tags(r[fi.get("漲跌價差", 10)]) if "漲跌價差" in fi else ""
                diff = diff.replace(",", "")
                try:
                    change = float(diff) * (-1 if sign == "-" else 1)
                    change_s = f"{change:.4f}"
                except ValueError:
                    change_s = "0.0000"
                daily_rows.append({
                    "Code": strip_tags(r[fi["證券代號"]]),
                    "Name": strip_tags(r[fi["證券名稱"]]),
                    "TradeVolume": strip_tags(r[fi.get("成交股數", 2)]),
                    "OpeningPrice": strip_tags(r[fi.get("開盤價", 5)]),
                    "HighestPrice": strip_tags(r[fi.get("最高價", 6)]),
                    "LowestPrice": strip_tags(r[fi.get("最低價", 7)]),
                    "ClosingPrice": strip_tags(r[fi.get("收盤價", 8)]),
                    "Change": change_s,
                })

    if not daily_rows:
        raise ValueError("rwd 資料中找不到個股收盤行情表")
    return index_rows, daily_rows


# ── 上櫃(TPEx)日資料 ─────────────────────────────────────
TPEX_DAILY = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TPEX_VALUATION = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"


def _tpex_daily():
    rows = json.loads(http_get(TPEX_DAILY).decode("utf-8"))
    out = []
    for r in rows:
        code = str(r.get("SecuritiesCompanyCode") or r.get("Code") or "").strip()
        if not code:
            continue
        out.append({
            "Code": code,
            "Name": str(r.get("CompanyName") or r.get("Name") or "").strip(),
            "TradeVolume": str(r.get("TradingShares") or ""),
            "OpeningPrice": str(r.get("Open") or r.get("OpeningPrice") or ""),
            "HighestPrice": str(r.get("High") or r.get("HighestPrice") or ""),
            "LowestPrice": str(r.get("Low") or r.get("LowestPrice") or ""),
            "ClosingPrice": str(r.get("Close") or r.get("ClosingPrice") or ""),
            "Change": strip_tags(r.get("Change") or "0"),
        })
    if not out:
        raise ValueError("TPEx 日資料回傳格式異常")
    return out


def _tpex_valuation():
    rows = json.loads(http_get(TPEX_VALUATION).decode("utf-8"))
    out = []
    for r in rows:
        code = str(r.get("SecuritiesCompanyCode") or r.get("Code") or "").strip()
        if not code:
            continue
        out.append({
            "Code": code,
            "Name": str(r.get("CompanyName") or r.get("Name") or "").strip(),
            "PEratio": str(r.get("PriceEarningRatio") or r.get("PEratio") or ""),
            "DividendYield": str(r.get("YieldRatio") or r.get("DividendYield") or ""),
            "PBratio": str(r.get("PriceBookRatio") or r.get("PBratio") or ""),
        })
    return out


# ── 興櫃(Emerging)日資料:候選端點探測 + 欄位自動比對 ────
ESB_CANDIDATES = [
    "https://www.tpex.org.tw/openapi/v1/tpex_esb_daily_close_quotes",
    "https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics",
    "https://www.tpex.org.tw/openapi/v1/tpex_emerging_daily_close_quotes",
]

_KEYS = {
    "Code":   ["SecuritiesCompanyCode", "StockNo", "Code", "證券代號", "股票代號"],
    "Name":   ["CompanyName", "StockName", "Name", "證券名稱", "公司名稱", "名稱"],
    "Close":  ["Close", "ClosingPrice", "LatestPrice", "AveragePrice", "WeightedAveragePrice",
               "AvgPrice", "成交均價", "均價", "收盤", "最後成交價"],
    "Open":   ["Open", "OpeningPrice", "開盤"],
    "High":   ["High", "HighestPrice", "最高"],
    "Low":    ["Low", "LowestPrice", "最低"],
    "Volume": ["TradingShares", "TradeVolume", "TradingVolume", "Volume", "成交股數"],
    "Change": ["Change", "漲跌", "漲跌價差"],
}


def _pick(row, keys):
    lower = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        v = lower.get(k.lower())
        if v not in (None, ""):
            return str(v)
    return ""


def _normalize_generic(rows):
    if not isinstance(rows, list):
        return []
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        code = _pick(r, _KEYS["Code"]).strip()
        close = strip_tags(_pick(r, _KEYS["Close"]))
        try:
            ok = bool(code) and float(close.replace(",", "")) > 0
        except ValueError:
            ok = False
        if not ok:
            continue
        out.append({
            "Code": code,
            "Name": _pick(r, _KEYS["Name"]).strip(),
            "TradeVolume": strip_tags(_pick(r, _KEYS["Volume"])),
            "OpeningPrice": strip_tags(_pick(r, _KEYS["Open"])),
            "HighestPrice": strip_tags(_pick(r, _KEYS["High"])),
            "LowestPrice": strip_tags(_pick(r, _KEYS["Low"])),
            "ClosingPrice": close,
            "Change": strip_tags(_pick(r, _KEYS["Change"])),
        })
    return out


def _tpex_emerging():
    last_err = None
    for url in ESB_CANDIDATES:
        try:
            rows = json.loads(http_get(url).decode("utf-8"))
            norm = _normalize_generic(rows)
            if norm:
                log(f"興櫃:使用 {url.rsplit('/', 1)[-1]},共 {len(norm)} 筆")
                return norm
            log(f"興櫃:{url.rsplit('/', 1)[-1]} 可連線但無可用欄位,嘗試下一個")
        except Exception as e:
            log(f"興櫃:{url.rsplit('/', 1)[-1]} 失敗({type(e).__name__}: {e}),嘗試下一個")
            last_err = e
    raise last_err or ValueError("所有興櫃候選端點皆無資料")


def _extend_tpex(kind, data):
    """把上櫃資料併入上市清單;失敗時僅記錄、不影響上市資料。"""
    try:
        extra = _tpex_daily() if kind == "daily" else _tpex_valuation()
        log(f"{kind}:已合併上櫃資料 {len(extra)} 筆")
        data = list(data) + extra
    except Exception as e:
        log(f"{kind}:上櫃資料取得失敗(僅顯示上市):{e}")
        data = list(data)
    if kind == "daily":
        try:
            esb = _tpex_emerging()
            known = {r.get("Code") for r in data}
            esb = [r for r in esb if r["Code"] not in known]
            data += esb
        except Exception as e:
            log(f"興櫃資料取得失敗(不影響上市/上櫃):{e}")
    return data


def get_data(kind):
    with _lock:
        hit = _cache.get(kind)
        if hit and time.time() - hit[0] < CACHE_SECONDS:
            return hit[1]

    errors = []
    try:
        data = from_openapi(kind)
        if kind in ("daily", "valuation"):
            data = _extend_tpex(kind, data)
        with _lock:
            _cache[kind] = (time.time(), data)
        return data
    except Exception as e:
        errors.append(f"OpenAPI: {type(e).__name__}: {e}")
        log(f"{kind} OpenAPI 失敗:{e}")

    if kind in ("daily", "index"):
        try:
            index_rows, daily_rows = from_rwd()
            daily_rows = _extend_tpex("daily", daily_rows)
            now = time.time()
            with _lock:
                _cache["index"] = (now, index_rows)
                _cache["daily"] = (now, daily_rows)
            return index_rows if kind == "index" else daily_rows
        except Exception as e:
            errors.append(f"rwd備援: {type(e).__name__}: {e}")
            log(f"{kind} rwd 備援也失敗:{e}")
            traceback.print_exc()
    else:
        errors.append("valuation 無備援來源,略過")

    raise RuntimeError(" | ".join(errors))


# ── 即時報價:MIS 端點 ─────────────────────────────────────
def get_realtime(codes):
    """codes: list,例如 ["2330","2317","t00"](t00 = 加權指數)"""
    global _mis_primed, _watch_codes, _watch_seen
    key = "rt:" + ",".join(sorted(codes))
    if _watch_codes != set(codes):
        _watch_codes = set(codes)
    _watch_seen = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < RT_CACHE_SECONDS:
            return hit[1]

    if not _mis_primed:
        try:
            http_get(MIS_INDEX_PAGE, timeout=15,
                     extra_headers={"Accept": "text/html"})
        except Exception:
            pass
        _mis_primed = True

    ex_parts = []
    for c in codes:
        if c == "t00":
            ex_parts.append("tse_t00.tw")        # 加權指數
        elif c == "o00":
            ex_parts.append("otc_o00.tw")        # 櫃買指數
        else:
            ex_parts.append(f"tse_{c}.tw")       # 同時查上市與上櫃,
            ex_parts.append(f"otc_{c}.tw")       # MIS 只會回傳存在的那個
    ex_ch = "|".join(ex_parts)
    url = (f"{MIS_API}?ex_ch={urllib.parse.quote(ex_ch, safe='|._')}"
           f"&json=1&delay=0&_={int(time.time() * 1000)}")
    raw = http_get(url, extra_headers={"Referer": MIS_INDEX_PAGE})
    j = json.loads(raw.decode("utf-8"))
    arr = j.get("msgArray") or []
    with _lock:
        _cache[key] = (time.time(), arr)
        _record_rt(arr)
    return arr


# ── 今日分時(Yahoo 1 分 K 為主,本機記錄為備援)────────────────
INTRA_CACHE_SECONDS = 60
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1m&range=1d"
_yahoo_suffix = {}      # 代號 -> ".TW" 或 ".TWO"(記住成功過的市場)
_intraday_rec = {}      # 代號 -> {"date", "prev", "pts": {"HH:MM": 價格}}


def _mis_num(v):
    try:
        p = float(str(v).replace(",", ""))
        return p if p > 0 else None
    except (TypeError, ValueError):
        return None


def mis_price(it):
    """證交所即時資料的價格判讀。回傳 (價格, 標記)。
    成交價(z)常在漲跌停鎖死、或短時間沒有成交時顯示為「-」:
      跌停鎖死 → 沒有買單,只剩跌停價的賣單 → 取最佳賣價
      漲停鎖死 → 沒有賣單,只剩漲停價的買單 → 取最佳買價"""
    p = _mis_num(it.get("z")) or _mis_num(it.get("pz"))
    if not p:
        first = lambda s: next((x for x in (_mis_num(v) for v in str(s or "").split("_")) if x), None)
        b, a = first(it.get("b")), first(it.get("a"))
        p = b or a
    if not p:
        return None, ""
    up, dn = _mis_num(it.get("u")), _mis_num(it.get("w"))
    if dn and abs(p - dn) < 1e-6:
        return p, "跌停"
    if up and abs(p - up) < 1e-6:
        return p, "漲停"
    return p, ""


def _rt_item_price(it):
    return mis_price(it)[0]


_mis_trade_date = None   # 證交所即時報價上的交易日(YYYYMMDD),用來判斷今天是否開市


def _in_session(now=None):
    now = now or datetime.now(TW_TZ)
    return now.weekday() < 5 and "09:00" <= now.strftime("%H:%M") <= "13:35"


def _record_rt(arr):
    """把即時報價寫入當日分時記錄(僅交易時段、僅當日成交)。呼叫端需持有 _lock。"""
    global _mis_trade_date
    now = datetime.now(TW_TZ)
    hm = now.strftime("%H:%M")
    today_c = now.strftime("%Y%m%d")
    for it in arr:
        if it.get("d"):
            _mis_trade_date = str(it["d"])
            break
    if not _in_session(now):
        return
    today = now.strftime("%Y-%m-%d")
    for it in arr:
        if it.get("d") and str(it["d"]) != today_c:     # 假日:證交所仍回傳上一交易日資料,不記錄
            continue
        code = it.get("c")
        p = _rt_item_price(it)
        try:
            y = float(str(it.get("y")).replace(",", ""))
        except (TypeError, ValueError):
            y = None
        if not code or p is None or not y:
            continue
        t = str(it.get("t") or "")[:5] or hm
        rec = _intraday_rec.get(code)
        if not rec or rec["date"] != today:
            rec = {"date": today, "prev": y, "pts": {}}
            _intraday_rec[code] = rec
        rec["pts"][t] = p


YAHOO_TIMEOUT = 8
YAHOO_PAUSE_SECONDS = 600
_yahoo_down_until = 0.0


def _market_of(code):
    """從即時報價快取判斷上市(tse)或上櫃(otc)。"""
    with _lock:
        for k, (ts, arr) in list(_cache.items()):
            if not k.startswith("rt:"):
                continue
            for it in arr or []:
                if it.get("c") == code and it.get("ex") in ("tse", "otc"):
                    return it["ex"]
    return None


def _yahoo_symbols(code):
    if code == "t00":
        return ["^TWII"]
    if code == "o00":
        return ["^TWOII"]
    s = _yahoo_suffix.get(code)
    if s:
        return [code + s]
    ex = _market_of(code)
    if ex == "tse":
        return [code + ".TW"]
    if ex == "otc":
        return [code + ".TWO"]
    return [code + ".TW", code + ".TWO"]


def _yahoo_intraday(code):
    global _yahoo_down_until
    if time.time() < _yahoo_down_until:
        raise ConnectionError("Yahoo 近期連線失敗,暫停嘗試中")
    last = None
    syms = _yahoo_symbols(code)
    if code not in ("t00", "o00"):                 # 轉上市/上櫃後舊快取失效時的備援
        syms += [x for x in (code + ".TW", code + ".TWO") if x not in syms]
    for sym in syms:
        url = YAHOO_CHART.format(sym=urllib.parse.quote(sym))
        h = dict(HEADERS)
        h.update({"Referer": "https://finance.yahoo.com/", "Accept": "application/json"})
        try:
            raw = _open(urllib.request.Request(url, headers=h), timeout=YAHOO_TIMEOUT)
            j = json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:          # 代號不在這個市場,試下一個
                if _yahoo_suffix.get(code) and sym == code + _yahoo_suffix[code]:
                    _yahoo_suffix.pop(code, None)
                last = e
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            _yahoo_down_until = time.time() + YAHOO_PAUSE_SECONDS
            raise ConnectionError(f"Yahoo 無法連線({type(e).__name__}),"
                                  f"{YAHOO_PAUSE_SECONDS // 60} 分鐘內改用本機記錄") from e
        except ValueError as e:        # 回應不是 JSON
            last = e
            continue
        res = ((j.get("chart") or {}).get("result") or [None])[0]
        if not res:
            last = ValueError(f"Yahoo 查無 {sym}")
            continue
        meta = res.get("meta") or {}
        ts = res.get("timestamp") or []
        quote = ((res.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        pts, date = {}, None
        for t, c in zip(ts, closes):
            if c is None:
                continue
            dt = datetime.fromtimestamp(t, TW_TZ)
            date = dt.strftime("%Y-%m-%d")
            pts[dt.strftime("%H:%M")] = round(float(c), 4)
        prev = meta.get("previousClose") or meta.get("chartPreviousClose")
        if pts and prev:
            if code not in ("t00", "o00"):
                _yahoo_suffix[code] = sym[len(code):]
            return {"prev": float(prev), "date": date, "source": "yahoo",
                    "points": [{"t": k, "p": v} for k, v in sorted(pts.items())]}
        last = ValueError(f"Yahoo {sym} 無分時資料")
    raise last or ValueError("Yahoo 無資料")


def get_intraday(code):
    """Yahoo 台股分時約延遲 20 分鐘:前段用 Yahoo,之後用本機即時記錄補到最新。"""
    ykey = "yahoo:" + code
    with _lock:
        hit = _cache.get(ykey)
    if hit and time.time() - hit[0] < INTRA_CACHE_SECONDS:
        yres, err = hit[1]
    else:
        yres, err = None, ""
        try:
            yres = _yahoo_intraday(code)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            log(f"今日分時 {code}:Yahoo 失敗({err}),改用本機記錄")
        with _lock:
            _cache[ykey] = (time.time(), (yres, err))

    today = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    with _lock:
        rec = _intraday_rec.get(code)
        rec = dict(rec, pts=dict(rec["pts"])) if rec else None
    rec_today = rec if rec and rec["date"] == today and rec["pts"] else None

    if yres and yres.get("date") == today:
        result = dict(yres, points=list(yres["points"]))
        if rec_today:
            last_t = result["points"][-1]["t"] if result["points"] else "00:00"
            extra = [{"t": k, "p": v} for k, v in sorted(rec_today["pts"].items()) if k > last_t]
            if extra:
                result["points"] += extra
                result["source"] = "yahoo+recorder"
        return result

    if rec_today:                       # Yahoo 還沒有今天的資料(開盤後約 20 分鐘內)
        return {"prev": rec_today["prev"], "date": today, "source": "recorder",
                "points": [{"t": k, "p": v} for k, v in sorted(rec_today["pts"].items())]}

    open_today = _mis_trade_date == datetime.now(TW_TZ).strftime("%Y%m%d")
    if _in_session() and open_today:
        raise RuntimeError("已開盤,今日走勢資料累積中,約 1 分鐘後會開始出現。")

    if yres:                            # 開盤前、收盤後隔日、休市日:顯示最近一個交易日
        return dict(yres, points=list(yres["points"]), previous_day=True)

    raise RuntimeError(
        "目前沒有分時資料。Yahoo 來源無法連線" + (f"({err})" if err else "") +
        ";交易時段內程式會每分鐘自動記錄,開盤後約 1 分鐘即可看到今日走勢。")


# ── 歷史日收盤(供曲線圖使用)──────────────────────────────
HIST_CACHE_SECONDS = 6 * 3600   # 日資料每天只變一次,快取 6 小時


def _norm_date(s):
    """民國或西元日期字串 -> YYYY-MM-DD;無法解析回傳 None。"""
    t = strip_tags(s).replace("*", "").strip()
    m = re.match(r"^(\d{2,4})/(\d{1,2})/(\d{1,2})$", t)
    if not m:
        return None
    y = int(m.group(1))
    if y < 1911:            # 民國年
        y += 1911
    return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _recent_months(n):
    """回傳最近 n 個月的 (年, 月),由舊到新。"""
    now = datetime.now(TW_TZ)
    y, m, out = now.year, now.month, []
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return list(reversed(out))


def _tables_of(j):
    """把各種回應格式統一成 [(fields, rows), ...]。"""
    if not isinstance(j, dict):
        return []
    if isinstance(j.get("tables"), list):
        return [(t.get("fields"), t.get("data") or []) for t in j["tables"]]
    if isinstance(j.get("aaData"), list):
        return [(j.get("fields"), j["aaData"])]
    if isinstance(j.get("data"), list):
        return [(j.get("fields"), j["data"])]
    return []


def _hist_points(j):
    """從回應中抽出 [{d: 日期, c: 收盤}],找不到則回傳 []。"""
    for fields, rows in _tables_of(j):
        if not rows:
            continue
        di, ci = 0, None
        if fields:
            for i, f in enumerate(fields):
                fs = str(f)
                if "日期" in fs:
                    di = i
                elif ci is None and "收盤" in fs:
                    ci = i
        if ci is None:
            # 常見排列:日期,成交量,成交金額,開,高,低,收 -> 索引 6
            ci = 6 if len(rows[0]) > 6 else len(rows[0]) - 1
        pts = []
        for r in rows:
            if not isinstance(r, (list, tuple)) or len(r) <= max(di, ci):
                continue
            d = _norm_date(r[di])
            try:
                c = float(strip_tags(r[ci]).replace(",", ""))
            except ValueError:
                continue
            if d and c > 0:
                pts.append({"d": d, "c": c})
        if pts:
            return pts
    return []


def _hist_urls(code, y, m):
    """依代號類型產生候選網址(依序嘗試)。"""
    ym, roc = f"{y}{m:02d}01", f"{y - 1911}/{m:02d}"
    if code == "t00":       # 加權指數
        return [f"https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={ym}&response=json"]
    if code == "o00":       # 櫃買指數
        return [
            f"https://www.tpex.org.tw/www/zh-tw/indices/summary?date={y}/{m:02d}/01&response=json",
            f"https://www.tpex.org.tw/web/stock/iNdex_info/histoy_index/history_index_result.php?d={roc}",
        ]
    return [                # 個股:先上市,再上櫃
        f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={ym}&stockNo={code}&response=json",
        f"https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock?code={code}&date={y}/{m:02d}/01&id=&response=json",
        f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?d={roc}&stkno={code}",
    ]


def get_history(code, months):
    key = f"hist:{code}:{months}"
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < HIST_CACHE_SECONDS:
            return hit[1]

    seen, points, tried = set(), [], []
    for y, m in _recent_months(months):
        got = False
        for url in _hist_urls(code, y, m):
            try:
                pts = _hist_points(json.loads(http_get(url).decode("utf-8")))
            except Exception as e:
                tried.append(f"{url.split('/')[2]}:{type(e).__name__}")
                continue
            if pts:
                for p in pts:
                    if p["d"] not in seen:
                        seen.add(p["d"])
                        points.append(p)
                got = True
                break
        if not got:
            tried.append(f"{y}/{m:02d}:無資料")
        time.sleep(0.3)     # 對來源放慢,避免被視為異常流量

    points.sort(key=lambda p: p["d"])
    if not points:
        raise RuntimeError("查無歷史資料(" + ", ".join(tried[:4]) + ")")
    log(f"歷史資料 {code}:{months} 個月,共 {len(points)} 個交易日")
    with _lock:
        _cache[key] = (time.time(), points)
    return points


HTML = r"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>台股觀察</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Noto+Sans+TC:wght@400;500;700&display=swap" rel="stylesheet">
<script>
(function () {
  var t = null;
  try { t = localStorage.getItem("tw-theme"); } catch (e) {}
  if (t !== "light" && t !== "dark") {
    t = (window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches) ? "dark" : "light";
  }
  document.documentElement.setAttribute("data-theme", t);
})();
</script>
<style>
  :root {
    --bg:#F3F4F6; --surface:#FFFFFF; --surface-2:#EEF1F4; --line:#DCE1E8;
    --text:#18202B; --muted:#596575; --faint:#8D98A6;
    --accent:#2F5D8A; --accent-ink:#FFFFFF;
    --up:#C8372D; --down:#1F8556; --flat:#6B7785;
    --up-soft:rgba(200,55,45,.10); --down-soft:rgba(31,133,86,.10);
    --shadow:0 1px 2px rgba(16,24,40,.06);
    color-scheme:light;
  }
  :root[data-theme="dark"] {
    --bg:#14181D; --surface:#1C2128; --surface-2:#242B34; --line:#303843;
    --text:#E4E8ED; --muted:#9AA5B1; --faint:#6B7682;
    --accent:#8AAFD3; --accent-ink:#14181D;
    --up:#E06B60; --down:#52B48A; --flat:#8C97A3;
    --up-soft:rgba(224,107,96,.14); --down-soft:rgba(82,180,138,.14);
    --shadow:none;
    color-scheme:dark;
  }
  * { box-sizing:border-box; }
  html, body { height:100%; }
  body { margin:0; background:var(--bg); color:var(--text); font-size:15px; line-height:1.45;
         font-family:'Noto Sans TC','PingFang TC','Microsoft JhengHei',sans-serif; }
  .mono { font-family:'IBM Plex Mono',ui-monospace,Consolas,monospace; font-variant-numeric:tabular-nums; }
  .wrap { display:flex; flex-direction:column; min-height:100vh; padding:12px clamp(12px,1.6vw,28px) 12px; }
  button, input, select { font-family:inherit; color:inherit; }
  :focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
  [hidden] { display:none !important; }

  /* 標題列 */
  .top { display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:8px 20px; margin-bottom:10px; }
  .title { display:flex; flex-wrap:wrap; align-items:baseline; gap:6px 14px; }
  .title h1 { font-size:21px; font-weight:700; margin:0; letter-spacing:.5px; }
  .title .meta { font-size:13px; color:var(--muted); display:flex; gap:10px; align-items:center; }
  .badge { font-size:12px; border:1px solid var(--line); border-radius:10px; padding:1px 9px; color:var(--faint); }
  .badge.live { color:var(--accent); border-color:var(--accent); font-weight:700; }
  .controls { display:flex; flex-wrap:wrap; align-items:center; gap:8px; }
  .field { display:flex; align-items:center; gap:6px; font-size:13px; color:var(--muted); }
  select, .inp { background:var(--surface); border:1px solid var(--line); border-radius:6px; padding:6px 9px; font-size:13.5px; }
  .seg { display:inline-flex; border:1px solid var(--line); border-radius:6px; overflow:hidden; background:var(--surface); }
  .seg button { background:transparent; border:none; border-right:1px solid var(--line); padding:6px 12px;
    font-size:13px; color:var(--muted); cursor:pointer; white-space:nowrap; }
  .seg button:last-child { border-right:none; }
  .seg button.on { background:var(--accent); color:var(--accent-ink); font-weight:700; }
  .btn { background:var(--surface); border:1px solid var(--accent); color:var(--accent); border-radius:6px;
    padding:6px 13px; font-size:13px; font-weight:700; cursor:pointer; white-space:nowrap; }
  .btn:hover { background:var(--surface-2); }
  .btn:disabled { opacity:.5; cursor:wait; }
  .btn.solid, .btn.on { background:var(--accent); color:var(--accent-ink); }

  #error-box { display:none; border:1px solid var(--up); background:var(--up-soft); border-radius:8px;
    padding:8px 14px; font-size:13.5px; margin-bottom:10px; line-height:1.6; word-break:break-all; }

  /* 跑馬燈:整條連續向左捲動,內容重複兩份以無縫循環;滑鼠移入暫停 */
  .ticker { position:relative; overflow:hidden; height:36px; flex:none; margin-bottom:10px;
    border:1px solid var(--line); border-radius:8px; background:var(--surface);
    -webkit-mask-image:linear-gradient(90deg, transparent 0, #000 32px, #000 calc(100% - 32px), transparent 100%);
            mask-image:linear-gradient(90deg, transparent 0, #000 32px, #000 calc(100% - 32px), transparent 100%); }
  .ticker-track { display:inline-flex; height:100%; white-space:nowrap; will-change:transform;
    animation:tick var(--tick-dur, 90s) linear infinite; }
  .ticker:hover .ticker-track, .ticker:focus-within .ticker-track { animation-play-state:paused; }
  @keyframes tick { to { transform:translateX(-50%); } }
  .tk { display:inline-flex; align-items:center; gap:8px; height:100%; padding:0 16px; background:none; border:none;
    border-right:1px solid var(--line); cursor:pointer; font-size:13.5px; }
  .tk:hover { background:var(--surface-2); }
  .tk.sel { box-shadow:inset 0 -2px 0 var(--accent); }
  .tk .n { color:var(--muted); font-weight:600; }
  .tk .p { font-weight:600; }
  .tk .c { font-size:12.5px; }
  /* 靜止:不自動捲動,可用滑鼠滾輪或捲軸左右瀏覽 */
  .ticker.still { overflow-x:auto; scrollbar-width:thin; -webkit-mask-image:none; mask-image:none; }
  .ticker.still .ticker-track { animation:none; }
  .ticker.still .tk[aria-hidden="true"] { display:none; }

  /* 面板 */
  .panel { background:var(--surface); border:1px solid var(--line); border-radius:10px; box-shadow:var(--shadow); margin-bottom:12px; }
  .panel-head { display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:8px 10px;
    padding:9px 14px; border-bottom:1px solid var(--line); }
  .panel-head h2 { font-size:15px; margin:0; font-weight:700; }
  .hint { font-size:12.5px; color:var(--faint); font-weight:400; margin-left:8px; }
  .tag2 { font-size:11.5px; color:var(--faint); font-weight:400; }
  /* 漲停紅底白字、跌停綠底白字(與下單系統相同) */
  .lamp { display:inline-block; border-radius:4px; padding:0 6px; color:#fff !important; font-weight:700; }
  .lamp.up { background:var(--up); }
  .lamp.down { background:var(--down); }
  .update-bar { display:flex; align-items:center; gap:10px; flex-wrap:wrap; padding:7px 14px; margin-bottom:10px;
    border:1px solid var(--accent); border-radius:8px; background:var(--surface); font-size:13.5px; flex:none; }
  .update-bar #update-text { flex:1; min-width:220px; }

  /* 市場總覽:窄畫面為可換行的小方塊,寬畫面為左側直欄清單 */
  #mkt { display:grid; grid-template-columns:repeat(auto-fill, minmax(150px, 1fr)); gap:6px; padding:8px; }
  .mk-group { display:contents; }
  .mk-title { display:none; }
  .mk { min-width:0; overflow:hidden; display:flex; flex-direction:column; gap:1px; text-align:left;
    background:var(--surface); border:1px solid var(--line); border-radius:7px; padding:6px 10px; cursor:pointer; font-size:15px; }
  .mk:hover { border-color:var(--muted); }
  .mk.sel { border-color:var(--accent); box-shadow:inset 3px 0 0 var(--accent); }
  .mk .nm { font-size:12.5px; color:var(--muted); white-space:nowrap; }
  .mk .tg { font-size:11px; color:var(--faint); }
  .mk .r2 { display:flex; align-items:baseline; gap:8px; white-space:nowrap; }
  .mk .pr { font-weight:600; }
  .mk .ch { font-size:12px; }

  /* 投組摘要 */
  .summary { display:grid; grid-template-columns:repeat(3, minmax(0,1fr)); gap:12px; margin-bottom:12px; }
  .summary:empty { display:none; }
  .pf { background:var(--surface-2); border:1px solid var(--line); border-radius:8px; padding:8px 14px; min-width:0; }
  .pf .lbl { font-size:12.5px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .pf .big { font-size:21px; font-weight:600; line-height:1.2; white-space:nowrap; }
  .pf .sub { font-size:12.5px; color:var(--faint); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

  /* 走勢圖 */
  .chart-cap { padding:8px 14px 0; font-size:13.5px; color:var(--muted); min-height:30px;
    display:flex; flex-wrap:wrap; gap:2px 14px; align-items:baseline; }
  .chart-cap .nm { color:var(--text); font-weight:700; font-size:14.5px; }
  .chart-cap .px { color:var(--text); font-size:18px; font-weight:600; }
  .chart-cap .detail { flex-basis:100%; display:flex; flex-wrap:wrap; gap:2px 16px; font-size:13px; }
  #chart-area { height:262px; padding:4px 8px 8px; overflow:hidden; }
  #chart-area svg { display:block; }
  .chart-msg { height:100%; display:flex; align-items:center; justify-content:center; color:var(--faint);
    font-size:13.5px; text-align:center; padding:0 20px; line-height:1.7; }

  /* 自選股 */
  .wl-panel { container-type:inline-size; container-name:wl; }
  .tbl-wrap { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; font-size:14px; min-width:600px; }
  th { padding:8px 10px; text-align:right; font-size:12.5px; font-weight:600; color:var(--muted);
    border-bottom:1px solid var(--line); white-space:nowrap; background:var(--surface); }
  th:first-child, td:first-child { text-align:left; padding-left:14px; }
  td { padding:7px 9px; text-align:right; border-bottom:1px solid var(--surface-2); white-space:nowrap; }
  tbody tr { cursor:pointer; }
  @media (hover:hover) { tbody tr:hover { background:var(--surface-2); } }
  tbody tr.sel { background:var(--surface-2); box-shadow:inset 3px 0 0 var(--accent); }
  .code { color:var(--accent); font-weight:600; }
  .nm2 { display:block; font-size:13px; font-weight:500; max-width:150px; overflow:hidden; text-overflow:ellipsis; }
  .sm { font-size:12.5px; margin-top:1px; }
  .warn { white-space:normal; color:var(--up); max-width:200px; }
  .hold { background:var(--surface-2); border:1px solid var(--line); border-radius:5px; padding:5px 7px;
    width:74px; text-align:right; font-size:13.5px; }
  .hold::-webkit-outer-spin-button, .hold::-webkit-inner-spin-button { -webkit-appearance:none; margin:0; }
  .hold[type=number] { -moz-appearance:textfield; }
  .rm { background:none; border:none; color:var(--faint); cursor:pointer; font-size:14px; padding:2px 6px; }
  .rm:hover { color:var(--up); }
  .add { display:flex; gap:8px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }
  .add .inp { width:190px; }
  #add-error { font-size:12.5px; color:var(--up); }
  .search { position:relative; }
  .suggest { position:absolute; right:0; top:calc(100% + 4px); width:310px; background:var(--surface);
    border:1px solid var(--line); border-radius:8px; box-shadow:0 8px 24px rgba(0,0,0,.14);
    z-index:20; display:none; max-height:340px; overflow:auto; }
  .suggest.open { display:block; }
  .sg { display:flex; gap:10px; align-items:baseline; padding:8px 12px; cursor:pointer; font-size:14px; }
  .sg.on, .sg:hover { background:var(--surface-2); }
  .sg .c { color:var(--accent); font-weight:600; min-width:64px; }
  .sg mark { background:transparent; color:var(--accent); font-weight:700; }
  .sg .tag { margin-left:auto; font-size:12px; color:var(--faint); white-space:nowrap; }
  .sg-empty { padding:10px 12px; color:var(--faint); font-size:13px; }

  @container wl (max-width:700px) {
    .wl-panel table { font-size:13.5px; }
    .wl-panel th, .wl-panel td { padding-left:6px; padding-right:6px; }
    .wl-panel th:first-child, .wl-panel td:first-child { padding-left:12px; }
    .wl-panel .hold { width:68px; padding:5px 5px; font-size:12.5px; }
    .wl-panel .nm2 { max-width:88px; }
    .wl-panel .sm { font-size:12px; }
    .wl-panel .rm { padding:2px 3px; }
    .wl-panel .add .inp { width:150px; }
  }
  .wl-foot { border-top:1px solid var(--line); flex:none; }
  details.settings { padding:7px 14px; font-size:13px; color:var(--muted); }
  details.settings + details.settings { border-top:1px solid var(--surface-2); }
  details.settings summary { cursor:pointer; font-weight:600; color:var(--text); }
  .set-row { display:flex; flex-wrap:wrap; gap:8px 18px; align-items:center; margin-top:8px; }
  .set-row label { display:flex; gap:6px; align-items:center; cursor:pointer; }
  .set-row .inp { width:72px; text-align:right; padding:4px 8px; }
  .note { color:var(--faint); font-size:12.5px; margin-top:6px; line-height:1.65; }

  /* ── 一頁式版面:畫面夠寬夠高時,整個儀表板固定在一個畫面內,不需要捲動 ── */
  @media (min-width:1180px) and (min-height:600px) {
    body { overflow:hidden; }
    .wrap { height:100vh; min-height:0; }
    .board { flex:1; min-height:0; display:grid; gap:12px;
      grid-template-columns:minmax(0, 1.1fr) minmax(640px, 0.9fr); grid-template-rows:auto minmax(0, 1fr); }
    .board > * { min-height:0; min-width:0; }
    .panel { margin-bottom:0; }
    .mkt-panel { grid-column:1 / -1; }
    #mkt { display:flex; flex-wrap:wrap; gap:8px 14px; padding:8px 10px; }
    .mk-group { display:flex; gap:6px; flex:1 1 auto; align-items:stretch; }
    .mk-title { display:flex; align-items:center; font-size:12px; font-weight:700; color:var(--faint);
      white-space:nowrap; padding:0 2px 0 4px; }
    .mk { flex:1 1 0; min-width:112px; padding:5px 8px; }
    .mk .r2 { gap:6px; }
    .mk .pr { font-size:14.5px; }
    .mk .ch { font-size:11.5px; }
    .mid { display:flex; flex-direction:column; gap:12px; }
    .summary { margin-bottom:0; flex:none; }
    .chart-panel { flex:1; min-height:0; display:flex; flex-direction:column; }
    #chart-area { flex:1; min-height:160px; height:auto; }
    .wl-panel { display:flex; flex-direction:column; }
    .tbl-wrap { flex:1; min-height:0; overflow:auto; }
    table { min-width:0; }
    thead th { position:sticky; top:0; z-index:1; }
  }
  @media (min-width:1180px) and (min-height:600px) and (max-height:720px) {
    .wrap { padding-top:8px; padding-bottom:8px; }
    .top { margin-bottom:6px; }
    .title h1 { font-size:18px; }
    .ticker { height:30px; margin-bottom:8px; }
    .board, .mid { gap:8px; }
    .pf { padding:4px 12px; }
    .pf .big { font-size:17px; }
    .pf .sub { display:none; }
    .panel-head { padding:6px 12px; }
    .mk { padding:4px 8px; font-size:14px; }
    td { padding-top:5px; padding-bottom:5px; }
  }
  @media (prefers-reduced-motion: reduce) { * { transition:none !important; } }
</style>
</head>
<body>
<div class="wrap">
  <header class="top">
    <div class="title">
      <h1>台股觀察</h1>
      <div class="meta"><span id="rt-badge" class="badge">收盤資料</span><span id="fetched-at" class="mono">尚未更新</span></div>
    </div>
    <div class="controls">
      <label class="field">自動更新
        <select id="auto-select">
          <option value="0">關閉</option>
          <option value="0.5">每 30 秒</option>
          <option value="1">每 1 分鐘</option>
          <option value="5">每 5 分鐘</option>
          <option value="10">每 10 分鐘</option>
          <option value="30" selected>每 30 分鐘</option>
          <option value="60">每 60 分鐘</option>
          <option value="120">每 120 分鐘</option>
        </select>
      </label>
      <div class="seg" id="theme-seg" role="group" aria-label="配色">
        <button data-theme="light">淺色</button><button data-theme="dark">深色</button>
      </div>
      <select id="ticker-mode" aria-label="跑馬燈" title="行情跑馬燈">
        <option value="run">跑馬燈:捲動</option>
        <option value="still">跑馬燈:靜止</option>
        <option value="off">跑馬燈:關閉</option>
      </select>
      <button class="btn" id="mode-btn" title="改用永遠置頂、可調透明度的小工具視窗">小工具模式</button>
      <button class="btn" id="quit-btn" title="結束程式">結束</button>
      <button class="btn" id="refresh-btn">重新整理</button>
    </div>
  </header>

  <div class="update-bar" id="update-bar" hidden>
    <span id="update-text"></span>
    <button class="btn solid" id="update-go">立即更新</button>
    <button class="btn" id="update-later">稍後</button>
  </div>
  <div class="ticker" id="ticker" aria-label="行情跑馬燈"><div class="ticker-track" id="ticker-track"></div></div>
  <div id="error-box" role="alert"></div>

  <div class="board">
    <aside class="panel mkt-panel" aria-label="市場總覽"><div id="mkt"></div></aside>

    <div class="mid">
      <section class="summary" id="summary" aria-label="投組摘要"></section>
      <section class="panel chart-panel" aria-label="走勢圖">
        <div class="panel-head">
          <h2>走勢圖<span class="hint">點選市場、跑馬燈或自選股切換</span></h2>
          <div class="seg" id="range-seg" role="group" aria-label="期間">
            <button data-range="today" class="on">今日</button>
            <button data-range="1">1 個月</button>
            <button data-range="3">3 個月</button>
            <button data-range="6">6 個月</button>
            <button data-range="12">1 年</button>
          </div>
        </div>
        <div class="chart-cap" id="chart-cap"></div>
        <div id="chart-area"></div>
      </section>
    </div>

    <section class="panel wl-panel" aria-label="自選股">
      <div class="panel-head">
        <div class="seg" id="wl-tabs" role="tablist" aria-label="自選股市場">
          <button data-t="tw" class="on">台股自選 <span id="wl-count" class="mono"></span></button>
          <button data-t="us">美股自選 <span id="us-count" class="mono"></span></button>
        </div>
        <div class="add">
          <span id="add-error"></span>
          <div class="search">
            <input class="inp" id="code-input" placeholder="代號或名稱,如 台積電" maxlength="20" autocomplete="off"
              aria-label="輸入股票代號或名稱" aria-autocomplete="list" aria-controls="suggest">
            <div class="suggest" id="suggest" role="listbox"></div>
          </div>
          <button class="btn solid" id="add-btn">加入</button>
        </div>
      </div>
      <div class="tbl-wrap">
        <table>
          <thead id="wl-thead"></thead>
          <tbody id="stock-tbody"></tbody>
        </table>
      </div>
      <div class="wl-foot">
        <details class="settings">
          <summary>損益計算設定</summary>
          <div class="set-row">
            <label>手續費折扣 <input class="inp mono" id="fee-disc" type="number" min="0.1" max="1" step="0.01"></label>
            <span class="note" style="margin:0">0.6 代表六折</span>
            <label><input type="checkbox" id="cost-incl-fee"> 均價已含買進手續費</label>
            <label><input type="checkbox" id="net-mode"> 損益扣除交易成本</label>
          </div>
          <div class="note">手續費 0.1425% × 折扣,單筆最低 20 元;證交稅於賣出時計算,00 開頭 ETF 為 0.1%,一般股票為 0.3%。</div>
        </details>
        <details class="settings">
          <summary>版本與更新 <span class="tag2" id="ver-tag"></span></summary>
          <div class="set-row">
            <label style="flex:1 1 320px">更新來源
              <input class="inp" id="upd-repo" style="flex:1;width:auto;min-width:240px;text-align:left"
                     placeholder="https://github.com/帳號/tw-dashboard"></label>
            <button class="btn" id="upd-save">儲存</button>
          </div>
          <div class="set-row">
            <button class="btn" id="upd-check">檢查更新</button>
            <button class="btn" id="upd-rollback" hidden>退回上一版</button>
            <span class="note" id="upd-msg" style="margin:0"></span>
          </div>
        </details>
        <details class="settings">
          <summary>資料說明</summary>
          <div class="note">
            台股來自臺灣證券交易所、證券櫃檯買賣中心與期交所,盤中報價約 5 秒延遲;今日分時前段取自 Yahoo 財經(延遲約 20 分鐘),
            最近時段由本機每分鐘記錄補足。美股、黃金、原油與匯率取自 Yahoo 財經。興櫃只有每日成交均價;盤後定價與零股僅涵蓋上市股票。
            股數以「股」為單位(1 張 = 1,000 股)。遇到股票分割、減資、合併時,請自行更新持有股數與均價。
            自選股與持股存在程式資料夾的 tw_dashboard_data.json。本工具僅供觀察參考,不構成投資建議。
          </div>
        </details>
      </div>
    </section>
  </div>
</div>

<script>
(() => {
  const DEFAULT_WATCHLIST = ["2330","2317","2454","1503","1513","1519"];
  const LS = { wl:"tw-watchlist", hold:"tw-holdings", auto:"tw-refresh-min", fee:"tw-feecfg", theme:"tw-theme", last:"tw-lastknown",
               us:"tw-us-watchlist", tab:"tw-wl-tab", ticker:"tw-ticker" };

  let watchlist = DEFAULT_WATCHLIST.slice(), holdings = {};
  let feeCfg = { disc:0.6, inclFee:false, net:true };
  let lastKnown = {};   // 代號 -> { name, price, date }
  try {
    const lk = JSON.parse(localStorage.getItem(LS.last) || "null");
    if (lk && typeof lk === "object") lastKnown = lk;
    const wl = JSON.parse(localStorage.getItem(LS.wl) || "null");
    if (Array.isArray(wl) && wl.length) watchlist = wl;
    const h = JSON.parse(localStorage.getItem(LS.hold) || "null");
    if (h && typeof h === "object") holdings = h;
    const f = JSON.parse(localStorage.getItem(LS.fee) || "null");
    if (f && typeof f === "object") feeCfg = Object.assign(feeCfg, f);
  } catch (e) {}

  let stockMap = null, valMap = {}, taiexDaily = null, rtMap = {}, rtLive = false;
  let busy = false, autoTimer = null;
  let usWatchlist = ["TSM", "NVDA", "AAPL"], wlTab = "tw", usQuotes = {}, txf = null, after = null;
  const US_IDX_LIST = [["^GSPC", "S&P 500"], ["^IXIC", "那斯達克"], ["^SOX", "費城半導體"], ["^DJI", "道瓊"]];
  const CMD_LIST = [["GC=F", "黃金"], ["CL=F", "西德州原油"], ["BZ=F", "布蘭特原油"], ["TWD=X", "美元/台幣"]];
  const US_NAMES = Object.fromEntries(US_IDX_LIST.concat(CMD_LIST));
  const THEAD_TW = `<tr><th>代號 / 名稱</th><th>成交價 / 漲跌</th><th>持有股數</th><th>持有均價</th>
    <th>市值 / 成本</th><th>損益 / 報酬率</th><th></th></tr>`;
  const THEAD_US = `<tr><th>代號 / 名稱</th><th>價格 / 漲跌</th><th>狀態</th><th>報價時間(台灣)</th><th></th></tr>`;
  const chart = { code:"t00", range:"today", data:null, loading:false, error:null, req:0, hover:[] };

  const $ = id => document.getElementById(id);
  const num = s => {
    if (s === null || s === undefined) return NaN;
    const v = parseFloat(String(s).replace(/,/g, ""));
    return Number.isFinite(v) ? v : NaN;
  };
  const fmt = (v, d = 2) => Number.isFinite(v)
    ? v.toLocaleString("zh-TW", { minimumFractionDigits:d, maximumFractionDigits:d }) : "—";
  const fmt0 = v => Number.isFinite(v) ? Math.round(v).toLocaleString("zh-TW") : "—";
  const colorOf = v => v > 0 ? "var(--up)" : v < 0 ? "var(--down)" : "var(--flat)";
  const sign = v => v > 0 ? "▲" : v < 0 ? "▼" : "─";
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c =>
    ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
  const save = (k, v) => {
    try { localStorage.setItem(k, typeof v === "string" ? v : JSON.stringify(v)); } catch (e) {}
    if (SYNC_KEYS.includes(k)) pushState();
  };
  const SYNC_KEYS = [LS.wl, LS.hold, LS.fee, LS.last, LS.theme, LS.auto, LS.us, LS.tab, LS.ticker];
  const postJSON = (path, body) => fetch(path, { method: "POST", headers: { "X-TW": "1", "Content-Type": "application/json" },
                                                 body: JSON.stringify(body || {}) });
  let pushTimer = null;
  function sendState(keepalive) {
    const st = {};
    for (const k of SYNC_KEYS) { const v = localStorage.getItem(k); if (v !== null) st[k] = v; }
    return fetch("/api/state", { method: "POST", keepalive: !!keepalive,
      headers: { "X-TW": "1", "Content-Type": "application/json" }, body: JSON.stringify({ state: st }) }).catch(() => {});
  }
  function pushState() {
    clearTimeout(pushTimer);
    pushTimer = setTimeout(() => { pushTimer = null; sendState(false); }, 300);
  }
  function flushState() {               // 關閉或離開頁面時,立刻送出尚未寫回的修改
    if (!pushTimer) return;
    clearTimeout(pushTimer);
    pushTimer = null;
    sendState(true);
  }
  function readAll() {
    try {
      const wl = JSON.parse(localStorage.getItem(LS.wl) || "null");
      if (Array.isArray(wl)) watchlist = wl;
      const h = JSON.parse(localStorage.getItem(LS.hold) || "null");
      if (h && typeof h === "object") holdings = h;
      const f = JSON.parse(localStorage.getItem(LS.fee) || "null");
      if (f && typeof f === "object") feeCfg = Object.assign({ disc:0.6, inclFee:false, net:true }, f);
      const lk = JSON.parse(localStorage.getItem(LS.last) || "null");
      if (lk && typeof lk === "object") lastKnown = lk;
      const us = JSON.parse(localStorage.getItem(LS.us) || "null");
      if (Array.isArray(us)) usWatchlist = us;
      const tb = localStorage.getItem(LS.tab);
      if (tb === "tw" || tb === "us") wlTab = tb;
    } catch (e) {}
  }
  async function syncFromServer() {
    try {
      const r = await fetch("/api/state", { cache: "no-store" });
      const st = ((await r.json()) || {}).state || {};
      if (SYNC_KEYS.some(k => k in st)) {
        for (const k of SYNC_KEYS) if (k in st) localStorage.setItem(k, st[k]);
        readAll();
        const t = st[LS.theme];
        if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
      } else {
        pushState();     // 第一次使用 v19:把這個瀏覽器原有的資料搬進資料檔
      }
    } catch (e) {}
  }

  // 與本機程式保持連線:所有畫面都關閉後,程式會自動結束
  const CID = "p-" + Math.random().toString(36).slice(2);
  let stopped = false;
  const beat = () => { if (!stopped) postJSON("/api/heartbeat?id=" + CID, {}).catch(() => {}); };
  const beatTimer = setInterval(beat, 20000);
  addEventListener("pagehide", () => { flushState(); try { navigator.sendBeacon("/api/bye?id=" + CID); } catch (e) {} });
  addEventListener("pageshow", e => { if (e.persisted) beat(); });
  document.addEventListener("visibilitychange", () => { if (document.hidden) flushState(); });
  function endPage(msgHtml) {
    flushState();
    stopped = true;
    clearInterval(beatTimer);
    if (autoTimer) clearInterval(autoTimer);
    if (typeof chartTimer !== "undefined") clearInterval(chartTimer);
    try { navigator.sendBeacon("/api/bye?id=" + CID); } catch (e) {}
    document.body.innerHTML = '<div style="max-width:520px;margin:18vh auto;padding:28px;text-align:center;line-height:1.8;'
      + 'font-size:16px;color:var(--text);background:var(--surface);border:1px solid var(--line);border-radius:12px">'
      + msgHtml + "</div>";
    setTimeout(() => { try { window.close(); } catch (e) {} }, 1200);
  }
  const getJSON = async (url, ms = 45000) => {
    const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), ms);
    let r;
    try {
      r = await fetch(url, { signal:ctl.signal });
    } catch (e) {
      throw new Error(e.name === "AbortError"
        ? "資料來源回應過慢,已停止等待,請稍後再試"
        : "無法連線到本機程式,請確認執行程式的黑色視窗仍開著");
    } finally {
      clearTimeout(timer);
    }
    if (!r.ok) {
      let detail = "";
      try { detail = (await r.json()).error || ""; } catch (e) {}
      throw new Error(detail || ("HTTP " + r.status));
    }
    return r.json();
  };

  // ── 交易成本 ──
  const FEE_RATE = 0.001425, MIN_FEE = 20;
  const taxRate = code => code.startsWith("00") ? 0.001 : 0.003;
  function calcPL(code, price, shares, cost) {
    const gross = (price - cost) * shares;
    const basis = cost * shares;
    if (!feeCfg.net) return { pl:gross, basis };
    const buyFee = feeCfg.inclFee ? 0 : Math.max(basis * FEE_RATE * feeCfg.disc, MIN_FEE);
    const sellFee = Math.max(price * shares * FEE_RATE * feeCfg.disc, MIN_FEE);
    const tax = price * shares * taxRate(code);
    return { pl:gross - buyFee - sellFee - tax, basis:basis + buyFee };
  }

  // ── 報價 ──
  // 成交價常在漲跌停鎖死時顯示「-」:跌停鎖死只剩賣單(取賣價),漲停鎖死只剩買單(取買價)
  function rtPrice(item) {
    const first = s => { for (const x of String(s || "").split("_")) { const v = num(x); if (v > 0) return v; } return NaN; };
    for (const v of [num(item.z), num(item.pz)]) if (v > 0) return v;
    const b = first(item.b), a = first(item.a);
    return Number.isFinite(b) ? b : a;
  }
  function rtTag(item, p) {
    const up = num(item.u), dn = num(item.w);
    if (p > 0 && dn > 0 && Math.abs(p - dn) < 1e-6) return "跌停";
    if (p > 0 && up > 0 && Math.abs(p - up) < 1e-6) return "漲停";
    return "";
  }
  function effective(code) {
    const live = liveQuote(code);
    if (live) return live;
    const lk = lastKnown[code];
    return lk && Number.isFinite(lk.price)
      ? { price:lk.price, chg:NaN, pct:NaN, lots:NaN, stale:true, date:lk.date } : null;
  }
  function liveQuote(code) {
    const s = stockMap ? stockMap[code] : null, rt = rtMap[code];
    if (rt) {
      const p = rtPrice(rt), y = num(rt.y);
      if (Number.isFinite(p) && y > 0) return { price:p, chg:p - y, pct:(p / y - 1) * 100, lots:num(rt.v), tag:rtTag(rt, p) };
      if (y > 0) return { price:y, chg:NaN, pct:NaN, lots:num(rt.v), tag:"尚未成交" };
    }
    if (s) {
      const close = num(s.ClosingPrice), chg = num(s.Change), prev = close - chg;
      return { price:close, chg, pct: prev ? (chg / prev) * 100 : NaN, lots:num(s.TradeVolume) / 1000 };
    }
    return null;
  }
  function indexQuote(key) {
    const rt = rtMap[key];
    if (rt) {
      const p = rtPrice(rt), y = num(rt.y);
      if (Number.isFinite(p) && y > 0) return { price:p, chg:p - y, pct:(p / y - 1) * 100 };
    }
    if (key === "t00" && taiexDaily) {
      const r = taiexDaily, close = num(r["收盤指數"]);
      const dir = r["漲跌(+/-)"] === "-" ? -1 : r["漲跌(+/-)"] === "+" ? 1 : 0;
      const pts = Math.abs(num(r["漲跌點數"])) * dir, pct = Math.abs(num(r["漲跌百分比"])) * dir;
      if (Number.isFinite(close)) return { price:close, chg:Number.isFinite(pts) ? pts : 0, pct:Number.isFinite(pct) ? pct : 0 };
    }
    return null;
  }
  const nameOf = code => (stockMap && stockMap[code] ? stockMap[code].Name : (rtMap[code] ? rtMap[code].n : "")) ||
    (lastKnown[code] ? lastKnown[code].name : "") || "";
  const labelOf = code => code === "t00" ? "加權指數" : code === "o00" ? "櫃買指數"
    : code === "txf" ? ((txf && txf.name) || "台指期")
    : code.startsWith("us:") ? (US_NAMES[code.slice(3)] || (usQuotes[code.slice(3)] && usQuotes[code.slice(3)].name
        ? code.slice(3) + " " + usQuotes[code.slice(3)].name : code.slice(3)))
    : (nameOf(code) ? code + " " + nameOf(code) : code);

  // ── 市場總覽的資料:台股、美股、商品與匯率 ──
  function marketGroups() {
    const tw = [["t00", "加權指數"], ["o00", "櫃買指數"]].map(([key, name]) => {
      const q = indexQuote(key);
      return { key, name, tag: "", price: q ? fmt(q.price) : "—", chg: q ? q.chg : null, pct: q ? q.pct : null };
    });
    tw.push({ key: "txf", name: "台指期" + (txf ? (txf.session === "night" ? " 夜盤" : " 日盤") : ""),
              tag: txf && !txf.active ? "已收盤" : "", price: txf ? fmt(txf.price, 0) : "—",
              chg: txf ? txf.chg : null, pct: txf ? txf.pct : null });
    const us = list => list.map(([sym, name]) => {
      const q = usQuotes[sym], ok = q && !q.error;
      return { key: "us:" + sym, name, tag: ok ? (q.state || "") : "", price: ok ? fmt(q.price, sym.endsWith("=X") ? 3 : 2) : "—",
               chg: ok ? q.chg : null, pct: ok ? q.pct : null };
    });
    return [["台股", tw], ["美股", us(US_IDX_LIST)], ["商品", us(CMD_LIST)]];
  }
  const pctTxt = it => it.chg != null && Number.isFinite(it.pct) ? sign(it.chg) + " " + fmt(Math.abs(it.pct)) + "%" : "—";

  // 走勢圖標題下方的細節列:選到哪一檔就顯示哪一檔
  function detailCap(code) {
    const parts = [];
    if (/^[0-9]/.test(code)) {
      const e = effective(code), v = valMap[code];
      if (e && Number.isFinite(e.lots)) parts.push("成交量 " + Math.round(e.lots).toLocaleString() + " 張");
      if (v && v.PEratio) parts.push("本益比 " + esc(v.PEratio));
      if (v && v.DividendYield) parts.push("殖利率 " + esc(v.DividendYield) + "%");
      if (after) {
        const f = after.fixed && after.fixed[code], o = after.odd && after.odd[code];
        if (f && f.vol) parts.push("盤後定價 " + fmt0(f.vol / 1000) + " 張");
        if (o && (o.price || o.vol)) parts.push("盤後零股 " + (o.price ? fmt(o.price) : "—") + (o.vol ? "(" + fmt0(o.vol) + " 股)" : ""));
      }
    } else if (code.startsWith("us:")) {
      const q = usQuotes[code.slice(3)];
      if (q && !q.error) {
        if (q.state) parts.push(esc(q.state));
        if (q.time) parts.push("報價時間 " + esc(q.time) + "(台灣)");
      }
    }
    return parts.length ? '<span class="detail">' + parts.map(p => "<span>" + p + "</span>").join("") + "</span>" : "";
  }
  function applyTab() {
    [...$("wl-tabs").children].forEach(b => b.classList.toggle("on", b.dataset.t === wlTab));
    $("code-input").placeholder = wlTab === "us" ? "美股代號,如 NVDA、TSM" : "代號或名稱,如 台積電";
  }
  function renderUSTable() {
    $("stock-tbody").innerHTML = usWatchlist.length ? usWatchlist.map(sym => {
      const q = usQuotes[sym], key = "us:" + sym, on = chart.code === key;
      const rm = `<button class="rm" data-rmus="${esc(sym)}" aria-label="移除 ${esc(sym)}">✕</button>`;
      if (!q || q.error) {
        return `<tr data-code="${esc(key)}" class="${on ? "sel" : ""}"><td><span class="mono code">${esc(sym)}</span></td>
          <td colspan="3" style="text-align:left;color:var(--faint)">${busy ? "載入中…" : "查無報價"}</td><td>${rm}</td></tr>`;
      }
      const col = colorOf(q.chg);
      const chgTxt = q.chg != null ? sign(q.chg) + " " + fmt(Math.abs(q.chg)) + (q.pct != null ? " (" + fmt(Math.abs(q.pct)) + "%)" : "") : "—";
      return `<tr data-code="${esc(key)}" class="${on ? "sel" : ""}">
        <td><span class="mono code">${esc(sym)}</span><span class="nm2">${esc(q.name && q.name.toUpperCase() !== sym ? q.name : "")}</span></td>
        <td><div class="mono" style="font-weight:600;color:${col}">${fmt(q.price)}</div><div class="mono sm" style="color:${col}">${chgTxt}</div></td>
        <td style="color:var(--muted)">${esc(q.state || "")}</td>
        <td class="mono" style="color:var(--faint)">${esc(q.time || "")}</td>
        <td>${rm}</td></tr>`;
    }).join("") : '<tr><td colspan="5" style="text-align:left;color:var(--faint);padding:16px">尚無美股自選股,在上方輸入代號加入,例如 NVDA、TSM</td></tr>';
  }

  const lampCls = tag => tag === "漲停" ? "lamp up" : tag === "跌停" ? "lamp down" : "";

  // ── 版本與更新 ──
  let updLater = false, updState = null;
  async function loadUpdate(force) {
    try {
      const r = force ? await postJSON("/api/update/check", {}) : await fetch("/api/update/status", { cache: "no-store" });
      updState = await r.json();
    } catch (e) { return; }
    const u = updState;
    $("ver-tag").textContent = "目前 v" + u.current + (u.available ? " · 有新版 v" + u.latest : "");
    if (document.activeElement !== $("upd-repo")) $("upd-repo").value = u.repo || "";
    $("upd-rollback").hidden = !u.has_backup;
    $("upd-msg").textContent = !u.configured ? "尚未設定更新來源" : u.error ? "檢查失敗:" + u.error
      : u.available ? "有新版本 v" + u.latest + (u.notes ? ":" + u.notes : "") : "已是最新版本(v" + u.current + ")";
    $("update-bar").hidden = !(u.available && !updLater);
    if (u.available) $("update-text").textContent = "有新版本 v" + u.latest + " 可以更新" + (u.notes ? ":" + u.notes : "");
  }
  async function doUpdate(path, question) {
    if (!confirm(question + "\n\n完成後程式會自動重新啟動,大約需要 10–20 秒。")) return;
    let ok = false, msg = "";
    try { const r = await postJSON(path, {}); const j = await r.json(); ok = r.ok; msg = j.error || ""; }
    catch (e) { msg = "無法連線到本機程式"; }
    if (!ok) { alert("無法完成:" + msg); return; }
    const cur = updState ? updState.current : null;
    flushState();
    stopped = true;
    document.body.innerHTML = '<div id="upd-wait" style="max-width:520px;margin:18vh auto;padding:28px;text-align:center;'
      + 'line-height:1.8;font-size:16px;color:var(--text);background:var(--surface);border:1px solid var(--line);'
      + 'border-radius:12px">正在重新啟動,請稍候…</div>';
    const t0 = Date.now();
    const poll = async () => {
      try {
        const r = await fetch("/api/version", { cache: "no-store" });
        if (r.ok && (await r.json()).version !== cur) { location.reload(); return; }
      } catch (e) {}
      if (Date.now() - t0 > 90000) {
        document.getElementById("upd-wait").textContent = "重新啟動的時間比預期久,請手動重新執行「啟動」。";
        return;
      }
      setTimeout(poll, 1500);
    };
    setTimeout(poll, 3000);
  }

  // ── 市場總覽、投組摘要、跑馬燈 ──
  function renderMarkets() {
    $("mkt").innerHTML = marketGroups().map(([title, items]) =>
      `<div class="mk-group"><div class="mk-title">${title}</div>` + items.map(it => {
        const on = chart.code === it.key;
        return `<button class="mk${on ? " sel" : ""}" data-key="${esc(it.key)}" aria-pressed="${on}">
          <span class="nm">${esc(it.name)}${it.tag ? ' <span class="tg">' + esc(it.tag) + "</span>" : ""}</span>
          <span class="r2"><span class="pr mono">${it.price}</span>
            <span class="ch mono" style="color:${it.chg != null ? colorOf(it.chg) : "var(--faint)"}">${pctTxt(it)}</span></span></button>`;
      }).join("") + "</div>").join("");
  }

  function renderSummary() {
    let cost = 0, mkt = 0, pl = 0, n = 0, staleN = 0;
    for (const code of watchlist) {
      const h = holdings[code];
      if (!h || !(h.shares > 0)) continue;
      const e = effective(code);
      if (!e || !Number.isFinite(e.price)) continue;
      n++;
      if (e.stale) staleN++;
      mkt += e.price * h.shares;
      if (h.cost > 0) { const r = calcPL(code, e.price, h.shares, h.cost); cost += r.basis; pl += r.pl; }
    }
    if (!n) { $("summary").innerHTML = ""; return; }
    const ret = cost > 0 ? (pl / cost) * 100 : NaN;
    $("summary").innerHTML = `<div class="pf"><div class="lbl">總成本</div><div class="big mono">${fmt0(cost)}</div>
        <div class="sub">${feeCfg.net && !feeCfg.inclFee ? "含買進手續費" : "&nbsp;"}</div></div>
      <div class="pf"><div class="lbl">總市值</div><div class="big mono">${fmt0(mkt)}</div>
        <div class="sub">持股 ${n} 檔${staleN ? ",其中 " + staleN + " 檔以最後報價計" : ""}</div></div>
      <div class="pf"><div class="lbl">未實現損益${feeCfg.net ? "(扣除交易成本)" : ""}</div>
        <div class="big mono" style="color:${colorOf(pl)}">${(pl > 0 ? "+" : "") + fmt0(pl)}</div>
        <div class="sub mono" style="color:${colorOf(Number.isFinite(ret) ? ret : 0)}">報酬率 ${Number.isFinite(ret) ? (ret > 0 ? "+" : "") + fmt(ret) + "%" : "—"}</div></div>`;
  }

  // 跑馬燈:市場項目 + 台股自選 + 美股自選。清單沒變時只更新數字,捲動不會中斷
  let tickerSig = "";
  function renderTicker() {
    const box = $("ticker");
    if (!box || box.hidden) return;
    const items = [];
    for (const [, list] of marketGroups()) for (const it of list) items.push(it);
    for (const code of watchlist) {
      const e = effective(code), live = e && !e.stale;
      items.push({ key: code, name: (nameOf(code) || code) + (e && e.tag ? " " + e.tag : ""), price: e ? fmt(e.price) : "—",
                   chg: live && Number.isFinite(e.chg) ? e.chg : null, pct: live && Number.isFinite(e.pct) ? e.pct : null,
                   tag: e ? e.tag || "" : "" });
    }
    for (const sym of usWatchlist) {
      const q = usQuotes[sym], ok = q && !q.error;
      items.push({ key: "us:" + sym, name: sym, price: ok ? fmt(q.price) : "—", chg: ok ? q.chg : null, pct: ok ? q.pct : null });
    }
    const track = $("ticker-track"), n = items.length, sig = items.map(i => i.key + "|" + i.name).join(",");
    if (sig !== tickerSig) {
      tickerSig = sig;
      const one = (it, dup) => `<button class="tk" data-key="${esc(it.key)}"${dup ? ' tabindex="-1" aria-hidden="true"' : ""}>
        <span class="n">${esc(it.name)}</span><span class="p mono"></span><span class="c mono"></span></button>`;
      track.innerHTML = items.map(it => one(it, false)).join("") + items.map(it => one(it, true)).join("");
      requestAnimationFrame(() => {                    // 固定每秒約 55 像素的速度
        const w = track.scrollWidth / 2;
        if (w > 0) track.style.setProperty("--tick-dur", Math.max(20, Math.round(w / 55)) + "s");
      });
    }
    const nodes = track.children;
    for (let i = 0; i < nodes.length; i++) {
      const it = items[i % n], el = nodes[i], col = it.chg != null ? colorOf(it.chg) : "var(--faint)";
      el.children[1].textContent = it.price;
      el.children[1].style.color = it.chg != null ? col : "var(--text)";
      el.children[1].className = "p mono " + lampCls(it.tag);
      el.children[2].textContent = pctTxt(it);
      el.children[2].style.color = col;
      el.classList.toggle("sel", chart.code === it.key);
    }
  }
  // 預設一律捲動,不受系統「動畫效果」設定影響(公司電腦或遠端桌面常把動畫關閉);不想看動態可選「靜止」
  function applyTicker(mode) {
    if (mode === "0") mode = "off";               // 相容 v24 的舊設定值
    if (!["run", "still", "off"].includes(mode)) mode = "run";
    const box = $("ticker");
    box.hidden = mode === "off";
    box.classList.toggle("still", mode === "still");
    $("ticker-mode").value = mode;
    save(LS.ticker, mode);
    if (mode !== "off") { tickerSig = ""; renderTicker(); }
  }

  // ── 自選股表格 ──
  function renderTable() {
    $("wl-thead").innerHTML = wlTab === "us" ? THEAD_US : THEAD_TW;
    $("wl-count").textContent = watchlist.length;
    $("us-count").textContent = usWatchlist.length;
    if (wlTab === "us") { renderUSTable(); return; }
    $("stock-tbody").innerHTML = watchlist.map(code => {
      const v = valMap[code], h = holdings[code] || {}, e = effective(code), on = chart.code === code;
      const rm = `<button class="rm" data-rm="${esc(code)}" aria-label="移除 ${esc(code)}">✕</button>`;
      const inp = (field, val, step, ph) => `<input class="hold mono" type="number" min="0" step="${step}"
        data-code="${esc(code)}" data-field="${field}" value="${val > 0 ? val : ""}" placeholder="${ph}"
        aria-label="${esc(code)} ${field === "shares" ? "持有股數" : "持有均價"}">`;
      if (!e) {
        return `<tr data-code="${esc(code)}" class="${on ? "sel" : ""}"><td class="mono code">${esc(code)}</td>
          <td colspan="5" style="text-align:left;color:var(--faint)">${busy ? "載入中…" : "查無資料,請確認代號是否正確"}</td>
          <td>${rm}</td></tr>`;
      }
      const col = e.stale ? "var(--faint)" : colorOf(e.chg);
      const gone = e.stale && !busy && stockMap;
      let costT = "—", mktT = "—", plT = "—", retT = "—", plC = "var(--faint)", retC = "var(--faint)";
      if (h.shares > 0 && Number.isFinite(e.price)) {
        mktT = fmt0(e.price * h.shares);
        if (h.cost > 0) {
          costT = fmt0(h.cost * h.shares);
          const r = calcPL(code, e.price, h.shares, h.cost);
          const ret = r.basis > 0 ? (r.pl / r.basis) * 100 : NaN;
          plT = (r.pl > 0 ? "+" : "") + fmt0(r.pl);
          retT = Number.isFinite(ret) ? (ret > 0 ? "+" : "") + fmt(ret) + "%" : "—";
          plC = colorOf(r.pl); retC = colorOf(ret);
        }
      }
      return `<tr data-code="${esc(code)}" class="${on ? "sel" : ""}">
        <td><span class="mono code">${esc(code)}</span><span class="nm2">${esc(nameOf(code))}</span>
          ${gone ? '<div class="sm warn">暫無報價:可能已下市、暫停交易或轉換市場中</div>' : ""}</td>
        <td><div class="mono" style="font-weight:600;color:${col}"><span class="${lampCls(e.tag)}">${fmt(e.price)}</span></div>
          <div class="mono sm" style="color:${col}">${e.stale ? "最後報價 " + esc(e.date || "") :
            Number.isFinite(e.chg) ? sign(e.chg) + " " + fmt(Math.abs(e.chg))
            + (Number.isFinite(e.pct) ? " (" + fmt(Math.abs(e.pct)) + "%)" : "") + (e.tag ? " " + e.tag : "")
            : (e.tag || "—")}</div></td>
        <td>${inp("shares", h.shares, 1, "")}</td>
        <td>${inp("cost", h.cost, 0.01, "")}</td>
        <td><div class="mono">${mktT}</div>
          <div class="mono sm" style="color:var(--faint)">${costT === "—" ? "&nbsp;" : "成本 " + costT}</div></td>
        <td><div class="mono" style="color:${plC}">${plT}</div>
          <div class="mono sm" style="color:${retC}">${retT === "—" ? "&nbsp;" : retT}</div></td>
        <td>${rm}</td></tr>`;
    }).join("");
  }

  function renderAll() { if (stopped) return; renderMarkets(); renderSummary(); renderTable(); renderTicker(); }

  function showError(msg) {
    const b = $("error-box");
    b.style.display = msg ? "block" : "none";
    b.textContent = msg || "";
  }

  // ── 載入報價 ──
  async function load() {
    if (busy || stopped) return;
    busy = true;
    $("refresh-btn").disabled = true;
    $("refresh-btn").textContent = "更新中…";
    showError(null);
    const codes = watchlist.concat(["t00", "o00"]).join(",");
    const [d, ix, v, rt, tx, us, ah] = await Promise.allSettled([
      getJSON("/api/daily"), getJSON("/api/index"), getJSON("/api/valuation"),
      getJSON("/api/realtime?codes=" + encodeURIComponent(codes)),
      getJSON("/api/txf"),
      getJSON("/api/us/quotes?symbols=" + encodeURIComponent(US_IDX_LIST.concat(CMD_LIST).map(x => x[0]).concat(usWatchlist).join(","))),
      getJSON("/api/afterhours?codes=" + encodeURIComponent(watchlist.join(","))),
    ]);
    if (d.status === "fulfilled" && Array.isArray(d.value)) {
      stockMap = {};
      for (const r of d.value) {
        const old = stockMap[r.Code];
        if (!old || (!(num(old.ClosingPrice) > 0) && num(r.ClosingPrice) > 0)) stockMap[r.Code] = r;
      }
    }
    if (ix.status === "fulfilled" && Array.isArray(ix.value)) {
      taiexDaily = ix.value.find(r => r["指數"] === "發行量加權股價指數") || taiexDaily;
    }
    if (v.status === "fulfilled" && Array.isArray(v.value)) {
      valMap = {};
      for (const r of v.value) valMap[r.Code] = r;
    }
    rtLive = false;
    if (rt.status === "fulfilled" && Array.isArray(rt.value)) {
      rtMap = {};
      for (const it of rt.value) {
        if (it.c) rtMap[it.c] = it;
        if (Number.isFinite(rtPrice(it))) rtLive = true;
      }
    }
    if (tx.status === "fulfilled" && tx.value && tx.value.price) txf = tx.value;
    if (us.status === "fulfilled" && Array.isArray(us.value)) {
      for (const q of us.value) if (q && q.symbol && !(q.error && usQuotes[q.symbol] && !usQuotes[q.symbol].error)) usQuotes[q.symbol] = q;
    }
    if (ah.status === "fulfilled" && ah.value && ah.value.date) after = ah.value;
    if (!stockMap && !rtLive) {
      const a = d.status === "rejected" ? String(d.reason.message || d.reason) : "";
      const b = rt.status === "rejected" ? String(rt.reason.message || rt.reason) : "";
      showError("無法取得報價。日資料:" + a + " / 即時資料:" + b + "。詳細記錄請看程式的終端機視窗。");
    }
    const badge = $("rt-badge");
    badge.textContent = rtLive ? "即時 · 約 5 秒延遲" : "收盤資料";
    badge.className = "badge" + (rtLive ? " live" : "");
    $("fetched-at").textContent = "更新於 " + new Date().toLocaleTimeString("zh-TW", { hour12:false });
    busy = false;
    $("refresh-btn").disabled = false;
    $("refresh-btn").textContent = "重新整理";
    const today = new Date().toLocaleDateString("sv-SE");
    for (const code of watchlist) {
      const q = liveQuote(code);
      if (q && Number.isFinite(q.price)) lastKnown[code] = { name:nameOf(code), price:q.price, date:today };
    }
    save(LS.last, lastKnown);
    renderAll();
    if (chart.range === "today" && !chart.loading) loadChart(true);
    else if (!chart.data && !chart.loading) renderChart();
  }

  // ── 走勢圖 ──
  function selectChart(code) {
    if (chart.code === code) return;
    chart.code = code;
    chart.data = null;
    renderAll();
    loadChart(false);
  }

  async function loadChart(silent) {
    if (stopped) return;
    const my = ++chart.req;
    if (!silent) { chart.loading = true; chart.error = null; renderChart(); }
    const isUS = chart.code.startsWith("us:"), sym = chart.code.slice(3);
    if (chart.code === "txf" && chart.range !== "today") {
      chart.loading = false; chart.data = null;
      chart.error = "台指期只提供今日走勢(由程式開啟期間每分鐘記錄)。";
      renderChart();
      return;
    }
    const url = chart.range === "today"
      ? (isUS ? "/api/us/intraday?symbol=" + encodeURIComponent(sym) : "/api/intraday?code=" + encodeURIComponent(chart.code))
      : (isUS ? "/api/us/history?symbol=" + encodeURIComponent(sym) : "/api/history?code=" + encodeURIComponent(chart.code))
        + "&months=" + chart.range;
    try {
      const data = await getJSON(url);
      if (my !== chart.req) return;
      chart.data = data;
      chart.error = null;
    } catch (e) {
      if (my !== chart.req) return;
      if (!silent || !chart.data) { chart.data = null; chart.error = String(e.message || e); }
    }
    chart.loading = false;
    renderChart();
  }

  const AX = "font-size:12px;font-family:'IBM Plex Mono',ui-monospace,monospace;";
  const txt = (x, y, s, anchor, fill) =>
    `<text x="${x}" y="${y}" text-anchor="${anchor}" style="${AX}fill:${fill || "var(--faint)"}">${s}</text>`;
  const hoverMarkup = (padT, ih) =>
    `<g id="hv" style="display:none"><line id="hv-l" y1="${padT}" y2="${padT + ih}"
      style="stroke:var(--muted);stroke-width:1;stroke-dasharray:3 3"/>
     <circle id="hv-c" r="4" style="fill:var(--surface);stroke-width:2"/>
     <text id="hv-t" style="font-size:13px;font-weight:600;fill:var(--text);font-family:'IBM Plex Mono',monospace;
      paint-order:stroke;stroke:var(--surface);stroke-width:5px;stroke-linejoin:round"></text></g>`;

  function extremes(vals) {
    let hi = 0, lo = 0;
    vals.forEach((v, i) => { if (v > vals[hi]) hi = i; if (v < vals[lo]) lo = i; });
    return [hi, lo];
  }
  function markerSvg(x, y, label, isHigh, W, padL, padR, padT, ih) {
    let ty = isHigh ? y - 9 : y + 18;
    if (ty < padT + 12) ty = y + 18;
    if (ty > padT + ih - 2) ty = y - 9;
    const anchor = x > W - padR - 90 ? "end" : x < padL + 90 ? "start" : "middle";
    return `<circle cx="${x}" cy="${y}" r="3.4" style="fill:var(--surface);stroke:var(--text);stroke-width:1.6"/>
      <text x="${x}" y="${ty}" text-anchor="${anchor}" style="font-size:12px;font-weight:600;fill:var(--text);
        font-family:'IBM Plex Mono',monospace;paint-order:stroke;stroke:var(--surface);stroke-width:4px;
        stroke-linejoin:round">${label}</text>`;
  }

  function attachHover(svg, padT) {
    const hv = chart.hover;
    if (!svg || !hv.length) return;
    const g = svg.querySelector("#hv"), ln = svg.querySelector("#hv-l");
    const dot = svg.querySelector("#hv-c"), tx = svg.querySelector("#hv-t");
    const W = +svg.getAttribute("width");
    svg.addEventListener("mousemove", ev => {
      const x = ev.clientX - svg.getBoundingClientRect().left;
      let best = 0, bd = Infinity;
      for (let i = 0; i < hv.length; i++) {
        const dd = Math.abs(hv[i].x - x);
        if (dd < bd) { bd = dd; best = i; }
      }
      const h = hv[best], right = h.x > W * 0.62;
      ln.setAttribute("x1", h.x); ln.setAttribute("x2", h.x);
      dot.setAttribute("cx", h.x); dot.setAttribute("cy", h.y);
      dot.style.stroke = h.col;
      tx.textContent = h.text;
      tx.setAttribute("x", right ? h.x - 10 : h.x + 10);
      tx.setAttribute("text-anchor", right ? "end" : "start");
      tx.setAttribute("y", padT + 14);
      g.style.display = "";
    });
    svg.addEventListener("mouseleave", () => { g.style.display = "none"; });
  }

  function renderChart() {
    if (stopped || !$("chart-area")) return;
    const area = $("chart-area"), cap = $("chart-cap"), label = labelOf(chart.code);
    chart.hover = [];
    if (chart.loading) {
      cap.innerHTML = `<span class="nm">${esc(label)}</span>`;
      area.innerHTML = '<div class="chart-msg">載入走勢資料中…</div>';
      return;
    }
    if (chart.error || !chart.data) {
      cap.innerHTML = `<span class="nm">${esc(label)}</span>`;
      area.innerHTML = '<div class="chart-msg">' + esc(chart.error ||
        (chart.range === "today" ? "尚未取得今日分時資料" : "此標的沒有可用的歷史資料(興櫃股票通常沒有日線紀錄)")) + "</div>";
      return;
    }
    if (chart.range === "today") drawIntraday(area, cap, label);
    else drawHistory(area, cap, label);
  }

  // 今日分時:X 軸固定 09:00–13:30,以昨收為中線,上方紅、下方綠
  function drawIntraday(area, cap, label) {
    const d = chart.data;
    const toMin = t => { const a = String(t).split(":"); return (+a[0]) * 60 + (+a[1]); };
    const ax = d.axis || { dur:270, ticks:[[0,"09:00"],[60,"10:00"],[120,"11:00"],[180,"12:00"],[270,"13:30"]] };
    const T0 = 0, T1 = ax.dur;
    const pts = (d.points || []).map(p => ({ m: p.o !== undefined ? +p.o : toMin(p.t) - 540, p:+p.p, t:p.t }))
      .filter(p => p.m >= T0 && p.m <= T1 && Number.isFinite(p.p)).sort((a, b) => a.m - b.m);
    const prev = +d.prev;
    if (!pts.length || !(prev > 0)) {
      cap.innerHTML = `<span class="nm">${esc(label)}</span>`;
      area.innerHTML = '<div class="chart-msg">今日尚無成交資料(開盤前或非交易日)</div>';
      return;
    }
    const last = pts[pts.length - 1].p, chg = last - prev, pct = (last / prev - 1) * 100;
    cap.innerHTML = `<span class="nm">${esc(label)}</span><span class="px mono">${fmt(last)}</span>
      <span class="mono" style="color:${colorOf(chg)}">${sign(chg)} ${fmt(Math.abs(chg))} (${fmt(Math.abs(pct))}%)</span>
      <span>${esc(d.prev_label || "昨收")} <span class="mono">${fmt(prev, chart.code === "txf" ? 0 : 2)}</span></span>
      <span>${d.note ? esc(d.note) + " · " + esc(d.source_label || "") : `${d.previous_day ? "最近交易日 " + esc(d.date || "") + "(今日尚未開盤或休市)" : esc(d.date || "") + " 分時"} · ${d.source === "recorder" ? "本機記錄(程式開啟後累積)" : d.source === "yahoo+recorder" ? "Yahoo 財經 + 即時記錄補足最近 20 分鐘" : "Yahoo 財經(延遲約 20 分鐘)"}`}</span>`;

    cap.innerHTML += detailCap(chart.code);
    const span = Math.max(Math.max(...pts.map(p => Math.abs(p.p - prev))), prev * 0.003) * 1.15;
    const lo = prev - span, hi = prev + span;
    const W = Math.max(320, area.clientWidth - 16), H = Math.max(140, area.clientHeight - 12);
    const padL = Math.max(fmt(hi).length, fmt(lo).length) * 7.6 + 16;
    const padR = 62, padT = 10, padB = 26, iw = W - padL - padR, ih = H - padT - padB;
    const X = m => padL + ((m - T0) / (T1 - T0)) * iw;
    const Y = v => padT + ((hi - v) / (hi - lo)) * ih;
    const refY = Y(prev);

    let grid = "";
    for (let i = 0; i <= 4; i++) {
      const val = lo + ((hi - lo) * i) / 4, y = Y(val), pc = (val / prev - 1) * 100;
      if (i !== 2) grid += `<line x1="${padL}" x2="${W - padR}" y1="${y}" y2="${y}" style="stroke:var(--surface-2)"/>`;
      grid += txt(padL - 8, y + 4, fmt(val), "end");
      grid += txt(W - padR + 8, y + 4, (pc > 0.0001 ? "+" : "") + fmt(pc) + "%", "start",
        i === 2 ? "var(--faint)" : pc > 0 ? "var(--up)" : "var(--down)");
    }
    // 時間刻度:首尾一定保留,中間的若與相鄰標籤太近就省略文字(避免重疊)
    const tk = ax.ticks, lastX = X(tk[tk.length - 1][0]);
    // 首個標籤靠左、最後一個靠右(會向內延伸約 40px),中間標籤置中(左右各約 20px)
    let prevX = -1e9, prevEdge = false;
    tk.forEach(([m, s], i) => {
      const x = X(m), edge = i === 0 || i === tk.length - 1;
      grid += `<line x1="${x}" x2="${x}" y1="${padT}" y2="${padT + ih}" style="stroke:var(--surface-2)"/>`;
      if (!edge && (x - prevX < (prevEdge ? 64 : 46) || lastX - x < 64)) return;
      grid += txt(x, H - 7, s, m === 0 ? "start" : m === T1 ? "end" : "middle");
      prevX = x; prevEdge = i === 0;
    });

    const line = pts.map((p, i) => (i ? "L" : "M") + X(p.m).toFixed(1) + " " + Y(p.p).toFixed(1)).join(" ");
    const fill = line + ` L${X(pts[pts.length - 1].m).toFixed(1)} ${refY.toFixed(1)} L${X(pts[0].m).toFixed(1)} ${refY.toFixed(1)} Z`;
    const lastCol = last >= prev ? "var(--up)" : "var(--down)";
    let marks = "";
    if (pts.length > 2) {
      const [hi, lo] = extremes(pts.map(p => p.p));
      if (pts[hi].p !== pts[lo].p) {
        marks = markerSvg(X(pts[hi].m), Y(pts[hi].p), "最高 " + fmt(pts[hi].p) + " · " + pts[hi].t, true, W, padL, padR, padT, ih)
              + markerSvg(X(pts[lo].m), Y(pts[lo].p), "最低 " + fmt(pts[lo].p) + " · " + pts[lo].t, false, W, padL, padR, padT, ih);
      }
    }

    area.innerHTML = `<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${esc(label)} 今日分時走勢">
      <defs>
        <clipPath id="clip-up"><rect x="0" y="0" width="${W}" height="${refY}"/></clipPath>
        <clipPath id="clip-dn"><rect x="0" y="${refY}" width="${W}" height="${H - refY}"/></clipPath>
      </defs>
      ${grid}
      <path d="${fill}" style="fill:var(--up-soft)" clip-path="url(#clip-up)"/>
      <path d="${fill}" style="fill:var(--down-soft)" clip-path="url(#clip-dn)"/>
      <line x1="${padL}" x2="${W - padR}" y1="${refY}" y2="${refY}" style="stroke:var(--muted);stroke-width:1;stroke-dasharray:4 4"/>
      <path d="${line}" style="fill:none;stroke:var(--up);stroke-width:1.8;stroke-linejoin:round" clip-path="url(#clip-up)"/>
      <path d="${line}" style="fill:none;stroke:var(--down);stroke-width:1.8;stroke-linejoin:round" clip-path="url(#clip-dn)"/>
      <circle cx="${X(pts[pts.length - 1].m)}" cy="${Y(last)}" r="3.5" style="fill:${lastCol}"/>
      ${marks}
      ${hoverMarkup(padT, ih)}
    </svg>`;
    chart.hover = pts.map(p => ({
      x:X(p.m), y:Y(p.p), col:p.p >= prev ? "var(--up)" : "var(--down)",
      text:`${p.t}  ${fmt(p.p)}  ${p.p >= prev ? "+" : ""}${fmt((p.p / prev - 1) * 100)}%`,
    }));
    attachHover(area.querySelector("svg"), padT);
  }

  // 歷史日線
  function drawHistory(area, cap, label) {
    const pts = (chart.data || []).filter(p => Number.isFinite(+p.c));
    if (pts.length < 2) {
      cap.innerHTML = `<span class="nm">${esc(label)}</span>`;
      area.innerHTML = '<div class="chart-msg">此期間的歷史資料不足</div>';
      return;
    }
    const first = +pts[0].c, last = +pts[pts.length - 1].c, chg = last - first, pct = (last / first - 1) * 100;
    cap.innerHTML = `<span class="nm">${esc(label)}</span><span class="px mono">${fmt(last)}</span>
      <span class="mono" style="color:${colorOf(chg)}">${sign(chg)} ${fmt(Math.abs(chg))} (${fmt(Math.abs(pct))}%)</span>
      <span>${esc(pts[0].d)} ~ ${esc(pts[pts.length - 1].d)} · 日收盤</span>`;

    cap.innerHTML += detailCap(chart.code);
    let lo = Math.min(...pts.map(p => +p.c)), hi = Math.max(...pts.map(p => +p.c));
    const pad = (hi - lo || hi * 0.01) * 0.08;
    lo -= pad; hi += pad;
    const W = Math.max(320, area.clientWidth - 16), H = Math.max(140, area.clientHeight - 12);
    const padL = Math.max(fmt(hi).length, fmt(lo).length) * 7.6 + 16;
    const padR = 20, padT = 10, padB = 26, iw = W - padL - padR, ih = H - padT - padB;
    const X = i => padL + (i / (pts.length - 1)) * iw;
    const Y = v => padT + ((hi - v) / (hi - lo)) * ih;

    let grid = "";
    for (let i = 0; i <= 4; i++) {
      const val = lo + ((hi - lo) * i) / 4, y = Y(val);
      grid += `<line x1="${padL}" x2="${W - padR}" y1="${y}" y2="${y}" style="stroke:var(--surface-2)"/>`;
      grid += txt(padL - 8, y + 4, fmt(val), "end");
    }
    const marks = [0, Math.round((pts.length - 1) / 3), Math.round(((pts.length - 1) * 2) / 3), pts.length - 1];
    marks.forEach((k, j) => {
      grid += txt(X(k), H - 7, pts[k].d.slice(5).replace("-", "/"), j === 0 ? "start" : j === 3 ? "end" : "middle");
    });

    const col = chg >= 0 ? "var(--up)" : "var(--down)", soft = chg >= 0 ? "var(--up-soft)" : "var(--down-soft)";
    const line = pts.map((p, i) => (i ? "L" : "M") + X(i).toFixed(1) + " " + Y(+p.c).toFixed(1)).join(" ");
    const fill = line + ` L${X(pts.length - 1).toFixed(1)} ${padT + ih} L${padL} ${padT + ih} Z`;
    const [hiI, loI] = extremes(pts.map(p => +p.c));
    const hmd = d => d.slice(5).replace("-", "/");
    const hlMarks = +pts[hiI].c === +pts[loI].c ? "" :
      markerSvg(X(hiI), Y(+pts[hiI].c), "最高 " + fmt(+pts[hiI].c) + " · " + hmd(pts[hiI].d), true, W, padL, padR, padT, ih) +
      markerSvg(X(loI), Y(+pts[loI].c), "最低 " + fmt(+pts[loI].c) + " · " + hmd(pts[loI].d), false, W, padL, padR, padT, ih);

    area.innerHTML = `<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${esc(label)} 歷史走勢">
      ${grid}
      <path d="${fill}" style="fill:${soft}"/>
      <path d="${line}" style="fill:none;stroke:${col};stroke-width:1.8;stroke-linejoin:round"/>
      <circle cx="${X(pts.length - 1)}" cy="${Y(last)}" r="3.5" style="fill:${col}"/>
      ${hlMarks}
      ${hoverMarkup(padT, ih)}
    </svg>`;
    chart.hover = pts.map((p, i) => ({ x:X(i), y:Y(+p.c), col, text:`${p.d}  ${fmt(+p.c)}` }));
    attachHover(area.querySelector("svg"), padT);
  }

  // ── 主題 ──
  function applyTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    [...$("theme-seg").children].forEach(b => b.classList.toggle("on", b.dataset.theme === t));
    save(LS.theme, t);
  }

  // ── 自動更新 ──
  function setAutoRefresh(min) {
    if (autoTimer) { clearInterval(autoTimer); autoTimer = null; }
    if (min > 0) autoTimer = setInterval(load, min * 60 * 1000);
    save(LS.auto, String(min));
  }

  // ── 新增股票 ──
  let sugg = [], sIdx = -1;
  const normQ = s => String(s || "").replace(/臺/g, "台").replace(/\s+/g, "").toUpperCase();

  function searchStocks(q) {
    q = normQ(q);
    if (!q || !stockMap) return [];
    const out = [];
    for (const code in stockMap) {
      const cu = code.toUpperCase();
      if (/^\d{6}$/.test(code) && !code.startsWith("00") && cu !== q) continue;  // 排除權證等
      const name = stockMap[code].Name || "", nn = normQ(name);
      let score = -1;
      if (cu === q || nn === q) score = 0;
      else if (cu.startsWith(q)) score = 1;
      else if (nn.startsWith(q)) score = 2;
      else if (nn.includes(q)) score = 3;
      if (score >= 0) out.push({ code, name, score });
    }
    out.sort((a, b) => a.score - b.score || a.code.length - b.code.length || a.code.localeCompare(b.code));
    return out.slice(0, 8);
  }

  function closeSuggest() { const b = $("suggest"); if (b) b.classList.remove("open"); }

  function renderSuggest() {
    const box = $("suggest"), q = $("code-input").value.trim();
    if (!q) { closeSuggest(); return; }
    if (!stockMap) {
      box.innerHTML = '<div class="sg-empty">股票清單載入中…</div>';
    } else if (!sugg.length) {
      box.innerHTML = '<div class="sg-empty">找不到符合「' + esc(q) + '」的股票</div>';
    } else {
      const nq = normQ(q);
      box.innerHTML = sugg.map((x, i) => {
        let nm = esc(x.name);
        const k = normQ(x.name).indexOf(nq);
        if (k >= 0 && !/\s/.test(x.name)) {
          nm = esc(x.name.slice(0, k)) + "<mark>" + esc(x.name.slice(k, k + nq.length)) + "</mark>" + esc(x.name.slice(k + nq.length));
        }
        return `<div class="sg${i === sIdx ? " on" : ""}" role="option" aria-selected="${i === sIdx}" data-code="${esc(x.code)}">
          <span class="c mono">${esc(x.code)}</span><span>${nm}</span>
          <span class="tag">${watchlist.includes(x.code) ? "已加入" : ""}</span></div>`;
      }).join("");
    }
    box.classList.add("open");
  }

  let adding = false;
  async function addUS() {
    const inp = $("code-input"), err = $("add-error"), sym = inp.value.trim().toUpperCase();
    err.textContent = "";
    if (!sym) return;
    if (!/^\^?[A-Z][A-Z0-9.\-=]{0,11}$/.test(sym)) { err.textContent = "美股代號格式不正確,例如 NVDA、BRK-B"; return; }
    if (usWatchlist.includes(sym)) { err.textContent = sym + " 已在清單中"; return; }
    adding = true;
    err.textContent = "查詢中…";
    try {
      const arr = await getJSON("/api/us/quotes?symbols=" + encodeURIComponent(sym));
      const q = Array.isArray(arr) ? arr[0] : null;
      if (!q || q.error) { err.textContent = "查無「" + sym + "」,請確認代號"; return; }
      usQuotes[sym] = q;
    } catch (e) {
      err.textContent = "查詢失敗,請稍後再試";
      return;
    } finally {
      adding = false;
    }
    err.textContent = "";
    usWatchlist.push(sym);
    save(LS.us, usWatchlist);
    inp.value = "";
    renderAll();
  }

  async function addStock(pick) {
    if (adding) return;
    if (wlTab === "us") return addUS();
    const inp = $("code-input"), err = $("add-error"), raw = inp.value.trim();
    err.textContent = "";
    let code = typeof pick === "string" ? pick : "";
    if (!code) {
      if (!raw) return;
      const up = raw.toUpperCase();
      if (stockMap && stockMap[up]) code = up;
      else if (stockMap && /^[0-9A-Z]{4,6}$/.test(up)) {
        adding = true;
        err.textContent = "查詢中…";
        try {
          const arr = await getJSON("/api/realtime?codes=" + encodeURIComponent(up), 15000);
          if (Array.isArray(arr) && arr.some(it => String(it.c).toUpperCase() === up)) code = up;
        } catch (e) {}
        adding = false;
        err.textContent = "";
        if (!code && sIdx >= 0 && sugg[sIdx]) code = sugg[sIdx].code;
        if (!code) { err.textContent = "找不到「" + raw + "」,請確認代號是否正確"; return; }
      }
      else if (sIdx >= 0 && sugg[sIdx]) code = sugg[sIdx].code;
      else if (!stockMap && /^[0-9A-Z]{1,6}$/.test(up)) code = up;
      else { err.textContent = "找不到「" + raw + "」,請換個關鍵字"; return; }
    }
    if (watchlist.includes(code)) { err.textContent = code + " 已在清單中"; closeSuggest(); return; }
    watchlist.push(code);
    save(LS.wl, watchlist);
    inp.value = "";
    sugg = []; sIdx = -1;
    closeSuggest();
    renderAll();
    load();
  }


  // ── 事件 ──
  $("refresh-btn").addEventListener("click", () => { load(); loadChart(true); });
  $("mode-btn").addEventListener("click", async () => {
    try {
      const r = await postJSON("/api/mode", { mode: "widget" });
      const j = await r.json();
      endPage(j.app_browser
        ? "已改用小工具模式,小工具視窗會在幾秒內出現。<br>這個分頁可以關閉了。"
        : "已改用小工具模式。這台電腦找不到 Edge 或 Chrome,小工具會以一般分頁開啟,無法置頂與調整透明度。<br>這個分頁可以關閉了。");
    } catch (e) { alert("切換失敗,請確認程式仍在執行。"); }
  });
  $("quit-btn").addEventListener("click", async () => {
    if (!confirm("確定要結束台股觀察嗎?")) return;
    try { await postJSON("/api/quit", {}); } catch (e) {}
    endPage("程式已結束。要再使用時,請重新執行「啟動.bat」。");
  });
  $("add-btn").addEventListener("click", () => addStock());
  $("wl-tabs").addEventListener("click", e => {
    const b = e.target.closest("button[data-t]");
    if (!b || b.dataset.t === wlTab) return;
    wlTab = b.dataset.t;
    save(LS.tab, wlTab);
    $("add-error").textContent = "";
    $("code-input").value = "";
    closeSuggest();
    applyTab();
    renderTable();
  });
  $("code-input").addEventListener("input", () => {
    if (wlTab === "us") { sugg = []; sIdx = -1; closeSuggest(); $("add-error").textContent = ""; return; }
    sugg = searchStocks($("code-input").value);
    sIdx = sugg.length ? 0 : -1;
    $("add-error").textContent = "";
    renderSuggest();
  });
  $("code-input").addEventListener("focus", () => { if ($("code-input").value.trim()) renderSuggest(); });
  $("code-input").addEventListener("blur", () => setTimeout(closeSuggest, 150));
  $("code-input").addEventListener("keydown", e => {
    if (e.isComposing) return;                       // 注音/倉頡選字中不處理
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      if (!sugg.length) return;
      e.preventDefault();
      sIdx = (sIdx + (e.key === "ArrowDown" ? 1 : sugg.length - 1)) % sugg.length;
      renderSuggest();
    } else if (e.key === "Enter") {
      e.preventDefault();
      addStock();
    } else if (e.key === "Escape") {
      closeSuggest();
    }
  });
  $("suggest").addEventListener("mousedown", e => {
    const it = e.target.closest(".sg[data-code]");
    if (it) { e.preventDefault(); addStock(it.dataset.code); }
  });

  $("mkt").addEventListener("click", e => {
    const b = e.target.closest("button.mk");
    if (b) selectChart(b.dataset.key);
  });
  $("ticker").addEventListener("click", e => {
    const b = e.target.closest("button.tk");
    if (b) selectChart(b.dataset.key);
  });
  $("ticker-mode").addEventListener("change", e => applyTicker(e.target.value));
  $("update-go").addEventListener("click", () => doUpdate("/api/update/apply", "要更新到 v" + (updState ? updState.latest : "") + " 嗎?"));
  $("update-later").addEventListener("click", () => { updLater = true; $("update-bar").hidden = true; });
  $("upd-check").addEventListener("click", () => { $("upd-msg").textContent = "檢查中…"; loadUpdate(true); });
  $("upd-rollback").addEventListener("click", () => doUpdate("/api/update/rollback", "要退回上一個版本嗎?"));
  $("upd-save").addEventListener("click", async () => {
    $("upd-msg").textContent = "儲存中…";
    try {
      const r = await postJSON("/api/update/source", { repo: $("upd-repo").value.trim() });
      if (!r.ok) { $("upd-msg").textContent = (await r.json()).error || "儲存失敗"; return; }
    } catch (e) { $("upd-msg").textContent = "儲存失敗"; return; }
    loadUpdate(true);
  });
  $("ticker").addEventListener("wheel", e => {         // 靜止時用一般滾輪也能左右瀏覽
    const t = $("ticker");
    if (!t.classList.contains("still") || !e.deltaY) return;
    t.scrollLeft += e.deltaY;
    e.preventDefault();
  }, { passive: false });

  $("stock-tbody").addEventListener("click", e => {
    const rmus = e.target.closest("[data-rmus]");
    if (rmus) {
      const sym = rmus.dataset.rmus;
      usWatchlist = usWatchlist.filter(x => x !== sym);
      save(LS.us, usWatchlist);
      if (chart.code === "us:" + sym) { chart.code = "t00"; chart.data = null; loadChart(false); }
      renderAll();
      return;
    }
    const rm = e.target.closest("[data-rm]");
    if (rm) {
      const code = rm.dataset.rm;
      watchlist = watchlist.filter(c => c !== code);
      delete holdings[code];
      delete lastKnown[code];
      save(LS.wl, watchlist);
      save(LS.hold, holdings);
      save(LS.last, lastKnown);
      if (chart.code === code) { chart.code = ""; selectChart("t00"); } else renderAll();
      return;
    }
    if (e.target.closest("input, button")) return;
    const tr = e.target.closest("tr[data-code]");
    if (tr) selectChart(tr.dataset.code);
  });

  $("stock-tbody").addEventListener("change", e => {
    const inp = e.target.closest(".hold");
    if (!inp) return;
    const code = inp.dataset.code, field = inp.dataset.field, val = parseFloat(inp.value);
    if (!holdings[code]) holdings[code] = { shares:0, cost:0 };
    holdings[code][field] = Number.isFinite(val) && val > 0 ? val : 0;
    if (!holdings[code].shares && !holdings[code].cost) delete holdings[code];
    save(LS.hold, holdings);
    renderAll();
  });
  $("stock-tbody").addEventListener("keydown", e => {
    if (e.key === "Enter" && e.target.closest(".hold")) e.target.blur();
  });

  $("range-seg").addEventListener("click", e => {
    const b = e.target.closest("button");
    if (!b) return;
    const r = b.dataset.range === "today" ? "today" : parseInt(b.dataset.range, 10);
    if (r === chart.range) return;
    chart.range = r;
    chart.data = null;
    [...$("range-seg").children].forEach(x => x.classList.toggle("on", x === b));
    loadChart(false);
  });

  $("theme-seg").addEventListener("click", e => {
    const b = e.target.closest("button");
    if (b) applyTheme(b.dataset.theme);
  });

  $("auto-select").addEventListener("change", e => setAutoRefresh(parseFloat(e.target.value) || 0));

  function initFee() {
    $("fee-disc").value = feeCfg.disc;
    $("cost-incl-fee").checked = feeCfg.inclFee;
    $("net-mode").checked = feeCfg.net;
  }
  function feeChanged() {
    let d = parseFloat($("fee-disc").value);
    if (!Number.isFinite(d)) d = 1;
    feeCfg.disc = Math.min(1, Math.max(0.1, d));
    $("fee-disc").value = feeCfg.disc;
    feeCfg.inclFee = $("cost-incl-fee").checked;
    feeCfg.net = $("net-mode").checked;
    save(LS.fee, feeCfg);
    renderAll();
  }
  ["fee-disc", "cost-incl-fee", "net-mode"].forEach(id => $(id).addEventListener("change", feeChanged));

  const chartTimer = setInterval(() => {
    if (chart.range !== "today" || chart.loading || document.hidden) return;
    const n = new Date(), hm = n.getHours() * 100 + n.getMinutes(), wd = n.getDay();
    if (chart.code === "txf" || chart.code.startsWith("us:") || (wd >= 1 && wd <= 5 && hm >= 900 && hm <= 1336)) loadChart(true);
  }, 60000);

  var rsz = null, lastBox = "";
  if (window.ResizeObserver) {
    new ResizeObserver(() => {
      const a = $("chart-area");
      if (!a) return;
      const box = a.clientWidth + "x" + a.clientHeight;
      if (box === lastBox) return;
      lastBox = box;
      clearTimeout(rsz);
      rsz = setTimeout(renderChart, 120);
    }).observe($("chart-area"));
  }
  window.addEventListener("resize", () => { clearTimeout(rsz); rsz = setTimeout(renderChart, 150); });

  // ── 初始化 ──
  (async () => {
    beat();
    readAll();
    await syncFromServer();
    applyTab();
    applyTicker(localStorage.getItem(LS.ticker) || "");
    applyTheme(document.documentElement.getAttribute("data-theme") || "light");
    initFee();
    const savedAuto = parseFloat(localStorage.getItem(LS.auto) || "30");
    const sel = $("auto-select");
    if ([...sel.options].some(o => parseFloat(o.value) === savedAuto)) sel.value = String(savedAuto);
    setAutoRefresh(parseFloat(sel.value) || 0);
    renderAll();
    loadChart(false);
    load();
    loadUpdate(false);
    setInterval(() => { if (!stopped) loadUpdate(false); }, 30 * 60 * 1000);
  })();
})();
</script>
</body>
</html>
"""


WIDGET_HTML = r"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>台股觀察 小工具</title>
<script>
(function () {
  var t = null;
  try { t = localStorage.getItem("tw-w-theme") || localStorage.getItem("tw-theme"); } catch (e) {}
  document.documentElement.setAttribute("data-theme", t === "dark" ? "dark" : "light");
})();
</script>
<style>
  :root { --surface:#FFFFFF; --surface-2:#EEF1F4; --line:#DCE1E8; --text:#18202B; --muted:#596575;
    --faint:#8D98A6; --accent:#2F5D8A; --accent-ink:#FFFFFF; --up:#C8372D; --down:#1F8556; --flat:#6B7785; color-scheme:light; }
  :root[data-theme="dark"] { --surface:#1C2128; --surface-2:#242B34; --line:#303843; --text:#E4E8ED;
    --muted:#9AA5B1; --faint:#6B7682; --accent:#8AAFD3; --accent-ink:#14181D; --up:#E06B60; --down:#52B48A; --flat:#8C97A3; color-scheme:dark; }
  * { box-sizing:border-box; }
  html, body { height:100%; }
  body { margin:0; background:var(--surface); color:var(--text); overflow-y:auto;
    font:13px/1.4 'Microsoft JhengHei','Noto Sans TC','PingFang TC',sans-serif; user-select:none; }
  .mono { font-family:Consolas,'IBM Plex Mono',ui-monospace,monospace; font-variant-numeric:tabular-nums; }
  .hd { display:flex; align-items:center; gap:8px; padding:6px 6px 6px 10px; border-bottom:1px solid var(--line);
    font-size:11.5px; color:var(--faint); position:sticky; top:0; background:var(--surface); z-index:2; }
  .hd b { color:var(--text); font-size:13px; margin-right:auto; }
  .st { color:var(--accent); font-weight:700; }
  .st.off { color:var(--up); }
  .gear { background:none; border:1px solid transparent; border-radius:6px; color:var(--muted); cursor:pointer;
    padding:3px; display:flex; }
  .gear:hover, .gear.on { border-color:var(--line); background:var(--surface-2); color:var(--text); }
  .gear svg { width:16px; height:16px; }
  .idx { display:grid; grid-template-columns:1fr 1fr; border-bottom:1px solid var(--line); }
  .idx > div { padding:6px 10px; min-width:0; }
  .idx > div + div { border-left:1px solid var(--line); }
  .lbl { font-size:11.5px; color:var(--muted); }
  .v { font-size:16px; font-weight:600; white-space:nowrap; }
  .s { font-size:11.5px; white-space:nowrap; }
  .pf { display:flex; justify-content:space-between; align-items:baseline; gap:8px; padding:6px 10px;
    background:var(--surface-2); border-bottom:1px solid var(--line); }
  .pf .v { font-size:15px; }
  .r { display:grid; grid-template-columns:minmax(0,1fr) auto auto; gap:10px; align-items:baseline;
    padding:5px 10px; border-bottom:1px solid var(--surface-2); }
  .r .n { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .r .c { color:var(--accent); font-weight:600; margin-right:6px; }
  .r .p { text-align:right; font-weight:600; }
  .r .q { text-align:right; min-width:64px; font-size:12px; }
  .msg { padding:16px 12px; color:var(--faint); text-align:center; line-height:1.6; }

  #panel { position:fixed; inset:34px 0 0 0; background:var(--surface); z-index:3; overflow-y:auto; padding:4px 12px 14px; }
  #panel[hidden] { display:none; }
  .sec { padding:10px 0; border-bottom:1px solid var(--surface-2); }
  .sec:last-child { border-bottom:none; }
  .sec h4 { margin:0 0 7px; font-size:12.5px; color:var(--muted); font-weight:600; }
  .row { display:flex; align-items:center; gap:10px; }
  input[type=range] { flex:1; accent-color:var(--accent); }
  .seg { display:inline-flex; border:1px solid var(--line); border-radius:6px; overflow:hidden; }
  .seg button { background:var(--surface); border:none; border-right:1px solid var(--line); padding:5px 12px;
    font:inherit; color:var(--muted); cursor:pointer; }
  .seg button:last-child { border-right:none; }
  .seg button.on { background:var(--accent); color:var(--accent-ink); font-weight:700; }
  select { font:inherit; color:inherit; background:var(--surface); border:1px solid var(--line); border-radius:6px; padding:4px 8px; }
  label { display:flex; align-items:center; gap:6px; cursor:pointer; }
  .btn { font:inherit; font-weight:700; background:var(--surface); color:var(--accent); border:1px solid var(--accent);
    border-radius:6px; padding:6px 12px; cursor:pointer; }
  .btn.danger { color:var(--up); border-color:var(--up); }
  .note { font-size:11.5px; color:var(--faint); margin-top:5px; line-height:1.5; }
</style>
</head>
<body>
<div class="hd">
  <b>台股觀察</b>
  <span class="st" id="st"></span><span class="mono" id="tm"></span>
  <button class="gear" id="gear" aria-label="小工具設定" title="設定">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.9 4.9l2.1 2.1M17 17l2.1 2.1M4.9 19.1L7 17M17 7l2.1-2.1"/></svg>
  </button>
</div>
<div id="w"><div class="msg">載入中…</div></div>

<div id="panel" hidden>
  <div class="sec" id="sec-win">
    <h4>透明度 <span class="mono" id="op-val"></span></h4>
    <div class="row"><input type="range" id="op" min="30" max="100" step="5" aria-label="透明度"></div>
    <div class="row" style="margin-top:8px"><label><input type="checkbox" id="top"> 永遠置頂</label></div>
  </div>
  <div class="sec" id="sec-nowin" hidden>
    <div class="note" style="margin:0">透明度與置頂需要在 Windows 上,並用 Edge 或 Chrome 開啟小工具。</div>
  </div>
  <div class="sec">
    <h4>配色</h4>
    <div class="seg" id="theme"><button data-t="light">淺色</button><button data-t="dark">深色</button></div>
  </div>
  <div class="sec">
    <h4>更新週期</h4>
    <select id="refresh" aria-label="更新週期">
      <option value="0.5">每 30 秒</option>
      <option value="1">每 1 分鐘</option>
      <option value="5">每 5 分鐘</option>
      <option value="10">每 10 分鐘</option>
      <option value="30">每 30 分鐘</option>
      <option value="60">每 60 分鐘</option>
    </select>
    <div class="note">自選股與持股請在網頁版編輯,小工具會自動同步。</div>
  </div>
  <div class="sec">
    <div class="row" style="flex-wrap:wrap">
      <button class="btn" id="to-page">改用網頁版</button>
      <button class="btn danger" id="quit">結束程式</button>
    </div>
  </div>
</div>

<script>
(() => {
  "use strict";
  const $ = id => document.getElementById(id);
  const num = s => { if (s === null || s === undefined) return NaN; const v = parseFloat(String(s).replace(/,/g, "")); return Number.isFinite(v) ? v : NaN; };
  const fmt = (v, d = 2) => Number.isFinite(v) ? v.toLocaleString("zh-TW", { minimumFractionDigits: d, maximumFractionDigits: d }) : "—";
  const fmt0 = v => Number.isFinite(v) ? Math.round(v).toLocaleString("zh-TW") : "—";
  const colorOf = v => v > 0 ? "var(--up)" : v < 0 ? "var(--down)" : "var(--flat)";
  const sign = v => v > 0 ? "▲" : v < 0 ? "▼" : "─";
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  const K = { wl: "tw-watchlist", hold: "tw-holdings", fee: "tw-feecfg", last: "tw-lastknown",
              theme: "tw-w-theme", mainTheme: "tw-theme", refresh: "tw-w-refresh", opacity: "tw-w-opacity", top: "tw-w-top" };
  let S = {}, isWin = false, stopped = false;
  let watchlist = [], holdings = {}, feeCfg = { disc: 0.6, inclFee: false, net: true }, lastKnown = {};
  let stockMap = null, taiexDaily = null, rtMap = {}, rtLive = false, dailyAt = 0, timer = null, busy = false, fails = 0;

  const post = (path, body) => fetch(path, { method: "POST", headers: { "X-TW": "1", "Content-Type": "application/json" },
                                             body: JSON.stringify(body || {}) });
  const getJSON = async url => { const r = await fetch(url, { cache: "no-store" }); if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); };
  const parse = (k, d) => { try { const v = JSON.parse(S[k] ?? "null"); return v ?? d; } catch (e) { return d; } };
  function setState(k, v) {
    S[k] = v;
    try { localStorage.setItem(k, v); } catch (e) {}
    post("/api/state", { state: { [k]: v } }).catch(() => {});
  }
  function readState() {
    const wl = parse(K.wl, []);
    watchlist = Array.isArray(wl) ? wl : [];
    holdings = parse(K.hold, {}) || {};
    feeCfg = Object.assign({ disc: 0.6, inclFee: false, net: true }, parse(K.fee, {}) || {});
    lastKnown = parse(K.last, {}) || {};
  }

  // ── 報價與損益(與網頁版相同算法)──
  const FEE_RATE = 0.001425, MIN_FEE = 20;
  function calcPL(code, price, shares, cost) {
    const gross = (price - cost) * shares, basis = cost * shares;
    if (!feeCfg.net) return { pl: gross, basis };
    const buyFee = feeCfg.inclFee ? 0 : Math.max(basis * FEE_RATE * feeCfg.disc, MIN_FEE);
    const sellFee = Math.max(price * shares * FEE_RATE * feeCfg.disc, MIN_FEE);
    const tax = price * shares * (code.startsWith("00") ? 0.001 : 0.003);
    return { pl: gross - buyFee - sellFee - tax, basis: basis + buyFee };
  }
  // 成交價常在漲跌停鎖死時顯示「-」:跌停鎖死只剩賣單(取賣價),漲停鎖死只剩買單(取買價)
  function rtPrice(it) {
    const first = s => { for (const x of String(s || "").split("_")) { const v = num(x); if (v > 0) return v; } return NaN; };
    for (const v of [num(it.z), num(it.pz)]) if (v > 0) return v;
    const b = first(it.b), a = first(it.a);
    return Number.isFinite(b) ? b : a;
  }
  function rtTag(it, p) {
    const up = num(it.u), dn = num(it.w);
    if (p > 0 && dn > 0 && Math.abs(p - dn) < 1e-6) return "跌停";
    if (p > 0 && up > 0 && Math.abs(p - up) < 1e-6) return "漲停";
    return "";
  }
  function effective(code) {
    const rt = rtMap[code], s = stockMap ? stockMap[code] : null;
    if (rt) {
      const p = rtPrice(rt), y = num(rt.y);
      if (Number.isFinite(p) && y > 0) return { price: p, chg: p - y, pct: (p / y - 1) * 100, tag: rtTag(rt, p) };
      if (y > 0) return { price: y, chg: NaN, pct: NaN, tag: "尚未成交" };
    }
    if (s) { const c = num(s.ClosingPrice), ch = num(s.Change), pv = c - ch; if (Number.isFinite(c)) return { price: c, chg: ch, pct: pv ? (ch / pv) * 100 : NaN }; }
    const lk = lastKnown[code];
    return lk && Number.isFinite(lk.price) ? { price: lk.price, chg: NaN, pct: NaN, stale: true } : null;
  }
  function indexQuote(key) {
    const rt = rtMap[key];
    if (rt) { const p = rtPrice(rt), y = num(rt.y); if (Number.isFinite(p) && y > 0) return { price: p, chg: p - y, pct: (p / y - 1) * 100 }; }
    if (key === "t00" && taiexDaily) {
      const r = taiexDaily, c = num(r["收盤指數"]), dir = r["漲跌(+/-)"] === "-" ? -1 : r["漲跌(+/-)"] === "+" ? 1 : 0;
      if (Number.isFinite(c)) return { price: c, chg: Math.abs(num(r["漲跌點數"])) * dir || 0, pct: Math.abs(num(r["漲跌百分比"])) * dir || 0 };
    }
    return null;
  }
  const nameOf = code => (stockMap && stockMap[code] ? stockMap[code].Name : "") || (rtMap[code] ? rtMap[code].n : "") ||
    (lastKnown[code] ? lastKnown[code].name : "") || "";

  function render() {
    let html = '<div class="idx">';
    for (const [key, label] of [["t00", "加權指數"], ["o00", "櫃買指數"]]) {
      const q = indexQuote(key);
      html += `<div><div class="lbl">${label}</div><div class="v mono">${q ? fmt(q.price) : "—"}</div>
        <div class="s mono" style="color:${q ? colorOf(q.chg) : "var(--faint)"}">${q ? sign(q.chg) + " " + fmt(Math.abs(q.chg)) + " (" + fmt(Math.abs(q.pct)) + "%)" : "—"}</div></div>`;
    }
    html += "</div>";
    let cost = 0, pl = 0, n = 0;
    for (const code of watchlist) {
      const h = holdings[code], e = effective(code);
      if (!h || !(h.shares > 0) || !(h.cost > 0) || !e || !Number.isFinite(e.price)) continue;
      const r = calcPL(code, e.price, h.shares, h.cost); cost += r.basis; pl += r.pl; n++;
    }
    if (n) {
      const ret = cost > 0 ? (pl / cost) * 100 : NaN;
      html += `<div class="pf"><span class="lbl">未實現損益</span><span>
        <span class="v mono" style="color:${colorOf(pl)}">${(pl > 0 ? "+" : "") + fmt0(pl)}</span>
        <span class="s mono" style="color:${colorOf(Number.isFinite(ret) ? ret : 0)}">${Number.isFinite(ret) ? (ret > 0 ? "+" : "") + fmt(ret) + "%" : ""}</span></span></div>`;
    }
    if (!watchlist.length) html += '<div class="msg">尚無自選股,請在網頁版加入</div>';
    for (const code of watchlist) {
      const e = effective(code), col = !e || e.stale ? "var(--faint)" : colorOf(e.chg);
      const q = !e ? "—" : e.stale ? "暫無報價" : Number.isFinite(e.pct) ? sign(e.chg) + " " + fmt(Math.abs(e.pct)) + "%" : (e.tag || "—");
      html += `<div class="r"><span class="n"><span class="c mono">${esc(code)}</span>${esc(nameOf(code))}${e && e.tag && Number.isFinite(e.pct) ? " " + e.tag : ""}</span>
        <span class="p mono" style="color:${col}">${e ? fmt(e.price) : "—"}</span><span class="q mono" style="color:${col}">${q}</span></div>`;
    }
    $("w").innerHTML = html;
  }

  async function loadState() {
    const j = await getJSON("/api/state");
    S = (j && j.state) || {};
    isWin = !!(j && j.win);
    readState();
  }

  async function load() {
    if (busy || stopped) return;
    busy = true;
    try {
      await loadState();
      const codes = watchlist.concat(["t00", "o00"]).join(",");
      const needDaily = !stockMap || Date.now() - dailyAt > 3600e3;
      const [d, ix, rt] = await Promise.allSettled([
        needDaily ? getJSON("/api/daily") : Promise.resolve(null),
        needDaily ? getJSON("/api/index") : Promise.resolve(null),
        getJSON("/api/realtime?codes=" + encodeURIComponent(codes)),
      ]);
      if (d.status === "fulfilled" && Array.isArray(d.value)) {
        const map = {};
        for (const r of d.value) { const o = map[r.Code]; if (!o || (!(num(o.ClosingPrice) > 0) && num(r.ClosingPrice) > 0)) map[r.Code] = r; }
        stockMap = map; dailyAt = Date.now();
      }
      if (ix.status === "fulfilled" && Array.isArray(ix.value)) taiexDaily = ix.value.find(r => r["指數"] === "發行量加權股價指數") || taiexDaily;
      rtLive = false;
      if (rt.status === "fulfilled" && Array.isArray(rt.value)) {
        rtMap = {};
        for (const it of rt.value) { if (it.c) rtMap[it.c] = it; if (Number.isFinite(rtPrice(it))) rtLive = true; }
      }
      fails = 0;
      $("st").textContent = rtLive ? "即時" : stockMap ? "收盤" : "無資料";
      $("st").className = "st" + (rtLive || stockMap ? "" : " off");
      $("tm").textContent = new Date().toLocaleTimeString("zh-TW", { hour12: false });
      render();
    } catch (e) {
      if (++fails >= 2) {
        $("st").textContent = "未連線"; $("st").className = "st off";
        $("w").innerHTML = '<div class="msg">無法連線到本機程式。<br>程式可能已結束,請重新執行「啟動.bat」。</div>';
      }
    } finally {
      busy = false;
    }
  }

  // ── 設定面板 ──
  function applyTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    [...$("theme").children].forEach(b => b.classList.toggle("on", b.dataset.t === t));
  }
  function setTimer(min) {
    if (timer) clearInterval(timer);
    timer = setInterval(load, Math.max(0.5, min) * 60000);
  }
  function syncPanel() {
    const op = Math.round(Math.min(1, Math.max(0.3, num(S[K.opacity] ?? "1") || 1)) * 100);
    $("op").value = op; $("op-val").textContent = op + "%";
    $("top").checked = (S[K.top] ?? "1") !== "0";
    $("sec-win").hidden = !isWin; $("sec-nowin").hidden = isWin;
    applyTheme(S[K.theme] || S[K.mainTheme] || "light");
    const rf = S[K.refresh] || "1";
    $("refresh").value = [...$("refresh").options].some(o => o.value === rf) ? rf : "1";
  }
  let opTimer = null;
  function sendWindowStyle() {
    clearTimeout(opTimer);
    opTimer = setTimeout(() => {
      const opacity = (+$("op").value) / 100, top = $("top").checked;
      setState(K.opacity, String(opacity));
      setState(K.top, top ? "1" : "0");
      post("/api/widget", { opacity, top }).catch(() => {});
    }, 120);
  }

  $("gear").addEventListener("click", () => {
    const p = $("panel"), open = p.hidden;
    if (open) syncPanel();
    p.hidden = !open;
    $("gear").classList.toggle("on", open);
  });
  $("op").addEventListener("input", () => { $("op-val").textContent = $("op").value + "%"; sendWindowStyle(); });
  $("top").addEventListener("change", sendWindowStyle);
  $("theme").addEventListener("click", e => {
    const b = e.target.closest("button");
    if (!b) return;
    applyTheme(b.dataset.t);
    setState(K.theme, b.dataset.t);
  });
  $("refresh").addEventListener("change", () => { setState(K.refresh, $("refresh").value); setTimer(+$("refresh").value); });
  $("to-page").addEventListener("click", async () => {
    try { await post("/api/mode", { mode: "page" }); } catch (e) {}
    $("panel").hidden = true;
    $("w").innerHTML = '<div class="msg">正在開啟網頁版,這個小工具會自動關閉…</div>';
  });
  $("quit").addEventListener("click", async () => {
    if (!confirm("確定要結束台股觀察嗎?")) return;
    stopped = true;
    try { await post("/api/quit", {}); } catch (e) {}
    $("panel").hidden = true;
    $("w").innerHTML = '<div class="msg">程式已結束,可以關閉這個視窗。</div>';
  });

  // ── 與本機程式保持連線:所有畫面都關閉後,程式會自動結束 ──
  const CID = "w-" + Math.random().toString(36).slice(2);
  const beat = () => { if (!stopped) post("/api/heartbeat?id=" + CID, {}).catch(() => {}); };
  setInterval(beat, 20000);
  addEventListener("pagehide", () => { try { navigator.sendBeacon("/api/bye?id=" + CID); } catch (e) {} });
  addEventListener("pageshow", e => { if (e.persisted) beat(); });

  // ── 啟動 ──
  (async () => {
    beat();
    try { await loadState(); } catch (e) {}
    syncPanel();
    setTimer(num(S[K.refresh] || "1") || 1);
    load();
  })();
})();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body, ctype):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status, obj):
        self._send(status, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def do_GET(self):
        try:
            self._route()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            log(f"處理 {self.path} 時發生錯誤:{type(e).__name__}: {e}")
            traceback.print_exc()
            try:
                self._send_json(500, {"error": f"本機程式內部錯誤:{type(e).__name__}: {e}"})
            except Exception:
                pass

    def do_POST(self):
        try:
            self._post()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            log(f"處理 POST {self.path} 時發生錯誤:{type(e).__name__}: {e}")
            try:
                self._send_json(500, {"error": f"{type(e).__name__}: {e}"})
            except Exception:
                pass

    def _post(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        n = int(self.headers.get("Content-Length") or 0)
        if n > 2_000_000:
            self._send_json(413, {"error": "資料太大"})
            return
        raw = self.rfile.read(n) if n else b""
        if not _origin_ok(self.headers.get("Origin")):
            self._send_json(403, {"error": "來源不允許"})
            return
        cid = (qs.get("id") or [""])[0][:40]

        if parsed.path == "/api/bye":                 # 頁面關閉時以 sendBeacon 通知(無法自訂標頭)
            if cid:
                client_bye(cid)
            self._send(204, b"", "text/plain")
            return
        if self.headers.get("X-TW") != "1":           # 其餘寫入一律要求自訂標頭,防止其他網站偽造
            self._send_json(403, {"error": "缺少必要標頭"})
            return

        if parsed.path == "/api/heartbeat":
            if cid:
                client_beat(cid)
            self._send_json(200, {"ok": True})
            return

        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except ValueError:
            self._send_json(400, {"error": "JSON 格式錯誤"})
            return

        if parsed.path == "/api/state":
            st = body.get("state")
            if not isinstance(st, dict) or len(st) > 50:
                self._send_json(400, {"error": "state 格式錯誤"})
                return
            clean = {k: v for k, v in st.items()
                     if isinstance(k, str) and re.fullmatch(r"tw-[a-z0-9-]{1,40}", k)
                     and isinstance(v, str) and len(v) <= 500_000}
            update_data(lambda d: d["state"].update(clean))
            self._send_json(200, {"ok": True, "saved": len(clean)})
            return

        if parsed.path == "/api/mode":
            mode = body.get("mode")
            if mode not in ("page", "widget"):
                self._send_json(400, {"error": "mode 錯誤"})
                return
            threading.Thread(target=set_mode, args=(mode,), daemon=True).start()
            self._send_json(200, {"ok": True, "app_browser": IS_WIN or bool(find_app_browser())})
            return

        if parsed.path == "/api/widget":
            try:
                opacity = min(1.0, max(0.3, float(body.get("opacity", 1))))
            except (TypeError, ValueError):
                opacity = 1.0
            if IS_WIN:
                hwnd = _native["hwnd"]
                if hwnd:
                    _apply_native_style(hwnd, {"opacity": opacity, "top": bool(body.get("top", True))})
            else:
                hwnd = None
            self._send_json(200, {"ok": True, "applied": bool(hwnd)})
            return

        if parsed.path == "/api/show":
            threading.Thread(target=open_ui, daemon=True).start()
            self._send_json(200, {"ok": True})
            return

        if parsed.path == "/api/update/check":
            self._send_json(200, update_status(True))
            return

        if parsed.path == "/api/update/source":
            repo = str(body.get("repo") or "").strip()
            if repo and not _update_base(repo):
                self._send_json(400, {"error": "網址格式不正確,請貼上 GitHub 倉庫網址,例如 https://github.com/帳號/tw-dashboard"})
                return
            update_data(lambda d: d.__setitem__("update_repo", repo))
            with _upd_lock:
                _upd.update(at=0.0, info=None)
            self._send_json(200, {"ok": True})
            return

        if parsed.path in ("/api/update/apply", "/api/update/rollback"):
            try:
                if parsed.path.endswith("apply"):
                    v = apply_update()
                    reason = f"更新為 v{v}"
                else:
                    rollback_update()
                    v, reason = None, "退回上一版"
            except Exception as e:
                self._send_json(400, {"error": str(e)})
                return
            self._send_json(200, {"ok": True, "version": v})
            _dtimer(1.0, restart_app, args=(reason,)).start()
            return

        if parsed.path == "/api/quit":
            self._send_json(200, {"ok": True})
            _dtimer(0.5, shutdown_app, args=("使用者按下結束程式",)).start()
            return

        self._send_json(404, {"error": "not found"})

    def _route(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/ping":
            self._send(200, APP_ID, "text/plain")
            return

        if parsed.path == "/widget":
            self._send(200, WIDGET_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return

        if parsed.path == "/api/version":
            self._send_json(200, {"version": APP_VERSION})
            return

        if parsed.path == "/api/update/status":
            self._send_json(200, update_status(False))
            return

        if parsed.path == "/api/state":
            d = load_data()
            self._send_json(200, {"mode": d.get("mode", "page"), "state": d.get("state", {}),
                                  "win": IS_WIN, "port": ACTIVE_PORT})
            return

        if parsed.path in ("/", "/index.html"):
            self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            return

        if parsed.path == "/api/realtime":
            qs = urllib.parse.parse_qs(parsed.query)
            raw = (qs.get("codes") or [""])[0]
            codes = [c for c in raw.split(",") if re.fullmatch(r"[0-9A-Za-z]{1,6}", c)]
            codes = codes[:60]
            if not codes:
                self._send_json(400, {"error": "缺少 codes 參數"})
                return
            try:
                self._send_json(200, get_realtime(codes))
            except Exception as e:
                log(f"/api/realtime 失敗:{e}")
                self._send_json(502, {"error": f"{type(e).__name__}: {e}"})
            return

        if parsed.path == "/api/txf":
            try:
                self._send_json(200, get_txf())
            except Exception as e:
                self._send_json(502, {"error": f"台指期報價取得失敗:{e}"})
            return

        if parsed.path.startswith("/api/us/"):
            qs = urllib.parse.parse_qs(parsed.query)
            try:
                if parsed.path == "/api/us/quotes":
                    syms = [x.strip().upper() for x in (qs.get("symbols") or [""])[0].split(",") if x.strip()]
                    syms = [x for x in syms if US_SYMBOL_RE.match(x)][:30]
                    self._send_json(200, get_us_quotes(syms))
                    return
                sym = (qs.get("symbol") or [""])[0].strip().upper()
                if not US_SYMBOL_RE.match(sym):
                    self._send_json(400, {"error": "美股代號格式不正確"})
                    return
                if parsed.path == "/api/us/intraday":
                    self._send_json(200, us_intraday(sym))
                    return
                if parsed.path == "/api/us/history":
                    try:
                        months = min(12, max(1, int((qs.get("months") or ["3"])[0])))
                    except ValueError:
                        months = 3
                    self._send_json(200, us_history(sym, months))
                    return
            except Exception as e:
                self._send_json(502, {"error": str(e)})
                return

        if parsed.path == "/api/afterhours":
            qs = urllib.parse.parse_qs(parsed.query)
            codes = [c for c in (qs.get("codes") or [""])[0].split(",") if re.fullmatch(r"[0-9A-Za-z]{1,6}", c)]
            try:
                a = get_afterhours()
                self._send_json(200, {"date": a["date"],
                                      "fixed": {c: a["fixed"][c] for c in codes if c in a["fixed"]},
                                      "odd": {c: a["odd"][c] for c in codes if c in a["odd"]}})
            except Exception as e:
                self._send_json(502, {"error": str(e)})
            return

        if parsed.path == "/api/intraday":
            qs = urllib.parse.parse_qs(parsed.query)
            code = (qs.get("code") or [""])[0]
            if not re.fullmatch(r"[0-9A-Za-z]{1,6}", code):
                self._send_json(400, {"error": "代號格式不正確"})
                return
            t0 = time.time()
            try:
                if code == "txf":
                    self._send_json(200, txf_intraday())
                    return
                r = add_tw_axis(get_intraday(code))
                log(f"今日分時 {code}:{r['source']},{len(r['points'])} 點,{time.time() - t0:.1f} 秒")
                self._send_json(200, r)
            except Exception as e:
                log(f"今日分時 {code} 失敗({time.time() - t0:.1f} 秒):{e}")
                self._send_json(502, {"error": str(e)})
            return

        if parsed.path == "/api/history":
            qs = urllib.parse.parse_qs(parsed.query)
            code = (qs.get("code") or [""])[0]
            if not re.fullmatch(r"[0-9A-Za-z]{1,6}", code):
                self._send_json(400, {"error": "代號格式不正確"})
                return
            try:
                months = int((qs.get("months") or ["3"])[0])
            except ValueError:
                months = 3
            months = min(12, max(1, months))
            try:
                self._send_json(200, get_history(code, months))
            except Exception as e:
                log(f"/api/history {code} 失敗:{e}")
                self._send_json(502, {"error": str(e)})
            return

        kind = {"/api/daily": "daily", "/api/index": "index",
                "/api/valuation": "valuation"}.get(parsed.path)
        if kind:
            try:
                self._send_json(200, get_data(kind))
            except Exception as e:
                log(f"/api/{kind} 失敗:{e}")
                self._send_json(502, {"error": str(e)})
            return

        self._send(404, b"not found", "text/plain")

    def log_message(self, fmt, *args):
        pass


_watch_codes = {"t00", "o00"}
_watch_seen = 0.0


def _bg_recorder():
    """交易時段每 60 秒抓一次即時報價並寫入分時記錄(與網頁共用快取)。"""
    last_err_log = 0.0
    while True:
        try:
            if _in_session() and time.time() - _watch_seen < 6 * 3600:
                get_realtime(sorted(_watch_codes))
        except Exception as e:
            if time.time() - last_err_log > 600:
                log(f"背景分時記錄暫時失敗:{type(e).__name__}: {e}")
                last_err_log = time.time()
        time.sleep(60)


APP_ID = b"tw-dashboard"
_NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # 連本機不走公司 Proxy


class Server(ThreadingHTTPServer):
    daemon_threads = True
    # Windows 上若允許位址重用,兩個程式可以搶同一個埠而不報錯,改為不允許以便偵測衝突
    allow_reuse_address = os.name != "nt"


def _app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _write_error_log(text):
    for d in (_app_dir(), os.environ.get("TEMP") or os.path.expanduser("~")):
        try:
            path = os.path.join(d, "tw_dashboard_error.log")
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"\n===== {datetime.now(TW_TZ):%Y-%m-%d %H:%M:%S} =====\n{text}\n")
            return path
        except OSError:
            continue
    return None


def _already_running(port):
    try:
        with _NO_PROXY.open(f"http://127.0.0.1:{port}/api/ping", timeout=1.5) as r:
            return r.read() == APP_ID
    except Exception:
        return False


def _pause(msg):
    try:
        input(msg)
    except Exception:
        time.sleep(20)



# ══════════════════════ v19:資料檔、生命週期、小工具視窗 ══════════════════════
import subprocess

ACTIVE_PORT = PORT
_server = None
WIDGET_TITLE = "台股觀察 小工具"
IS_WIN = os.name == "nt"
DATA_LOCK = threading.Lock()


# ── 資料檔:自選股、持股、設定都存在程式資料夾,不同瀏覽器看到同一份 ──
def _data_path():
    return os.path.join(_app_dir(), "tw_dashboard_data.json")


def load_data():
    try:
        with open(_data_path(), encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            if d.get("mode") not in ("page", "widget"):
                d["mode"] = "page"
            if not isinstance(d.get("state"), dict):
                d["state"] = {}
            return d
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"資料檔讀取失敗,改用空白資料:{e}")
    return {"mode": "page", "state": {}}


def update_data(fn):
    with DATA_LOCK:
        d = load_data()
        fn(d)
        path = _data_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
        return d


# ── 生命週期:畫面定時回報;全部關閉後自動結束 ──
CLOSE_GRACE = 15            # 最後一個畫面關閉後等待秒數(容許重新整理)
CLIENT_TIMEOUT = 6 * 3600   # 長時間沒回報才視為已關閉(瀏覽器休眠分頁不會誤判)
WIDGET_CLIENT_TIMEOUT = 600  # 小工具視窗的保險時限
NEVER_TIMEOUT = 300         # 啟動後一直沒有畫面連線,5 分鐘後結束
_clients = {}
_hb_lock = threading.Lock()
_ever_client = False
_empty_since = None
_started_at = time.time()


def client_beat(cid):
    global _ever_client, _empty_since
    with _hb_lock:
        _clients[cid] = time.time()
        _ever_client = True
        _empty_since = None


def client_bye(cid):
    global _empty_since
    with _hb_lock:
        _clients.pop(cid, None)
        if not _clients and _empty_since is None:
            _empty_since = time.time()


def _watchdog():
    global _empty_since
    while True:
        time.sleep(3)
        now = time.time()
        with _hb_lock:
            for k, t in list(_clients.items()):
                # 小工具視窗不會被瀏覽器休眠,10 分鐘沒回報即視為已關閉;網頁分頁可能休眠,給較長時限
                limit = WIDGET_CLIENT_TIMEOUT if k.startswith("w-") else CLIENT_TIMEOUT
                if now - t > limit:
                    _clients.pop(k, None)
                    if not _clients and _empty_since is None:
                        _empty_since = now
            if _clients:
                continue
            if not _ever_client:
                if now - _started_at < NEVER_TIMEOUT:
                    continue
                reason = "啟動後一直沒有開啟畫面"
            else:
                if _empty_since is None:
                    _empty_since = now
                if now - _empty_since < CLOSE_GRACE:
                    continue
                reason = "所有畫面都已關閉"
        shutdown_app(reason)
        return


def shutdown_app(reason):
    log(f"程式結束:{reason}")
    try:
        close_widget()
    except Exception:
        pass
    if _server is not None:
        threading.Thread(target=_server.shutdown, daemon=True).start()


# ── Windows 視窗控制(置頂、透明度、位置)──
if IS_WIN:
    import ctypes
    from ctypes import wintypes
    _u32 = ctypes.WinDLL("user32", use_last_error=True)
    _WNDENUMPROC = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    _u32.EnumWindows.argtypes = [_WNDENUMPROC, wintypes.LPARAM]
    _u32.GetWindowTextLengthW.argtypes = [wintypes.HWND]
    _u32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    _u32.IsWindowVisible.argtypes = [wintypes.HWND]
    _u32.IsIconic.argtypes = [wintypes.HWND]
    _u32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND, ctypes.c_int, ctypes.c_int,
                                  ctypes.c_int, ctypes.c_int, wintypes.UINT]
    _u32.GetWindowLongW.argtypes = [wintypes.HWND, ctypes.c_int]
    _u32.GetWindowLongW.restype = ctypes.c_long
    _u32.SetWindowLongW.argtypes = [wintypes.HWND, ctypes.c_int, ctypes.c_long]
    _u32.SetWindowLongW.restype = ctypes.c_long
    _u32.SetLayeredWindowAttributes.argtypes = [wintypes.HWND, wintypes.COLORREF, wintypes.BYTE, wintypes.DWORD]
    _u32.PostMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    _u32.GetWindowRect.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.RECT)]
    _u32.MoveWindow.argtypes = [wintypes.HWND, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, wintypes.BOOL]
    _u32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
    _u32.SetForegroundWindow.argtypes = [wintypes.HWND]
    _u32.SystemParametersInfoW.argtypes = [wintypes.UINT, wintypes.UINT, ctypes.c_void_p, wintypes.UINT]
    _u32.GetSystemMetrics.argtypes = [ctypes.c_int]
    _u32.MessageBoxW.argtypes = [wintypes.HWND, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.UINT]
    try:
        _u32.SetProcessDPIAware()          # 以實際像素計算,高 DPI 螢幕位置才準確
    except Exception:
        pass
    HWND_TOPMOST = wintypes.HWND(-1)
    HWND_NOTOPMOST = wintypes.HWND(-2)
    GWL_EXSTYLE, WS_EX_LAYERED, LWA_ALPHA = -20, 0x00080000, 0x2
    SWP_NOSIZE, SWP_NOMOVE, SWP_NOACTIVATE = 0x1, 0x2, 0x10
    WM_CLOSE, SW_RESTORE, SPI_GETWORKAREA = 0x0010, 9, 0x0030


def find_widget_hwnd():
    if not IS_WIN:
        return None
    found = []

    def cb(hwnd, lp):
        n = _u32.GetWindowTextLengthW(hwnd)
        if n and _u32.IsWindowVisible(hwnd):
            buf = ctypes.create_unicode_buffer(n + 1)
            _u32.GetWindowTextW(hwnd, buf, n + 1)
            if buf.value.startswith(WIDGET_TITLE):
                found.append(hwnd)
        return True

    _u32.EnumWindows(_WNDENUMPROC(cb), 0)
    return found[0] if found else None


def _widget_cfg(d):
    st = d.get("state", {})
    try:
        opacity = float(st.get("tw-w-opacity", "1"))
    except (TypeError, ValueError):
        opacity = 1.0
    return min(1.0, max(0.3, opacity)), st.get("tw-w-top", "1") != "0"


def apply_widget_style(hwnd, opacity, top):
    if not IS_WIN or not hwnd:
        return
    ex = _u32.GetWindowLongW(hwnd, GWL_EXSTYLE)
    if opacity >= 0.999:
        if ex & WS_EX_LAYERED:                 # 完全不透明時移除分層屬性,避免部分顯示卡繪圖異常
            _u32.SetWindowLongW(hwnd, GWL_EXSTYLE, ex & ~WS_EX_LAYERED)
    else:
        if not ex & WS_EX_LAYERED:
            _u32.SetWindowLongW(hwnd, GWL_EXSTYLE, ex | WS_EX_LAYERED)
        _u32.SetLayeredWindowAttributes(hwnd, 0, int(round(255 * opacity)), LWA_ALPHA)
    _u32.SetWindowPos(hwnd, HWND_TOPMOST if top else HWND_NOTOPMOST, 0, 0, 0, 0,
                      SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE)


def _screen_scale():
    try:
        return max(1.0, _u32.GetDpiForSystem() / 96.0)
    except Exception:
        return 1.0


def _default_rect(d):
    r = wintypes.RECT()
    _u32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(r), 0)
    sc = _screen_scale()
    try:
        n = len(json.loads(d.get("state", {}).get("tw-watchlist", "[]")))
    except Exception:
        n = 6
    w = int(340 * sc)
    h = int(min(150 + max(n, 1) * 30 + 40, 680) * sc)
    m = int(16 * sc)
    return [r.right - w - m, r.bottom - h - m, w, h]


def _rect_on_screen(rect):
    try:
        vx, vy = _u32.GetSystemMetrics(76), _u32.GetSystemMetrics(77)
        vw, vh = _u32.GetSystemMetrics(78), _u32.GetSystemMetrics(79)
        x, y, w, h = rect
        return w >= 150 and h >= 100 and x + 60 > vx and y >= vy - 10 and x < vx + vw - 60 and y < vy + vh - 60
    except Exception:
        return False


def find_app_browser():
    cands = []
    if IS_WIN:
        for base in (os.environ.get("ProgramFiles(x86)"), os.environ.get("ProgramFiles"), os.environ.get("LOCALAPPDATA")):
            if base:
                cands.append(os.path.join(base, "Microsoft", "Edge", "Application", "msedge.exe"))
        for base in (os.environ.get("ProgramFiles"), os.environ.get("ProgramFiles(x86)"), os.environ.get("LOCALAPPDATA")):
            if base:
                cands.append(os.path.join(base, "Google", "Chrome", "Application", "chrome.exe"))
    elif sys.platform == "darwin":
        cands += ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"]
    for c in cands:
        if os.path.isfile(c):
            return c
    return None


def _place_widget_when_ready():
    if not IS_WIN:
        return
    hwnd = None
    for _ in range(40):
        time.sleep(0.5)
        hwnd = find_widget_hwnd()
        if hwnd:
            break
    if not hwnd:
        log("找不到小工具視窗,無法設定置頂與透明度")
        return
    d = load_data()
    rect = d.get("widget_rect")
    if not (isinstance(rect, list) and len(rect) == 4 and _rect_on_screen(rect)):
        rect = _default_rect(d)
    _u32.MoveWindow(hwnd, int(rect[0]), int(rect[1]), int(rect[2]), int(rect[3]), True)
    apply_widget_style(hwnd, *_widget_cfg(d))


def widget_profile_dir():
    base = os.environ.get("LOCALAPPDATA") or _app_dir()
    return os.path.join(base, "TwDashboard", "widget-profile")


def widget_browser_args(exe, url):
    """小工具使用獨立的瀏覽器執行環境:
    - 獨立設定檔:確保下列參數生效(若沿用使用者已開啟的 Edge,參數會被忽略)
    - 關閉顯示卡合成:Edge/Chrome 預設以 DirectComposition 繪製,Windows 的視窗透明度
      只會讓畫面與黑色混合而變暗;改用軟體繪製後,透明度才會真正看得到後方內容
    """
    return [exe, f"--app={url}", f"--user-data-dir={widget_profile_dir()}",
            "--no-first-run", "--no-default-browser-check",
            "--disable-gpu", "--disable-gpu-compositing", "--disable-direct-composition",
            "--window-size=340,480"]


def open_widget():
    hwnd = find_widget_hwnd()
    if hwnd:
        _u32.ShowWindow(hwnd, SW_RESTORE)
        _u32.SetForegroundWindow(hwnd)
        return True
    url = f"http://localhost:{ACTIVE_PORT}/widget"
    exe = find_app_browser()
    if exe:
        try:
            subprocess.Popen(widget_browser_args(exe, url),
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError as e:
            log(f"無法啟動 {exe}:{e}")
            exe = None
    if not exe:
        log("找不到 Edge 或 Chrome,小工具改以一般瀏覽器分頁開啟")
        webbrowser.open(url)
    threading.Thread(target=_place_widget_when_ready, daemon=True).start()
    return bool(exe)


def close_widget():
    hwnd = find_widget_hwnd()
    if hwnd:
        _u32.PostMessageW(hwnd, WM_CLOSE, 0, 0)


def open_page():
    webbrowser.open(f"http://localhost:{ACTIVE_PORT}")


def open_ui():
    if load_data().get("mode") == "widget":
        open_widget()
    else:
        open_page()


def set_mode(mode):
    update_data(lambda d: d.__setitem__("mode", mode))
    if mode == "widget":
        open_widget()
    else:
        open_page()
        _dtimer(4, close_widget).start()


def _widget_keeper():
    """小工具模式下定時:維持置頂與透明度、記住視窗位置與大小。"""
    while True:
        time.sleep(15)
        if not IS_WIN:
            continue
        try:
            d = load_data()
            if d.get("mode") != "widget":
                continue
            hwnd = find_widget_hwnd()
            if not hwnd:
                continue
            apply_widget_style(hwnd, *_widget_cfg(d))
            if not _u32.IsIconic(hwnd):
                r = wintypes.RECT()
                _u32.GetWindowRect(hwnd, ctypes.byref(r))
                rect = [r.left, r.top, r.right - r.left, r.bottom - r.top]
                if rect != d.get("widget_rect") and _rect_on_screen(rect):
                    update_data(lambda x: x.__setitem__("widget_rect", rect))
        except Exception as e:
            log(f"小工具視窗維護失敗:{type(e).__name__}: {e}")


def _origin_ok(origin):
    return origin in (None, "", f"http://localhost:{ACTIVE_PORT}", f"http://127.0.0.1:{ACTIVE_PORT}")


def _trim_log():
    try:
        p = os.path.join(_app_dir(), "tw_dashboard.log")
        if os.path.getsize(p) > 1_000_000:
            os.replace(p, p + ".old")
    except OSError:
        pass




# ══════════════════════ v22:台指期(日盤/夜盤)、盤後定價與零股、美股 ══════════════════════
from concurrent.futures import ThreadPoolExecutor

US_INDICES = [("^GSPC", "S&P 500"), ("^IXIC", "那斯達克"), ("^SOX", "費城半導體"), ("^DJI", "道瓊")]
US_COMMODITIES = [("GC=F", "黃金"), ("CL=F", "西德州原油"), ("BZ=F", "布蘭特原油"), ("TWD=X", "美元/台幣")]
US_INDEX_NAMES = dict(US_INDICES + US_COMMODITIES)
US_SYMBOL_RE = re.compile(r"^\^?[A-Z][A-Z0-9.\-=]{0,11}$")      # 含期貨(GC=F)與匯率(TWD=X)代號
TAIFEX_QUOTE = "https://mis.taifex.com.tw/futures/api/getQuoteList"


def _fnum(v):
    try:
        x = float(str(v).replace(",", ""))
        return x if x == x else None
    except (TypeError, ValueError):
        return None


def _cached(key, ttl, fn):
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _lock:
        _cache[key] = (time.time(), val)
    return val


# ── 時段判斷(一律以台灣時間)──
def tx_session(now=None):
    """台指期:日盤 08:45–13:45;夜盤 15:00–翌日 05:00。回傳 (時段, 是否交易中)。"""
    now = now or datetime.now(TW_TZ)
    hm, wd = now.strftime("%H:%M"), now.weekday()
    if wd < 5 and "08:45" <= hm <= "13:45":
        return "day", True
    if (wd < 5 and hm >= "15:00") or (1 <= wd <= 5 and hm < "05:00"):
        return "night", True
    if wd < 5 and "13:45" < hm < "15:00":
        return "day", False
    return "night", False


def tx_session_id(sess, now=None):
    now = now or datetime.now(TW_TZ)
    if sess == "night" and now.strftime("%H:%M") < "05:00":
        now = now - timedelta(days=1)
    return f"{now.strftime('%Y-%m-%d')}-{sess}"


def us_is_dst(now=None):
    """美國夏令時間:三月第二個週日 2:00 到十一月第一個週日 2:00(美東)。"""
    u = (now or datetime.now(TW_TZ)).astimezone(timezone.utc)

    def nth_sunday(month, n):
        d = datetime(u.year, month, 1, tzinfo=timezone.utc)
        return d + timedelta(days=(6 - d.weekday()) % 7 + 7 * (n - 1))

    return nth_sunday(3, 2).replace(hour=7) <= u < nth_sunday(11, 1).replace(hour=6)


def us_open_tw(now=None):
    return "21:30" if us_is_dst(now) else "22:30"


def widget_auto_view(now=None):
    """小工具依時段自動切換:台股 → 台指期夜盤 → 美股。"""
    now = now or datetime.now(TW_TZ)
    hm, wd = now.strftime("%H:%M"), now.weekday()
    if wd < 5 and "08:30" <= hm < "14:30":
        return "tw"
    if wd < 5 and hm >= us_open_tw(now):
        return "us"
    if wd == 0 and hm < "08:30":
        return "tw"
    if 1 <= wd <= 5 and hm < "08:30":
        return "us"                     # 美股收盤後到台股開盤前,顯示美股最新收盤
    if wd < 5:
        return "night"                  # 台股收盤後到美股開盤前
    return "tw"


# ── 台指期 ──
def _taifex_list(market_type):
    def fetch():
        body = json.dumps({"MarketType": market_type, "SymbolType": "F", "KindID": "1", "CID": "TXF",
                           "ExpireMonth": "", "RowSize": "全部", "PageNo": "", "SortColumn": "",
                           "AscDesc": "A"}).encode("utf-8")
        h = dict(HEADERS)
        h.update({"Content-Type": "application/json;charset=UTF-8", "Origin": "https://mis.taifex.com.tw",
                  "Referer": "https://mis.taifex.com.tw/futures/"})
        j = json.loads(_open(urllib.request.Request(TAIFEX_QUOTE, data=body, headers=h, method="POST"),
                             timeout=15).decode("utf-8"))
        rows = ((j.get("RtData") or {}).get("QuoteList")) or j.get("QuoteList") or []
        if not isinstance(rows, list):
            raise ValueError("期交所回應格式不符")
        return rows
    return _cached(f"taifex:{market_type}", 10, fetch)


def _pick_near_tx(rows):
    """從報價清單挑出台指期近月(成交量最大者),略過價差單。"""
    cands = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        sid = str(r.get("SymbolID") or "")
        name = str(r.get("DispCName") or r.get("DispEName") or sid)
        if not sid.startswith("TXF") or "/" in sid:          # 價差單代號含「/」;名稱中的「116/12」是月份
            continue
        price = _fnum(r.get("CLastPrice") or r.get("LastPrice"))
        if not price:
            continue
        ref = _fnum(r.get("CRefPrice") or r.get("RefPrice") or r.get("Reference"))
        vol = _fnum(r.get("CTotalVolume") or r.get("TotalVolume")) or 0
        t = str(r.get("CTime") or "").zfill(6) if r.get("CTime") else ""
        cands.append({"symbol": sid, "name": name, "price": price, "ref": ref, "volume": vol,
                      "time": f"{t[:2]}:{t[2:4]}" if t else ""})
    if not cands:
        return None
    q = max(cands, key=lambda c: c["volume"])
    mm = re.match(r"^TXF([A-L])\d", q["symbol"])
    if mm:
        q["month"] = ord(mm.group(1)) - 64
        q["name"] = f"台指期近月({q['month']}月)"
    if q["ref"]:
        q["chg"] = q["price"] - q["ref"]
        q["pct"] = q["chg"] / q["ref"] * 100
    else:
        q["chg"] = q["pct"] = None
    return q


def get_txf():
    sess, active = tx_session()
    q = _pick_near_tx(_taifex_list("0" if sess == "day" else "1"))
    if not q:
        raise RuntimeError("期交所報價中找不到台指期近月")
    return dict(q, session=sess, active=active)


def _record_txf(q):
    sess = q["session"]
    sid = tx_session_id(sess)
    t = q.get("time") or datetime.now(TW_TZ).strftime("%H:%M")
    with _lock:
        rec = _intraday_rec.get("txf")
        if not rec or rec.get("sid") != sid:
            rec = {"sid": sid, "session": sess, "prev": q.get("ref"), "pts": {}}
            _intraday_rec["txf"] = rec
        rec["pts"][t] = q["price"]
        if q.get("ref"):
            rec["prev"] = q["ref"]


def _bg_futures():
    """台指期交易時段每 60 秒記錄一次(期交所沒有公開的分時歷史)。"""
    last_err = 0.0
    while True:
        try:
            sess, active = tx_session()
            if active and time.time() - _watch_seen < 6 * 3600:
                _record_txf(get_txf())
        except Exception as e:
            if time.time() - last_err > 600:
                log(f"台指期記錄暫時失敗:{type(e).__name__}: {e}")
                last_err = time.time()
        time.sleep(60)


TX_AXIS = {
    "day": {"start": "08:45", "dur": 300, "ticks": [[0, "08:45"], [75, "10:00"], [195, "12:00"], [300, "13:45"]]},
    "night": {"start": "15:00", "dur": 840, "ticks": [[0, "15:00"], [180, "18:00"], [360, "21:00"],
                                                     [540, "00:00"], [720, "03:00"], [840, "05:00"]]},
}
TW_AXIS = {"start": "09:00", "dur": 270, "ticks": [[0, "09:00"], [60, "10:00"], [120, "11:00"], [180, "12:00"], [270, "13:30"]]}


def _offset(hm, start):
    a = int(hm[:2]) * 60 + int(hm[3:5])
    b = int(start[:2]) * 60 + int(start[3:5])
    return (a - b) % 1440


def txf_intraday():
    sess, active = tx_session()
    sid = tx_session_id(sess)
    with _lock:
        rec = _intraday_rec.get("txf")
        rec = dict(rec, pts=dict(rec["pts"])) if rec else None
    if not rec or rec.get("sid") != sid or not rec["pts"]:
        raise RuntimeError("台指期走勢由程式開啟期間每分鐘記錄,目前尚無資料。"
                           + ("交易時段內約 1 分鐘後就會出現。" if active else "下個交易時段開始後會自動記錄。"))
    ax = TX_AXIS[sess]
    pts = [{"t": k, "o": _offset(k, ax["start"]), "p": v} for k, v in rec["pts"].items()]
    pts = sorted((p for p in pts if p["o"] <= ax["dur"]), key=lambda p: p["o"])
    label = "夜盤" if sess == "night" else "日盤"
    return {"prev": rec.get("prev") or pts[0]["p"], "date": sid[:10], "points": pts, "axis": ax,
            "prev_label": "參考價", "note": f"{sid[:10]} {label}" + ("" if active else "(已收盤)"),
            "source_label": "本機每分鐘記錄(程式開啟期間)"}


def add_tw_axis(r):
    r = dict(r)
    r["axis"] = TW_AXIS
    r["points"] = [dict(p, o=_offset(p["t"], "09:00")) for p in r.get("points", [])]
    return r


# ── 盤後定價與盤後零股(證交所,收盤後才有當日資料)──
AFTER_EP = {"fixed": "BFT41U", "odd": "TWT53U"}


def _parse_after(j, kind):
    out = {}
    for fields, rows in (_tables_of(j) or []):
        fs = [str(f) for f in (fields or [])]
        ci = next((i for i, f in enumerate(fs) if "代號" in f), None)
        if ci is None:
            continue
        vi = next((i for i, f in enumerate(fs) if "成交股數" in f), None)
        pi = next((i for i, f in enumerate(fs) if "收盤" in f), None) if kind == "odd" else None
        if pi is None:
            pi = next((i for i, f in enumerate(fs) if "成交價" in f), None)
        for r in rows:
            if not isinstance(r, (list, tuple)) or len(r) <= ci:
                continue
            code = strip_tags(r[ci])
            vol = _fnum(strip_tags(r[vi])) if vi is not None and vi < len(r) else None
            price = _fnum(strip_tags(r[pi])) if pi is not None and pi < len(r) else None
            if code and (vol or price):
                out[code] = {"vol": vol, "price": price}
    return out


def get_afterhours():
    def fetch():
        res = {"date": None, "fixed": {}, "odd": {}}
        for kind, ep in AFTER_EP.items():
            for back in range(7):
                d = (datetime.now(TW_TZ) - timedelta(days=back)).strftime("%Y%m%d")
                url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/{ep}?date={d}"
                       f"&selectType=ALL&type=ALL&response=json")
                try:
                    j = json.loads(http_get(url, timeout=20).decode("utf-8"))
                except Exception:
                    continue
                if j.get("stat") not in (None, "OK"):
                    continue
                parsed = _parse_after(j, kind)
                if parsed:
                    res[kind] = parsed
                    res["date"] = res["date"] or f"{d[:4]}-{d[4:6]}-{d[6:]}"
                    break
                time.sleep(0.3)
        if not res["fixed"] and not res["odd"]:
            raise RuntimeError("查無盤後定價與零股資料")
        log(f"盤後資料:定價 {len(res['fixed'])} 檔、零股 {len(res['odd'])} 檔({res['date']})")
        return res
    return _cached("afterhours", 1800, fetch)


# ── 美股(Yahoo 財經)──
def _yahoo_chart(symbol, rng, interval, ttl):
    def fetch():
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
               f"?range={rng}&interval={interval}")
        h = dict(HEADERS)
        h.update({"Referer": "https://finance.yahoo.com/", "Accept": "application/json"})
        j = json.loads(_open(urllib.request.Request(url, headers=h), timeout=12).decode("utf-8"))
        res = ((j.get("chart") or {}).get("result") or [None])[0]
        if not res:
            raise ValueError(f"Yahoo 查無 {symbol}")
        return res
    return _cached(f"ychart:{symbol}:{rng}:{interval}", ttl, fetch)


def _tw_hm(ts):
    return datetime.fromtimestamp(ts, TW_TZ).strftime("%H:%M")


def _us_state(meta):
    now = time.time()
    p = meta.get("currentTradingPeriod") or {}
    for key, label in (("pre", "盤前"), ("regular", "交易中"), ("post", "盤後")):
        seg = p.get(key) or {}
        if seg.get("start") and seg.get("end") and seg["start"] <= now < seg["end"]:
            return label
    return "休市"


def us_quote(symbol):
    res = _yahoo_chart(symbol, "1d", "5m", 45)
    meta = res.get("meta") or {}
    price = _fnum(meta.get("regularMarketPrice"))
    prev = _fnum(meta.get("chartPreviousClose") or meta.get("previousClose"))
    if not price:
        raise ValueError(f"{symbol} 無報價")
    chg = price - prev if prev else None
    t = meta.get("regularMarketTime")
    return {"symbol": symbol, "name": US_INDEX_NAMES.get(symbol) or meta.get("shortName") or meta.get("longName") or symbol,
            "price": price, "prev": prev, "chg": chg, "pct": (chg / prev * 100) if prev else None,
            "state": _us_state(meta), "time": datetime.fromtimestamp(t, TW_TZ).strftime("%m/%d %H:%M") if t else "",
            "currency": meta.get("currency") or "USD"}


_US_POOL = ThreadPoolExecutor(max_workers=6)


def get_us_quotes(symbols):
    def one(s):
        try:
            return us_quote(s)
        except Exception as e:
            return {"symbol": s, "error": f"{type(e).__name__}: {e}"}
    return list(_US_POOL.map(one, symbols))


def us_intraday(symbol):
    res = _yahoo_chart(symbol, "1d", "1m", 60)
    meta = res.get("meta") or {}
    reg = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
    start, end = reg.get("start"), reg.get("end")
    if not start or not end:
        raise ValueError("Yahoo 未提供交易時段")
    ts = res.get("timestamp") or []
    closes = (((res.get("indicators") or {}).get("quote") or [{}])[0].get("close")) or []
    pts = []
    for t, c in zip(ts, closes):
        if c is None or not (start <= t <= end):
            continue
        pts.append({"t": _tw_hm(t), "o": int((t - start) // 60), "p": round(float(c), 4)})
    dur = int((end - start) // 60)
    m0 = datetime.fromtimestamp(start, TW_TZ).minute
    ticks, o = [[0, _tw_hm(start)]], (60 - m0) if m0 else 60
    while o < dur - 20:
        ticks.append([o, _tw_hm(start + o * 60)])
        o += 60
    ticks.append([dur, _tw_hm(end)])
    prev = _fnum(meta.get("chartPreviousClose") or meta.get("previousClose"))
    if not pts or not prev:
        raise RuntimeError("美股尚未開盤,或 Yahoo 暫無今日分時資料")
    trade_date = datetime.fromtimestamp(start, timezone(timedelta(hours=-4 if us_is_dst() else -5))).strftime("%Y-%m-%d")
    return {"prev": prev, "date": trade_date, "points": pts, "prev_label": "前收",
            "axis": {"start": _tw_hm(start), "dur": dur, "ticks": ticks},
            "note": f"{trade_date} 交易日(台灣時間 {_tw_hm(start)}–{_tw_hm(end)})",
            "source_label": "Yahoo 財經"}


def us_history(symbol, months):
    rng = {1: "1mo", 3: "3mo", 6: "6mo", 12: "1y"}.get(months, "3mo")
    res = _yahoo_chart(symbol, rng, "1d", 3600)
    ts = res.get("timestamp") or []
    closes = (((res.get("indicators") or {}).get("quote") or [{}])[0].get("close")) or []
    et = timezone(timedelta(hours=-5))
    pts = [{"d": datetime.fromtimestamp(t, et).strftime("%Y-%m-%d"), "c": round(float(c), 4)}
           for t, c in zip(ts, closes) if c is not None]
    if not pts:
        raise RuntimeError("查無歷史資料")
    return pts



# ══════════════════════ v29:自動更新(OTA)══════════════════════
# 發布方式:在 GitHub 倉庫放 tw_dashboard.py 與 version.json,程式定期檢查並提示更新。
#   version.json 範例:{"version": 30, "file": "tw_dashboard.py", "sha256": "…", "notes": "改版說明", "date": "2026-12-01"}
# 安全檢查:下載後比對 SHA-256、確認程式碼可正常解析且版本號正確,才會替換;舊版保留為 .bak 可退回。
import hashlib
import shutil

DEFAULT_UPDATE_REPO = "https://github.com/hec00914/tw-dashboard"   # 預設更新來源(同事不需自行輸入)
UPDATE_CHECK_HOURS = 6
_upd = {"at": 0.0, "repo": "", "info": None, "error": ""}
_upd_lock = threading.Lock()


def _script_path():
    return os.path.abspath(__file__)


def update_repo():
    return (load_data().get("update_repo") or DEFAULT_UPDATE_REPO or "").strip()


def _update_base(repo):
    """把 GitHub 倉庫網址轉成原始檔下載位置;其他 http(s) 網址視為直接的發布資料夾。"""
    m = re.match(r"^https?://github\.com/([^/\s]+)/([^/\s#?]+?)(?:\.git)?/?$", repo or "")
    if m:
        return f"https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}/main/"
    if re.match(r"^https?://\S+$", repo or ""):
        return repo if repo.endswith("/") else repo + "/"
    return None


def check_update(force=False):
    repo = update_repo()
    base = _update_base(repo)
    if not base:
        raise ValueError("尚未設定更新來源,或網址格式不正確")
    with _upd_lock:
        if not force and _upd["repo"] == repo and _upd["info"] and time.time() - _upd["at"] < UPDATE_CHECK_HOURS * 3600:
            return _upd["info"]
    raw = http_get(f"{base}version.json?t={int(time.time())}", timeout=15)
    info = json.loads(raw.decode("utf-8-sig"))
    if not isinstance(info, dict) or not str(info.get("version", "")).isdigit() or not info.get("sha256"):
        raise ValueError("version.json 格式不正確")
    info["version"] = int(info["version"])
    info.setdefault("file", "tw_dashboard.py")
    with _upd_lock:
        _upd.update(at=time.time(), repo=repo, info=info, error="")
    if info["version"] > APP_VERSION:
        log(f"發現新版本 v{info['version']}(目前 v{APP_VERSION})")
    return info


def update_status(force=False):
    st = {"current": APP_VERSION, "repo": update_repo(), "latest": None, "available": False,
          "notes": "", "date": "", "error": "", "has_backup": os.path.exists(_script_path() + ".bak")}
    st["configured"] = bool(_update_base(st["repo"]))
    if not st["configured"]:
        return st
    try:
        info = check_update(force)
        st.update(latest=info["version"], available=info["version"] > APP_VERSION,
                  notes=str(info.get("notes", ""))[:500], date=str(info.get("date", ""))[:20])
    except Exception as e:
        st["error"] = f"{type(e).__name__}: {e}"
        with _upd_lock:
            _upd["error"] = st["error"]
    return st


def _verify_source(data, info):
    if hashlib.sha256(data).hexdigest().lower() != str(info["sha256"]).lower():
        raise ValueError("下載的檔案與發布說明的指紋不符(可能下載不完整),已取消更新")
    src = data.decode("utf-8")
    compile(src, "tw_dashboard.py", "exec")            # 能正常解析才替換
    if not re.search(rf"^APP_VERSION\s*=\s*{info['version']}\b", src, flags=re.M):
        raise ValueError("下載的程式版本號與發布說明不一致,已取消更新")
    return src


def apply_update():
    info = check_update(force=True)
    if info["version"] <= APP_VERSION:
        raise ValueError("目前已是最新版本")
    base = _update_base(update_repo())
    data = http_get(f"{base}{info['file']}?t={int(time.time())}", timeout=60)
    src = _verify_source(data, info)
    path = _script_path()
    shutil.copy2(path, path + ".bak")                  # 保留舊版,可一鍵退回
    tmp = path + ".new"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(src)
    os.replace(tmp, path)
    log(f"已更新為 v{info['version']},準備重新啟動")
    return info["version"]


def rollback_update():
    path, bak = _script_path(), _script_path() + ".bak"
    if not os.path.exists(bak):
        raise ValueError("沒有可退回的舊版本")
    with open(bak, encoding="utf-8") as f:
        compile(f.read(), "tw_dashboard.py", "exec")
    os.replace(bak, path)
    log("已退回上一版,準備重新啟動")


def restart_app(reason):
    """啟動新的程式,再結束目前的程式;新程式會等舊程式完全結束後才接手。"""
    args = [sys.executable, _script_path(), "--after-update"]
    kw = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL,
          "cwd": _app_dir(), "close_fds": True}
    if IS_WIN:
        kw["creationflags"] = 0x00000008 | 0x00000200    # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
    subprocess.Popen(args, **kw)
    shutdown_app(reason)


def _update_bg():
    time.sleep(15)
    while True:
        try:
            if _update_base(update_repo()):
                check_update()
        except Exception as e:
            log(f"檢查更新失敗:{type(e).__name__}: {e}")
        time.sleep(UPDATE_CHECK_HOURS * 3600)


# ══════════════════════ Windows 原生小工具視窗(v21)══════════════════════
# 不再借用瀏覽器:以 Windows 原生視窗 + GDI 繪製,透明度與置頂都是系統正式支援的做法。
NATIVE_CID = "native-widget"
UI_FONT = os.environ.get("TW_UI_FONT", "Microsoft JhengHei UI")
NUM_FONT = os.environ.get("TW_NUM_FONT", "Consolas")
_native = {"hwnd": None, "thread": None, "snap": None, "h": 0, "wake": threading.Event(), "fonts": {}}

PALETTE = {
    "light": {"bg": (255, 255, 255), "bg2": (238, 241, 244), "line": (220, 225, 232), "text": (24, 32, 43),
              "muted": (89, 101, 117), "faint": (141, 152, 166), "accent": (47, 93, 138),
              "up": (200, 55, 45), "down": (31, 133, 86), "flat": (107, 119, 133), "white": (255, 255, 255)},
    "dark": {"bg": (28, 33, 40), "bg2": (36, 43, 52), "line": (48, 56, 67), "text": (228, 232, 237),
             "muted": (154, 165, 177), "faint": (107, 118, 130), "accent": (138, 175, 211),
             "up": (224, 107, 96), "down": (82, 180, 138), "flat": (140, 151, 163), "white": (255, 255, 255)},
}

if IS_WIN:
    _g32 = ctypes.WinDLL("gdi32", use_last_error=True)
    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    LRESULT = ctypes.c_ssize_t
    WNDPROC = ctypes.WINFUNCTYPE(LRESULT, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)

    class WNDCLASSEXW(ctypes.Structure):
        _fields_ = [("cbSize", wintypes.UINT), ("style", wintypes.UINT), ("lpfnWndProc", WNDPROC),
                    ("cbClsExtra", ctypes.c_int), ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE),
                    ("hIcon", wintypes.HICON), ("hCursor", wintypes.HICON), ("hbrBackground", wintypes.HBRUSH),
                    ("lpszMenuName", wintypes.LPCWSTR), ("lpszClassName", wintypes.LPCWSTR), ("hIconSm", wintypes.HICON)]

    class PAINTSTRUCT(ctypes.Structure):
        _fields_ = [("hdc", wintypes.HDC), ("fErase", wintypes.BOOL), ("rcPaint", wintypes.RECT),
                    ("fRestore", wintypes.BOOL), ("fIncUpdate", wintypes.BOOL), ("rgbReserved", ctypes.c_byte * 32)]

    _k32.GetModuleHandleW.argtypes = [wintypes.LPCWSTR]
    _k32.GetModuleHandleW.restype = wintypes.HMODULE
    _u32.RegisterClassExW.argtypes = [ctypes.POINTER(WNDCLASSEXW)]
    _u32.RegisterClassExW.restype = wintypes.ATOM
    _u32.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
                                     ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                     wintypes.HWND, wintypes.HMENU, wintypes.HINSTANCE, wintypes.LPVOID]
    _u32.CreateWindowExW.restype = wintypes.HWND
    _u32.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    _u32.DefWindowProcW.restype = LRESULT
    _u32.GetMessageW.argtypes = [ctypes.POINTER(wintypes.MSG), wintypes.HWND, wintypes.UINT, wintypes.UINT]
    _u32.GetMessageW.restype = wintypes.BOOL
    _u32.TranslateMessage.argtypes = [ctypes.POINTER(wintypes.MSG)]
    _u32.DispatchMessageW.argtypes = [ctypes.POINTER(wintypes.MSG)]
    _u32.DispatchMessageW.restype = LRESULT
    _u32.PostQuitMessage.argtypes = [ctypes.c_int]
    _u32.BeginPaint.argtypes = [wintypes.HWND, ctypes.POINTER(PAINTSTRUCT)]
    _u32.BeginPaint.restype = wintypes.HDC
    _u32.EndPaint.argtypes = [wintypes.HWND, ctypes.POINTER(PAINTSTRUCT)]
    _u32.InvalidateRect.argtypes = [wintypes.HWND, ctypes.c_void_p, wintypes.BOOL]
    _u32.GetClientRect.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.RECT)]
    _u32.LoadCursorW.argtypes = [wintypes.HINSTANCE, ctypes.c_void_p]
    _u32.LoadCursorW.restype = wintypes.HICON
    _u32.LoadIconW.argtypes = [wintypes.HINSTANCE, ctypes.c_void_p]
    _u32.LoadIconW.restype = wintypes.HICON
    _u32.DestroyWindow.argtypes = [wintypes.HWND]
    _u32.CreatePopupMenu.restype = wintypes.HMENU
    _u32.AppendMenuW.argtypes = [wintypes.HMENU, wintypes.UINT, ctypes.c_size_t, wintypes.LPCWSTR]
    _u32.TrackPopupMenu.argtypes = [wintypes.HMENU, wintypes.UINT, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                    wintypes.HWND, ctypes.c_void_p]
    _u32.TrackPopupMenu.restype = ctypes.c_int
    _u32.DestroyMenu.argtypes = [wintypes.HMENU]
    _u32.ScreenToClient.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.POINT)]
    _u32.ClientToScreen.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.POINT)]
    _u32.GetCursorPos.argtypes = [ctypes.POINTER(wintypes.POINT)]
    _u32.FillRect.argtypes = [wintypes.HDC, ctypes.POINTER(wintypes.RECT), wintypes.HBRUSH]
    _u32.DrawTextW.argtypes = [wintypes.HDC, wintypes.LPCWSTR, ctypes.c_int, ctypes.POINTER(wintypes.RECT), wintypes.UINT]
    _u32.SetLayeredWindowAttributes.restype = wintypes.BOOL
    _g32.CreateCompatibleDC.argtypes = [wintypes.HDC]
    _g32.CreateCompatibleDC.restype = wintypes.HDC
    _g32.CreateCompatibleBitmap.argtypes = [wintypes.HDC, ctypes.c_int, ctypes.c_int]
    _g32.CreateCompatibleBitmap.restype = wintypes.HBITMAP
    _g32.SelectObject.argtypes = [wintypes.HDC, wintypes.HGDIOBJ]
    _g32.SelectObject.restype = wintypes.HGDIOBJ
    _g32.DeleteObject.argtypes = [wintypes.HGDIOBJ]
    _g32.DeleteDC.argtypes = [wintypes.HDC]
    _g32.BitBlt.argtypes = [wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                            wintypes.HDC, ctypes.c_int, ctypes.c_int, wintypes.DWORD]
    _g32.SetBkMode.argtypes = [wintypes.HDC, ctypes.c_int]
    _g32.SetTextColor.argtypes = [wintypes.HDC, wintypes.COLORREF]
    _g32.CreateSolidBrush.argtypes = [wintypes.COLORREF]
    _g32.CreateSolidBrush.restype = wintypes.HBRUSH
    _g32.CreateFontW.argtypes = [ctypes.c_int] * 5 + [wintypes.DWORD] * 8 + [wintypes.LPCWSTR]
    _g32.CreateFontW.restype = wintypes.HFONT

    WS_POPUP, WS_EX_LAYERED_, WS_EX_TOPMOST, WS_EX_APPWINDOW = 0x80000000, 0x00080000, 0x00000008, 0x00040000
    WM_DESTROY, WM_PAINT, WM_CLOSE, WM_ERASEBKGND = 0x0002, 0x000F, 0x0010, 0x0014
    WM_CONTEXTMENU, WM_NCHITTEST, WM_LBUTTONUP, WM_EXITSIZEMOVE = 0x007B, 0x0084, 0x0202, 0x0232
    WM_APP_UPDATE = 0x8001
    WM_TIMER = 0x0113
    WM_MOUSEMOVE, WM_LBUTTONDOWN, WM_MOUSEWHEEL, WM_SIZE = 0x0200, 0x0201, 0x020A, 0x0005
    WM_NCCALCSIZE, WM_GETMINMAXINFO, WM_NCLBUTTONDOWN = 0x0083, 0x0024, 0x00A1
    WS_THICKFRAME, WS_EX_TOOLWINDOW = 0x00040000, 0x00000080
    HTLEFT, HTRIGHT, HTTOP, HTTOPLEFT, HTTOPRIGHT, HTBOTTOM, HTBOTTOMLEFT, HTBOTTOMRIGHT = 10, 11, 12, 13, 14, 15, 16, 17

    class MINMAXINFO(ctypes.Structure):
        _fields_ = [("ptReserved", wintypes.POINT), ("ptMaxSize", wintypes.POINT), ("ptMaxPosition", wintypes.POINT),
                    ("ptMinTrackSize", wintypes.POINT), ("ptMaxTrackSize", wintypes.POINT)]

    _u32.SetCapture.argtypes = [wintypes.HWND]
    _u32.SendMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    _u32.SendMessageW.restype = LRESULT
    _g32.CreatePen.argtypes = [ctypes.c_int, ctypes.c_int, wintypes.COLORREF]
    _g32.CreatePen.restype = wintypes.HGDIOBJ
    _g32.Polyline.argtypes = [wintypes.HDC, ctypes.POINTER(wintypes.POINT), ctypes.c_int]
    _g32.IntersectClipRect.argtypes = [wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int]
    _g32.SelectClipRgn.argtypes = [wintypes.HDC, ctypes.c_void_p]
    _u32.SetTimer.argtypes = [wintypes.HWND, ctypes.c_size_t, wintypes.UINT, ctypes.c_void_p]
    _u32.SetTimer.restype = ctypes.c_size_t
    _u32.InvalidateRect.argtypes = [wintypes.HWND, ctypes.c_void_p, wintypes.BOOL]
    _g32.GetTextExtentPoint32W.argtypes = [wintypes.HDC, wintypes.LPCWSTR, ctypes.c_int, ctypes.POINTER(wintypes.SIZE)]
    HTCLIENT, HTCAPTION = 1, 2
    SW_SHOWNOACTIVATE, SW_RESTORE_ = 4, 9
    TPM_RETURNCMD, TPM_RIGHTBUTTON, TPM_NONOTIFY = 0x0100, 0x0002, 0x0080
    MF_STRING, MF_POPUP, MF_SEPARATOR, MF_CHECKED = 0x0, 0x10, 0x800, 0x8
    DT_LEFT, DT_RIGHT, DT_VCENTER, DT_SINGLELINE, DT_END_ELLIPSIS, DT_NOPREFIX = 0x0, 0x2, 0x4, 0x20, 0x8000, 0x800
    SWP_NOZORDER = 0x4
    SRCCOPY, TRANSPARENT_BK = 0x00CC0020, 1


def _rgb(c):
    return c[0] | (c[1] << 8) | (c[2] << 16)


def _fmt(v, d=2):
    return f"{v:,.{d}f}" if isinstance(v, (int, float)) and v == v else "—"


def _num(s):
    try:
        v = float(str(s).replace(",", ""))
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _sign(v):
    return "▲" if v > 0 else "▼" if v < 0 else "─"


def _ckey(v):
    return "up" if v and v > 0 else "down" if v and v < 0 else "flat"


def _native_cfg(st):
    try:
        refresh = float(st.get("tw-w-refresh", "1"))
    except (TypeError, ValueError):
        refresh = 1.0
    opacity, top = _widget_cfg({"state": st})
    theme = st.get("tw-w-theme") or st.get("tw-theme") or "light"
    view = st.get("tw-w-view", "auto")
    return {"theme": theme if theme in PALETTE else "light", "opacity": opacity, "top": top,
            "refresh": min(60.0, max(0.5, refresh)),
            "view": view if view in ("auto", "tw", "night", "us") else "auto",
            "ticker": st.get("tw-w-ticker", "1") != "0",
            "chart": "window" if st.get("tw-w-chart") == "window" else "inline"}


def _build_snapshot():
    """在背景執行緒取得資料並整理成畫面要顯示的文字與顏色。"""
    d = load_data()
    st = d.get("state", {})
    cfg = _native_cfg(st)

    def jload(k, default):
        try:
            v = json.loads(st.get(k, "null"))
            return v if v is not None else default
        except ValueError:
            return default

    wl = [c for c in jload("tw-watchlist", []) if isinstance(c, str)]
    holdings = jload("tw-holdings", {}) or {}
    fee = {"disc": 0.6, "inclFee": False, "net": True}
    fee.update(jload("tw-feecfg", {}) or {})
    last = jload("tw-lastknown", {}) or {}

    daily, taiex, rt = {}, None, {}
    try:
        for r in get_data("daily"):
            o = daily.get(r.get("Code"))
            if not o or (not (_num(o.get("ClosingPrice")) or 0) > 0 and (_num(r.get("ClosingPrice")) or 0) > 0):
                daily[r.get("Code")] = r
    except Exception as e:
        log(f"小工具:每日行情取得失敗:{e}")
    try:
        taiex = next((r for r in get_data("index") if r.get("指數") == "發行量加權股價指數"), None)
    except Exception:
        pass
    try:
        for it in get_realtime(wl + ["t00", "o00"]):
            if it.get("c"):
                rt[it["c"]] = it
    except Exception as e:
        log(f"小工具:即時報價取得失敗:{e}")

    def rt_price(it):
        return mis_price(it)[0]

    def quote(code):
        it = rt.get(code)
        if it:
            (p, tag), y = mis_price(it), _num(it.get("y"))
            if p and y:
                return {"price": p, "chg": p - y, "pct": (p / y - 1) * 100, "tag": tag}
            if y:                                   # 有即時資料但完全沒有價格:不要退回前一天的資料
                return {"price": y, "chg": None, "pct": None, "tag": "尚未成交"}
        r = daily.get(code)
        if r:
            c, ch = _num(r.get("ClosingPrice")), _num(r.get("Change"))
            if c:
                pv = c - (ch or 0)
                return {"price": c, "chg": ch or 0, "pct": ((ch or 0) / pv * 100) if pv else None}
        lk = last.get(code) if isinstance(last, dict) else None
        if isinstance(lk, dict) and _num(lk.get("price")):
            return {"price": _num(lk["price"]), "chg": None, "pct": None, "stale": True}
        return None

    def index_quote(key):
        it = rt.get(key)
        if it:
            p, y = rt_price(it), _num(it.get("y"))
            if p and y:
                return {"price": p, "chg": p - y, "pct": (p / y - 1) * 100}
        if key == "t00" and taiex:
            c = _num(taiex.get("收盤指數"))
            sg = -1 if taiex.get("漲跌(+/-)") == "-" else 1 if taiex.get("漲跌(+/-)") == "+" else 0
            if c:
                return {"price": c, "chg": abs(_num(taiex.get("漲跌點數")) or 0) * sg,
                        "pct": abs(_num(taiex.get("漲跌百分比")) or 0) * sg}
        return None

    def name_of(code):
        r = daily.get(code)
        if r and r.get("Name"):
            return r["Name"]
        if code in rt and rt[code].get("n"):
            return rt[code]["n"]
        lk = last.get(code) if isinstance(last, dict) else None
        return (lk or {}).get("name", "") if isinstance(lk, dict) else ""

    view = cfg["view"] if cfg["view"] != "auto" else widget_auto_view()
    txf, usq = None, {}
    if view in ("night", "us") or cfg["ticker"]:
        try:
            txf = get_txf()
        except Exception as e:
            log(f"小工具:台指期報價取得失敗:{e}")
    us_wl = [x for x in jload("tw-us-watchlist", ["TSM", "NVDA", "AAPL"]) if isinstance(x, str) and US_SYMBOL_RE.match(x)]
    if view == "us" or cfg["ticker"]:
        usq = {q["symbol"]: q for q in get_us_quotes([x for x, _ in US_INDICES] + [x for x, _ in US_COMMODITIES] + us_wl)}
    elif view == "night":
        usq = {q["symbol"]: q for q in get_us_quotes(["GC=F", "CL=F", "BZ=F"])}

    def idx_tw(key, label):
        q = index_quote(key)
        return (label, _fmt(q["price"]) if q else "—",
                f"{_sign(q['chg'])} {_fmt(abs(q['chg']))} ({_fmt(abs(q['pct']))}%)" if q else "—",
                _ckey(q["chg"]) if q else "faint", key)

    def idx_txf():
        if not txf:
            return ("台指期", "—", "—", "faint", "txf")
        lbl = "台指期 " + ("夜盤" if txf["session"] == "night" else "日盤") + ("" if txf["active"] else "(收)")
        c, p = txf.get("chg"), txf.get("pct")
        return (lbl, _fmt(txf["price"], 0),
                f"{_sign(c)} {_fmt(abs(c), 0)} ({_fmt(abs(p))}%)" if c is not None and p is not None else "—", _ckey(c), "txf")

    def idx_us(sym):
        q = usq.get(sym)
        if not q or q.get("error"):
            return (US_INDEX_NAMES.get(sym, sym), "—", "—", "faint", "us:" + sym)
        c, p = q.get("chg"), q.get("pct")
        return (US_INDEX_NAMES.get(sym, sym), _fmt(q["price"]),
                f"{_sign(c)} {_fmt(abs(c))} ({_fmt(abs(p))}%)" if c is not None and p is not None else "—", _ckey(c), "us:" + sym)

    if view == "us":
        indices = [idx_txf(), idx_us("^SOX"), idx_us("^GSPC"), idx_us("^IXIC"), idx_us("GC=F"), idx_us("CL=F")]
    elif view == "night":
        indices = [idx_txf(), idx_us("GC=F"), idx_us("CL=F"), idx_us("BZ=F")]    # 台股收盤後不再顯示加權指數
    else:
        indices = [idx_tw("t00", "加權指數"), idx_tw("o00", "櫃買指數")]

    rows, pf = [], None
    if view == "us":
        for x in us_wl:
            q = usq.get(x)
            if not q or q.get("error"):
                rows.append((x, "", "—", "—", "faint", "us:" + x, ""))
                continue
            c, p = q.get("chg"), q.get("pct")
            nm = q.get("name") or ""
            rows.append((x, nm if nm.upper() != x else "", _fmt(q["price"]),
                         f"{_sign(c)} {_fmt(abs(p))}%" if p is not None else "—", _ckey(c), "us:" + x, ""))
        gs = usq.get("^GSPC") or {}
        status, ok = gs.get("state") or "無資料", not gs.get("error") and bool(gs)
        empty = not us_wl
    else:
        cost = pl = 0.0
        n = 0
        rows = []
        for code in wl:
            q = quote(code)
            h = holdings.get(code) if isinstance(holdings, dict) else None
            if q and isinstance(h, dict) and (h.get("shares") or 0) > 0 and (h.get("cost") or 0) > 0:
                shares, avg, price = float(h["shares"]), float(h["cost"]), q["price"]
                basis = avg * shares
                if fee.get("net", True):
                    disc = float(fee.get("disc", 0.6) or 0.6)
                    buy = 0 if fee.get("inclFee") else max(basis * 0.001425 * disc, 20)
                    sell = max(price * shares * 0.001425 * disc, 20)
                    tax = price * shares * (0.001 if code.startswith("00") else 0.003)
                    pl += (price - avg) * shares - buy - sell - tax
                    cost += basis + buy
                else:
                    pl += (price - avg) * shares
                    cost += basis
                n += 1
            if not q:
                rows.append((code, name_of(code), "—", "—", "faint", code, ""))
            elif q.get("stale"):
                rows.append((code, name_of(code), _fmt(q["price"]), "暫無報價", "faint", code, ""))
            else:
                pct, tag = q.get("pct"), q.get("tag") or ""
                rows.append((code, name_of(code) + (" 尚未成交" if tag == "尚未成交" else ""), _fmt(q["price"]),
                             f"{_sign(q['chg'])} {_fmt(abs(pct))}%" if pct is not None else "—",
                             _ckey(q["chg"]) if q.get("chg") is not None else "faint", code, tag))
        pf = None
        if n:
            ret = pl / cost * 100 if cost else None
            pf = (("+" if pl > 0 else "") + f"{pl:,.0f}",
                  (("+" if ret > 0 else "") + _fmt(ret) + "%") if ret is not None else "", _ckey(pl))
        live = any(rt_price(it) for it in rt.values())
        if view == "night":
            status, ok = ("交易中" if txf and txf["active"] else "收盤" if txf else "無資料"), bool(txf)
        else:
            status, ok = ("即時" if live else "收盤" if daily else "無資料"), bool(live or daily)
        empty = not wl
    title = {"tw": "台股觀察", "night": "台股觀察 · 夜盤", "us": "台股觀察 · 美股"}[view]

    # 跑馬燈:全部市場 + 商品 + 台股自選 + 美股自選(不論目前顯示哪一種畫面)
    ticker = []
    if cfg["ticker"]:
        def pct_txt(c, p):
            return f"{_sign(c)} {_fmt(abs(p))}%" if c is not None and p is not None else "—"
        for key, label in (("t00", "加權"), ("o00", "櫃買")):
            q = index_quote(key)
            ticker.append((label, _fmt(q["price"]) if q else "—", pct_txt(q["chg"], q["pct"]) if q else "—",
                           _ckey(q["chg"]) if q else "faint"))
        if txf:
            ticker.append(("台指期" + ("夜盤" if txf["session"] == "night" else "日盤"), _fmt(txf["price"], 0),
                           pct_txt(txf.get("chg"), txf.get("pct")), _ckey(txf.get("chg"))))
        for sym, label in US_INDICES + US_COMMODITIES:
            q = usq.get(sym)
            if q and not q.get("error"):
                ticker.append((label, _fmt(q["price"], 3 if sym.endswith("=X") else 2),
                               pct_txt(q.get("chg"), q.get("pct")), _ckey(q.get("chg"))))
        for code in wl:
            q = quote(code)
            if q:
                ticker.append(((name_of(code) or code) + (f" {q['tag']}" if q.get("tag") else ""), _fmt(q["price"]),
                               "—" if q.get("stale") else pct_txt(q.get("chg"), q.get("pct")),
                               "faint" if q.get("stale") else _ckey(q.get("chg"))))
        for x in us_wl:
            q = usq.get(x)
            if q and not q.get("error"):
                ticker.append((x, _fmt(q["price"]), pct_txt(q.get("chg"), q.get("pct")), _ckey(q.get("chg"))))
    return {"cfg": cfg, "status": status, "ok": ok, "view": view, "title": title,
            "time": datetime.now(TW_TZ).strftime("%H:%M:%S"),
            "indices": indices, "pf": pf, "rows": rows, "empty": empty, "ticker": ticker}


# ── 版面(以 96 DPI 的像素計;實際繪製時乘上縮放比例)──
W_LOGICAL, HEAD_H, IDX_H, PF_H, ROW_H, PAD_B = 320, 30, 60, 28, 26, 6
TICK_H, TICK_SPEED = 24, 38           # 跑馬燈高度與速度(每秒像素,以 96 DPI 計)


def _ticker_on(snap):
    return bool(snap and snap.get("ticker") and snap["cfg"].get("ticker"))


def _layout_height(snap):
    if not snap:
        return HEAD_H + IDX_H + 2 * ROW_H + PAD_B
    rows = max(1, len(snap["rows"])) if not snap["empty"] else 1
    idx_rows = (len(snap["indices"]) + 1) // 2
    return (HEAD_H + (TICK_H if _ticker_on(snap) else 0) + IDX_H * idx_rows
            + (PF_H if snap["pf"] else 0) + rows * ROW_H + PAD_B)


def _font(key, size, weight, face):
    f = _native["fonts"].get(key)
    if not f:
        sc = _screen_scale()
        f = _g32.CreateFontW(-int(round(size * sc)), 0, 0, 0, weight, 0, 0, 0, 1, 0, 0, 5, 0, face)
        _native["fonts"][key] = f
    return f


def _ticker_layout(hdc, items, f_name, f_price, f_pct, S):
    """量測跑馬燈每一項的寬度;只在資料更新時重算。"""
    key = (id(items), _screen_scale())
    cached = _native.get("tk_cache")
    if cached and cached[0] == key:
        return cached[1]
    size = wintypes.SIZE()

    def width(font, txt):
        _g32.SelectObject(hdc, font)
        _g32.GetTextExtentPoint32W(hdc, txt, len(txt), ctypes.byref(size))
        return size.cx

    out = []
    for name, price, pct, ck in items:
        segs = [(name, f_name, "muted", width(f_name, name)),
                (price, f_price, "text" if ck == "faint" else ck, width(f_price, price)),
                (pct, f_pct, ck, width(f_pct, pct))]
        out.append((segs, S(10) + sum(sg[3] for sg in segs) + S(6) * 2 + S(14)))
    lay = {"items": out, "total": sum(w for _, w in out)}
    _native["tk_cache"] = (key, lay)
    return lay


def _cursor_over_ticker(hwnd):
    pt, r = wintypes.POINT(), wintypes.RECT()
    _u32.GetCursorPos(ctypes.byref(pt))
    _u32.GetWindowRect(hwnd, ctypes.byref(r))
    top = r.top + HEAD_H * _screen_scale()
    return r.left <= pt.x < r.right and top <= pt.y < top + TICK_H * _screen_scale()


def _apply_native_style(hwnd, cfg):
    _u32.SetLayeredWindowAttributes(hwnd, 0, int(round(255 * cfg["opacity"])), LWA_ALPHA)
    _u32.SetWindowPos(hwnd, HWND_TOPMOST if cfg["top"] else HWND_NOTOPMOST, 0, 0, 0, 0,
                      SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE)


MENU_OPACITY = [100, 90, 80, 70, 60, 50, 40, 30]
MENU_REFRESH = [(0.5, "每 30 秒"), (1, "每 1 分鐘"), (5, "每 5 分鐘"), (10, "每 10 分鐘"), (30, "每 30 分鐘"), (60, "每 60 分鐘")]


def _show_menu(hwnd, x, y):
    cfg = _native["snap"]["cfg"] if _native["snap"] else _native_cfg(load_data().get("state", {}))
    menu, cmds = _build_menu(cfg)
    try:
        _u32.SetForegroundWindow(hwnd)
        cid = _u32.TrackPopupMenu(menu, TPM_RETURNCMD | TPM_RIGHTBUTTON | TPM_NONOTIFY, x, y, 0, hwnd, None)
    finally:
        _u32.DestroyMenu(menu)
    if cid in cmds:
        _do_command(hwnd, *cmds[cid])


def _gear_hit(hwnd, cx, cy):
    rc = wintypes.RECT()
    _u32.GetClientRect(hwnd, ctypes.byref(rc))
    sc = _screen_scale()
    return cy < HEAD_H * sc and cx > rc.right - 32 * sc





# ══ v29 小工具:可調整大小、清單捲動、點擊看走勢(展開/另開視窗)、漲跌停亮燈 ══
CHART_H = 170                       # 展開式走勢圖高度
MIN_W, MIN_H = 280, 150
EDGE = 6                            # 邊緣可拖曳調整大小的寬度


class _Painter:
    """GDI 繪圖小幫手:統一處理座標取整數、畫刷與畫筆的建立與釋放。"""

    def __init__(self, mem, pal, sc):
        self.mem, self.pal, self.sc, self.objs = mem, pal, sc, []

    def S(self, v):
        return int(round(v * self.sc))

    def fill(self, x, y, w, h, color):
        b = _g32.CreateSolidBrush(_rgb(self.pal[color]))
        self.objs.append(b)
        _u32.FillRect(self.mem, ctypes.byref(wintypes.RECT(int(x), int(y), int(x + w), int(y + h))), b)

    def text(self, s, x, y, w, h, color, font, align=0):
        _g32.SelectObject(self.mem, font)
        _g32.SetTextColor(self.mem, _rgb(self.pal[color]))
        r = wintypes.RECT(int(x), int(y), int(x + w), int(y + h))
        _u32.DrawTextW(self.mem, s, -1, ctypes.byref(r),
                       align | DT_VCENTER | DT_SINGLELINE | DT_END_ELLIPSIS | DT_NOPREFIX)

    def poly(self, pts, color, width=1, style=0):
        if len(pts) < 2:
            return
        pen = _g32.CreatePen(style, max(1, int(width)), _rgb(self.pal[color]))
        self.objs.append(pen)
        old = _g32.SelectObject(self.mem, pen)
        arr = (wintypes.POINT * len(pts))(*[wintypes.POINT(int(x), int(y)) for x, y in pts])
        _g32.Polyline(self.mem, arr, len(pts))
        _g32.SelectObject(self.mem, old)

    def close(self):
        for o in self.objs:
            _g32.DeleteObject(o)
        self.objs = []


def _fonts():
    return {"title": _font("title", 13, 700, UI_FONT), "ui": _font("ui", 12.5, 400, UI_FONT),
            "small": _font("small", 11.5, 400, UI_FONT), "num": _font("num", 13, 700, NUM_FONT),
            "nums": _font("nums", 12, 400, NUM_FONT), "big": _font("big", 17, 700, NUM_FONT)}


def _cfg_now():
    snap = _native.get("snap")
    return snap["cfg"] if snap else _native_cfg(load_data().get("state", {}))


def _chart_inline_open():
    return bool(_native.get("chart")) and _cfg_now().get("chart", "inline") == "inline"


def _target_size(snap):
    """使用者調整過大小就用他的設定;否則依內容自動決定高度。展開走勢圖時再加上圖表高度。"""
    sc = _screen_scale()
    custom = load_data().get("widget_size")
    if isinstance(custom, list) and len(custom) == 2:
        w, h = max(int(MIN_W * sc), int(custom[0])), max(int(MIN_H * sc), int(custom[1]))
    else:
        w, h = int(round(W_LOGICAL * sc)), int(round(_layout_height(snap) * sc))
    if _chart_inline_open():
        h += int(round(CHART_H * sc))
    return w, h


def _resize_to_content(hwnd, snap):
    w, h = _target_size(snap)
    if (w, h) == (_native.get("w"), _native.get("h")):
        return
    _native["w"], _native["h"] = w, h
    r, wa = wintypes.RECT(), wintypes.RECT()
    _u32.GetWindowRect(hwnd, ctypes.byref(r))
    _u32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(wa), 0)
    if _native.get("anchor_bottom"):                  # 第一次開啟:貼齊右下角
        left = max(wa.left, wa.right - w - int(round(16 * _screen_scale())))
        top = max(wa.top, wa.bottom - h - int(round(16 * _screen_scale())))
    else:
        left = r.left
        top = r.top if r.top + h <= wa.bottom else max(wa.top, wa.bottom - h)
    _u32.SetWindowPos(hwnd, None, left, top, w, h, SWP_NOZORDER | SWP_NOACTIVATE)


def _edge_hit(hwnd, x, y):
    """滑鼠在視窗邊緣時回傳調整大小的代碼。"""
    rc = wintypes.RECT()
    _u32.GetClientRect(hwnd, ctypes.byref(rc))
    e = max(4, int(EDGE * _screen_scale()))
    l, r, t, b = x < e, x >= rc.right - e, y < e, y >= rc.bottom - e
    if t and l: return HTTOPLEFT
    if t and r: return HTTOPRIGHT
    if b and l: return HTBOTTOMLEFT
    if b and r: return HTBOTTOMRIGHT
    if l: return HTLEFT
    if r: return HTRIGHT
    if t: return HTTOP
    if b: return HTBOTTOM
    return None


def _draw_chart(p, x, y, w, h, chart, f):
    """今日走勢:昨收虛線、價格折線、最高/最低、開收盤時間;回傳關閉按鈕的範圍。"""
    S = p.S
    p.fill(x, y, w, h, "bg")
    p.fill(x, y, w, 1, "line")
    hh = S(26)
    close = (x + w - S(28), y + S(2), x + w - S(4), y + hh - S(2))
    cx0, cy0, cx1, cy1 = close
    p.poly([(cx0 + S(7), cy0 + S(6)), (cx1 - S(7), cy1 - S(6))], "muted", S(1.5))
    p.poly([(cx0 + S(7), cy1 - S(6)), (cx1 - S(7), cy0 + S(6))], "muted", S(1.5))
    p.text(chart.get("label", ""), x + S(10), y, w - S(200), hh, "text", f["title"])
    data = chart.get("data") or {}
    prev = _num(data.get("prev"))
    ax = data.get("axis") or {}
    dur = ax.get("dur") or 270
    ticks = ax.get("ticks") or []
    pts = []
    for q in data.get("points") or []:
        o, v = q.get("o"), _num(q.get("p"))
        if o is not None and v and 0 <= o <= dur:
            pts.append((o, v))
    top, bot = y + hh + S(6), y + h - S(22)
    left, right = x + S(12), x + w - S(12)
    if not pts or not prev:
        msg = chart.get("error") or ("載入走勢中…" if chart.get("loading") else "目前沒有今日走勢資料")
        p.text(msg, x + S(10), top, w - S(20), bot - top, "faint", f["small"], 0x1)
        return close
    last = pts[-1][1]
    chg = last - prev
    ck = _ckey(chg)
    p.text(f"{_fmt(last)}  {_sign(chg)} {_fmt(abs(chg / prev * 100))}%", x + w - S(210), y, S(178), hh, ck, f["nums"], DT_RIGHT)
    vals = [v for _, v in pts] + [prev]
    vmax, vmin = max(vals), min(vals)
    lo, hi = vmin, vmax
    if hi == lo:
        hi, lo = hi + 1, lo - 1
    pad = (hi - lo) * 0.1
    lo, hi = lo - pad, hi + pad
    X = lambda o: left + (right - left) * o / dur
    Y = lambda v: top + (bot - top) * (hi - v) / (hi - lo)
    p.poly([(left, Y(prev)), (right, Y(prev))], "faint", 1, 2)      # 昨收(虛線)
    col = "up" if last >= prev else "down"
    p.poly([(X(o), Y(v)) for o, v in pts], col, max(1, S(1.6)))
    lx, ly = X(pts[-1][0]), Y(last)
    p.fill(lx - S(2), ly - S(2), S(5), S(5), col)
    p.text(f"高 {_fmt(vmax)}", left, top - S(4), S(140), S(14), "faint", f["small"])
    p.text(f"低 {_fmt(vmin)}", left, bot - S(10), S(140), S(14), "faint", f["small"])
    if ticks:
        p.text(str(ticks[0][1]), left, bot + S(3), S(70), S(16), "faint", f["small"])
        p.text(str(ticks[-1][1]), right - S(70), bot + S(3), S(70), S(16), "faint", f["small"], DT_RIGHT)
    return close


def _paint(hwnd):
    ps = PAINTSTRUCT()
    hdc = _u32.BeginPaint(hwnd, ctypes.byref(ps))
    p = None
    try:
        rc = wintypes.RECT()
        _u32.GetClientRect(hwnd, ctypes.byref(rc))
        W, H = rc.right, rc.bottom
        mem = _g32.CreateCompatibleDC(hdc)
        bmp = _g32.CreateCompatibleBitmap(hdc, W, H)
        old_bmp = _g32.SelectObject(mem, bmp)
        _g32.SetBkMode(mem, TRANSPARENT_BK)
        snap = _native["snap"]
        cfg = _cfg_now()
        p = _Painter(mem, PALETTE[cfg["theme"]], _screen_scale())
        S, fill, text = p.S, p.fill, p.text
        f = _fonts()
        sel = (_native.get("chart") or {}).get("key")
        hits = []

        fill(0, 0, W, H, "bg")
        text(snap["title"] if snap else "台股觀察", S(10), 0, W - S(180), S(HEAD_H), "text", f["title"])
        gx, gy = W - S(25), S(HEAD_H) // 2            # 選單圖示:三條橫線
        for dy in (-5, 0, 5):
            fill(gx, gy + S(dy) - max(1, S(1)) // 2, S(14), max(1, S(1.6)), "muted")
        if snap:
            text(snap["time"], W - S(110), 0, S(78), S(HEAD_H), "faint", f["nums"], DT_RIGHT)
            text(snap["status"], W - S(160), 0, S(46), S(HEAD_H), "accent" if snap["ok"] else "up", f["small"], DT_RIGHT)
        y = S(HEAD_H)
        fill(0, y - 1, W, 1, "line")
        if _ticker_on(snap):
            th = S(TICK_H)
            fill(0, y, W, th, "bg2")
            lay = _ticker_layout(mem, snap["ticker"], f["small"], f["num"], f["nums"], S)
            total = lay["total"]
            if total > 0:
                x = -int(_native["tk_off"] % total)
                while x < W:
                    for segs, w_item in lay["items"]:
                        if x + w_item > 0 and x < W:
                            cx = x + S(10)
                            for s_txt, s_font, s_col, s_w in segs:
                                text(s_txt, cx, y, s_w + S(2), th, s_col, s_font)
                                cx += s_w + S(6)
                            fill(x + w_item - 1, y + S(6), 1, th - S(12), "line")
                        x += w_item
                        if x >= W:
                            break
            y += th
            fill(0, y - 1, W, 1, "line")
        chart_h = S(CHART_H) if _chart_inline_open() else 0
        if not snap:
            text("載入中…", 0, y, W, S(IDX_H), "faint", f["ui"], 0x1)
        else:
            half = W // 2
            nrow = (len(snap["indices"]) + 1) // 2
            for i, (label, val, chg, ck, *rest) in enumerate(snap["indices"]):
                key = rest[0] if rest else ""
                tx, ty = (i % 2) * half, y + (i // 2) * S(IDX_H)
                if key and key == sel:
                    fill(tx, ty, S(3), S(IDX_H), "accent")
                text(label, tx + S(10), ty + S(4), half - S(14), S(16), "muted", f["small"])
                text(val, tx + S(10), ty + S(19), half - S(14), S(22), "text", f["big"])
                text(chg, tx + S(10), ty + S(40), half - S(14), S(16), ck, f["nums"])
                if key:
                    hits.append((tx, ty, tx + half, ty + S(IDX_H), key, label))
            for r_ in range(nrow):
                fill(half, y + r_ * S(IDX_H), 1, S(IDX_H), "line")
                if r_:
                    fill(0, y + r_ * S(IDX_H), W, 1, "line")
            y += nrow * S(IDX_H)
            fill(0, y - 1, W, 1, "line")
            if snap["pf"]:
                fill(0, y, W, S(PF_H), "bg2")
                plt, rett, ck = snap["pf"]
                text("未實現損益", S(10), y, S(100), S(PF_H), "muted", f["small"])
                text(rett, W - S(74), y, S(64), S(PF_H), ck, f["nums"], DT_RIGHT)
                text(plt, S(100), y, W - S(100) - S(80), S(PF_H), ck, f["num"], DT_RIGHT)
                y += S(PF_H)
                fill(0, y - 1, W, 1, "line")
            # 自選股清單:超出可視範圍時可用滾輪捲動
            top, bottom = y, H - chart_h
            rows = snap["rows"]
            content = (len(rows) if rows else 1) * S(ROW_H)
            max_scroll = max(0, content - (bottom - top))
            _native["scroll"] = min(max(0, _native.get("scroll", 0)), max_scroll)
            _g32.IntersectClipRect(mem, 0, top, W, bottom)
            ry = top - _native["scroll"]
            if snap["empty"]:
                text("尚無自選股,請在網頁版加入", 0, ry, W, S(ROW_H), "faint", f["ui"], 0x1)
            for code, name, price, pct, ck, *rest in rows:
                key = rest[0] if rest else ""
                tag = rest[1] if len(rest) > 1 else ""
                if ry + S(ROW_H) > top and ry < bottom:
                    if key and key == sel:
                        fill(0, ry, S(3), S(ROW_H), "accent")
                    text(code, S(10), ry, S(56), S(ROW_H), "accent", f["num"])
                    text(name, S(66), ry, W - S(66) - S(160), S(ROW_H), "text", f["ui"])
                    if tag in ("漲停", "跌停"):            # 亮燈:漲停紅底白字、跌停綠底白字
                        fill(W - S(160) + S(4), ry + S(3), S(84), S(ROW_H) - S(6), "up" if tag == "漲停" else "down")
                        text(price, W - S(160), ry, S(84) - S(2), S(ROW_H), "white", f["num"], DT_RIGHT)
                    else:
                        text(price, W - S(160), ry, S(84), S(ROW_H), ck, f["num"], DT_RIGHT)
                    text(pct, W - S(74), ry, S(64), S(ROW_H), ck, f["nums"], DT_RIGHT)
                    fill(S(10), ry + S(ROW_H) - 1, W - S(20), 1, "bg2")
                    if key:
                        hits.append((0, max(ry, top), W, min(ry + S(ROW_H), bottom), key, f"{code} {name}".strip()))
                ry += S(ROW_H)
            _g32.SelectClipRgn(mem, None)
            if max_scroll > 0:                          # 捲軸提示
                vis = bottom - top
                bar = max(S(16), vis * vis // content)
                pos = top + (vis - bar) * _native["scroll"] // max_scroll
                fill(W - S(4), pos, S(3), bar, "faint")
        _native["close_inline"] = None
        if chart_h:
            _native["close_inline"] = _draw_chart(p, 0, H - chart_h, W, chart_h, _native["chart"], f)
        _native["hits"] = hits
        _g32.BitBlt(hdc, 0, 0, W, H, mem, 0, 0, SRCCOPY)
        _g32.SelectObject(mem, old_bmp)
        _g32.DeleteObject(bmp)
        _g32.DeleteDC(mem)
    finally:
        if p:
            p.close()
        _u32.EndPaint(hwnd, ctypes.byref(ps))


# ── 走勢圖資料與開關 ──
def _chart_data(key):
    if key.startswith("us:"):
        return us_intraday(key[3:])
    if key == "txf":
        return txf_intraday()
    return add_tw_axis(get_intraday(key))


def _fetch_chart(key):
    try:
        data, err = _chart_data(key), ""
    except Exception as e:
        data, err = None, str(e)
    ch = _native.get("chart")
    if not ch or ch.get("key") != key:
        return
    ch.update(data=data or ch.get("data"), error="" if data else err, loading=False)
    if _native.get("hwnd"):
        _u32.PostMessageW(_native["hwnd"], WM_APP_UPDATE, 0, 0)


def _toggle_chart(hwnd, key, label):
    cur = _native.get("chart")
    if cur and cur.get("key") == key:
        _close_chart(hwnd)
        return
    _native["chart"] = {"key": key, "label": label, "data": None, "error": "", "loading": True}
    if _cfg_now().get("chart") == "window":
        _ensure_chart_window(hwnd)
    _resize_to_content(hwnd, _native["snap"])
    _u32.InvalidateRect(hwnd, None, False)
    threading.Thread(target=_fetch_chart, args=(key,), daemon=True).start()


def _close_chart(hwnd):
    _native["chart"] = None
    ch = _native.get("chart_hwnd")
    if ch:
        _u32.DestroyWindow(ch)
    if hwnd:
        _resize_to_content(hwnd, _native["snap"])
        _u32.InvalidateRect(hwnd, None, False)


# ── 另開視窗模式 ──
def _chart_paint(hwnd):
    ps = PAINTSTRUCT()
    hdc = _u32.BeginPaint(hwnd, ctypes.byref(ps))
    p = None
    try:
        rc = wintypes.RECT()
        _u32.GetClientRect(hwnd, ctypes.byref(rc))
        W, H = rc.right, rc.bottom
        mem = _g32.CreateCompatibleDC(hdc)
        bmp = _g32.CreateCompatibleBitmap(hdc, W, H)
        old = _g32.SelectObject(mem, bmp)
        _g32.SetBkMode(mem, TRANSPARENT_BK)
        p = _Painter(mem, PALETTE[_cfg_now()["theme"]], _screen_scale())
        _native["close_win"] = _draw_chart(p, 0, 0, W, H, _native.get("chart") or {}, _fonts())
        p.fill(0, 0, W, 1, "line"); p.fill(0, H - 1, W, 1, "line"); p.fill(0, 0, 1, H, "line"); p.fill(W - 1, 0, 1, H, "line")
        _g32.BitBlt(hdc, 0, 0, W, H, mem, 0, 0, SRCCOPY)
        _g32.SelectObject(mem, old)
        _g32.DeleteObject(bmp)
        _g32.DeleteDC(mem)
    finally:
        if p:
            p.close()
        _u32.EndPaint(hwnd, ctypes.byref(ps))


def _in_rect(r, x, y):
    return bool(r) and r[0] <= x < r[2] and r[1] <= y < r[3]


def _xy(lp):
    return ctypes.c_short(lp & 0xFFFF).value, ctypes.c_short((lp >> 16) & 0xFFFF).value


def _chart_wndproc(hwnd, msg, wp, lp):
    try:
        if msg == WM_PAINT:
            _chart_paint(hwnd)
            return 0
        if msg == WM_ERASEBKGND:
            return 1
        if msg == WM_NCCALCSIZE and wp:
            return 0                                    # 不要系統邊框,整個視窗都是內容
        if msg == WM_NCHITTEST:
            pt = wintypes.POINT(*_xy(lp))
            _u32.ScreenToClient(hwnd, ctypes.byref(pt))
            if _in_rect(_native.get("close_win"), pt.x, pt.y):
                return HTCLIENT
            return _edge_hit(hwnd, pt.x, pt.y) or HTCAPTION
        if msg == WM_LBUTTONUP:
            if _in_rect(_native.get("close_win"), *_xy(lp)):
                _close_chart(_native.get("hwnd"))
            return 0
        if msg == WM_GETMINMAXINFO:
            mmi = ctypes.cast(lp, ctypes.POINTER(MINMAXINFO)).contents
            mmi.ptMinTrackSize.x, mmi.ptMinTrackSize.y = int(300 * _screen_scale()), int(160 * _screen_scale())
            return 0
        if msg == WM_SIZE:
            _u32.InvalidateRect(hwnd, None, False)
        if msg == WM_EXITSIZEMOVE:
            r = wintypes.RECT()
            _u32.GetWindowRect(hwnd, ctypes.byref(r))
            rect = [r.left, r.top, r.right - r.left, r.bottom - r.top]
            update_data(lambda d: d.__setitem__("chart_rect", rect))
            return 0
        if msg == WM_DESTROY:
            _native["chart_hwnd"] = None
            if _cfg_now().get("chart") == "window":
                _native["chart"] = None
                if _native.get("hwnd"):
                    _u32.InvalidateRect(_native["hwnd"], None, False)
            return 0
    except Exception as e:
        log(f"走勢圖視窗處理訊息時發生錯誤:{type(e).__name__}: {e}")
    return _u32.DefWindowProcW(hwnd, msg, wp, lp)


_CHART_WNDPROC_REF = WNDPROC(_chart_wndproc) if IS_WIN else None


def _ensure_chart_window(main):
    ch = _native.get("chart_hwnd")
    if ch:
        _u32.InvalidateRect(ch, None, False)
        return
    sc = _screen_scale()
    w, h = int(440 * sc), int(240 * sc)
    saved = load_data().get("chart_rect")
    if isinstance(saved, list) and len(saved) == 4 and _rect_on_screen(saved):
        x, y, w, h = [int(v) for v in saved]
    else:
        r, wa = wintypes.RECT(), wintypes.RECT()
        _u32.GetWindowRect(main, ctypes.byref(r))
        _u32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(wa), 0)
        x = r.left - w - int(8 * sc)
        if x < wa.left:
            x = r.right + int(8 * sc)
        y = max(wa.top, min(r.top, wa.bottom - h))
    cfg = _cfg_now()
    ex = WS_EX_LAYERED_ | WS_EX_TOOLWINDOW | (WS_EX_TOPMOST if cfg["top"] else 0)
    ch = _u32.CreateWindowExW(ex, "TwDashboardChart", "台股觀察 走勢圖", WS_POPUP | WS_THICKFRAME,
                              x, y, w, h, None, None, _k32.GetModuleHandleW(None), None)
    if not ch:
        log(f"無法開啟走勢圖視窗(錯誤碼 {ctypes.get_last_error()})")
        return
    _native["chart_hwnd"] = ch
    _apply_native_style(ch, cfg)
    _u32.ShowWindow(ch, SW_SHOWNOACTIVATE)


def _build_menu(cfg):
    """回傳 (選單, 指令對照表)。"""
    cmds = {}
    root = _u32.CreatePopupMenu()
    info = _upd.get("info")
    if info and info.get("version", 0) > APP_VERSION:
        cmds[1000] = ("update", None)
        _u32.AppendMenuW(root, MF_STRING, 1000, f"有新版本 v{info['version']}:立即更新")
        _u32.AppendMenuW(root, MF_SEPARATOR, 0, None)
    sub_op = _u32.CreatePopupMenu()
    for i, pct in enumerate(MENU_OPACITY):
        cmds[100 + i] = ("opacity", pct / 100)
        flag = MF_CHECKED if abs(cfg["opacity"] - pct / 100) < 0.026 else 0
        _u32.AppendMenuW(sub_op, MF_STRING | flag, 100 + i, f"{pct}%" + ("(不透明)" if pct == 100 else ""))
    _u32.AppendMenuW(root, MF_POPUP, sub_op, "透明度")
    cmds[200] = ("top", not cfg["top"])
    _u32.AppendMenuW(root, MF_STRING | (MF_CHECKED if cfg["top"] else 0), 200, "永遠置頂")
    sub_th = _u32.CreatePopupMenu()
    for i, (k, label) in enumerate((("light", "淺色"), ("dark", "深色"))):
        cmds[300 + i] = ("theme", k)
        _u32.AppendMenuW(sub_th, MF_STRING | (MF_CHECKED if cfg["theme"] == k else 0), 300 + i, label)
    _u32.AppendMenuW(root, MF_POPUP, sub_th, "配色")
    sub_rf = _u32.CreatePopupMenu()
    for i, (v, label) in enumerate(MENU_REFRESH):
        cmds[400 + i] = ("refresh", v)
        _u32.AppendMenuW(sub_rf, MF_STRING | (MF_CHECKED if abs(cfg["refresh"] - v) < 1e-6 else 0), 400 + i, label)
    _u32.AppendMenuW(root, MF_POPUP, sub_rf, "更新週期")
    sub_vw = _u32.CreatePopupMenu()
    for i, (k, label) in enumerate((("auto", "自動(依時段切換)"), ("tw", "台股"), ("night", "台指期夜盤"), ("us", "美股"))):
        cmds[800 + i] = ("view", k)
        _u32.AppendMenuW(sub_vw, MF_STRING | (MF_CHECKED if cfg.get("view", "auto") == k else 0), 800 + i, label)
    _u32.AppendMenuW(root, MF_POPUP, sub_vw, "顯示內容")
    sub_tk = _u32.CreatePopupMenu()
    for i, (on, label) in enumerate(((True, "捲動"), (False, "關閉"))):
        cmds[900 + i] = ("ticker", on)
        _u32.AppendMenuW(sub_tk, MF_STRING | (MF_CHECKED if bool(cfg.get("ticker", True)) == on else 0), 900 + i, label)
    _u32.AppendMenuW(root, MF_POPUP, sub_tk, "跑馬燈")
    sub_ch = _u32.CreatePopupMenu()
    for i, (k, label) in enumerate((("inline", "在小工具內展開"), ("window", "另開一個視窗"))):
        cmds[1100 + i] = ("chart", k)
        _u32.AppendMenuW(sub_ch, MF_STRING | (MF_CHECKED if cfg.get("chart", "inline") == k else 0), 1100 + i, label)
    _u32.AppendMenuW(root, MF_POPUP, sub_ch, "點擊後的走勢圖")
    cmds[1200] = ("reset_size", None)
    _u32.AppendMenuW(root, MF_STRING, 1200, "恢復預設大小")
    _u32.AppendMenuW(root, MF_SEPARATOR, 0, None)
    cmds[500] = ("refresh_now", None)
    _u32.AppendMenuW(root, MF_STRING, 500, "立即更新報價")
    cmds[600] = ("to_page", None)
    _u32.AppendMenuW(root, MF_STRING, 600, "改用網頁版(可編輯自選股)")
    cmds[700] = ("quit", None)
    _u32.AppendMenuW(root, MF_STRING, 700, "結束程式")
    return root, cmds


def _do_command(hwnd, action, value):
    keys = {"opacity": "tw-w-opacity", "top": "tw-w-top", "theme": "tw-w-theme", "refresh": "tw-w-refresh",
            "view": "tw-w-view", "ticker": "tw-w-ticker", "chart": "tw-w-chart"}
    if action in keys:
        sval = ("1" if value else "0") if action in ("top", "ticker") else str(value)
        if action == "chart" and _native.get("chart"):
            _close_chart(hwnd)                          # 切換顯示方式前先收起目前的走勢圖
        update_data(lambda d: d["state"].update({keys[action]: sval}))
        cfg = _native_cfg(load_data().get("state", {}))
        if _native["snap"]:
            _native["snap"] = dict(_native["snap"], cfg=cfg)
        _apply_native_style(hwnd, cfg)
        if _native.get("chart_hwnd"):
            _apply_native_style(_native["chart_hwnd"], cfg)
            _u32.InvalidateRect(_native["chart_hwnd"], None, False)
        _resize_to_content(hwnd, _native["snap"])
        _u32.InvalidateRect(hwnd, None, False)
        if action in ("refresh", "view", "ticker"):
            _native["wake"].set()
    elif action == "reset_size":
        update_data(lambda d: d.pop("widget_size", None))
        _resize_to_content(hwnd, _native["snap"])
        _u32.InvalidateRect(hwnd, None, False)
    elif action == "refresh_now":
        _native["wake"].set()
    elif action == "update":
        threading.Thread(target=_widget_update, daemon=True).start()
    elif action == "to_page":
        threading.Thread(target=set_mode, args=("page",), daemon=True).start()
    elif action == "quit":
        threading.Thread(target=shutdown_app, args=("使用者從小工具結束程式",), daemon=True).start()


def _widget_update():
    try:
        v = apply_update()
        restart_app(f"更新為 v{v}")
    except Exception as e:
        log(f"小工具更新失敗:{e}")
        _u32.MessageBoxW(_native.get("hwnd"), f"更新失敗:{e}", "台股觀察", 0x30)


def _wndproc(hwnd, msg, wp, lp):
    try:
        if msg == WM_PAINT:
            _paint(hwnd)
            return 0
        if msg == WM_ERASEBKGND:
            return 1
        if msg == WM_NCCALCSIZE and wp:
            return 0                                    # 不要系統邊框,保留可調整大小的能力
        if msg == WM_NCHITTEST:
            pt = wintypes.POINT(*_xy(lp))
            _u32.ScreenToClient(hwnd, ctypes.byref(pt))
            if _gear_hit(hwnd, pt.x, pt.y) or _in_rect(_native.get("close_inline"), pt.x, pt.y):
                return HTCLIENT
            edge = _edge_hit(hwnd, pt.x, pt.y)
            if edge:
                return edge
            sc = _screen_scale()
            band = HEAD_H + (TICK_H if _ticker_on(_native.get("snap")) else 0)
            return HTCAPTION if pt.y < band * sc else HTCLIENT   # 標題列與跑馬燈可拖曳;其他區域可點選
        if msg == WM_LBUTTONDOWN:
            _native["down"] = _xy(lp)
            _u32.SetCapture(hwnd)
            return 0
        if msg == WM_MOUSEMOVE:
            d = _native.get("down")
            if d:
                x, y = _xy(lp)
                if abs(x - d[0]) > 4 or abs(y - d[1]) > 4:      # 按住拖動 → 移動視窗
                    _native["down"] = None
                    _u32.ReleaseCapture()
                    _u32.SendMessageW(hwnd, WM_NCLBUTTONDOWN, HTCAPTION, 0)
            return 0
        if msg == WM_LBUTTONUP:
            down, _native["down"] = _native.get("down"), None
            _u32.ReleaseCapture()
            x, y = _xy(lp)
            if _gear_hit(hwnd, x, y):
                pt = wintypes.POINT(x, y)
                _u32.ClientToScreen(hwnd, ctypes.byref(pt))
                _show_menu(hwnd, pt.x, pt.y)
            elif _in_rect(_native.get("close_inline"), x, y):
                _close_chart(hwnd)
            elif down:
                for l, t, r, b, key, label in _native.get("hits") or []:
                    if l <= x < r and t <= y < b:
                        _toggle_chart(hwnd, key, label)
                        break
            return 0
        if msg == WM_MOUSEWHEEL:
            delta = ctypes.c_short((wp >> 16) & 0xFFFF).value
            _native["scroll"] = max(0, _native.get("scroll", 0) - int(delta / 120 * ROW_H * 2 * _screen_scale()))
            _u32.InvalidateRect(hwnd, None, False)
            return 0
        if msg == WM_CONTEXTMENU:
            pt = wintypes.POINT()
            _u32.GetCursorPos(ctypes.byref(pt))
            _show_menu(hwnd, pt.x, pt.y)
            return 0
        if msg == WM_GETMINMAXINFO:
            mmi = ctypes.cast(lp, ctypes.POINTER(MINMAXINFO)).contents
            mmi.ptMinTrackSize.x, mmi.ptMinTrackSize.y = int(MIN_W * _screen_scale()), int(MIN_H * _screen_scale())
            return 0
        if msg == WM_SIZE:
            _u32.InvalidateRect(hwnd, None, False)
        if msg == WM_EXITSIZEMOVE:
            r = wintypes.RECT()
            _u32.GetWindowRect(hwnd, ctypes.byref(r))
            w, h = r.right - r.left, r.bottom - r.top
            rect = [r.left, r.top, w, h]
            if (w, h) != (_native.get("w"), _native.get("h")):     # 使用者調整了大小:記住
                base_h = h - (int(round(CHART_H * _screen_scale())) if _chart_inline_open() else 0)
                update_data(lambda d: (d.__setitem__("widget_size", [w, max(base_h, int(MIN_H * _screen_scale()))]),
                                       d.__setitem__("widget_rect", rect)))
                _native["w"], _native["h"] = w, h
            else:
                update_data(lambda d: d.__setitem__("widget_rect", rect))
            _native["anchor_bottom"] = False
            _u32.InvalidateRect(hwnd, None, False)
            return 0
        if msg == WM_APP_UPDATE:
            snap = _native["snap"]
            if snap:
                _resize_to_content(hwnd, snap)
                _apply_native_style(hwnd, snap["cfg"])
            _u32.InvalidateRect(hwnd, None, False)
            if _native.get("chart_hwnd"):
                _u32.InvalidateRect(_native["chart_hwnd"], None, False)
            return 0
        if msg == WM_TIMER:                             # 跑馬燈:每 40 毫秒前進一點;滑鼠停在上面時暫停
            now = time.monotonic()
            dt = min(0.2, now - _native.get("tk_t", now))
            _native["tk_t"] = now
            if _ticker_on(_native["snap"]) and not _cursor_over_ticker(hwnd):
                sc = _screen_scale()
                _native["tk_off"] = _native.get("tk_off", 0.0) + dt * TICK_SPEED * sc
                rc = wintypes.RECT()
                _u32.GetClientRect(hwnd, ctypes.byref(rc))
                r = wintypes.RECT(0, int(HEAD_H * sc), rc.right, int((HEAD_H + TICK_H) * sc) + 1)
                _u32.InvalidateRect(hwnd, ctypes.addressof(r), False)
            return 0
        if msg == WM_CLOSE:
            _u32.DestroyWindow(hwnd)
            return 0
        if msg == WM_DESTROY:
            if _native.get("chart_hwnd"):
                _u32.DestroyWindow(_native["chart_hwnd"])
            _native["hwnd"] = None
            client_bye(NATIVE_CID)
            _u32.PostQuitMessage(0)
            return 0
    except Exception as e:
        log(f"小工具視窗處理訊息時發生錯誤:{type(e).__name__}: {e}")
    return _u32.DefWindowProcW(hwnd, msg, wp, lp)


_WNDPROC_REF = WNDPROC(_wndproc) if IS_WIN else None    # 必須保留參考,避免被回收


def _native_worker():
    while True:
        hwnd = _native["hwnd"]
        if not hwnd:
            return
        try:
            _native["snap"] = _build_snapshot()
        except Exception as e:
            log(f"小工具資料更新失敗:{type(e).__name__}: {e}")
        client_beat(NATIVE_CID)
        ch = _native.get("chart")
        if ch:
            _fetch_chart(ch["key"])                     # 走勢圖跟著更新
        if _native["hwnd"]:
            _u32.PostMessageW(_native["hwnd"], WM_APP_UPDATE, 0, 0)
        cfg = _native["snap"]["cfg"] if _native["snap"] else {"refresh": 1}
        _native["wake"].wait(cfg["refresh"] * 60)
        _native["wake"].clear()


def _register_class(name, proc):
    wc = WNDCLASSEXW()
    wc.cbSize = ctypes.sizeof(WNDCLASSEXW)
    wc.lpfnWndProc = proc
    wc.hInstance = _k32.GetModuleHandleW(None)
    wc.hCursor = _u32.LoadCursorW(None, ctypes.c_void_p(32512))
    wc.hIcon = wc.hIconSm = _u32.LoadIconW(None, ctypes.c_void_p(32512))
    wc.lpszClassName = name
    _u32.RegisterClassExW(ctypes.byref(wc))             # 重複註冊會失敗但不影響


def _native_main():
    try:
        _register_class("TwDashboardWidget", _WNDPROC_REF)
        _register_class("TwDashboardChart", _CHART_WNDPROC_REF)
        d = load_data()
        cfg = _native_cfg(d.get("state", {}))
        _native.update(chart=None, chart_hwnd=None, scroll=0, hits=[], down=None, close_inline=None, close_win=None)
        w, h = _target_size(None)
        rect = d.get("widget_rect")
        if isinstance(rect, list) and len(rect) == 4 and _rect_on_screen([rect[0], rect[1], w, h]):
            x, y = int(rect[0]), int(rect[1])
            _native["anchor_bottom"] = False
        else:
            _native["anchor_bottom"] = True
            dr = _default_rect(d)
            x, y = dr[0] + dr[2] - w, dr[1] + dr[3] - h
        ex = WS_EX_LAYERED_ | WS_EX_APPWINDOW | (WS_EX_TOPMOST if cfg["top"] else 0)
        hwnd = _u32.CreateWindowExW(ex, "TwDashboardWidget", WIDGET_TITLE, WS_POPUP | WS_THICKFRAME,
                                    x, y, w, h, None, None, _k32.GetModuleHandleW(None), None)
        if not hwnd:
            raise OSError(f"CreateWindowExW 失敗(錯誤碼 {ctypes.get_last_error()})")
        _native["hwnd"], _native["w"], _native["h"] = hwnd, w, h
        _native["tk_off"], _native["tk_t"] = 0.0, time.monotonic()
        _apply_native_style(hwnd, cfg)
        _u32.SetWindowPos(hwnd, None, x, y, w, h, SWP_NOZORDER | SWP_NOACTIVATE | 0x0020)   # 套用無邊框設定
        _u32.ShowWindow(hwnd, SW_SHOWNOACTIVATE)
        _u32.SetTimer(hwnd, 1, 40, None)
        client_beat(NATIVE_CID)
        threading.Thread(target=_native_worker, daemon=True).start()
        log("小工具視窗已開啟")

        msg = wintypes.MSG()
        while _u32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
            _u32.TranslateMessage(ctypes.byref(msg))
            _u32.DispatchMessageW(ctypes.byref(msg))
    except Exception as e:
        _native["hwnd"] = None
        log(f"原生小工具無法開啟,改用網頁版:{type(e).__name__}: {e}")
        traceback.print_exc()
        client_bye(NATIVE_CID)
        open_page()
    finally:
        for f in _native["fonts"].values():
            try:
                _g32.DeleteObject(f)
            except Exception:
                pass
        _native["fonts"].clear()
        _native["wake"].set()


def start_native_widget():
    t = _native["thread"]
    if t and t.is_alive():
        h = _native["hwnd"]
        if h:
            _u32.ShowWindow(h, SW_RESTORE_)
            _u32.SetForegroundWindow(h)
        return
    _native["snap"] = None
    _native["wake"].clear()
    t = threading.Thread(target=_native_main, daemon=True)
    _native["thread"] = t
    t.start()


# Windows 一律使用原生小工具;其他系統沿用網頁小工具
_open_widget_browser = open_widget


def open_widget():
    if IS_WIN:
        start_native_widget()
        return True
    return _open_widget_browser()


def close_widget():
    if IS_WIN:
        h = _native["hwnd"]
        if h:
            _u32.PostMessageW(h, WM_CLOSE, 0, 0)


def run():
    global _server, ACTIVE_PORT
    for stream in (sys.stdout, sys.stderr):      # 避免主控台編碼不支援中文時直接當掉
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass
    _trim_log()

    after_update = "--after-update" in sys.argv
    if after_update:                                    # 更新後重新啟動:等舊程式完全結束再接手
        for _ in range(60):
            if not _already_running(PORT) and _port_free(PORT):
                break
            time.sleep(0.5)

    server, last_err = None, None
    for port in range(PORT, PORT + 10):
        if _already_running(port):
            # 已在執行:請原本的程式依目前模式叫出畫面,這個新程式直接結束
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{port}/api/show", data=b"{}", method="POST",
                                             headers={"X-TW": "1", "Content-Type": "application/json"})
                _NO_PROXY.open(req, timeout=5).read()
            except Exception:
                webbrowser.open(f"http://localhost:{port}")
            log("程式已在執行中,已叫出畫面")
            return
        try:
            server = Server(("127.0.0.1", port), Handler)
            break
        except OSError as e:
            last_err = e
            log(f"連接埠 {port} 無法使用({e}),改試下一個")
    if server is None:
        raise RuntimeError(f"連接埠 {PORT}–{PORT + 9} 都無法使用:{last_err}")

    _server, ACTIVE_PORT = server, port
    mode = load_data().get("mode", "page")
    log(f"台股觀察 v{APP_VERSION} 已啟動:http://localhost:{port}(模式:{'小工具' if mode == 'widget' else '網頁'})")
    print("=" * 46)
    print(f"  台股觀察儀表板(本機伺服器 v{APP_VERSION})")
    print(f"  網址: http://localhost:{port}")
    print("  關閉所有畫面後,程式會自動結束")
    print("=" * 46)
    threading.Thread(target=_bg_recorder, daemon=True).start()
    threading.Thread(target=_bg_futures, daemon=True).start()
    threading.Thread(target=_watchdog, daemon=True).start()
    if after_update and load_data().get("mode") != "widget":
        _dtimer(10, lambda: None if _ever_client else open_ui()).start()   # 原本的網頁會自動重新整理
    else:
        _dtimer(0.8, open_ui).start()
    threading.Thread(target=_update_bg, daemon=True).start()
    server.serve_forever()
    server.server_close()                               # 立即釋放連接埠,讓更新後的新程式能接手
    log("伺服器已停止")


def main():
    try:
        run()
    except KeyboardInterrupt:
        log("已停止。")
    except Exception:
        tb = traceback.format_exc()
        path = _write_error_log(tb)
        if sys.stdout is None and IS_WIN:      # 背景執行時用訊息視窗告知
            last = tb.strip().splitlines()[-1] if tb.strip() else ""
            _u32.MessageBoxW(None, f"台股觀察啟動失敗。\n\n{last}\n\n錯誤記錄:{path or '無法寫入'}\n\n"
                                   "可執行「除錯啟動.bat」查看詳細訊息。", "台股觀察", 0x10)
        else:
            print("\n" + "=" * 46)
            print("  程式啟動失敗,錯誤訊息如下:\n")
            print(tb)
            if path:
                print(f"  錯誤記錄已存到:{path}")
            _pause("  請將此畫面截圖給開發者。按 Enter 關閉視窗…")


if __name__ == "__main__":
    main()

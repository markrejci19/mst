import os
import re
import time
import random
import shutil
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import requests
import urllib3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# ===================== REQUESTS GLOBAL =====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== PATHS =====================
INPUT_PENDING_DIR = os.path.join("Input", "Chưa xử lý")
INPUT_DONE_DIR = os.path.join("Input", "Đã xử lý")
OUTPUT_DIR = "Output"

# ===================== EXCEL COLUMNS =====================
COL_STT = "STT"
COL_CIF = "CIF"
COL_CUSTOMER = "CusTomer_Name"
COL_MST = "Mã số thuế"

# ===================== ABBREVIATION MAP =====================
abbreviation_map = {
    "CT": "CÔNG TY",
    "CTY": "CÔNG TY",
    "TNHH": "TRÁCH NHIỆM HỮU HẠN",
    "CP": "CỔ PHẦN",
    "TM": "THƯƠNG MẠI",
    "DV": "DỊCH VỤ",
    "XD": "XÂY DỰNG",
    "KT": "KỸ THUẬT",
    "ĐT": "ĐẦU TƯ",
    "DT": "ĐẦU TƯ",
    "CTGT": "CÔNG TRÌNH GIAO THÔNG",
    "MTV": "MỘT THÀNH VIÊN",
    "VLXD": "VẬT LIỆU XÂY DỰNG",
    "SX": "SẢN XUẤT",
    "GT": "GIAO THÔNG",
}

# ===================== SELENIUM CONFIG =====================
HEADLESS = False
PAGE_LOAD_TIMEOUT = 60

# Giảm tốc độ để hạn chế Cloudflare
SCRAPE_MIN_SLEEP = 6.0
SCRAPE_MAX_SLEEP = 12.0
LONG_BREAK_EVERY = 60
LONG_BREAK_RANGE = (240, 420)

# Dùng profile để giữ cookie/session
SELENIUM_PROFILE_DIR = os.path.abspath("./chrome_profile_selenium")

# Nếu gặp challenge: pause để bạn giải thủ công trong browser
PAUSE_ON_CLOUDFLARE = True

# Warm-up: mở masothue trước khi chạy
WARMUP_MANUAL = True
WARMUP_URL = "https://masothue.com/"

# ===================== PROXY (API only) =====================
PROXY_URL = "http://hoangnk4:Tp3%4006012026@ho-proxy02.tpb.vn:8080"
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}
REQUESTS_VERIFY_SSL = False  # bỏ qua cert do proxy MITM

# ===================== APIs =====================
VITAX_URL_TMPL = "https://api.vitax.one/api/partner/Invoices/getMST?mst={mst}"
VIETQR_URL_TMPL = "https://api.vietqr.io/v2/business/{mst}"
REQUESTS_TIMEOUT = 30
API_MAX_RETRIES = 5
API_BACKOFF_BASE = 2.0
API_BACKOFF_CAP = 60.0

# ===================== MASOTHUE =====================
MASOTHUE_HOME = "https://masothue.com/"
XPATH_MASOTHUE_SEARCH = '//*[@id="search"]'
XPATH_MASOTHUE_TABLE_1 = '//*[@id="main"]/section[1]/div/table[1]'
XPATH_MASOTHUE_TABLE_2 = '//*[@id="main"]/section[1]/div/table[2]'
WAIT_MASOTHUE_TABLE_TIMEOUT = 30

# ===================== TVPL (chỉ dùng khi masothue fail) =====================
TVPL_SEARCH_TMPL = (
    "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-ma-so-thue-doanh-nghiep"
    "?timtheo=ma-so-thue&tukhoa={mst}"
)
XPATH_TVPL_RESULT_TABLE = '//*[@id="dvResultSearch"]/table'
XPATH_TVPL_DETAIL_TABLE = '//*[@id="dv_ttdn"]'


# ===================== HELPERS =====================
def ensure_dirs() -> None:
    os.makedirs(INPUT_PENDING_DIR, exist_ok=True)
    os.makedirs(INPUT_DONE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(SELENIUM_PROFILE_DIR, exist_ok=True)


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def normalize_key(k: str) -> str:
    k = clean_text(k)
    k = re.sub(r"\s*:\s*$", "", k)
    return k


def mst_digits(mst: str) -> str:
    return re.sub(r"\D", "", mst or "")


def is_13_numbers(mst: str) -> bool:
    return bool(re.fullmatch(r"\d{13}", mst or ""))


def fix_dash(mst: str) -> str:
    return (mst[:10] + "-" + mst[10:]) if is_13_numbers(mst) else mst


def normalize_mst(v: Any) -> str:
    s = str(v or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9\-]", "", s)
    s = fix_dash(s)
    return s


def sleep_between(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))


def looks_like_cloudflare_challenge(html_or_text: str) -> bool:
    t = (html_or_text or "").lower()
    return (
        ("just a moment" in t and "cloudflare" in t)
        or ("checking your browser" in t)
        or ("cf-chl" in t)
        or ("challenge-platform" in t)
        or ("turnstile" in t)
    )


def expand_abbreviations(name: str) -> str:
    s = clean_text(name)
    if not s:
        return ""
    tokens = re.split(r"(\s+|[-/.])", s.upper())
    out: List[str] = []
    for tok in tokens:
        if tok.strip() == "":
            out.append(tok)
            continue
        out.append(abbreviation_map.get(tok.strip(), tok))
    return "".join(out)


def slugify_vi(text: str) -> str:
    import unicodedata
    s = (text or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


# ===================== REQUESTS JSON + RETRY (API) =====================
def safe_json(resp: requests.Response) -> Tuple[Optional[dict], str]:
    try:
        return resp.json(), ""
    except Exception:
        snippet = (resp.text or "")[:200].replace("\n", " ").replace("\r", " ")
        ctype = resp.headers.get("Content-Type", "")
        return None, f"non_json_response status={resp.status_code} content_type={ctype} body[:200]={snippet!r}"


def request_json_with_retry(session: requests.Session, url: str, timeout: int = REQUESTS_TIMEOUT) -> dict:
    last_exc = None
    for attempt in range(API_MAX_RETRIES):
        try:
            r = session.get(url, timeout=timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = min(API_BACKOFF_CAP, (API_BACKOFF_BASE ** attempt) + random.uniform(0, 1.5))
                else:
                    wait_s = min(API_BACKOFF_CAP, (API_BACKOFF_BASE ** attempt) + random.uniform(0, 1.5))
                time.sleep(wait_s)
                continue

            if 500 <= r.status_code <= 599:
                wait_s = min(API_BACKOFF_CAP, (API_BACKOFF_BASE ** attempt) + random.uniform(0, 1.5))
                time.sleep(wait_s)
                continue

            r.raise_for_status()
            js, jerr = safe_json(r)
            if js is None:
                if attempt < API_MAX_RETRIES - 1:
                    wait_s = min(API_BACKOFF_CAP, (API_BACKOFF_BASE ** attempt) + random.uniform(0, 1.5))
                    time.sleep(wait_s)
                    continue
                raise ValueError(jerr)
            return js

        except Exception as e:
            last_exc = e
            if attempt < API_MAX_RETRIES - 1:
                wait_s = min(API_BACKOFF_CAP, (API_BACKOFF_BASE ** attempt) + random.uniform(0, 1.5))
                time.sleep(wait_s)
                continue
            raise last_exc
    raise RuntimeError("unreachable")


def api_vitax_get_name(mst: str, session: requests.Session) -> str:
    js = request_json_with_retry(session, VITAX_URL_TMPL.format(mst=mst))
    result = js.get("result") or {}
    return (result.get("name") or "").strip()


def api_vietqr_get_name(mst: str, session: requests.Session) -> str:
    js = request_json_with_retry(session, VIETQR_URL_TMPL.format(mst=mst))
    if str(js.get("code") or "") != "00":
        return ""
    data = js.get("data") or {}
    return (data.get("name") or "").strip()


def build_masothue_link(name: str, mst: str) -> str:
    mst_n = normalize_mst(mst)
    slug = slugify_vi(name)
    if not mst_n or not slug:
        return ""
    return f"https://masothue.com/{mst_n}-{slug}"


def api_get_correct_name_for_failed_link(mst: str, session: requests.Session) -> Tuple[str, str]:
    try:
        name = api_vitax_get_name(mst, session)
        if name:
            return name, "vitax"
    except Exception:
        pass
    try:
        name = api_vietqr_get_name(mst, session)
        if name:
            return name, "vietqr"
    except Exception:
        pass
    return "", ""


# ===================== Selenium (Selenium Manager) =====================
def make_driver() -> webdriver.Chrome:
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument(f"--user-data-dir={SELENIUM_PROFILE_DIR}")

    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)  # Selenium Manager auto-match Chrome
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def pause_for_manual_challenge(driver, reason: str) -> None:
    print("\n" + "=" * 80)
    print(f"[MANUAL ACTION REQUIRED] {reason}")
    print("Trình duyệt đang mở. Hãy tự giải Cloudflare (nếu có), rồi quay lại terminal và bấm Enter để tiếp tục.")
    print("=" * 80 + "\n")
    input("Press Enter to continue...")


def safe_get(driver, url: str) -> None:
    driver.get(url)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    html = driver.page_source or ""
    if looks_like_cloudflare_challenge(html):
        if PAUSE_ON_CLOUDFLARE:
            pause_for_manual_challenge(driver, f"Cloudflare challenge detected at {url}")
            WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if looks_like_cloudflare_challenge(driver.page_source or ""):
                raise RuntimeError("cloudflare_still_present_after_manual")
        else:
            raise RuntimeError("cloudflare_challenge_detected")


def parse_table_element_to_kv(table_el) -> Dict[str, str]:
    out: Dict[str, str] = {}
    rows = table_el.find_elements(By.XPATH, ".//tr")
    for r in rows:
        tds = r.find_elements(By.XPATH, "./td")
        if len(tds) >= 2:
            k = normalize_key(tds[0].text)
            v = clean_text(tds[1].text)
            if k:
                out[k] = v
    return out


# ---------- MASOTHUE ----------
def masothue_fetch_from_current_page(driver) -> Dict[str, str]:
    try:
        WebDriverWait(driver, WAIT_MASOTHUE_TABLE_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, XPATH_MASOTHUE_TABLE_1))
        )
    except TimeoutException:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, XPATH_MASOTHUE_TABLE_2))
        )

    kv: Dict[str, str] = {"masothue_url": driver.current_url}

    try:
        t1 = driver.find_element(By.XPATH, XPATH_MASOTHUE_TABLE_1)
        for k, v in parse_table_element_to_kv(t1).items():
            kv[f"mst_t1_{k}"] = v
    except Exception:
        pass

    try:
        t2 = driver.find_element(By.XPATH, XPATH_MASOTHUE_TABLE_2)
        for k, v in parse_table_element_to_kv(t2).items():
            kv[f"mst_t2_{k}"] = v
    except Exception:
        pass

    if len(kv) <= 1:
        raise RuntimeError("masothue_no_tables")

    return kv


def masothue_open_link(driver, url: str) -> Dict[str, str]:
    safe_get(driver, url)
    return masothue_fetch_from_current_page(driver)


def masothue_search_by_mst(driver, mst: str) -> Dict[str, str]:
    safe_get(driver, MASOTHUE_HOME)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.XPATH, XPATH_MASOTHUE_SEARCH)))
    el = driver.find_element(By.XPATH, XPATH_MASOTHUE_SEARCH)
    el.clear()
    el.send_keys(normalize_mst(mst))
    el.send_keys(Keys.ENTER)

    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    if looks_like_cloudflare_challenge(driver.page_source or ""):
        if PAUSE_ON_CLOUDFLARE:
            pause_for_manual_challenge(driver, f"Cloudflare after masothue search MST={mst}")
        else:
            raise RuntimeError("masothue_cloudflare_after_search")

    return masothue_fetch_from_current_page(driver)


# ---------- TVPL ----------
def tvpl_fetch_detail(driver) -> Dict[str, str]:
    detail_table = WebDriverWait(driver, 25).until(
        EC.presence_of_element_located((By.XPATH, XPATH_TVPL_DETAIL_TABLE))
    )
    kv: Dict[str, str] = {"tvpl_detail_url": driver.current_url}
    for k, v in parse_table_element_to_kv(detail_table).items():
        kv[f"tvpl_{k}"] = v
    if len(kv) <= 1:
        raise RuntimeError("tvpl_detail_empty")
    return kv


def tvpl_pick_best_row(table_el, mst: str):
    mst_norm = mst_digits(mst)
    rows = table_el.find_elements(By.XPATH, ".//tbody/tr[contains(@class,'item_mst')]")
    if not rows:
        rows = table_el.find_elements(By.XPATH, ".//tbody/tr")
    best = None
    for r in rows:
        try:
            cell = r.find_element(By.XPATH, ".//td[2]//strong").text
        except Exception:
            cell = ""
        if mst_digits(cell) == mst_norm and mst_norm:
            best = r
            break
    return best or (rows[0] if rows else None)


def tvpl_search_by_mst(driver, mst: str) -> Dict[str, str]:
    url = TVPL_SEARCH_TMPL.format(mst=normalize_mst(mst))
    safe_get(driver, url)

    table = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, XPATH_TVPL_RESULT_TABLE)))
    row = tvpl_pick_best_row(table, mst)
    if row is None:
        raise RuntimeError("tvpl_no_rows")

    try:
        a = row.find_element(By.XPATH, ".//td[2]//a")
    except Exception:
        a = row.find_element(By.XPATH, ".//td[3]//a")
    a.click()
    return tvpl_fetch_detail(driver)


# ===================== PIPELINE =====================
def list_input_excels() -> List[str]:
    if not os.path.exists(INPUT_PENDING_DIR):
        return []
    return sorted(
        os.path.join(INPUT_PENDING_DIR, fn)
        for fn in os.listdir(INPUT_PENDING_DIR)
        if fn.lower().endswith(".xlsx")
    )


def read_excel(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, dtype=str)
    for col in [COL_STT, COL_CIF, COL_CUSTOMER, COL_MST]:
        if col not in df.columns:
            raise ValueError(f"Excel thiếu cột '{col}'. Columns={list(df.columns)}")
    df[COL_MST] = df[COL_MST].apply(normalize_mst)
    df[COL_CUSTOMER] = df[COL_CUSTOMER].astype(str).fillna("")
    return df


def write_excel(df: pd.DataFrame, path: str) -> None:
    df.to_excel(path, index=False)


def process_excel(excel_path: str) -> None:
    base = os.path.splitext(os.path.basename(excel_path))[0]
    out_full = os.path.join(OUTPUT_DIR, f"{base}__FULL.xlsx")
    out_failed = os.path.join(OUTPUT_DIR, f"{base}__FAILED.xlsx")
    out_links = os.path.join(OUTPUT_DIR, f"{base}__LINKS.xlsx")

    df = read_excel(excel_path)
    total = len(df)

    # Step 1: tạo masothue link từ customer name (KHÔNG dùng tvpl ở bước 1)
    df["mst_norm"] = df[COL_MST].apply(normalize_mst)
    df["customer_name_expanded"] = df[COL_CUSTOMER].apply(expand_abbreviations)
    df["customer_slug"] = df["customer_name_expanded"].apply(slugify_vi)
    df["link_masothue"] = df.apply(
        lambda r: f"https://masothue.com/{r['mst_norm']}-{r['customer_slug']}" if r["mst_norm"] and r["customer_slug"] else "",
        axis=1,
    )

    # API fields (chỉ dùng khi link fail)
    df["api_name"] = ""
    df["api_source"] = ""
    df["api_error"] = ""
    df["link_masothue_api"] = ""

    # crawl status
    df["crawl_status"] = ""
    df["crawl_source"] = ""
    df["crawl_error"] = ""

    print(f"\n[FILE] {excel_path}")
    print(f"[ROWS] {total}")
    print(f"[WARMUP] {WARMUP_MANUAL} | profile={SELENIUM_PROFILE_DIR}")

    driver = make_driver()

    # Warm-up để bạn giải challenge 1 lần (nếu có)
    if WARMUP_MANUAL:
        safe_get(driver, WARMUP_URL)
        pause_for_manual_challenge(driver, "Warm-up: open masothue.com and solve challenge if needed")

    with requests.Session() as session:
        session.proxies.update(PROXIES)
        session.verify = REQUESTS_VERIFY_SSL
        session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json,*/*;q=0.8"})

        ok = 0
        fail = 0
        api_called = 0

        try:
            for n, idx in enumerate(df.index, 1):
                mst = str(df.at[idx, "mst_norm"] or "").strip()
                link_mst = str(df.at[idx, "link_masothue"] or "").strip()
                print(f"[{n}/{total}] MST={mst}")

                try:
                    kv = masothue_open_link(driver, link_mst)
                    df.at[idx, "crawl_status"] = "ok_masothue_link"
                    df.at[idx, "crawl_source"] = "customer_masothue_link"
                    for k, v in kv.items():
                        if k not in df.columns:
                            df[k] = ""
                        df.at[idx, k] = v
                    ok += 1
                except Exception as e1:
                    api_called += 1
                    name_api, src = api_get_correct_name_for_failed_link(mst, session)
                    df.at[idx, "api_name"] = name_api
                    df.at[idx, "api_source"] = src

                    if name_api:
                        df.at[idx, "link_masothue_api"] = build_masothue_link(name_api, mst)
                        try:
                            kv2 = masothue_open_link(driver, df.at[idx, "link_masothue_api"])
                            df.at[idx, "crawl_status"] = "ok_masothue_link"
                            df.at[idx, "crawl_source"] = f"api_masothue_link({src})"
                            for k, v in kv2.items():
                                if k not in df.columns:
                                    df[k] = ""
                                df.at[idx, k] = v
                            ok += 1
                        except Exception as e2:
                            # fallback search MST: masothue -> tvpl
                            try:
                                kv3 = masothue_search_by_mst(driver, mst)
                                df.at[idx, "crawl_status"] = "ok_masothue_search"
                                df.at[idx, "crawl_source"] = "fallback_search"
                                for k, v in kv3.items():
                                    if k not in df.columns:
                                        df[k] = ""
                                    df.at[idx, k] = v
                                ok += 1
                            except Exception as e3:
                                try:
                                    kv4 = tvpl_search_by_mst(driver, mst)
                                    df.at[idx, "crawl_status"] = "ok_tvpl_search"
                                    df.at[idx, "crawl_source"] = "fallback_search"
                                    for k, v in kv4.items():
                                        if k not in df.columns:
                                            df[k] = ""
                                        df.at[idx, k] = v
                                    ok += 1
                                except Exception as e4:
                                    df.at[idx, "crawl_status"] = "error"
                                    df.at[idx, "crawl_source"] = "failed_all"
                                    df.at[idx, "crawl_error"] = f"e1={e1} | e2={e2} | e3={e3} | e4={e4}"
                                    fail += 1
                    else:
                        df.at[idx, "api_error"] = f"api_no_name_after_link_fail e1={e1}"
                        try:
                            kv3 = masothue_search_by_mst(driver, mst)
                            df.at[idx, "crawl_status"] = "ok_masothue_search"
                            df.at[idx, "crawl_source"] = "fallback_search"
                            for k, v in kv3.items():
                                if k not in df.columns:
                                    df[k] = ""
                                df.at[idx, k] = v
                            ok += 1
                        except Exception as e3:
                            try:
                                kv4 = tvpl_search_by_mst(driver, mst)
                                df.at[idx, "crawl_status"] = "ok_tvpl_search"
                                df.at[idx, "crawl_source"] = "fallback_search"
                                for k, v in kv4.items():
                                    if k not in df.columns:
                                        df[k] = ""
                                    df.at[idx, k] = v
                                ok += 1
                            except Exception as e4:
                                df.at[idx, "crawl_status"] = "error"
                                df.at[idx, "crawl_source"] = "failed_all"
                                df.at[idx, "crawl_error"] = f"e1={e1} | e3={e3} | e4={e4}"
                                fail += 1

                print(f"  -> OK={ok} | FAIL={fail} | api_called={api_called}")

                sleep_between(SCRAPE_MIN_SLEEP, SCRAPE_MAX_SLEEP)
                if n % LONG_BREAK_EVERY == 0:
                    lb = random.uniform(*LONG_BREAK_RANGE)
                    print(f"[BREAK] Processed {n}. Long break {lb:.1f}s")
                    time.sleep(lb)

                if n % 30 == 0:
                    write_excel(df, out_full)
                    cols = [
                        COL_STT, COL_CIF, COL_CUSTOMER, COL_MST, "mst_norm",
                        "customer_name_expanded", "link_masothue",
                        "api_name", "api_source", "link_masothue_api", "api_error",
                        "crawl_status", "crawl_source", "crawl_error",
                    ]
                    write_excel(df[[c for c in cols if c in df.columns]].copy(), out_links)

        finally:
            try:
                driver.quit()
            except Exception:
                pass

    write_excel(df, out_full)
    df_failed = df[~df["crawl_status"].astype(str).str.startswith("ok_")].copy()
    write_excel(df_failed, out_failed)

    cols = [
        COL_STT, COL_CIF, COL_CUSTOMER, COL_MST, "mst_norm",
        "customer_name_expanded", "link_masothue",
        "api_name", "api_source", "link_masothue_api", "api_error",
        "crawl_status", "crawl_source", "crawl_error",
    ]
    write_excel(df[[c for c in cols if c in df.columns]].copy(), out_links)

    print(f"[OUTPUT] Full:   {out_full}")
    print(f"[OUTPUT] Failed: {out_failed} (rows={len(df_failed)})")
    print(f"[OUTPUT] Links:  {out_links}")

    dst = os.path.join(INPUT_DONE_DIR, os.path.basename(excel_path))
    shutil.move(excel_path, dst)
    print(f"[MOVE] {excel_path} -> {dst}")


def main():
    ensure_dirs()
    files = list_input_excels()
    if not files:
        print(f"Không có file .xlsx trong: {INPUT_PENDING_DIR}")
        return
    for fp in files:
        process_excel(fp)


if __name__ == "__main__":
    main()

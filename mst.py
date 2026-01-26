import json
import random
import re
import time
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from tqdm import tqdm

API_TEMPLATE = "https://api.vietqr.io/v2/business/{mst}"
MASOTHUE_TEMPLATE = "https://masothue.com/{mst}-{slug}"


def normalize_mst(value: str) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9\-]", "", s)
    return s if s else None


def mst_len_alnum(mst: str) -> int:
    # độ dài tính theo ký tự chữ/số (không tính '-')
    return len(re.sub(r"[^0-9A-Za-z]", "", mst or ""))


def slugify_vi(text: str) -> str:
    """
    Bỏ dấu tiếng Việt + đ/Đ -> d, tạo slug a-z0-9-.
    """
    import unicodedata

    if not text:
        return ""
    s = str(text).strip()
    if not s:
        return ""

    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def call_vietqr(mst: str, session: requests.Session, timeout: int = 20) -> Dict[str, Any]:
    url = API_TEMPLATE.format(mst=mst)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def sleep_between_requests(min_seconds: float, max_seconds: float) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def is_nullish(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    return s == "" or s.lower() in {"null", "none", "nan"}


def main(
    input_csv: str = "mst_links_v3.csv",
    output_csv: str = "mst_links_v4.csv",
    min_sleep_seconds: float = 3.0,
    max_sleep_seconds: float = 5.0,
    max_retries: int = 3,
):
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)

    required_cols = {"mst", "name", "link"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV thiếu cột: {missing}. Các cột hiện có: {list(df.columns)}")

    # normalize mst
    df["mst"] = df["mst"].apply(normalize_mst)

    # lọc các dòng cần chạy lại: link null/empty AND mst >= 10 (không tính '-')
    mask_retry = df["link"].apply(is_nullish) & df["mst"].apply(lambda x: mst_len_alnum(x or "") >= 10)
    idxs = df.index[mask_retry].tolist()

    total = len(idxs)
    ok = 0
    fail = 0
    print(f"Tổng bản ghi cần chạy lại (link null & mst>=10): {total}")

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Python script for business lookup)",
        "Accept": "application/json,*/*;q=0.8",
    }

    with requests.Session() as session:
        session.headers.update(headers)

        pbar = tqdm(total=total, desc="Retry missing links", unit="row")
        for n, i in enumerate(idxs, 1):
            mst = df.at[i, "mst"]
            if not mst:
                fail += 1
                pbar.set_postfix_str(f"done={n}/{total} | ok={ok} | fail={fail}")
                pbar.update(1)
                continue

            existing_name = (df.at[i, "name"] or "").strip()
            business_name = existing_name
            err = None

            if not existing_name:
                for attempt in range(max_retries):
                    try:
                        url = API_TEMPLATE.format(mst=mst)
                        r = session.get(url, timeout=20)
                        if r.status_code == 429:
                            err = "rate_limited"
                            retry_after = r.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    time.sleep(float(retry_after))
                                except ValueError:
                                    sleep_between_requests(min_sleep_seconds, max_sleep_seconds)
                            else:
                                sleep_between_requests(min_sleep_seconds, max_sleep_seconds)
                            continue
                        r.raise_for_status()
                        js = r.json()
                        code = str(js.get("code") or "")
                        if code != "00":
                            err = f"api_code={code}"
                        else:
                            data = js.get("data") or {}
                            business_name = (data.get("name") or "").strip()
                            if business_name:
                                err = None
                                break
                            err = "empty_name"
                    except Exception as exc:
                        err = str(exc)

                    sleep_between_requests(min_sleep_seconds, max_sleep_seconds)

            if mst and business_name:
                if is_nullish(df.at[i, "name"]):
                    df.at[i, "name"] = business_name
                slug = slugify_vi(business_name)
                df.at[i, "link"] = MASOTHUE_TEMPLATE.format(mst=mst, slug=slug)
                ok += 1
            else:
                fail += 1

            pbar.set_postfix_str(f"done={n}/{total} | ok={ok} | fail={fail}")
            pbar.update(1)

            # ghi ngay sau mỗi bản ghi
            df.to_csv(output_csv, index=False, encoding="utf-8-sig")

            # 1 request -> sleep 3-5 giây
            sleep_between_requests(min_sleep_seconds, max_sleep_seconds)

        pbar.close()

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Done. Total retried={total} | OK={ok} | FAIL={fail}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()

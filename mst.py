import json
import re
import time
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from tqdm import tqdm


API_TEMPLATE = "https://api.vietqr.io/v2/business/{mst}"


def normalize_mst(value: str) -> Optional[str]:
    """
    Chuẩn hoá MST để gọi API:
    - giữ số và dấu '-' (ví dụ: 0101234567-001)
    - bỏ khoảng trắng
    - loại ký tự lạ
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9\-]", "", s)
    return s if s else None


def read_mst_txt(path: str) -> List[str]:
    """
    Đọc txt_mst.txt dạng:
    MST_CLEAN
    8703744430-001
    0110985790
    ...
    """
    msts: List[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.upper() == "MST_CLEAN":
                continue
            mst = normalize_mst(line)
            if mst:
                msts.append(mst)
    return msts


def call_vietqr(mst: str, session: requests.Session, timeout: int = 20) -> Dict[str, Any]:
    url = API_TEMPLATE.format(mst=mst)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def flatten_json(js: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trả về dict phẳng để ghi CSV.
    Giữ:
    - code, desc
    - data.* (nếu có)
    - raw_json (để debug)
    """
    out: Dict[str, Any] = {}
    out["code"] = js.get("code", "")
    out["desc"] = js.get("desc", "")

    data = js.get("data") or {}
    if isinstance(data, dict):
        for k, v in data.items():
            out[f"data_{k}"] = v
    else:
        out["data"] = data

    out["raw_json"] = json.dumps(js, ensure_ascii=False)
    return out


def main(
    txt_path: str = "txt_mst.txt",
    out_csv: str = "vietqr_output.csv",
    sleep_seconds: float = 0.2,
):
    msts = read_mst_txt(txt_path)
    if not msts:
        raise ValueError(f"Không đọc được MST nào từ file: {txt_path}")

    total = len(msts)
    ok = 0
    fail = 0
    rows: List[Dict[str, Any]] = []

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Python script for business lookup)",
        "Accept": "application/json,*/*;q=0.8",
    }

    with requests.Session() as session:
        session.headers.update(headers)

        pbar = tqdm(total=total, desc="Calling VietQR", unit="mst")
        for i, mst in enumerate(msts, 1):
            row: Dict[str, Any] = {"mst_input": mst}

            try:
                js = call_vietqr(mst, session=session)
                row.update(flatten_json(js))
                ok += 1
            except Exception as e:
                row["code"] = ""
                row["desc"] = f"ERROR: {e}"
                row["raw_json"] = ""
                fail += 1

            rows.append(row)

            # cập nhật tiến trình + thống kê
            pbar.set_postfix_str(f"done={i}/{total} | ok={ok} | fail={fail}")
            pbar.update(1)

            time.sleep(sleep_seconds)

        pbar.close()

    df = pd.DataFrame(rows)

    # Sắp xếp cột cho dễ nhìn: mst_input, code, desc trước
    first_cols = [c for c in ["mst_input", "code", "desc"] if c in df.columns]
    other_cols = [c for c in df.columns if c not in first_cols]
    df = df[first_cols + other_cols]

    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"Done. Total={total} | OK={ok} | FAIL={fail}")
    print(f"Output CSV: {out_csv}")


if __name__ == "__main__":
    main()
import os
import re
import time
import unicodedata
from typing import Optional, Dict, Any

import pandas as pd
import requests
from tqdm import tqdm


API_TEMPLATE = "https://api.vietqr.io/v2/business/{mst}"
MASOTHUE_TEMPLATE = "https://masothue.com/{id}-{slug}"


def slugify_vi(text: str) -> str:
    """
    Chuyển tên DN tiếng Việt -> slug kiểu masothue.com:
    - bỏ dấu
    - lowercase
    - ký tự không phải a-z0-9 -> '-'
    - gộp nhiều '-' liên tiếp
    """
    if text is None:
        return ""
    text = text.strip().lower()

    # bỏ dấu unicode
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    # đ/Đ
    text = text.replace("đ", "d")

    # thay ký tự không hợp lệ bằng '-'
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def normalize_mst(value) -> Optional[str]:
    """
    Chuẩn hoá MST:
    - giữ số và dấu '-' (một số MST có hậu tố dạng 0101234567-001)
    - bỏ khoảng trắng
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9\-]", "", s)
    return s if s else None


def call_vietqr(mst: str, session: requests.Session, timeout: int = 20) -> Dict[str, Any]:
    url = API_TEMPLATE.format(mst=mst)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def save_html(url: str, out_path: str, session: requests.Session, timeout: int = 30) -> int:
    """
    Tải và lưu HTML. Trả về status_code.
    """
    r = session.get(url, timeout=timeout)
    status = r.status_code

    # Lưu cả khi không phải 200 để debug (tuỳ bạn). Ở đây chỉ lưu khi 200:
    if status == 200:
        # masothue.com trả HTML utf-8, dùng bytes để giữ nguyên
        with open(out_path, "wb") as f:
            f.write(r.content)
    return status


def main(
    excel_path: str = "test.xlsx",
    out_dir: str = "html_masothue",
    out_csv: str = "output.csv",
    sleep_seconds: float = 0.5,
):
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Không thấy file: {excel_path}")

    os.makedirs(out_dir, exist_ok=True)

    df = pd.read_excel(excel_path, dtype=str)  # đọc tất cả dạng str để không mất số 0 đầu
    # Bạn có thể đổi tên cột ở đây nếu file thực tế khác
    mst_col = "Mã số thuế"
    if mst_col not in df.columns:
        raise ValueError(f"Không thấy cột '{mst_col}' trong Excel. Các cột hiện có: {list(df.columns)}")

    df["mst_norm"] = df[mst_col].apply(normalize_mst)

    # Chuẩn bị kết quả
    df["api_code"] = ""
    df["api_desc"] = ""
    df["business_id"] = ""
    df["business_name"] = ""
    df["masothue_url"] = ""
    df["html_status"] = ""
    df["html_file"] = ""

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Python script for business lookup; +https://example.com)",
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    }

    with requests.Session() as session:
        session.headers.update(headers)

        for idx in tqdm(df.index, desc="Processing"):
            mst = df.at[idx, "mst_norm"]
            if not mst:
                df.at[idx, "api_desc"] = "Missing MST"
                continue

            # 1) Call API VietQR
            try:
                js = call_vietqr(mst, session=session)
            except Exception as e:
                df.at[idx, "api_desc"] = f"API error: {e}"
                continue

            df.at[idx, "api_code"] = js.get("code", "")
            df.at[idx, "api_desc"] = js.get("desc", "")

            data = js.get("data") or {}
            business_id = data.get("id") or ""
            business_name = data.get("name") or ""

            df.at[idx, "business_id"] = business_id
            df.at[idx, "business_name"] = business_name

            if not business_id or not business_name:
                # Có thể API trả code khác "00" hoặc data rỗng
                continue

            # 2) Tạo URL masothue.com
            slug = slugify_vi(business_name)
            masothue_url = MASOTHUE_TEMPLATE.format(id=business_id, slug=slug)
            df.at[idx, "masothue_url"] = masothue_url

            # 3) Tải HTML và lưu file
            safe_id = re.sub(r"[^0-9\-]", "_", business_id)
            out_path = os.path.join(out_dir, f"{safe_id}.html")

            try:
                status = save_html(masothue_url, out_path, session=session)
                df.at[idx, "html_status"] = str(status)
                if status == 200:
                    df.at[idx, "html_file"] = out_path
            except Exception as e:
                df.at[idx, "html_status"] = f"HTML error: {e}"

            # nghỉ một chút để hạn chế bị chặn
            time.sleep(sleep_seconds)

    # Lưu file kết quả
    df.to_csv(out_csv, index=False)
    print(f"Done. HTML saved in: {out_dir}")
    print(f"Result Excel: {out_csv}")


if __name__ == "__main__":
    main()
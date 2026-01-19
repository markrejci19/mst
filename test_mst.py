import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


@dataclass
class HcmLgspConfig:
    base_url: str
    access_key: str
    secret_key: str
    app_name: str
    partner_code: str
    partner_code_cus: str
    timeout: int = 30


class DoanhNghiepLookupError(Exception):
    pass


def build_authorization_header(cfg: HcmLgspConfig) -> str:
    payload = {
        "AccessKey": cfg.access_key,
        "SecretKey": cfg.secret_key,
        "AppName": cfg.app_name,
        "PartnerCode": cfg.partner_code,
        "PartnerCodeCus": cfg.partner_code_cus,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def tra_cuu_1_mst(
    session: requests.Session,
    cfg: HcmLgspConfig,
    mst: str,
) -> Dict[str, Any]:
    url = cfg.base_url.rstrip("/") + "/TraCuuThongTinDoanhNghiep"
    headers = {
        "Authorization": build_authorization_header(cfg),
        "Accept": "application/json",
    }
    params = {"MaSoDoanhNghiep": mst.strip()}

    resp = session.get(url, headers=headers, params=params, timeout=cfg.timeout)

    try:
        data = resp.json()
    except Exception as e:
        raise DoanhNghiepLookupError(
            f"MST={mst}: Response không phải JSON. HTTP {resp.status_code}. Body: {resp.text[:500]}"
        ) from e

    # kiểm tra theo body chuẩn của đặc tả
    if data.get("StatusCode") != 200 or data.get("Status") != "SUCCESS" or data.get("ThrowException") is True:
        raise DoanhNghiepLookupError(
            f"MST={mst}: Fail. StatusCode={data.get('StatusCode')}, "
            f"Status={data.get('Status')}, Description={data.get('Description')}"
        )

    return data


def tra_cuu_nhieu_mst(
    cfg: HcmLgspConfig,
    mst_list: List[str],
    max_workers: int = 8,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """
    Trả về:
      - successes: {mst: full_response_json}
      - errors: {mst: error_message}
    """
    mst_list = [m.strip() for m in mst_list if m and m.strip()]
    successes: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(tra_cuu_1_mst, session, cfg, mst): mst for mst in mst_list}

            for fut in as_completed(futures):
                mst = futures[fut]
                try:
                    successes[mst] = fut.result()
                except Exception as e:
                    errors[mst] = str(e)

    return successes, errors


if __name__ == "__main__":
    cfg = HcmLgspConfig(
        base_url="https://hcmesb.tphcm.gov.vn",
        access_key="rTkhYCBwHM",
        secret_key="DWkQgY1YSS",
        app_name="TPHCM",
        partner_code="000.00.01.H29",
        partner_code_cus="000.00.01.H29",
        timeout=30,
    )

    mst_list = [
        "8703744430-001",
        "0100107123-001",
        "8075641943-001",
        "8106812020-002",
        "8295425881",
        "0110985790",
        "3700963160-001",
        "8326967220",
        "8525769946",
        "6000379029",
        "018018863",
        "0108824161",
        "1001221902",
        "0311978680",
        "0108411943",
        "0105025499",
        "0102556477",
        "8280145320",
        "0106660373",
        "1602191441",
        "0104178559",
        "8021114820-001",
        "0309496111",
        "0109569210",
    ]

    successes, errors = tra_cuu_nhieu_mst(cfg, mst_list, max_workers=8)

    # In nhanh kết quả OK
    for mst, payload in successes.items():
        dn = payload.get("ResultObject") or {}
        print(f"\n=== {mst} ===")
        print("NAME:", dn.get("NAME"))
        print("ENTERPRISE_GDT_CODE:", dn.get("ENTERPRISE_GDT_CODE"))
        print("ENTERPRISE_STATUS_NAME:", dn.get("ENTERPRISE_STATUS_NAME"))
        print("HO_ADDRESS_FULLTEXT:", dn.get("HO_ADDRESS_FULLTEXT"))

    # In lỗi (nếu có)
    if errors:
        print("\n\n--- ERRORS ---")
        for mst, msg in errors.items():
            print(mst, "=>", msg)
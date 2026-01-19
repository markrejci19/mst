import requests
import re
import csv
import time

def clean_mst(mst):
    """Loại bỏ ký tự đặc biệt và giữ số trước dấu '-' nếu có."""
    mst = re.sub(r'[^A-Za-z0-9]', '', mst)
    mst = mst.split('-')[0]
    return mst

def slugify(name):
    """Chuyển tên công ty thành slug URL-friendly."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", "-", name).strip('-')
    return name

def main():
    with open("txt_mst.txt", encoding="utf-8") as f:
        lines = f.readlines()
    if "MST_CLEAN" in lines[0].upper():
        msts = [line.strip() for line in lines[1:] if line.strip()]
    else:
        msts = [line.strip() for line in lines if line.strip()]

    # Chỉ lấy MST_CLEAN >= 10 ký tự
    msts = [clean_mst(mst) for mst in msts if len(clean_mst(mst)) >= 10]

    total = len(msts)
    success = 0
    fail = 0
    processed = 0
    results = []

    print(f"Total MSTs to process: {total}")

    for idx, mst_clean in enumerate(msts, 1):
        url = f"https://api.vitax.one/api/partner/Invoices/getMST?mst={mst_clean}"
        name = None
        mst_out = None
        link = ''
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            result = data.get("result", {})
            name = result.get("name")
            mst_out = result.get("mst")
            if mst_out and name:
                slug = slugify(name)
                link = f"https://masothue.com/{mst_out}-{slug}"
                success += 1
            else:
                fail += 1
        except Exception as e:
            fail += 1
            name = ''
            mst_out = mst_clean
        processed += 1
        results.append([mst_out or mst_clean, name or '', link])
        percent = processed / total * 100
        print(f"[{processed}/{total}] - {percent:.2f}% | Thành công: {success} | Thất bại: {fail}", end='\r')
        time.sleep(0.02)  # Để dễ nhìn tiến trình, có thể bỏ nếu tốc độ API chậm

    print()  # Xuống dòng khỏi tiến trình
    print(f"Xử lý xong {processed}/{total} mã. Thành công: {success}. Thất bại: {fail}.")

    with open("mst_links.csv", "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['mst', 'name', 'link'])
        writer.writerows(results)

if __name__ == "__main__":
    main()
import requests
import re
import csv
import time

def is_13_numbers(mst):
    return bool(re.fullmatch(r"\d{13}", mst))

def fix_dash(mst):
    # 13 số: chèn gạch dạng NNNNNNNNNNN-00N
    return mst[:10] + '-' + mst[10:] if is_13_numbers(mst) else mst

def slugify(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", "-", name).strip('-')
    return name

def main():
    rows = []
    with open("mst_links.csv", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    to_run = [row for row in rows if is_13_numbers(row['mst'])]
    total = len(to_run)
    print(f"Chỉ chạy lại các bản ghi có mst là 13 ký tự số (không có dấu gạch): {total}")

    success = 0
    fail = 0
    processed = 0

    for idx, row in enumerate(rows):
        mst_old = row['mst']
        # Chỉ xử lý nếu là mst 13 ký tự số
        if not is_13_numbers(mst_old):
            continue

        mst = fix_dash(mst_old)
        url = f"https://api.vitax.one/api/partner/Invoices/getMST?mst={mst}"
        name = ''
        link = ''
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            result = data.get('result', {})
            name = result.get('name')
            if mst and name:
                slug = slugify(name)
                link = f"https://masothue.com/{mst}-{slug}"
                success += 1
            else:
                fail += 1
        except Exception:
            fail += 1
            name = ''
            link = ''
        # Update lại mst, name, link
        row['mst'] = mst
        row['name'] = name or ''
        row['link'] = link or ''
        processed += 1
        percent = processed / total * 100 if total else 100
        print(f"[{processed}/{total}] - {percent:.2f}% | Thành công: {success} | Thất bại: {fail}", end='\r')
        time.sleep(0.01)

    print()
    print(f"Đã xử lý lại {processed}/{total} bản ghi có 13 ký tự số MST. Thành công: {success}. Thất bại: {fail}.")

    with open("mst_links_new.csv", "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['mst', 'name', 'link'])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

if __name__ == "__main__":
    main()
import csv
import re
import unicodedata

def slugify_vi(text: str) -> str:
    if not text:
        return ""

    # 1) Chuẩn hoá: tách dấu ra khỏi chữ (NFD/NFKD đều được)
    s = unicodedata.normalize("NFD", text)

    # 2) Bỏ toàn bộ dấu (các ký tự combining mark)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

    # 3) Xử lý riêng đ/Đ (vì không phải combining mark)
    s = s.replace("đ", "d").replace("Đ", "d")

    # 4) Lowercase + chỉ giữ [a-z0-9 ] rồi thay khoảng trắng bằng '-'
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)      # ký tự khác -> space
    s = re.sub(r"\s+", "-", s).strip("-")   # spaces -> '-'
    return s

def main():
    input_file = "mst_links_new.csv"
    output_file = "mst_links_v2.csv"

    rows = []
    with open(input_file, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mst = (row.get("mst") or "").strip()
            name = (row.get("name") or "").strip()

            # chỉ cập nhật link khi có đủ mst + name
            if mst and name:
                slug = slugify_vi(name)
                row["link"] = f"https://masothue.com/{mst}-{slug}"
            rows.append(row)

    with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["mst", "name", "link"])
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
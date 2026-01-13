import os
import csv
from bs4 import BeautifulSoup

def html_tables_to_csv(html_path, output_dir):
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    tables = soup.find_all("table")
    for idx, table in enumerate(tables):
        rows = []
        for tr in table.find_all("tr"):
            cols = []
            for td in tr.find_all(["td", "th"]):
                # Lấy text, loại bỏ xuống dòng và khoảng trắng thừa
                cols.append(td.get_text(separator=" ", strip=True))
            if cols:
                rows.append(cols)
        if rows:
            basename = os.path.splitext(os.path.basename(html_path))[0]
            csv_path = os.path.join(output_dir, f"{basename}_table{idx+1}.csv")
            with open(csv_path, "w", newline="", encoding="utf-8-sig") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(rows)
            print(f"Đã lưu: {csv_path}")

if __name__ == "__main__":
    html_file = "/workspaces/mst/html_masothue/0102234896.html"
    output_dir = "/workspaces/mst/html_masothue"
    html_tables_to_csv(html_file, output_dir)
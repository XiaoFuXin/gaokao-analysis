import requests
import csv
import os

url = "https://www.shanghairanking.cn/api/pub/v1/bcur"
params = {
    "bcur_type": 11,
    "year": 2026,
    "offset": 0,
    "limit": 500
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.shanghairanking.cn/rankings/bcur/2026"
}

resp = requests.get(url, params=params, headers=headers)
data = resp.json()

rankings=data['data']['rankings']
schools = [item for item in rankings if isinstance(item, dict)]
top_500=schools[:500]
#保存
os.makedirs("data", exist_ok=True)
csv_path = "data/软科2026排名.csv"
with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["排名", "学校名称", "总分"])

    for school in top_500:
        rank = school.get("ranking", "")
        name = school.get("univNameCn", "")
        score = school.get("score", "")
        writer.writerow([rank, name, score])

print(f"已保存{len(top_500)}所学校")
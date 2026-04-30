import re
from db_utils import fetch_all_raw, insert_clean_data, create_tables
from collections import defaultdict

def load_raw_data():
    rows = fetch_all_raw()
    return rows

#清洗专业名
def normalize_major(name):
    cleaned_name=name.strip()
    cleaned_name = re.sub(r'\s+', '', cleaned_name)  
    cleaned_name = re.sub(r'[（(][^)）]*[）)]', '', cleaned_name)
    return cleaned_name

#过滤无效行
def filter_valid(rows):
    invalid_values = ('', '0', '--','-')
    cleaned_rows = []
    for row in rows:
        if (row[6] is None) or (row[7] is None) or (row[6] in invalid_values) or (row[7] in invalid_values):
            continue
        cleaned_rows.append(row)
    return cleaned_rows

#转化为字典
def clean_and_normalize():
    rows = load_raw_data()
    valid_rows = filter_valid(rows)

    result = []
    for row in valid_rows:
        record = {
            'year': row[1],
            'province': row[2],
            'category': row[3],
            'university': row[4],
            'major': normalize_major(row[5]),
            'min_score': int(row[6]),
            'min_rank': int(row[7]),
            'enroll_plan': int(row[8]) if row[8] and row[8] != '0' else 0
        }
        result.append(record)

    print(f"标准化完成，共 {len(result)} 条有效记录")
    return result

#去重
def deduplicate(records):
    uniq={}
    for record in records:
        key=(record['university'], record['major'],record['category'], record['year'], record['year'])
        if key not in uniq or record['min_score'] < uniq[key]['min_score']:
            uniq[key] = record
    return list(uniq.values())

#保留三年齐全的字典
def keep_three_years(records):
    year_sets=defaultdict(set)
    for record in records:
        key=(record['university'], record['major'], record['category'])
        year_sets[key].add(record['year'])

    valid_keys=set()
    for k,ys in year_sets.items():
        if len(ys) == 3:
            valid_keys.add(k)
    result = [r for r in records
              if (r['university'], r['major'], r['category']) in valid_keys]
    return result

#将清洗后的数据写入 scores 表
def save_to_db(records):
    insert_clean_data(records)
    print(f"已写入 {len(records)} 条记录到 scores 表")


if __name__ == '__main__':
    clean_data = clean_and_normalize()
    dedup_data = deduplicate(clean_data)
    final_data = keep_three_years(dedup_data)
    print(f"最终清洗结果: {len(final_data)} 条, 是3的倍数: {len(final_data) % 3 == 0}")
    save_to_db(final_data)
    print("数据清洗完成")
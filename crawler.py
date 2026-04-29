import requests
from db_utils import insert_raw_data, create_tables
#请求头
headers = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

province_ID="36"   #选全国高校对江西省的招生
province_name="江西"
target_year=[2023,2022,2021] #分析年份

#获取全国高校的ID和名称
def get_school_list():
    schools=[]
    #避免死循环
    for page in range(1,100):
        url = f"https://api.eol.cn/web/api/?keyword=&page={page}&uri=apidata/api/gk/school/lists&size=100"
        resp=requests.get(url,headers=headers)
        data=resp.json()
        items=data['data']['item']
        if not items:
            break
        for item in items:
            schools.append({
                'school_id': item['school_id'],
                'name': item['name']
            })
        #time.sleep(0.5)
    return schools

#获取某学校某年在江西的录取专业数据(只保留本科一本)
def fetch_scores(school_id,year):
    url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialscore/{school_id}/{year}/{province_ID}.json"
    try:
        resp=requests.get(url,headers=headers,timeout=5)
        if resp.status_code!=200:
            return []
        data=resp.json()
        if 'data'not in data:
            return []
    except Exception as e:
        print(f"请求失败 {school_id} {year}: {e}")
        return []

    result=[]
    for key,block in data['data'].items():
        parts = key.split('_')
        if len(parts) < 2 or parts[1]!='7':
            continue

        category="物理类" if parts[0] =='1' else "历史类"

        for item in block.get('item',[]):
            if not item.get('min') or item['min']=='--':
                continue

            result.append({
                'year': year,
                'province': province_name,
                'category': category,
                'university': '',
                'major': item.get('spname', ''),
                'min_score': item.get('min'),
                'min_rank': item.get('min_section'),
                'enroll_plan': item.get('plan_num') or 0
            })

    return result


def main():
    #创建表
    create_tables()

    #获取全国高校列表
    print("正在获取全国高校列表...")
    schools=get_school_list()
    print(f"共获取 {len(schools)} 所高校")

    #逐年逐校爬取
    for year in target_year:
        for i,school in enumerate(schools):
            sid=school['school_id']
            sname=school['name']

            items=fetch_scores(sid,year)
            #没有则跳过
            if not items:
                continue
            #填充学校名称
            for item in items:
                item['university']=sname
            # 批量入库
            try:
                insert_raw_data(items)
                print(f"[{i + 1}/{len(schools)}] {sname} {year}年: 入库 {len(items)} 条")
            except Exception as e:
                print(f"入库失败 {sname} {year}: {e}")
            #time.sleep(0.5)
    print("爬取完成！")

if __name__ == '__main__':
    main()
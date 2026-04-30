#对数据进行分析
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False

from db_utils import get_connection

#从MySQL读取scores表从CSV读取软科排名合并返回 DataFrame
def load_data():
    conn = get_connection()
    df=pd.read_sql("select * from scores", conn)
    conn.close()

    ranking = pd.read_csv("data/软科2026排名.csv")
    #用校名合并两表
    df=df.merge(ranking,left_on='university',right_on='学校名称',how='left')
    return df

def huatu(top, title, kind, filename):
    plt.figure(figsize=(12, 8))
    bars = plt.barh(range(len(top)), top['cv'].values)
    if kind == '专业':
        labels = top['major'] + ' - ' + top['university']
    else:
        labels = top['university']
    plt.yticks(range(len(top)), labels, fontsize=8 if kind == '专业' else 10)
    plt.xlabel('变异系数')
    plt.title(title)
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(f'outputs/figures/{filename}.png', dpi=150)
    plt.close()
# ========== 分析一：变异系数 ==========
def analysis_volatility(df):
    grouped=df.groupby(['university', 'major', 'category'])
    stats=grouped['min_rank'].agg(['std', 'mean']).reset_index()
    #计算变异系数:std/mean
    stats['cv'] = stats['std'] / stats['mean']
    #过滤均值为0的清空,以防意外
    stats = stats[stats['mean'] > 0]
    # 只保留平均位次 ≥ 100 的专业，排除高位次噪声
    stats = stats[stats['mean'] >= 100]
    # 专业稳定和不稳定的Top50
    top50_major_large = stats.nlargest(50, 'cv')
    top50_major_small = stats.nsmallest(50, 'cv')

    # 按 (university, year) 先得到学校年度的平均位次，再算三年的变异系数
    school_year = df.groupby(['university', 'year'])['min_rank'].mean().reset_index()
    school_stats = school_year.groupby('university')['min_rank'].agg(['std', 'mean']).reset_index()
    school_stats['cv'] = school_stats['std'] / school_stats['mean']
    school_stats = school_stats[school_stats['mean'] > 0]
    # 院校Top20
    top20_school_large = school_stats.nlargest(20, 'cv')
    top20_school_small = school_stats.nsmallest(20, 'cv')

    #画图(调用函数)
    huatu(top50_major_large, '录取位次波动最大的 Top50 专业 (高风险)', '专业', 'volatility_major_unstable')
    huatu(top50_major_small, '录取位次最稳定的 Top50 专业 (低风险)', '专业', 'volatility_major_stable')
    huatu(top20_school_large, '录取位次波动最大的 Top20 院校 (高风险)', '学校', 'volatility_school_unstable')
    huatu(top20_school_small, '录取位次最稳定的 Top20 院校 (低风险)', '学校', 'volatility_school_stable')

    print("分析一完成，四张图表已保存。")

#分析二：性价比(通过软科排名和录取位次对比)
def analysis_value(df):
    os.makedirs('outputs/figures', exist_ok=True)

    # 1. 每所学校平均录取位次
    school_avg = df.groupby('university')['min_rank'].mean().reset_index()
    school_avg.columns = ['university', 'avg_rank']

    # 2. 合并软科排名
    ranking = df[['university', '排名']].drop_duplicates()
    merged = school_avg.merge(ranking, on='university', how='left')
    merged = merged.dropna(subset=['排名', 'avg_rank'])
    merged = merged[(merged['排名'] > 0) & (merged['avg_rank'] > 0)]

    # 3. 归一化
    rank_min, rank_max = merged['排名'].min(), merged['排名'].max()
    avg_min, avg_max = merged['avg_rank'].min(), merged['avg_rank'].max()

    merged['norm_rank'] = (merged['排名'] - rank_min) / (rank_max - rank_min)
    merged['norm_avg_rank'] = (merged['avg_rank'] - avg_min) / (avg_max - avg_min)

    # 4. 性价比指数 = 归一化排名 - 归一化位次
    merged['value_index'] = merged['norm_rank'] - merged['norm_avg_rank']

    # 5. 最高性价比 Top15（指数最小）和最低性价比 Top15（指数最大）
    top_value = merged.nsmallest(15, 'value_index')
    bottom_value = merged.nlargest(15, 'value_index')

    # 6. 柱状图：高性价比 Top15
    plt.figure(figsize=(10, 6))
    colors = ['#2ecc71' if v < 0 else '#e74c3c' for v in top_value['value_index']]
    plt.barh(range(len(top_value)), top_value['value_index'], color=colors)
    plt.yticks(range(len(top_value)), top_value['university'])
    plt.xlabel('性价比指数')
    plt.title('性价比最高的15所院校')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('outputs/figures/value_top15.png', dpi=150)
    plt.close()

    # 7. 柱状图：低性价比 Top15
    plt.figure(figsize=(10, 6))
    colors = ['#e74c3c' if v > 0 else '#2ecc71' for v in bottom_value['value_index']]
    plt.barh(range(len(bottom_value)), bottom_value['value_index'], color=colors)
    plt.yticks(range(len(bottom_value)), bottom_value['university'])
    plt.xlabel('性价比指数')
    plt.title('性价比最低的15所院校')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('outputs/figures/value_bottom15.png', dpi=150)
    plt.close()

    print("分析二完成，两张柱状图已保存。")


#分析三：专业热度涨幅——位次上升指数
def analysis_trend(df):
    os.makedirs('outputs/figures', exist_ok=True)

    # 按专业和科类计算每年录取位次中位数
    yearly = df.groupby(['major', 'category', 'year'])['min_rank'].median().reset_index()

    # 分离2021和2023年数据
    df_2021 = yearly[yearly['year'] == 2021][['major', 'category', 'min_rank']]
    df_2023 = yearly[yearly['year'] == 2023][['major', 'category', 'min_rank']]

    # 合并
    merged = df_2021.merge(df_2023, on=['major', 'category'], suffixes=('_2021', '_2023'))

    # 计算热度指数
    merged['trend'] = (merged['min_rank_2021'] - merged['min_rank_2023']) / merged['min_rank_2021']
    merged = merged[merged['min_rank_2021'] > 0]

    # 热度上升和下降各前10
    top_rising = merged.nlargest(10, 'trend')
    top_falling = merged.nsmallest(10, 'trend')

    # 柱状图：热度上升 Top10
    plt.figure(figsize=(10, 6))
    colors = ['#e74c3c' if v > 0 else '#3498db' for v in top_rising['trend']]
    plt.barh(range(len(top_rising)), top_rising['trend'], color=colors)
    labels_rising = top_rising['major'] + ' (' + top_rising['category'] + ')'
    plt.yticks(range(len(top_rising)), labels_rising, fontsize=9)
    plt.xlabel('热度指数（正值 = 越来越难考）')
    plt.title('热度上升最快的 10 个专业')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('outputs/figures/trend_rising_top10.png', dpi=150)
    plt.close()

    # 柱状图：热度下降 Top10
    plt.figure(figsize=(10, 6))
    colors = ['#3498db' if v < 0 else '#e74c3c' for v in top_falling['trend']]
    plt.barh(range(len(top_falling)), top_falling['trend'], color=colors)
    labels_falling = top_falling['major'] + ' (' + top_falling['category'] + ')'
    plt.yticks(range(len(top_falling)), labels_falling, fontsize=9)
    plt.xlabel('热度指数（负值 = 越来越容易考）')
    plt.title('热度下降最快的 10 个专业')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('outputs/figures/trend_falling_top10.png', dpi=150)
    plt.close()

    print("分析三完成，两张图表已保存。")


if __name__ == '__main__':
    df = load_data()
    analysis_volatility(df)
    analysis_value(df)
    analysis_trend(df)
    print("全部分析完成！")
# 数据库工具模块：封装所有数据库操作

import pymysql
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

#获取数据库连接
def get_connection():
    conn=pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4'
    )
    return conn

def create_tables():
    conn = get_connection()
    cursor=conn.cursor()

    # 原始数据表
    cursor.execute("""
        create table if not exists raw_scores(
            id int auto_increment primary key,
            year int,
            province varchar(20),
            category varchar(10),
            university varchar(100),
            major varchar(100),
            min_score varchar(20),
            min_rank varchar(20),
            enroll_plan varchar(20)
        )default charset=utf8mb4;   
    """)

    #清洗后的主表
    cursor.execute("""
        create table if not exists scores(
            id int auto_increment primary key,
            year int,
            province varchar(20),
            category varchar(10),
            university varchar(100),
            major varchar(100),
            min_score int,
            min_rank int,
            enroll_plan int     
        )default charset=utf8mb4;   
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("数据表创建完毕")

#往 raw_scores 表插入一条或多条原始数据
def insert_raw_data(data_list):
    conn = get_connection()
    cursor = conn.cursor()
    sql="""
        insert into raw_scores(
            year, province, category, university, major, min_score, min_rank, enroll_plan   
        )
        values(
            %(year)s, %(province)s, %(category)s, %(university)s, %(major)s,%(min_score)s, %(min_rank)s, %(enroll_plan)s
        )
    """
    cursor.executemany(sql, data_list)
    conn.commit()
    cursor.close()
    conn.close()


#读取全部原始数据
def fetch_all_raw():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("select * from raw_scores")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


#往 scores 表插入清洗后的数据
def insert_clean_data(data_list):
    conn = get_connection()
    cursor = conn.cursor()
    sql="""
        insert into scores(
            year, province, category, university, major, min_score, min_rank, enroll_plan
        )
        values(
            %(year)s, %(province)s, %(category)s, %(university)s, %(major)s,%(min_score)s, %(min_rank)s, %(enroll_plan)s
        )
    """
    cursor.executemany(sql, data_list)
    conn.commit()
    cursor.close()
    conn.close()


#读取全部清洗后的数据(用于分析)
def fetch_all_scores():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("select * from scores")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, desc, dense_rank
from pyspark.sql.window import Window

def main():
    # 1. 初始化 SparkSession
    spark = (SparkSession.builder
             .appName("DoubanMoviesAnalysis")
             .getOrCreate())
    
    # 2. 定位数据文件路径
    # 优先检查容器内路径，其次检查当前路径，再次检查本地Mac下载目录，最后使用OBS占位符
    possible_paths = [
        "/opt/spark/work/douban_movies.csv",
        "douban_movies.csv",
        "/Users/yc/Downloads/douban_movies.csv",
        "/Users/yc/Desktop/云计算技术课设/douban_movies.csv"
    ]
    data_path = None
    for p in possible_paths:
        if os.path.exists(p):
            data_path = p
            break
            
    if not data_path:
        # 如果都没有找到，默认使用 SWR / OBS 里的 s3a 路径（可由用户指定参数传入）
        if len(sys.argv) > 1:
            data_path = sys.argv[1]
        else:
            data_path = "s3a://yangchi-cloud-bucket/douban_movies.csv"
            
    print(f"Using dataset path: {data_path}")
    
    # 3. 加载数据
    t_start_spark = time.time()
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    
    # 【任务 A-1：数据清洗】
    print("================== A-1. 数据清洗 ==================")
    # 打印 Schema
    print("DataFrame Schema:")
    df.printSchema()
    
    # 打印前 5 行
    print("First 5 rows:")
    df.show(5, truncate=30)
    
    # 统计各字段缺失值比例
    total_count = df.count()
    print(f"Total row count before cleaning: {total_count}")
    
    print("Missing value statistics:")
    missing_stats = {}
    for col_name in df.columns:
        # 统计空值或者为空字符串、NaN、Null、\\N等占位符的值
        missing_count = df.filter(
            col(col_name).isNull() | 
            (col(col_name) == "") | 
            (col(col_name).rlike("^(?i)(nan|null|\\\\N)$"))
        ).count()
        ratio = missing_count / total_count
        missing_stats[col_name] = (missing_count, ratio)
        print(f"  Column '{col_name}': missing={missing_count}, ratio={ratio:.4%}")
        
    # 缺失值清洗策略：
    # 策略 1：rating_score (数值型) -> 使用均值填充 (fillna)
    # 策略 2：genres (类别型) -> 直接剔除缺失行 (dropna)，因为类别缺失无法推断
    # 先计算 rating_score 的平均值
    avg_score_row = df.select(mean("rating_score")).collect()
    avg_score = avg_score_row[0][0] if avg_score_row[0][0] else 7.0
    print(f"\nAverage rating_score: {avg_score:.2f}")
    
    # 执行填充与过滤
    df_cleaned = df.fillna({"rating_score": avg_score})
    df_cleaned = df_cleaned.dropna(subset=["genres"])
    
    total_count_cleaned = df_cleaned.count()
    print(f"Total row count after cleaning: {total_count_cleaned}")
    print(f"Removed rows count: {total_count - total_count_cleaned}")
    
    # 输出 rating_score 和 rating_count 字段基本统计信息
    print("\nDescriptive statistics for rating_score and rating_count:")
    df_cleaned.select("rating_score", "rating_count").summary("mean", "stddev", "min", "max").show()
    
    # 注册临时表用于 SQL 统计
    df_cleaned.createOrReplaceTempView("movies")
    
    # 【任务 A-2：Spark SQL 统计分析】
    print("================== A-2. Spark SQL 统计分析 ==================")
    
    # 查询 1：GROUP BY 聚合
    # 统计各个国家/地区的电影均分和电影数量，筛选出电影数量大于等于 50 部的国家，按均分降序排列，取 Top 10
    print("Query 1 (GROUP BY Country Avg Rating):")
    query_1 = """
        SELECT countries, count(*) as movie_count, round(avg(rating_score), 2) as avg_rating
        FROM movies
        GROUP BY countries
        HAVING movie_count >= 50
        ORDER BY avg_rating DESC
        LIMIT 10
    """
    spark.sql(query_1).show()
    
    # 查询 2：ORDER BY Top-N
    # 筛选评分大于等于 9.0 的电影中，按评价人数 (rating_count) 降序排列，获取最热门的 Top 10 电影
    print("Query 2 (ORDER BY Top-N Popular Movies):")
    query_2 = """
        SELECT title, rating_score, rating_count, directors, year
        FROM movies
        WHERE rating_score >= 9.0
        ORDER BY rating_count DESC
        LIMIT 10
    """
    spark.sql(query_2).show()
    
    # 查询 3：时间维度趋势分析
    # 统计 1990 年到 2024 年之间，各年份上映的电影总数和平均评分，按年份升序排列
    print("Query 3 (Time Dimension Trend 1990-2024):")
    query_3 = """
        SELECT cast(year as int) as release_year, count(*) as count, round(avg(rating_score), 2) as avg_rating
        FROM movies
        WHERE year >= 1990 AND year <= 2024
        GROUP BY release_year
        ORDER BY release_year ASC
    """
    spark.sql(query_3).show()
    
    # 查询 4：窗口函数 (Window Function)
    # 按国家/地区分区，在每个国家内部根据电影评分 (rating_score) 从高到低进行排名 (dense_rank)，筛选出各国家评分排名前 3 的电影
    # 为避免结果过多，仅筛选电影总数大于 100 部的国家
    print("Query 4 (Window Function - Top 3 movies per Country):")
    query_4 = """
        WITH ranked_movies AS (
            SELECT title, countries, rating_score, rating_count,
                   DENSE_RANK() OVER (PARTITION BY countries ORDER BY rating_score DESC, rating_count DESC) as rank
            FROM movies
            WHERE countries IN (
                SELECT countries FROM movies GROUP BY countries HAVING count(*) >= 100
            )
        )
        SELECT countries, rank, title, rating_score, rating_count
        FROM ranked_movies
        WHERE rank <= 3
        ORDER BY countries ASC, rank ASC
    """
    spark.sql(query_4).show(30, truncate=20)
    
    t_end_spark = time.time()
    t_spark_total = t_end_spark - t_start_spark
    print(f"PySpark job execution time: {t_spark_total:.4f} seconds")
    
    # 关闭 SparkSession
    spark.stop()

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
import sys

def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    
    # 获取输入文件路径，默认使用 s3a OBS 路径或参数传入，也可以使用本地测试路径
    input_path = "s3a://yangchi-cloud-bucket/sample.txt"
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        
    print(f"Reading text from: {input_path}")
    
    try:
        lines = spark.sparkContext.textFile(input_path)
        word_counts = (
            lines.flatMap(lambda line: line.split())
                 .map(lambda word: (word, 1))
                 .reduceByKey(lambda a, b: a + b)
                 .sortBy(lambda x: x[1], ascending=False)
        )
        print("Top 10 words:")
        for word, count in word_counts.take(10):
            print(f"  {word}: {count}")
    except Exception as e:
        print(f"Error during wordcount job: {e}")
        
    spark.stop()

if __name__ == "__main__":
    main()

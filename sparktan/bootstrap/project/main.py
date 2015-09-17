from pyspark.context import SparkContext, SparkConf

if __name__=="__main__":
    sc = SparkContext()
    logs = sc.textFile('s3n://parselypig/output/processed_logs/blog.parsely.com/2015-09-01/*.gz')
    print logs.count()

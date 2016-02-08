from pyspark.context import SparkContext, SparkConf

if __name__=="__main__":
    sc = SparkContext()
    logs = sc.textFile('s3n://PATH_TO_YOUR_INPUT_DATA.gz')
    print logs.count()

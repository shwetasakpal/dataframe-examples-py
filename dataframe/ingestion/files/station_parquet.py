from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os.path
import yaml

if __name__== '__main__':
    spark = SparkSession\
            .builder\
            .appName("Read parquet file")\
            .config('spark.jars.package','org.apache.hadoop:hadoop-aws:2.7.4')\
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path =os.path.abspath(current_dir+"/../../../"+"application.yml")
    app_secret_path = os.path.abspath(current_dir+"/../../../"+".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf,Loader=yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret =yaml.load(secret,Loader=yaml.FullLoader)

    print("reading parquet file from s3")

    station_df = spark.read \
                .parquet("s3a://"+app_conf["s3_conf"]["s3_bucket"]+"/parquet_1")\
                .repartition(5)

    print("# of records = " + str(station_df.count()))
    print("# of partitions = " + str(station_df.getNumPartitions))
    station_df.printSchema()
    print("Summery of station,")
    station_df.describe().show()

    spark.stop()

# spark-submit --master yarn --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/station_parquet.py


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StructType,BooleanType,DoubleType,StringType
import os.path
import yaml

if __name__ == '__main__':
    spark = SparkSession\
            .builder\
            .appName("Covid Data")\
            .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.4')\
            .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir+"/../../../"+"application.yml")
    app_secrets_path = os.path.abspath(current_dir+"/../../../"+".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf,Loader=yaml.FullLoader)

    secret = open(app_secrets_path)
    app_secret = yaml.load(secret,Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    covid_df = spark.read\
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/Covid")\
                .repartition(2)
    print("# of records = " + str(covid_df.count()))
    print("# of partitions = " + str(covid_df.rdd.getNumPartitions))

    covid_df.printSchema()

    print("Summery of NYC Open Market Order (OMO) charges dataset,")
    covid_df.describe().show()

    spark.stop()

    # spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/Covid_data.py

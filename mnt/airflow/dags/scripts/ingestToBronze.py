from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col,min,max, substring, year, month, dayofmonth
from typing import List
from py4j.java_gateway import java_import
import logging
import argparse

def is_exist_path(spark, path_to_check):
    """
    Check if path is already exist in hdfs or not
    """
    sc = spark.sparkContext

    # get the Hadoop Configuration
    hadoop_conf = sc._jsc.hadoopConfiguration()

    # import the necessary Hadoop classes
    java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(sc._jvm, "org.apache.hadoop.fs.Path")

    # get the Hadoop FileSystem object
    fs = sc._jvm.FileSystem.get(hadoop_conf)

    # create a Path object
    path = sc._jvm.Path(path_to_check)

    # check if the path exists
    if fs.exists(path):
        logging.info(f"Path {path_to_check} exists.")
        return True
    else:
        logging.info(f"Path {path_to_check} does not exist.")
        return False

def main(table_name: str, jdbcHostname: str, jdbcPort: int, jdbcDatabase: str, \
                        jdbcUsername: str, jdbcPassword: str) -> None:
    #create spark session
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    #set up connection to postgres
    jdbcPort = 5432  
    jdbcUrl = f"jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
    connectionProperties = {
        "user": jdbcUsername,
        "password": jdbcPassword,
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000000"
    }

    #get the lastest record from datalake
    
    tblQuery = None
    chunk_size = 1000000
    min_index = 0
    max_index = min_index + chunk_size
    tblLocation = f"/datalake/bronze/{table_name}"

    if is_exist_path(spark, tblLocation):
        df = spark.read.parquet(tblLocation)
        min_index = df.agg(max('index')).collect()[0][0]
        max_index = min_index + chunk_size
        tblQuery = f"(SELECT * \
                      FROM flights 
                      WHERE index BETWEEN {min_index} AND {max_index}) AS tbl \
                    "
    else:
        tblQuery = f"(SELECT * \
                    FROM flights \
                    WHERE index BETWEEN {min_index} AND {max_index}) \
                    AS subquery"

    while True:
        output_df = spark.read.jdbc(url=jdbcUrl, table=tblQuery, properties=connectionProperties)
        
        if output_df.rdd.isEmpty():
            break
        
        output_df = output_df.withColumn("year", year("searchDate")) \
                        .withColumn("month", month("searchDate")) \
                        .withColumn("day", dayofmonth("searchDate"))
        
        #write data to datalake
        output_df.write.partitionBy("year","month","day").mode("append").parquet(tblLocation)

        min_index = max_index
        max_index += chunk_size
    spark.stop()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Ingest data from PostgreSQL to HDFS")
    parser.add_argument("--tblName", help="Name of the Postgres table to ingest data from", required=True)
    parser.add_argument("--jdbcHostname", help="Host name use to connect to postgres", required=True)
    parser.add_argument("--jdbcPort", help="default 5432", required=True)
    parser.add_argument("--jdbcDatabase", required=True)
    parser.add_argument("--jdbcUsername", required=True)
    parser.add_argument("--jdbcPassword", required=True)
    args = parser.parse_args()

    # Call the main function
    main(args.tblName, args.jdbcHostname, args.jdbcPort, args.jdbcDatabase, args.jdbcUsername, args.jdbcPassword)
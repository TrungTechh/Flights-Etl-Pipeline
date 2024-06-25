from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as f
from pyspark.sql.functions import col, substring, year, month, dayofmonth, max, date_format, to_date, trim, udf, \
                            split, from_unixtime, expr, when, array
from pyspark.sql.types import IntegerType, ArrayType, TimestampType
from py4j.java_gateway import java_import

from time import time
import re
from typing import List
from py4j.java_gateway import java_import
from datetime import datetime, date, timedelta
import logging
import argparse

def main(silver_table: str) -> None:
    #create spark session
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    database = "flight_db"
    path = "/datalake/gold/"

    #read data from silver
    flights = spark.read.parquet(f'/datalake/silver/{silver_table}')
    flights = flights.drop('year','month','day')
    flights.persist(StorageLevel.DISK_ONLY)

    #get list of flights handled by just one airline.
    flight_one_airline = flights.filter( f.size(f.array_distinct(f.col("airlineCodeArray"))) == 1 )
    flight_one_airline.persist(StorageLevel.DISK_ONLY)

    #get revenue and seat remain in month
    revenue_n_seat_remain_ym = flight_one_airline.filter(flights['flightDate'] < (date.today() + timedelta(days=1)) ) \
                    .withColumn("year", f.year(f.col("flightDate")) ) \
                    .withColumn("month", f.month(f.col("flightDate")) ) \
                    .withColumn("airline", f.element_at(f.col("airlineCodeArray"), 1) ) \
                    .groupBy("year","month","airline") \
                    .agg( f.sum("totalFare").alias("total_fare"), f.round(f.avg("seatsRemaining"),2).alias("avg_seat_remaining")) \
                    .repartition(10000)
    
    flight_one_airline.unpersist()

    #save and unpersist
    revenue_n_seat_remain_ym.persist(StorageLevel.MEMORY_AND_DISK)
    revenue_n_seat_remain_ym.write.partitionBy("month").option("path", path+"revenue_n_seat_remain_ym") \
                            .mode("append").saveAsTable(database+ ".revenue_n_seat_remain_ym")
    revenue_n_seat_remain_ym.unpersist()

    #get relation between fare basic code and travel duration
    fbc_travel_duration_relation = flights.groupBy("fareBasisCode").agg( f.round(f.avg("travelDuration"),2).alias("avg_duration")).repartition(10000)

    #save and unpersist
    fbc_travel_duration_relation.persist(StorageLevel.MEMORY_AND_DISK)
    fbc_travel_duration_relation.write.option("path", path+"fbc_travel_duration_relation") \
                            .mode("append").saveAsTable(database+ ".fbc_travel_duration_relation")
    fbc_travel_duration_relation.unpersist()
    flights.unpersist()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Deliver silver table to gold table")
    parser.add_argument("--silverTableName", help="Name of Silver table", required=True)
    args = parser.parse_args()

    # Call the main function
    main(args.silverTableName)
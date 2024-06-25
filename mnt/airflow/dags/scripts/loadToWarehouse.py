from pyspark.sql import SparkSession
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

from ingestToBronze import is_exist_path

def main(table_name: str) -> None:
    #create spark session
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    big_table = spark.read.parquet(f'/datalake/silver/{table_name}')

    #Create dimension date table
    dim_date_path = '/warehouse/dim_date'
    distinct_dates = big_table.select(f.explode(f.array_distinct(f.array("searchDate", "flightDate"))).alias("day")).distinct()#get all date values
    if is_exist_path(spark, dim_date_path):
        dim_date = spark.read.parquet(dim_date_path).repartition(10000, col("date"))
        #dim_date right outer join with distinct_dates to avoid get the exist date in dimension date table
        dim_date = dim_date.join(distinct_dates, dim_date["day"] == distinct_dates["day"], "right") \
                            .filter(dim_date["day"].isNull())
    else:
        dim_date = distinct_dates.withColumn("year", f.date_format(col("date"), "yyyy").cast("int")) \
                            .withColumn("month", f.date_format(col("date"), "MM").cast("int")) \
                            .withColumn("day", f.date_format(col("date"), "DD").cast("int")) \
                            .withColumn("quarter", f.quarter(col("date"))) \
                            .withColumn("month_name", f.date_format(col("date"), "MMMM")) \
                            .withColumn("day_of_week_number", f.date_format(col("date"), "u").cast("int")) \
                            .withColumn("day_of_week_name", f.date_format(col("date"), "EEEE"))

    #Save dimension date table to hdfs and hive metastore
    spark.conf.set("spark.unsafe.sorter.spill.read.ahead.enabled", "false")
    dim_date.write.partitionBy("year", "month", "day").option("path", "/warehouse/dim_date") \
                                .mode("append").saveAsTable("warehouse_db.date_dim")
    
    #Create dimension airline table
    combined_airline_df = big_table.select(f.arrays_zip("airlineCodeArray", "airlineNameArray").alias("combined")) #explode the airline array
    exploded_airline_df = combined_airline_df.select(f.explode("combined").alias("exploded"))
    dim_airline = exploded_airline_df.select(
                    col("exploded.airlineCodeArray").alias("code"),
                    col("exploded.airlineNameArray").alias("name")
                ).distinct()
    
    #Save dimension airline table to hdfs and hive metastore
    dim_airline_path = '/warehouse/dim_airline'
    dim_airline.write.option("path", dim_airline_path) \
                    .mode("append").saveAsTable("warehouse_db.dim_airline")
    
    #Do samething at airport table
    combined_airport_df = big_table.select(f.arrays_zip("airportCodeArray", "airportNameArray").alias("combined")) #explode the airport array
    exploded_airport_df = combined_airport_df.select(f.explode("combined").alias("exploded"))
    dim_airport = exploded_airport_df.select(
                    col("exploded.airportCodeArray").alias("code"),
                    col("exploded.airportNameArray").alias("name")
                ).distinct()
    dim_airport_path = '/warehouse/dim_airport'
    dim_airport.write.option("path", dim_airport_path) \
                    .mode("append").saveAsTable("warehouse_db.dim_airport")
    
    #remove all array type columns and add count_segments column
    non_array_columns = [field.name for field in big_table.schema.fields if not isinstance(field.dataType, ArrayType)]
    fact_flight_activites = big_table.withColumn("count_segments", f.size(big_table["arrivalAirportArray"])) \
                                    .select([col(column) for column in non_array_columns])
    #save fact table
    fact_path = '/warehouse/fact_flight_activites'
    fact_flight_activites.write.partitionBy("year","month","day").option("path", fact_path) \
                    .mode("append").saveAsTable("warehouse_db.fact_flight_activites")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description=" Data modeling transformed data in silver")
    parser.add_argument("--tblName", help="Name of silver table", required=True)
    args = parser.parse_args()

    # Call the main function
    main(args.tblName)
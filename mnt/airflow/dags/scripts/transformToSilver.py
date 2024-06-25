from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, substring, year, month, dayofmonth, max, date_format, to_date, trim, udf, \
                            split, from_unixtime, expr, when, array
from pyspark.sql.types import IntegerType, ArrayType, TimestampType
from py4j.java_gateway import java_import

from time import time
import re
from typing import List
from py4j.java_gateway import java_import
from datetime import datetime
import logging
import argparse

from ingestToBronze import is_exist_path

def duration_to_minutes(duration: str) -> int:
    """
    convert ISO81 string to total minutes
    """
    match = re.match(r'PT(\d+H)?(\d+M)?', duration)
    if not match:
        return None
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    return hours * 60 + minutes

def iso_to_timestamp(iso_string: str) -> datetime:
    """
    Convert ISO 8601 string to timestamp
    """
    return datetime.fromisoformat(iso_string.replace("Z", "+00:00"))


def main(table_name: str) -> None:
    spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())
    df = spark.read.parquet(f'/datalake/bronze/{table_name}')

    #convert to string to date and 
    df = df.withColumn("searchDate", to_date(col("searchDate"), "yyyy-MM-dd")) \
            .withColumn("flightDate", to_date(col("flightDate"), "yyyy-MM-dd")) \
            .withColumn("startingAirport", trim(col("startingAirport"))) \
            .withColumn("destinationAirport", trim(col("destinationAirport"))) \
            .withColumn("fareBasisCode", trim(col("fareBasisCode")))

    #convert travelDuration to total of minutes
    duration_to_minutes_udf = udf(duration_to_minutes, IntegerType())
    df = df.withColumn("travelDuration", duration_to_minutes_udf(df["travelDuration"]))

    #filter out rows where baseFare is greater than totalFare and set remaining lesser than 0
    df = df.filter((df["baseFare"] <= df["totalFare"]) & (df["seatsRemaining"] >= 0))

    #convert segment columns to array type
    def split_to_array(df,new_column, old_column):
        new_df = df.withColumn(new_column, 
                    when(df[old_column].isNull(), array())
                    .otherwise(split(col(old_column), r'\|\|')))
        return new_df
    
    # Split the column into an array of strings
    df = (
        split_to_array(df, "arrivalTimeArray", "segmentsArrivalTimeRaw")
        .transform(lambda df: split_to_array(df, "departureTimeArray", "segmentsDepartureTimeRaw"))
        .transform(lambda df: split_to_array(df, "arrivalAirportArray", "segmentsArrivalAirportCode"))
        .transform(lambda df: split_to_array(df, "departureAirportArray", "segmentsDepartureAirportCode"))
        .transform(lambda df: split_to_array(df, "airlineCodeArray", "segmentsAirlineCode"))
        .transform(lambda df: split_to_array(df, "airlineNameArray", "segmentsAirlineName"))
        .transform(lambda df: split_to_array(df, "equipDescriptionArray", "segmentsEquipmentDescription"))
        .transform(lambda df: split_to_array(df, "CabinCodeArray", "segmentsCabinCode"))
    )

    # Persist the DataFrame after initial transformations
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    #Modify each element of the array to a timestamp
    iso_to_timestamp_udf = udf(iso_to_timestamp, TimestampType())
    df = df.withColumn("arrivalTimeArray", expr("transform(arrivalTimeArray, x -> iso_to_timestamp_udf(x))")) \
            .withColumn("departureTimeArray", expr("transform(departureTimeArray, x -> iso_to_timestamp_udf(x))"))

    #Column have array<int> type will replace 0 will null
    replace_and_convert_second_udf = \
        expr("transform(durationSecondsArray, x -> if(x is null or trim(x) in ('None', 'null'), 0, cast(x as int)))")
    df = split_to_array(df, "durationSecondsArray", "segmentsDurationInSeconds")
    df = df.withColumn("durationSecondsArray", replace_and_convert_second_udf)
    replace_and_convert_distance_udf = \
        expr("transform(distanceArray, x -> if(x is null or trim(x) in ('None', 'null'), 0, cast(x as int)))")
    df = split_to_array(df, "distanceArray", "segmentsDistance")
    df = df.withColumn("distanceArray", replace_and_convert_distance_udf)

    #drop old segment columns
    segment_columns = [col_name for col_name in df.columns if col_name.startswith("segment")]
    df = df.drop(*segment_columns)

    # Persist the DataFrame after additional transformations
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    #Incremental load to silver
    offset = 0
    chunk_size = 1000000
    min_index = df.agg(min(df.index)).collect()[0][0]
    max_index = min_index + chunk_size

    tblQuery = f"SELECT * \
                FROM {table_name} \
                WHERE index BETWEEN {min_index-1} AND {max_index}"
    df.createOrReplaceTempView(table_name)

    while True:
        start = time.time()
        output_df = spark.sql(tblQuery)

        if output_df.rdd.isEmpty():
            break

        #write data to datalake
        output_df.write.partitionBy("year","month","day").mode("append").parquet("/datalake/silver/flights")
        logging.info("It tooks " + str(time.time() - start) +" seconds to insert another chunks")

        min_index = max_index + 1
        max_index = max_index + chunk_size
    
    #clear
    spark.catalog.dropTempView(table_name)
    df.unpersist()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Transform data from bronze to silver")
    parser.add_argument("--tblName", help="Name of the bronze table to transform", required=True)
    args = parser.parse_args()

    # Call the main function
    main(args.tblName)
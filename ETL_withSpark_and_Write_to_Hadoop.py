#!/usr/bin/env python
"""Perform ETL with Spark and Save the Spark dataframe into HDFS
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re
# from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import warnings
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

""" focused_tweets 
root
 |-- data: struct (nullable = true)
 |    |-- author_id: string (nullable = true)
 |    |-- created_at: string (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- text: string (nullable = true)
 |-- public_metrics: struct (nullable = true)
 |    |-- retweet_count: integer (nullable = true)
 |    |-- reply_count: integer (nullable = true)
 |    |-- like_count: integer (nullable = true)
 |    |-- quote_count: integer (nullable = true)
 |-- includes: struct (nullable = true)
 |    |-- users: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- location: string (nullable = true)
 |    |    |    |-- username: string (nullable = true)
 |-- matching_rules: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- tag: string (nullable = true)
"""
######################################################################################################

def regexp_extract_all(my_str, re_pattern):
    re_pattern = re.compile(re_pattern, re.M)
    all_matches = re.findall(re_pattern, my_str)
    return all_matches

def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("TwitterStreamClimateChange") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "climateChange") \
        .option("multiline", "true") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    str_climate_change = raw_events.select(raw_events.value.cast('string'))
    
    ############ START Update ################################################################################

    # added - write raw file
    str_climate_change.write.mode("overwrite").parquet("/tmp/raw_tweets")
    # added - read the raw file
    read_raw_climate = spark.read.parquet('/tmp/raw_tweets')
    
    # added - create a forced schema
    final_schema_new = StructType([
                           StructField('data', StructType([
                               StructField('author_id', StringType(), True),
                               StructField('created_at', StringType(), True),
                               StructField('id', StringType(), True),
                               StructField('text', StringType(), True),
                               StructField('public_metrics', 
                                   StructType([
                                       StructField('retweet_count', IntegerType(), True),
                                       StructField('reply_count', IntegerType(), True),
                                       StructField('like_count', IntegerType(), True),
                                       StructField('quote_count', IntegerType(), True)]))])),    
                           StructField('includes', StructType([
                               StructField('users', ArrayType(
                                   StructType([
                                       StructField('id', StringType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('location', StringType(), True),
                                       StructField('username', StringType(), True)])))])),
                           StructField('matching_rules', ArrayType(
                               StructType([
                                   StructField('id', StringType(), True),
                                   StructField('tag', StringType(), True)])))
                        ])
                         
    # added - map to the schema
    full_climate_new = read_raw_climate.rdd.map(lambda x: json.loads(x.value)).toDF(schema=final_schema_new)
    full_climate_new.printSchema()
    full_climate_new.show(10)

    # added - register temp table of all the climate data                         
    full_climate_new.registerTempTable("full_climate_forquery")
    # added - write forced schema table to parquet
    full_climate_forced = spark.sql(
        "select \
        data.id as tweet_id, \
        data.author_id, \
        data.created_at as tweet_created_at, \
        data.public_metrics.retweet_count, \
        data.public_metrics.reply_count, \
        data.public_metrics.like_count, \
        includes.users.location as user_location, \
        includes.users.name as user_fullname, \
        includes.users.username, \
        data.text as tweet_text \
        from full_climate_forquery")
    
    full_climate_forced.printSchema()
    full_climate_forced.show(10)
    
    ############ END Update ################################################################################
    
    pattern_hashtag = '(?<=^|(?<=[^a-zA-Z0-9-_\\\\.]))#([A-Za-z]+[A-Za-z0-9_]+)'
    pattern_url = '(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+'    
    
    udf_regexp_extract_all = udf(regexp_extract_all, ArrayType(StringType()))
    focused_tweets = full_climate_forced.withColumn('hashtag_name', udf_regexp_extract_all('tweet_text', lit(pattern_hashtag)))
    focused_tweets = focused_tweets.withColumn('url', udf_regexp_extract_all('tweet_text', lit(pattern_url)))
    focused_tweets.printSchema()
    focused_tweets.show(10)
    
# after stop reading tweets from TwitterAPI by ctrl+c and re-read tweets continuously, the 1st batch of data was appended with new batch data, which resulted in duplicates of 1st batch data so we decided to go with "overwrite" the parquet

#     focused_tweets.write.mode("append").parquet("/tmp/focused_tweets")   
    focused_tweets.write.mode("overwrite").parquet("/tmp/focused_tweets") 
    

if __name__ == "__main__":
    main()

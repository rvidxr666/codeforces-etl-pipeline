import argparse
import datetime as dt
import json
import sys
import time

import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (DecimalType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

# CLI args parser
parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--db-hostname", help="hostname of the database")
parser.add_argument("--sql-conn-version", help="sql connection version")
parser.add_argument("--dw-port-number", help="sql connection version")


db_hostname = vars(parser.parse_args()).get("db_hostname", None).strip()
db_hostname = "localhost" if not db_hostname else db_hostname

sql_conn_version = vars(parser.parse_args()).get("sql_conn_version", None).strip()
sql_conn_version = "8.0.32" if not sql_conn_version else sql_conn_version

dw_port_number = vars(parser.parse_args()).get("dw_port_number", None).strip()
dw_port_number = "3307" if not dw_port_number else dw_port_number

print("db_hostname: ", db_hostname)
print("sql_conn_version: ", sql_conn_version)
print("dw_port_number: ",dw_port_number )

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .master("local[*]")\
           .config("spark.jars", f"/usr/share/java/mysql-connector-j-{sql_conn_version}.jar")\
           .getOrCreate()


# Schemas
contest_schema = StructType([
                                StructField('id', IntegerType(), False),
                                StructField('name', StringType(), False),
                                StructField('start', TimestampType(), True),
                                StructField('duration', IntegerType(), True),
                                StructField('type', StringType(), True)
                            ])

load_submissions_schema = StructType([
                                StructField('id', IntegerType(), False),
                                StructField('timestamp', TimestampType(), False),
                                StructField('id_contest', IntegerType(), False),
                                StructField('id_problem', StringType(), False),
                                StructField('problem_name', StringType(), False),
                                StructField('tags', StringType(), False),
                                StructField('author', StringType(), False),
                                StructField('programming_language', StringType(), False),
                                StructField('verdict', StringType(), False),
                                StructField('time_consumed', IntegerType(), False),
                                StructField('memory_usage', IntegerType(), False),
                            ])

programming_languages_schema =  StructType([
                                    StructField('id', IntegerType(), False),
                                    StructField('name', StringType(), False),
                                ])

user_schema = StructType([  
                            StructField('country', StringType(), False),
                            StructField('rating', IntegerType(), False),
                            StructField('nickname', StringType(), False),
                            StructField('title', StringType(), False),
                            StructField('registration_date', TimestampType(), False),
                        ])



def query_api(url, headers = {}, **params):
    if params:
        url = url + "?"

        for param, value in params.items():
            url += param + "=" + value + "&" 

    resp = requests.get(url=url, headers=headers)
    return resp


def process_response(response):
    print(response.status_code)
    print(response.reason)

    converted_response = json.loads(response.content.decode("utf-8")) 
    return converted_response


def translate_unix_to_timestamp(unix_time):
    timestamp = dt.datetime.fromtimestamp(unix_time)
    return timestamp


def write_to_db(df, mode, table_name):
    df.write \
        .format("jdbc") \
        .mode(mode)\
        .option("truncate", "true")\
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://{db_hostname}:{dw_port_number}/dt_warehouse") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "password") \
        .save()


def read_from_db(query):
        
        df = spark.read\
            .format("jdbc") \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("url", f"jdbc:mysql://{db_hostname}:{dw_port_number}/dt_warehouse") \
            .option("query", query) \
            .option("user", "root") \
            .option("password", "password") \
            .load()

        return df


def loading_contests(contests, initial = False, amount_of_contests = 40):
    lst_of_rows = [
                    (
                        contest["id"], 
                        contest["name"], 
                        translate_unix_to_timestamp(contest["startTimeSeconds"]), 
                        contest["durationSeconds"], 
                        contest["type"]
                    )
                    for contest in contests[:amount_of_contests] if contest["phase"] == "FINISHED"
                ]
            
    contest_df = spark.createDataFrame(lst_of_rows, schema=contest_schema)
    write_to_db(df=contest_df, mode="overwrite", table_name = "contest_stg")
    
    return [row.id for row in contest_df.select("id").distinct().collect()]


def extract_submissions(contest_ids):

    for i, contest_id in enumerate(contest_ids):        
        sumbissions = establish_connection("https://codeforces.com/api/contest.status", contestId=str(contest_id))
        contest_sumbissions = [
                                    (
                                    subm["id"], 
                                    translate_unix_to_timestamp(subm["creationTimeSeconds"]), 
                                    subm["contestId"],
                                    str(subm["problem"]["contestId"]) + "/" + str(subm["problem"]["index"]),
                                    subm["problem"]["name"],
                                    ",".join(subm["problem"]["tags"]), 
                                    subm["author"]["members"][0].get("handle", "unknown") if subm["author"]["members"][0] else "unknown",
                                    subm["programmingLanguage"], 
                                    subm["verdict"],
                                    subm["timeConsumedMillis"],
                                    subm["memoryConsumedBytes"]
                                    )  

                                    for subm in sumbissions
                            ]
        print("curr contest_id", contest_id)
        curr_subm_df = spark.createDataFrame(contest_sumbissions, schema = load_submissions_schema)

        mode = "overwrite" if i == 0 else "append"

        write_to_db(df=curr_subm_df, mode=mode, table_name="load_submissions")

        print("contest_id: ", contest_id, " loaded!")

    
    print("Loaded Submissions")

def handle_error_response(response, api_params):
    if "handles: User with handle" in response.get("comment"):
        users = api_params["handles"]\
                          .replace("&", "")\
                          .split(";")
        
        comment_as_list = response.get("comment").split(" ")
        incorrect_user = comment_as_list[comment_as_list.index("handle") + 1]

        users.remove(incorrect_user)
        # sys.exit(0)
        return {"handles": ";".join(users)}
    else:
        print(f"Action on the following response {response} is not implemented yet!")
        sys.exit(1)


def establish_connection(url, **api_params):
    while True:
        try:
            response = process_response(query_api(url, **api_params))
            if response and "result" not in response:
                api_params = handle_error_response(response, api_params)
                continue
            else:
                entities = response["result"]

            return entities
        except json.decoder.JSONDecodeError as e:
            time.sleep(3)
            print(f"{e} error occured")
            continue
        except requests.exceptions.ConnectionError as e:
            time.sleep(3)
            print(f"{e} error occured")
            continue


def load_dimensions():
    # Load problems
    problems_df = read_from_db("SELECT DISTINCT id_problem, problem_name AS name, tags FROM load_submissions")
    write_to_db(df=problems_df, mode="overwrite", table_name="problem_stg")

    # Load programming languages
    programming_languages_df = read_from_db("SELECT DISTINCT programming_language FROM load_submissions")
    prog_lang_prepared = programming_languages_df.withColumn("id", lit(0))\
                                                 .select("id", col("programming_language").alias("name"))
    write_to_db(df=prog_lang_prepared, mode="overwrite", table_name="programming_language_stg")

    # Load verdicts
    verdicts_df = read_from_db("SELECT DISTINCT verdict FROM load_submissions")
    verdicts_prepared = verdicts_df.withColumn("id", lit(0))\
                                                 .select("id", col("verdict").alias("name"))
    write_to_db(df=verdicts_prepared, mode="overwrite", table_name="verdict_stg")


def load_users():
    unique_names = [row.author for row in read_from_db("SELECT DISTINCT author FROM load_submissions").collect()]
    print("Entering the load users function")

    rows = []
    for i, name in enumerate(unique_names):
       
       rows.append(name)

       if (i!=0 and i%500== 0) or (i == len(unique_names)-1):
            unique_names_string = ";".join(rows)
            print("Sleeping....")
            time.sleep(1)
            raw_rows = establish_connection("https://codeforces.com/api/user.info", handles=unique_names_string)
            rows = []

            unpacked_rows = [
                (
                    raw_row.get("country", "unknown"),
                    raw_row.get("rating", 0),
                    raw_row["handle"],
                    raw_row.get("rank", "none"),
                    translate_unix_to_timestamp(raw_row["registrationTimeSeconds"])
                )
                for raw_row in raw_rows
                ]
            
            mode = "overwrite" if i == 500 else "append"
            df_users = spark.createDataFrame(unpacked_rows, schema=user_schema).withColumn("id", lit(0))
            write_to_db(df=df_users, mode=mode, table_name="user_stg")



if __name__ == "__main__":

    # Current time before the job
    curr_time_before = dt.datetime.now()

    # Extracting last contests
    last_contests = establish_connection("https://codeforces.com/api/contest.list", gym="false")

    # Load Contests dim table
    contest_ids = loading_contests(last_contests, initial=True)

    # Load Submissions (Facts) to the temp table
    extract_submissions(contest_ids)

    # Load dimensional tables (Problem, Verdict, Programming Language)
    load_dimensions()

    # Load Users Dimensional Table
    load_users()

    # Current time after the job
    curr_time_after = dt.datetime.now()

    # print(dt.datetime.combine(dt.datetime.date.min, curr_time_after) - dt.datetime.combine(dt.datetime.date.min, curr_time_before))







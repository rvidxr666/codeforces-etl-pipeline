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
import pymysql
import logging
import traceback



### Global vars
global initial
datetime_format = "%Y-%m-%d %H:%M:%S"
date_format = "%Y-%m-%d"



# CLI args parser
parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--db-hostname", help="hostname of the database")
parser.add_argument("--db-name", help="name of the database")
parser.add_argument("--sql-conn-version", help="sql connection version")
parser.add_argument("--dw-port-number", help="sql connection version")
parser.add_argument("--dates", help="Dates from which to query and load submissions")
parser.add_argument("--count-init-contests", help="Amount of contests that should be processed during the initial run", default="")


parser_args_dict = vars(parser.parse_args())

db_hostname = vars(parser.parse_args()).get("db_hostname", "localhost")
db_hostname = "localhost" if db_hostname is None else db_hostname

db_name = vars(parser.parse_args()).get("db_name", "dt_warehouse" )
db_name = "dt_warehouse" if not db_name else db_name

sql_conn_version = vars(parser.parse_args()).get("sql_conn_version", "8.0.32")
sql_conn_version = "8.0.32" if not sql_conn_version else sql_conn_version

dw_port_number = vars(parser.parse_args()).get("dw_port_number", 3307)
dw_port_number = 3307 if not dw_port_number else int(dw_port_number)

dates = parser_args_dict.get("dates", "")

count_init_contests = parser_args_dict.get("count_init_contests", "")
count_init_contests = int(count_init_contests) if count_init_contests.isdigit() else 40

print("db_hostname: ", db_hostname)
print("sql_conn_version: ", sql_conn_version)
print("dw_port_number: ",dw_port_number )
print("dates: ",dates)

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .master("local[*]")\
           .config("spark.jars", f"/usr/share/java/mysql-connector-j-{sql_conn_version}.jar")\
           .getOrCreate()

# Create a direct MySQL connection
mysql_connection = pymysql.connect(
    host=db_hostname,
    user="root",
    password="password",
    database=db_name,
    port=dw_port_number
)


# Schemas
contest_schema = StructType([
                                StructField('id', IntegerType(), False),
                                StructField('name', StringType(), False),
                                StructField('start', TimestampType(), True),
                                StructField('duration', IntegerType(), True),
                                StructField('end', TimestampType(), True),
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

def load_last_execution_date() -> dt.date:
    global initial
    last_execution_date = None
    last_execution_date_row = read_from_db("SELECT * FROM last_execution") \
                                            .select("last_execution_date") \
                                            .first()
    if last_execution_date_row:
        initial = False
        last_execution_date = last_execution_date_row[0]

    return last_execution_date

def parse_dates() -> (dt.date | None, dt.date | None):

    if not dates:
        return None, None

    date_str_split = dates.split(":")
    if len(date_str_split) < 2:
        return None, None

    try:
        from_date = dt.datetime.strptime(date_str_split[0], date_format).date()
        to_date = dt.datetime.strptime(date_str_split[1], date_format).date()
    except ValueError as ve:
        print(f"Wasn't able to get the dates provided because of the following error -> {str(ve)}")
        return None, None

    if from_date > to_date:
        return to_date, from_date

    return from_date, to_date



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

def execute_sql(sql_statement: str):
    try:
        with mysql_connection.cursor() as cursor:
            cursor.execute(sql_statement)
            mysql_connection.commit()
    except Exception as e:
        print(f"Something went wrong! Exception -> {str(e)}")


def loading_contests(
        contests,
        initial = True,
        from_date = None,
        to_date = None,
        amount_of_contests = 40
):

    lst_of_rows = []

    # Initial Run
    if initial and from_date is None:
        cnt_processed_contests = 0
        for contest in contests:
            if cnt_processed_contests == amount_of_contests:
                break
            if contest["phase"] == "FINISHED" and \
               translate_unix_to_timestamp(contest["startTimeSeconds"] + contest["durationSeconds"]).date() < to_date:
               lst_of_rows.append(
                    (
                        contest["id"],
                        contest["name"],
                        translate_unix_to_timestamp(contest["startTimeSeconds"]),
                        contest["durationSeconds"],
                        translate_unix_to_timestamp(contest["startTimeSeconds"] + contest["durationSeconds"]),
                        contest["type"]
                    )
                )
               cnt_processed_contests += 1

    # Manual Parameters provided
    elif from_date is not None and to_date is not None:
        for contest in contests:
            contest_start_time = translate_unix_to_timestamp(contest["startTimeSeconds"])
            contest_end_time = translate_unix_to_timestamp(contest["startTimeSeconds"] + contest["durationSeconds"])
            if (
                    from_date <= contest_end_time.date() < to_date
                    and contest["phase"] == "FINISHED"
            ):
                lst_of_rows.append(
                    (
                        contest["id"],
                        contest["name"],
                        contest_start_time,
                        contest["durationSeconds"],
                        contest_end_time,
                        contest["type"]
                    )
                )

    # Run from previous execution date until today
    # elif not initial and from_date is not None:
    #     for contest in contests:
    #         contest_start_time = translate_unix_to_timestamp(contest["startTimeSeconds"])
    #         if contest_start_time.date() >= from_date and contest["phase"] == "FINISHED":
    #             lst_of_rows.append(
    #                 (
    #                     contest["id"],
    #                     contest["name"],
    #                     contest_start_time,
    #                     contest["durationSeconds"],
    #                     contest["type"]
    #                 )
    #             )
    else:
        raise Exception("Incorrect combination of attributes provided for loading contests!")

            
    contest_df = spark.createDataFrame(lst_of_rows, schema=contest_schema)
    write_to_db(df=contest_df, mode="overwrite", table_name = "contest_stg")
    
    return [row.id for row in contest_df.select("id").distinct().collect()]


def extract_submissions(contest_ids):


    if not contest_ids:
        subm_df = spark.createDataFrame((), schema = load_submissions_schema)
        write_to_db(df=subm_df, mode="overwrite", table_name="load_submissions")
        return

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
                                        subm["author"]["members"][0].get("handle", "unknown") if subm["author"]["members"] else "unknown",
                                        subm["programmingLanguage"],
                                        subm["verdict"],
                                        subm["timeConsumedMillis"],
                                        subm["memoryConsumedBytes"]
                                    )
                                    for subm in sumbissions if subm["author"]["participantType"] == "CONTESTANT"
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
    exception = None
    retry = 0
    while retry < 3:
        api_response = ""
        try:
            logging.info("Establishing connection to the API!")
            api_response = query_api(url, **api_params)
            response = process_response(api_response)
            if response and "result" not in response:
                api_params = handle_error_response(response, api_params)
                continue
            else:
                entities = response["result"]

            return entities
        except json.decoder.JSONDecodeError as e:
            exception = e
            time.sleep(15)
            logging.error(f"{e} error occured")
            logging.error(f"Response from the API -> {api_response.text}")
            retry += 1
            continue
        except requests.exceptions.ConnectionError as e:
            exception = e
            time.sleep(15)
            logging.error(f"{e} error occured")
            logging.error(f"Response from the API -> {api_response.text}")
            retry += 1
            continue
        except Exception as e:
            exception = e
            time.sleep(15)
            logging.error(f"{e} error occured")
            logging.error(f"Response from the API -> {api_response.text}")
            retry += 1
            continue

    if retry == 3:
        raise exception


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
    execute_sql("TRUNCATE TABLE user_stg")
    print("Entering the load users function")

    rows = []
    for i, name in enumerate(unique_names):
       
       rows.append(name)

       if (i!=0 and i%500== 0) or (i == len(unique_names)-1):
            unique_names_string = ";".join(rows)
            # print("Sleeping....")
            # time.sleep(1)
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
            
            # mode = "overwrite" if i == 500 or i == len(unique_names)-1 else "append"
            df_users = spark.createDataFrame(unpacked_rows, schema=user_schema).withColumn("id", lit(0))
            write_to_db(df=df_users, mode="append", table_name="user_stg")



if __name__ == "__main__":

    try:
        print("Starting the execution of the job!")

        # Job's execution date
        curr_date = dt.date.today()

        # Check the type of run
        from_date = load_last_execution_date()
        to_date = curr_date
        initial = False if from_date is not None else True

        # Parse 'dates' cli flag
        provided_dates = parse_dates()

        if all(provided_dates):
            from_date = provided_dates[0]
            to_date = provided_dates[1]
            # initial = False

        print(f"Last job execution date -> {from_date.strftime(date_format)}") \
            if from_date is not None else None

        # Extracting last contests
        last_contests = establish_connection("https://codeforces.com/api/contest.list", gym="false")

        # Load Contests dim table
        contest_ids = loading_contests(last_contests, initial, from_date, to_date, count_init_contests)

        # Load Submissions (Facts) to the temp table
        extract_submissions(contest_ids)

        # Load dimensional tables (Problem, Verdict, Programming Language)
        load_dimensions()

        # Load Users Dimensional Table
        load_users()

        if not initial:
            # Execute the SQL statement to update the last execution date in the table in the DB
            execute_sql(f"UPDATE last_execution SET last_execution_date = '{to_date.strftime(date_format)}'")

        if initial:
            # Execute the SQL statement to input the last execution date to the table in the DB
            execute_sql(f"INSERT INTO last_execution VALUES ('{to_date.strftime(date_format)}')")



    except Exception as e:
        logging.error(f"The following exception has occurred -> {str(e)}")
        logging.error(traceback.format_exc())
        raise e

    # print(dt.datetime.combine(dt.datetime.date.min, curr_time_after) - dt.datetime.combine(dt.datetime.date.min, curr_time_before))

# For testing
# if __name__ == "__main__":
#     curr_time_after = dt.datetime.now()
#     print(f"UPDATE last_execution SET last_execution_timestamp = '{curr_time_after.strftime(date_format)}'")






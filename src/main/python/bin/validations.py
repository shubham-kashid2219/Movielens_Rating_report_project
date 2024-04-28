import logging
import logging.config
import os

# Loading the logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

# Creating function to validate spark object by printing current date
def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date""")
        logger.info("Validate the spark object by printing Current Date " + str(opDF.collect()))
    except Exception as exp:
        logger.error("Error in method - get_curr_date(). Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark obect is validated. Spark object is ready.")

def df_count(df, dfName):
    try:
        logger.info(f"The DataFrame validation by counting records using df.count() is started for DataFrame {dfName}.")
        df_count = df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the mothod df_count(). Please check the stack Trace." + str(exp))
    else:
        logger.info(f"The validation of DataFrame {dfName} by counting records using df_count() is completed. \n")

def df_top10_rec(df,dfName):
    try:
        logger.info(f"The DataFrame validation by fetching top 10 records using df.top10_rec() is started for DataFrame {dfName}.")
        logger.info(f"The DataFrame top 10 records are: ")
        # Fetching top 10 records from dataframe and then converting into pandas dataframe coz logging is
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in the method df_top10_rec(). Please check the stack trace. " + str(exp))
        raise
    else:
        logger.info(f"The validation of DataFrame {dfName} by fetching top 10 records using df_to10_rec() is completed. \n")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The DataFrame validation by printing schema of DataFrame {dfName} is started.")
        sch = df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is:")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in method - df_print_schema(). Please check the stack trace. " + str(exp))
        raise
    else:
        logger.info(f"The schema validation of DataFrame {dfName} is completed. \n")

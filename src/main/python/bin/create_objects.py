from pyspark.sql import SparkSession
import logging
import logging.config

# Loading the logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() method is started. The '{envn}' is used.")
        if envn == "TEST":
            master = "local"
        else:
            master = "yarn"
        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the stack trace." + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark object is created successfully. \n")
    return spark

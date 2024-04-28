import logging
import logging.config

# Loading logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")

logger = logging.getLogger(__name__)


def extract_files(spark, file_dir, file_format, sep, schema, header, inferSchema, mode):
    try:
        logger.info("The extract_file() function is started.")
        df = spark.read. \
            option('sep', sep). \
            format(file_format). \
            option("mode", mode). \
            schema(schema). \
            option("header", header). \
            option("inferschema", inferSchema). \
            load(file_dir)
    except Exception as exp:
        logger.error("Error in method - extract_files(). Please check stack trace." + str(exp), exc_info=True)
    else:
        logger.info(f"The input file {file_dir} is loaded to dataframe. The extract_files() function is completed. \n")
    return df






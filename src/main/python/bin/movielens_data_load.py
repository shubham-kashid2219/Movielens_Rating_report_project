import logging
import logging.config
import os

# Loading logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)


def load_file(df, format, filePath, split_no, headerReq, compressionType, partition_column, mode):
    try:
        logger.info(" Loading - load_file() is started.")
        if format == "parquet":
            df.write \
                .format(format) \
                .mode(mode) \
                .partitionBy(partition_column) \
                .save(filePath)
            for i in os.listdir(filePath):
                logger.info(i)

        elif format == "csv":
            df.coalesce(split_no) \
                .write \
                .mode(mode) \
                .format(format) \
                .save(filePath, header=headerReq, compression=compressionType)

    except Exception as exp:
        logger.error("Error in method - load_file(). Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Loading - load_file() is completed. \n")
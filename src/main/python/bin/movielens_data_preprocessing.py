import logging
import logging.config
from pyspark.sql.functions import regexp_extract, trim, col
from pyspark.sql.types import IntegerType, FloatType

# Loading logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)
def perform_data_preprocessing(df1, df2, df3):
    try:
        logger.info(f"Function perform_data_preprocessing() is started.")
        # Extract the year from the "Title" column of the dataframe
        year_pattern = r"\((\d{4})\)"
        movies_df_clean = df1.withColumn("ReleaseYear", regexp_extract("Title", year_pattern, 1).cast(IntegerType())) \
                     .withColumn("Title", trim(col("Title")))

        # Filtering only movies, which were released after 1989.
        movies_df_clean = movies_df_clean.filter(col("ReleaseYear") > 1989)
        if (movies_df_clean.filter(col("ReleaseYear").isNull()).count()) == 0:
            logger.info("movies_df dataframe is cleaned. And ready to use in further transformation.")
        else:
            raise ValueError(" Column 'ReleaseYear' contains null values.")
        # Considering the ratings of all persons aged 18-49 years.
        users_df_clean = df2.filter((col("Age") >= 18) & (col("Age") <= 49 ) )
        if (users_df_clean.filter((col("Age") < 18) & (col("Age") > 49)).count()) == 0:
            logger.info("users_df dataframe is cleaned. And ready to use in further transformation.")
        else:
            raise ValueError("Age values outside the range 18-49 exist.")
        # We don't need "Zip-code" for further transformation but for safe side we are filling the null with constant value 1
        users_df_clean = users_df_clean.fillna({"Zip-code": 1})
        # "Rating" from ratings_df is of IntegerType() we are converting it into FloatType()
        ratings_df_clean= df3.withColumn("Rating", df3.Rating.cast(FloatType()))

    except Exception as exp:
        logger.error("Error in the method - perform_data_preprocessing(). Please check the Stack Trace. "+ str(exp), exc_info=True)
        raise
    else:
        logger.info("Function perform_data_preprocessing() is executed successfully. \n")

    return movies_df_clean, users_df_clean, ratings_df_clean

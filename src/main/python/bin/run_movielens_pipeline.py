# Import all the necessary modules
import sys
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
import logging
import logging.config
import os
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType
from movielens_data_ingest import extract_files
import time
from movielens_data_preprocessing import perform_data_preprocessing
from movielens_data_transform import rating_report
from movielens_data_load import load_file

# Loading the logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")

def main():
    try:
        start_time = time.time()
        logging.info("main() method is started ")
        # Get all variables

        # Get spark object
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate spark object
        get_curr_date(spark)

        # Initiate movielens_data_ingestion script

        # Create common variables needed for ingestion
        file_format = 'csv'
        sep = '::'
        # As we don't have header and don't want to infer the schema from the data
        inferSchema = 'false'
        header = 'false'
        mode = "permissive"
        # Load the movie.dat file
        movies_schema = StructType([
            StructField("MovieID", IntegerType(), True),
            StructField("Title", StringType(), True),
            StructField("Genres", StringType(), True)
        ])
        movie_file = 'movies.dat'
        movie_file_dir = gav.staging_dimension + '\\' + movie_file

        # Reading "movies.dat" file in movies_df dataframe
        movies_df = extract_files(spark, movie_file_dir, file_format, sep, movies_schema, header, inferSchema, mode)

        # Validate movies_df dataframe
        df_count(movies_df, 'movies_df')
        df_top10_rec(movies_df, 'movies_df')
        df_print_schema(movies_df, 'movies_df')

        # Load the users.dat file
        users_schema = StructType([
            StructField("UserID", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Occupation", IntegerType(), True),
            StructField("Zip-code", IntegerType(), True)
        ])
        users_file = 'users.dat'
        users_file_dir = gav.staging_dimension + '\\' + users_file

        # Reading 'users.dat' file in users_df dataframe
        users_df = extract_files(spark, users_file_dir, file_format, sep, users_schema, header, inferSchema, mode )

        # Validate users_df dataframe
        df_count(users_df, 'users_df')
        df_top10_rec(users_df, 'users_df')
        df_print_schema(users_df, 'users_df')

        # Load the rating.dat file
        ratings_schema = StructType([
            StructField("UserID", IntegerType(), True),
            StructField("MovieID", IntegerType(), True),
            StructField("Rating", IntegerType(), True),
            StructField("Timestamp", StringType(), True)
        ])
        ratings_file = 'ratings.dat'
        ratings_file_dir = gav.staging_fact + '\\' + ratings_file

        # Reading "ratings.dat" file in ratings_df dataframe
        ratings_df = extract_files(spark, ratings_file_dir, file_format, sep, ratings_schema, header, inferSchema, mode)

        # Validate ratings_df dataframe
        df_count(ratings_df, 'ratings_df')
        df_top10_rec(ratings_df, 'ratings_df')
        df_print_schema(ratings_df, 'ratings_df')

        # Initiate movielens_data_preprocessing script
            # Perform data cleaning operation
        movies_df_clean, users_df_clean, ratings_df_clean = perform_data_preprocessing(movies_df, users_df, ratings_df)

        # Initiate movielens_data_transformation script
            # Apply transformation logics
        avg_rating_per_genre_year_df = rating_report(ratings_df_clean, movies_df_clean, users_df_clean)

        # Validate
        df_count(avg_rating_per_genre_year_df, 'avg_rating_per_genre_year_df')
        df_top10_rec(avg_rating_per_genre_year_df, 'avg_rating_per_genre_year_df')
        df_print_schema(avg_rating_per_genre_year_df, 'avg_rating_per_genre_year_df')

        # Initiate movielens_data_load script
        # Perform data loading operation
        format = "parquet"
        output_path =  gav.output_path
        partition_column = "ReleaseYear"
        mode = "overwrite"
        load_file(df = avg_rating_per_genre_year_df, format = format, filePath = output_path, split_no = 'false', headerReq = 'false', compressionType = None, partition_column = partition_column, mode =  mode)

        # Stop the SparkSession
        spark.stop()
        logging.info("run_movielens_pipeline is completed. \n")
        logging.info("--- %s seconds ---" % (time.time() - start_time))
    except Exception as exp:
        logging.error("Error occured in main() method. Please check the stack trace to go to the respectice module and fix it." + str(exp), exc_info=True)
        sys.exit(1)
if __name__ == "__main__":
    logging.info("run_movielens_pipeline.py is started")
    main()
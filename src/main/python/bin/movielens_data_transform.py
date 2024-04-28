import logging
import logging.config
from pyspark.sql.functions import broadcast, explode, split, col, avg, count

# Loading logging configuration file
logging.config.fileConfig(fname="../util/logging_to_file.conf")
logger = logging.getLogger(__name__)

def rating_report(ratings_df_clean, movies_df_clean, users_df_clean):
    '''
    # Rating_report:
        Transform Logic:
            --> Calculate the average ratings, per genre and year.
        Final Layout of the DataFrame(columns):
            Genres
            ReleaseYear
            AverageRating
    '''
    try:
        logger.info("Transformation function city_report() is started.")

        # Here we are using broadcast join for joining two small dimension dataframes (movies_df_clean, users_df_clean) with one fact table (rating_df_clean).
        join_df = ratings_df_clean.join(broadcast(movies_df_clean), "MovieID").join(broadcast(users_df_clean), "UserID")

        # There are multiple genres are there for one movie, so we need to explode it
        join_df = join_df.withColumn("Genres", explode(split(col("Genres"), "\\|")))

        # Calculating the average ratings, per genre and year.

        avg_rating_per_genre_year_df = join_df.groupBy("Genres", "ReleaseYear") \
            .agg(avg("Rating").alias("AverageRating"), count("Genres").alias("RatingCountPerGenre")) \
            .orderBy("ReleaseYear")
    except Exception as exp:
        logger.error("Error in method rating_report(). Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transformation function city_report() is successfully executed. \n")

    return avg_rating_per_genre_year_df



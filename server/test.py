from pyspark.sql import SparkSession, Row, functions
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 --conf spark.cassandra.connection.host=ip-172-31-40-156.ap-southeast-1.compute.internal pyspark-shell'

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "ip-172-31-40-156.ap-southeast-1.compute.internal").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Get the raw data
    # lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    # users = lines.map(parseInput)
    # Convert that to a DataFrame
    # usersDataset = spark.createDataFrame(users)

    # Write it into Cassandra
    # usersDataset.write\
        # .format("org.apache.spark.sql.cassandra")\
        # .mode('append')\
        # .options(table="users", keyspace="movielens")\
        # .save()

    # Read it back from Cassandra into a new Dataframe
    readMovies = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="movies", keyspace="movie_lens")\
    .load()

    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movie_lens")\
    .load()

    readMovies.createOrReplaceTempView("movies")
    readRatings.createOrReplaceTempView("ratings")

    sqlDF = spark.sql("""
    SELECT 
      m.*
    FROM movies m 
    JOIN ratings r ON r.movie_id = m.movie_id
    ORDER BY AVG(r.rating) DESC
    """)
    sqlDF.show()

    # Stop the session
    spark.stop()
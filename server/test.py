from pyspark.sql import SparkSession, Row, functions

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
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="movies", keyspace="movie_lens")\
    .load()

    readUsers.createOrReplaceTempView("movies")

    sqlDF = spark.sql("SELECT * FROM movies LIMIT 10")
    sqlDF.show()

    # Stop the session
    spark.stop()
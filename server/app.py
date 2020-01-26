from pyspark.sql import SparkSession, Row, functions
import os

SPARK_HOST = "ip-172-31-40-156.ap-southeast-1.compute.internal"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 --conf spark.cassandra.connection.host={SPARK_HOST} pyspark-shell'

def get_spark():
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", SPARK_HOST).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

from flask import Flask
app = Flask(__name__)

@app.route('/best-movies')
def best_movies():
    spark = get_spark()
    readUsers = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="movies", keyspace="movie_lens")\
    .load()

    readUsers.createOrReplaceTempView("movies")

    sqlDF = spark.sql("""
    SELECT 
      m.*
    FROM movies m 
    JOIN ratings r ON r.movie_id = m.movie_id
    ORDER BY AVG(m.rating) DESC
    """)
    sqlDF.
  return 'Server Works!'
  
@app.route('/worst-movies')
def worst_movies():
  return 'Hello from Server'
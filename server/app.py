import os
from pyspark.sql import SparkSession, Row, functions
from flask import Flask, jsonify

SPARK_HOST = "ip-172-31-40-156.ap-southeast-1.compute.internal"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 --conf spark.cassandra.connection.host={SPARK_HOST} pyspark-shell'


class Movie:
  def __init__(self, id, title, avg, genres):
      self.id = id
      self.title = title
      self.averageRating = avg
      self.genres = genres

app = Flask(__name__)

def get_spark():
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", SPARK_HOST).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_movies(script, mapper):
    spark = get_spark()
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

    dataRowList = spark.sql(script).cache().collect()

    spark.stop()

    return list(map(mapper, dataRowList))


@app.route('/best-movies')
def best_movies():
  def map(row):
    return Movie(row[0], row[1], row[2], row[4] )

  movies = get_movies("""
  SELECT 
    m.movie_id,
    m.title,
    AVG(r.rating) AS average_rating,
    AVG(r.rating) * COUNT(r.rating) AS rating_factor,
    m.genres
  FROM movies m 
  JOIN ratings r ON r.movie_id = m.movie_id
  GROUP BY m.movie_id, m.title, m.genres
  ORDER BY rating_factor DESC
  """, map)

  return jsonify(movies)
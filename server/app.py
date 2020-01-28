import os
from pyspark.sql import SparkSession, Row, functions
from flask import Flask, jsonify, request

SPARK_HOST = "ip-172-31-40-156.ap-southeast-1.compute.internal"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.0 --conf spark.cassandra.connection.host={SPARK_HOST} pyspark-shell'

app = Flask(__name__)

def get_spark():
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", SPARK_HOST).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_movies(script, mapper):
    spark = get_spark()
    spark.clearCache()
    readMovies = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="movies", keyspace="movie_l")\
    .load()

    readRatings = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movie_l")\
    .load()

    readMovies.createOrReplaceTempView("movies")
    readRatings.createOrReplaceTempView("ratings")

    dataRowList = spark.sql(script).collect()

    spark.stop()

    return list(map(mapper, dataRowList))


def mapRowToMovie(row):
  return { 'id': row[0], 'title': row[1], 'averageRating': row[2], 'genres': row[4] }


@app.route('/best-movies')
def best_movies():
  limit = request.args.get('top') or 20
  genre = request.args.get('genre')
  genreWhere = f"WHERE m.genres LIKE '%{genre}%'" if genre else ''

  movies = get_movies(f"""
  SELECT 
    m.movie_id,
    m.title,
    AVG(r.rating) AS average_rating,
    AVG(r.rating) * COUNT(r.rating) AS rating_factor,
    m.genres
  FROM movies m 
  JOIN ratings r ON r.movie_id = m.movie_id
  {genreWhere}
  GROUP BY m.movie_id, m.title, m.genres
  ORDER BY rating_factor DESC
  LIMIT {limit}
  """, mapRowToMovie)

  return jsonify(movies)


@app.route('/worst-movies')
def worst_movies():
  limit = request.args.get('top') or 20
  genre = request.args.get('genre')

  genreWhere = f"WHERE m.genres LIKE '%{genre}%'" if genre else ''
  movies = get_movies(f"""
  SELECT 
    m.movie_id,
    m.title,
    AVG(r.rating) AS average_rating,
    AVG(r.rating) * COUNT(r.rating) AS rating_factor,
    m.genres
  FROM movies m 
  JOIN ratings r ON r.movie_id = m.movie_id
  {genreWhere}
  GROUP BY m.movie_id, m.title, m.genres
  ORDER BY rating_factor ASC
  LIMIT {limit}
  """, mapRowToMovie)

  return jsonify(movies)
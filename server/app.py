from flask import Flask
app = Flask(__name__)

@app.route('/best-movies')
def best_movies():
  return 'Server Works!'
  
@app.route('/worst-movies')
def worst_movies():
  return 'Hello from Server'
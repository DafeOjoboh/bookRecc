"""
from flask import Flask, render_template, request, jsonify
import requests

app = Flask(__name__)

# Define the base URL of your backend application
BACKEND_BASE_URL = '<your-backend-base-url>'

# Define the home page route
@app.route('/')
def home():
    return render_template('index.html')

# Define the recommendation route
@app.route('/recommend', methods=['POST'])
def recommend():
    book_title = request.form['book_title']
    # Send request to backend application for recommendations
    response = requests.post(f'{BACKEND_BASE_URL}/recommend', json={'book_title': book_title})
    recommendations = response.json()['recommendations']
    return render_template('recommendations.html', recommendations=recommendations)

if __name__ == '__main__':
    app.run(debug=True)
"""

"""
import os

from flask import (Flask, redirect, render_template, request,
                   send_from_directory, url_for)

app = Flask(__name__)


@app.route('/')
def index():
   print('Request for index page received')
   return render_template('home.html')
"""
"""
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')
"""
"""
@app.route('/recommend', methods=['POST'])
def recommend():
   book_title = request.form.get('book_title')

   book_title = request.form['book_title']
    # Send request to backend application for recommendations
    #response = request.post(f'{BACKEND_BASE_URL}/recommend', json={'book_title': book_title})
   recommendations = response.json()['recommendations']
   return render_template('recommendations.html', recommendations=recommendations)


   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('recommendations.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()

   """
from flask import Flask, request, jsonify
import pyodbc
import numpy as np
from pyspark.ml.recommendation import ALSModel
import configparser


# Set config parameters for Azure SQL Database
parser = configparser.ConfigParser()
parser.read("app.conf")
server = parser.get("sql_conf","server")
database = parser.get("sql_conf","database")
username = parser.get("sql_conf","username")
password = parser.get("sql_conf","password")


conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# Load the trained ALS model
als_model = ALSModel.load('<path-to-your-als-model>')

# Fetch book metadata from SQL Database
def fetch_book_metadata(book_title):
    cursor.execute(f"SELECT title, author, genre, rating FROM books WHERE title LIKE '%{book_title}%'")
    rows = cursor.fetchall()
    book_metadata = []
    for row in rows:
        book_metadata.append({
            'title': row.title,
            'author': row.author,
            'genre': row.genre,
            'rating': row.rating
        })
    return book_metadata

# Generate book recommendations
def generate_recommendations(book_title, book_metadata, als_model):
    # Fetch book metadata from SQL Database
    book_metadata = fetch_book_metadata(book_title)
    
    # Convert book metadata to Spark DataFrame
    book_df = spark.createDataFrame(book_metadata)

    # Use the trained ALS model to generate recommendations
    user_id = 0  # Assuming a single user
    recommendations = als_model.recommendForUserSubset(book_df, user_id).collect()[0].recommendations

    # Extract relevant information from recommendations
    recommended_books = []
    for book in recommendations:
        recommended_books.append({
            'title': book.title,
            'author': book.author,
            'genre': book.genre,
            'rating': book.rating
        })

    return recommended_books

# Create a Flask app
app = Flask(__name__)

# Update the '/recommend' route to generate book recommendations
@app.route('/recommend', methods=['POST'])
def recommend_books():
    book_title = request.json['book_title']
    book_metadata = fetch_book_metadata(book_title)
    recommendations = generate_recommendations(book_title, book_metadata, als_model)
    return jsonify({'recommendations': recommendations})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

from flask import Flask, request, jsonify
import pyodbc
import numpy as np
import configparser
from book_rec import load_data, merge_dataset, get_books_of_tolkien_readers, books_above_threshold, pivot_table, compute_corr


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
#als_model = ALSModel.load('<path-to-your-als-model>')

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
def generate_recommendations(recommendations):
    # Fetch book metadata from SQL Database
    #book_metadata = fetch_book_metadata(book_title)
    
    # Convert book metadata to Spark DataFrame
    #book_df = spark.createDataFrame(book_metadata)

    # Use the trained ALS model to generate recommendations
    #user_id = 0  # Assuming a single user
    #recommendations = als_model.recommendForUserSubset(book_df, user_id).collect()[0].recommendations

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
    
    ratings_df = load_data('Downloads/BX-Book-rating.csv')  
    books_df = load_data('Downloads/BX-Books.csv')
    mergeColumm = "ISBN"
    threshold = 8
    input_book = book_title
    dataset = merge_dataset(ratings_df, books_df, mergeColumm)
    dataset = get_books_of_tolkien_readers(dataset, input_book)

    ratings_data_raw = books_above_threshold(dataset, threshold)
    dataset_for_corr = pivot_table(ratings_data_raw)

    top10, bottom10 = compute_corr(dataset_for_corr, ratings_data_raw)
    recommendations = generate_recommendations(top10)
    return jsonify({'recommendations': recommendations})

    #book_metadata = fetch_book_metadata(book_title)
    #recommendations = generate_recommendations(book_title, book_metadata, als_model)
    #return jsonify({'recommendations': recommendations})

if __name__ == '__main__':
    app.run()

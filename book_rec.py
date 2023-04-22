import pandas as pd
import numpy as np

def load_data(file_path):
    """
    Load data from CSV file stored in raw bucket.

    Args:
        file_path (str): Path to CSV file.

    Returns:
        pandas.DataFrame: Book ratings.
    """
    df = pd.read_csv(file_path, encoding='cp1251', sep=';')
    return df

def merge_dataset(df1, df2, merge_column):
    """
    merges two dataframes

    Args:
        dataframe1: ratings dataframe
        dataframe2: books dataframe
        merge_column: common column to combine on

    Returns:
        A dataframe of the combined dataframes
    """
    df = pd.merge(df1, df2, on=[merge_column])
    return df



def get_books_of_tolkien_readers(dataset, input_book):
    """
    Extract books read by readers who have read Tolkien's books.

    Args:
        dataset (pandas.DataFrame): Book ratings and books data.

    Returns:
        pandas.DataFrame: Books read by Tolkien readers.
    """
    dataset_lowercase = dataset.apply(lambda x: x.str.lower() if(x.dtype == 'object') else x)
    tolkien_readers = dataset_lowercase['User-ID'][(dataset_lowercase['Book-Title'] == input_book) & (dataset_lowercase['Book-Author'].str.contains("tolkien"))]
    tolkien_readers = tolkien_readers.tolist()
    tolkien_readers = np.unique(tolkien_readers)
    books_of_tolkien_readers = dataset_lowercase[(dataset_lowercase['User-ID'].isin(tolkien_readers))]
    return books_of_tolkien_readers

def books_above_threshold(dataset, threshold):
    """

    """
    # Number of ratings per other books in dataset
    number_of_rating_per_book = dataset.groupby(['Book-Title']).agg('count').reset_index()

    #select only books which have actually higher number of ratings than threshold
    books_to_compare = number_of_rating_per_book['Book-Title'][number_of_rating_per_book['User-ID'] >= threshold]
    books_to_compare = books_to_compare.tolist()

    ratings_data_raw = dataset[['User-ID', 'Book-Rating', 'Book-Title']][dataset['Book-Title'].isin(books_to_compare)]
    return ratings_data_raw

def pivot_table(df):

    # group by User and Book and compute mean
    ratings_data_raw_nodup = df.groupby(['User-ID', 'Book-Title'])['Book-Rating'].mean()

    # reset index to see User-ID in every row
    ratings_data_raw_nodup = ratings_data_raw_nodup.to_frame().reset_index()

    dataset_for_corr = ratings_data_raw_nodup.pivot(index='User-ID', columns='Book-Title', values='Book-Rating')
    return dataset_for_corr

input_data='the fellowship of the ring (the lord of the rings, part 1)'
LoR_list = [input_data]

def compute_corr(df1, df2, ):
    # for each of the trilogy book compute:

    result_list = []
    worst_list = []

    for LoR_book in LoR_list:
        
        result_list = []
        worst_list = []
        #Take out the Lord of the Rings selected book from correlation dataframe
        dataset_of_other_books = df1.copy(deep=False)
        dataset_of_other_books.drop([LoR_book], axis=1, inplace=True)
      
        # empty lists
        book_titles = []
        correlations = []
        avgrating = []

        # corr computation
        for book_title in list(dataset_of_other_books.columns.values):
            book_titles.append(book_title)
            correlations.append(df1[LoR_book].corr(dataset_of_other_books[book_title]))
            tab=(df2[df2['Book-Title']==book_title].groupby(df2['Book-Title']).mean())
            avgrating.append(tab['Book-Rating'].min())
    # final dataframe of all correlation of each book   
    corr_fellowship = pd.DataFrame(list(zip(book_titles, correlations, avgrating)), columns=['book','corr','avg_rating'])
    corr_fellowship.head()

    # top 10 books with highest corr
    result_list.append(corr_fellowship.sort_values('corr', ascending = False).head(10))
    
    #worst 10 books
    worst_list.append(corr_fellowship.sort_values('corr', ascending = False).tail(10))
    
    return result_list, worst_list

print("Correlation for book:", LoR_list[0])
#print("Average rating of LOR:", ratings_data_raw[ratings_data_raw['Book-Title']=='the fellowship of the ring (the lord of the rings, part 1'].groupby(ratings_data_raw['Book-Title']).mean()))
rslt = result_list[0]

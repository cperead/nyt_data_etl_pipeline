#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
import sqlite3

# To import variable values from config_vars.py
import sys
# appending a path
sys.path.append('../')
import config_vars
from config_vars import *


def create_df_article(df_Ar):
	"""
	Crate a Dataframe 'article', take the column 'byline_original'
	clean the authors date and with the result create a new column 'authors'
	:param df_Ar: Dataframe with the original articles data to be modified
	"""

	# Separate date and time. Create two new columns
	df_Ar['a_date'] = pd.to_datetime(df_Ar['pub_date']).dt.date
	df_Ar['a_time'] = pd.to_datetime(df_Ar['pub_date']).dt.time

	# Clean 'authors' column
	# Create a new column of authors separated by comma

	# These strings to be cleaned where found by an inspection of the data contained on 'byline_original'
	dic = { # Strings at the start 
			'By '                    : '',
			'By ‘'                   : '',
			'Show.$'                 : 'Show',     
			'Text by '               : '',
			'Video by '              : '',
			'Videos by '             : '',
			'Written by '            : '', 
			'Compiled by '           : '',
			'Artwork by '            : '',
			'Produced by '           : '',
			'Reporting by '          : '',
			'Selected by '           : '',
			'Photographs by '        : '',
			'Interviews by '         : '',
			'Interviews by '         : '',
			'Introduction by '       : '',
			'Photo Essay by '        : '',
			'Illustrations by '      : '',
			'Photographs, Text by '  : '',

			# Strings in the middle
			' Text by '              : '',
			' Video by '             : '',
			' posters by '           : '',
			' Flowers by '           : '',
			' photographs by '       : '',

			# Strings in the middle that need to be replaced with ','
			'\;'                     : ',',
			' and '                  : ', ',
			' with Text by '         : ', ',
			' with Photographs by '  : ', ',

			# Strings at the end
			' for The New York Times': '',
			' For The New York Times': ''
		}

	df_Ar['authors'] = df_Ar['byline_original'].replace(dic, regex=True)

	# Create a row index. from 0 to ...
	# Reset index, BUT save old
	df_Ar = df_Ar.reset_index()

	# Rename columns
	df_Ar.rename(columns={'index': 'article_id', '_id': 'original_id'}, inplace=True)
	return df_Ar


def create_df_article_author(df_Ar_Au, df_Ar):
	"""
	Crate a Dataframe 'article_author', where each row is a realtion between one article_id and one author
	This build the "composite table" as a part of the normalization of the column 'byline_original'
	containing all the authors.
	:param df_Ar_Au: Dataframe containing the resulted "composite table" 'article_author'
	:param df_Ar: Dataframe with the articles data and author data cleaned
	"""

	# Copy authors column
	df_temp = df_Ar[['authors']].copy()
	df_Ar_Au = df_temp.set_index(['authors'])

	# Explode: Transform each element of a list-like to a row, replicating index values.
	df_Ar_Au = df_temp.apply(lambda x: x.str.split(',').explode())

	## Clean authors column
	# Trim head and tail from white spaces
	df_Ar_Au.authors = df_Ar_Au.authors.str.strip() 

	# Let only one whitespace between words
	df_Ar_Au.authors = df_Ar_Au.authors.replace(r'\s+', ' ', regex=True) 

	# Index it, BUT save OLD index, that is the index of the article table!!!
	df_Ar_Au = df_Ar_Au.reset_index()

	# Replace where author is empty with a NaN
	# and then drop the rows with NaN value
	df_Ar_Au.authors.replace('', np.nan, inplace=True)
	df_Ar_Au.dropna(subset=['authors'], inplace=True)

	## Rename columns
	df_Ar_Au.rename(columns={'index': 'article_id', 'authors': 'author_name'}, inplace=True)

	# Drop duplicates 
	df_Ar_Au = df_Ar_Au.drop_duplicates()

	return df_Ar_Au


def create_df_author(df_Au, df_Ar_Au):
	"""
	Crate a Dataframe 'author', where each row contain the name of the author and the author_id
	:param df_Au: Dataframe containig the 'author' data.
	:param df_Ar_Au: Dataframe containing the "composite table" 'article_author'
	"""

	# Find unique authors
	unique_authors_list = df_Ar_Au.author_name.unique()

	# Create DataFrame with Unique authors
	df_Au = pd.DataFrame(unique_authors_list, columns =['author_name'])

	# Reset index, BUT save old
	df_Au = df_Au.reset_index()

	# Rename columns
	df_Au.rename(columns={'index': 'author_id'}, inplace=True)

	return df_Au


def modify_df_article_author(df_Ar_Au, df_Au):
	"""
	Modify DataFrame 'article_author', removing duplicates
	:param df_Ar_Au: Dataframe containing the "composite table" 'article_author'
	:param df_Au: Dataframe containig the 'author' data.
	"""

	df_Ar_Au = df_Ar_Au.merge(df_Au, on=['author_name'])

	# Drop column author_name
	df_Ar_Au = df_Ar_Au.drop('author_name', axis=1)

	return df_Ar_Au


def create_table_article(conn, df_Ar):
	"""
	Create table 'article' in the SQLite Database and 
	populate it with the data stored in the 'article' Dataframe
	:param conn: database connection reference
	:param df_Ar: Dataframe with the articles data and author data cleaned
	"""

	try:
		cur = conn.cursor()
		cur.execute('''
			CREATE TABLE IF NOT EXISTS article (
			    article_id       INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT NOT NULL,
			    original_id      VARCHAR,
			    abstract         VARCHAR,
			    web_url          VARCHAR,
			    snippet          VARCHAR,
			    lead_paragraph   VARCHAR,
			    print_section    VARCHAR,
			    print_page       VARCHAR,
			    pub_date         VARCHAR,
			    document_type    VARCHAR,
			    news_desk        VARCHAR,
			    section_name     VARCHAR,
			    type_of_material VARCHAR,
			    word_count       INTEGER DEFAULT 0,
			    headline_main    VARCHAR,
			    headline_print_headline VARCHAR,
			    byline_original         VARCHAR,
			    byline_organization     VARCHAR,
			    a_date           VARCHAR,
			    a_time           VARCHAR,
			    authors          VARCHAR
			);
		''')
		conn.commit()

		try:
			df_Ar.to_sql('article', conn, if_exists='append', index = False)
		except Exception as e:
			print(">>> A 'df.to_sql' exception : ", e, "\n")
	
	except sqlite3.Error as er:
		print(">>> A 'SQLite error' error : ", er, '\n')



def create_table_author(conn, df_Au):
	"""
	Create table 'author' in the SQLite Database and 
	populate it with the data stored in the 'author' Dataframe
	:param conn: database connection reference
	:param df_Au: Dataframe with the authors data
	"""

	try:
		cur = conn.cursor()
		cur.execute('''
			CREATE TABLE IF NOT EXISTS author (
			    author_id   INTEGER PRIMARY KEY AUTOINCREMENT,
			    author_name TEXT UNIQUE
			);
		''')
		conn.commit()

		try:
			df_Au.to_sql('author', conn, if_exists='append', index = False)
		except Exception as e:
			print(">>> A 'df.to_sql' exception : ", e, "\n")
	
	except sqlite3.Error as er:
		print(">>> A 'SQLite error' error : ", er, '\n')


def create_table_article_author(conn, df_Ar_Au):
	"""
	Create table 'article_author' in the SQLite Database and 
	populate it with the data stored in the 'article_author' Dataframe
	:param conn: database connection reference
	:param df_Au: Dataframe with the article_author data
	"""

	try:
		cur = conn.cursor()
		cur.execute('''
			CREATE TABLE IF NOT EXISTS article_author (
			    article_id        INTEGER NOT NULL,
			    author_id         INTEGER NOT NULL,
			    PRIMARY KEY ( article_id, author_id )
			);
		''')
		conn.commit()

		try:
			df_Ar_Au.to_sql('article_author', conn, if_exists='append', index = False)
		except Exception as e:
			print(">>> A 'df.to_sql' exception : ", e, "\n")
	
	except sqlite3.Error as er:
		print(">>> A 'SQLite error' error : ", er, '\n')


def test_database(db_path, db_name, table_name):
	"""
	Only for test purposes.
	Show first five rows of a table as parameter.
	:param db_path: database path
	:param db_name: database name
	:param table_name: table to be checked
	"""

	conn = sqlite3.connect(db_path + db_name)
	cur = conn.cursor()
	cur.execute(
		f'''  
			SELECT * 
			FROM {table_name}
			LIMIT 5
		''')
	
	for row in cur.fetchall():
		print (row)

	cur.close()
	conn.close



def main():
	"""
	Read a CSV file with the NYT articles extracted.
	Normalizes the authors data in Dataframes
	Create a database with three tables 'article', 'author' and 'article_author' this as a composite table
	Populate the tables with the data stored in the Dataframes
	"""

	# Variables
	# To set the values edit config_vars.py
	input_filepath = f'/{OUTPUT_DATA_DIR}/{JSON_TO_CSV_FILE_NAME}'
	output_filepath = f'/{OUTPUT_DATA_DIR}/{CLEAN_CSV_FILE_NAME}'
	db_path = f'/{SQLite_NYT_DB_DIR}/'
	db_name = SQLITE_NYT_DB_NAME
	

	## Create DataFrames and Normalize Data
	# Create-transform df 'article'
	df_Ar = pd.read_csv (input_filepath, low_memory=False)
	df_Ar = create_df_article(df_Ar)
	df_Ar.to_csv(output_filepath, index=False)

	# Create df 'article_author'
	df_Ar_Au = pd.DataFrame()
	df_Ar_Au = create_df_article_author(df_Ar_Au, df_Ar)

	# Create df 'author'
	df_Au = pd.DataFrame()
	df_Au =create_df_author(df_Au, df_Ar_Au)

	# Modify df 'article_author'
	# Normalization
	df_Ar_Au = modify_df_article_author(df_Ar_Au, df_Au)

	## Create Database
	# Drop Database If Exists ==> Remove the file
	if os.path.exists(db_path + db_name):
		os.remove(db_path + db_name)

	# Create Database
	conn = sqlite3.connect(db_path + db_name)

	# Create Table 'article'
	create_table_article(conn, df_Ar)

	# Create Table 'author'
	create_table_author(conn, df_Au)

	# Create Table 'article_author'
	create_table_article_author(conn, df_Ar_Au)

	# Close Database connection
	conn.close

	# Only for test purposes
	#test_database(db_path, db_name, table_name = 'author')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3

import pymongo
import os
import json
import pandas as pd

# Gets variables values from configuration file config_vars.py
# MONGODB_URL, MONGODB_NYT_DB_NAME, MONGODB_NYT_COL_NAME
# INPUT_DATA_DIR, OUTPUT_DATA_DIR, JSON_TO_CSV_FILE_NAME
# To import variable values from config_vars.py
# import sys
# # appending a path
# sys.path.append('../')
# import config_vars
from config_vars import *


def json_to_csv(input_directory, output_directory_file):
    """
    Converts many JSON files with articles to one CSV file containing all articles
    :param input_directory: directory where JSON files are located to be loaded
    :param output_directory_file: file path and file name of resulted CSV file 
    """

    # Instantiate mongo client
    myclient = pymongo.MongoClient(MONGODB_URL)
    
    # Create database
    nyt_db = myclient[MONGODB_NYT_DB_NAME]
    
    # Database collection
    nyt_articles_coll = nyt_db[MONGODB_NYT_COL_NAME]
    
    # Read json files and insert in MongoDB
    # Read data directory
    directory = input_directory
    
    # Iterate over .json files on the directory
    for filename in os.listdir(directory):
        file_data = ''
        json_file = os.path.join(directory, filename)

        # only process files not directories
        if not os.path.isfile(json_file):
            continue

        # only process .json files
        if not json_file.lower().endswith('.json'):
            continue
                
        print(json_file)
        
        # read one json file
        try:
            with open(json_file, encoding="utf8") as file:
                file_data = json.load(file)
        except Exception as e:
            print(">>> An 'open file' exception : ", e, " occurred on file:", json_file, "\n")
            continue

        # insert in the collection
        try:
            nyt_articles_coll.insert_many(file_data["response"]["docs"])
        except Exception as e:
            print(">>> An 'insert_many' exception : ", e, " occurred on file:", json_file, "\n")
            continue
            
    
    # print the total number of articles
    total_articles = nyt_articles_coll.count_documents({})
    print("The number of articles in this collections is", total_articles)
    
    # Query the mongodb and
    # Create a DataFrame and Save it to .csv file
    # Creating a Cursor instance using aggregate() function
    cursor = nyt_articles_coll.aggregate(
        [
            {
                "$project": {
                    '_id': 1,
                    'abstract' : 1,
                    'web_url' : 1,
                    'snippet' : 1,
                    'lead_paragraph' : 1,
                    'print_section' : 1,
                    'print_page' : 1,
                    'a_source' : 1,
                    'headline_main' : '$headline.main',
                    'headline_print_headline' : '$headline.print_headline',
                    'pub_date' : 1,
                    'document_type' : 1,
                    'news_desk' : 1,
                    'section_name' : 1,
                    'byline_original' : '$byline.original',
                    'byline_organization' : '$byline.organization',
                    'type_of_material' : 1,
                    'word_count' : 1                
                }
            }
        ]
    )
    
    # Expand the cursor and construct the DataFrame 'articles'
    df_raw =  pd.DataFrame(list(cursor))
    df_raw.to_csv(output_directory_file, index=False)


def main():
    """
    Function that convert JSON files to one CSV file
    """

	# Variables
	# To set the values edit config_vars.py
    input_directory = f'/{INPUT_DATA_DIR}'
    output_directory_file = f'/{OUTPUT_DATA_DIR}/{JSON_TO_CSV_FILE_NAME}'
    json_to_csv(input_directory, output_directory_file)


if __name__ == '__main__':
    main()

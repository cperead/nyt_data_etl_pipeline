# NYT Data ETL Pipeline: <br> Building a Data Pipeline for Extracting, Transforming, and Loading New York Times Data

<br>

---
## Introduction
The 'NYT ETL Data Pipeline' is a comprehensive Data Engineering project that aims to create an automated data pipeline for extracting, transforming, and loading data from the New York Times Archive into a normalized relational database.  
Using Docker and Docker-Compose, all stages of the pipeline are containerized and orchestrated for ease of use.  
The ultimate goal of the project is to build a scalable API with five query-endpoints that provide users, like Data Scientist or Data Analyst, quick access to the normalized database, particularly the author's information, by querying the SQLite database.

<br>

---
# ETL Pipeline Overview
The "NYT ETL Data Pipeline" is a **Data Engineering** project that involves building a data **pipeline** for **extracting**, **transforming**, and **loading** New York Times Archive data.   
The project includes several steps:
* *Requesting* the New York Times Archive API using a **Bash** Shell Script to download JSON files on a monthly basis. 
* The downloaded JSON files are then *converted* and *flattened* to a single CSV file using **MongoDB**. 
* The data in this CSV file is *cleaned* using **Python** to address issues in the author’s column, which contains, for example, multiple authors for each article.
* The authors' data is then *normalized* and *stored* in a **SQLite** database in three tables: article, author, and article_author (a composite table) to enable efficient querying. 
* Finally, an API is created using **FastAPI**, allowing users to *query* or *modify* the **SQLite** database and retrieve relevant data.  

All stages are *containerized* with **Docker**  
and *orquestrated* with **Docker-Compose**.

<kbd>
  <img src="images/etl_pipeline.png">
</kbd>  

<br>
<br>

***

# Requirements
- Linux
- Bash - Curl
- Python 3.10
- Python libraries: pymongo, pandas, numpy, json, uvicorn, databases
- SQLite
- MongoDB
- FastAPI
- Docker Containers
- Docker Compose

<br>

****  

# Data Modeling Stages

## 1. Requesting Raw Data - New York Times Archive API
The New York Times Archive API is requested to obtain information of news articles published by month. The API provides a way to retrieve articles based on year and month.  

The following JSON fields are for every article.
> • **abstract**: A brief summary.  
> • **web_url**: The URL for the article on the New York Times website.  
> • **snippet**: A short excerpt.  
> • **lead_paragraph:** The lead paragraph of the article.  
> • **print_section**: The print section of the article.  
> • **print_page**: The print page number of the article.  
> • **source**: The source of the article.  
> • **multimedia**: ignored (An array of multimedia objects associated with the article, such as images or videos.)  
> • **headline**:  
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; • **main**: The main headline.  
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; • **print_headline**: The printed headline.  
> • **keywords**: ignored (An array of relevant keywords associated with the article.)  
> • **pub_date**: Date/time The date the article was published.  
> • **document_type**: The type of document, such as article or blog post.  
> • **news_desk**:  The desk responsible for producing the article.  
> • **section_name**: The section in which the article appeared.  
> • **byline**:   
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; • **original**: The original byline text as it appeared in the article.  It contains information about the individu  
> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; • **organization**: The name of the organization to which the contributor belongs.  
> • **type_of_material**: The type of content of the article, such as news, opinion, or review.  
> • **_id**: Unique identifier for the article.  
> • **word_count**: The number of words in the article.   
> • **uri**:  Unique identifier for the article that can be used to access the article through the New York Times API.  

### Raw Data - JSON Schema
```json
{
    "abstract": "String"
    "web_url": "String"
    "snippet": "String"
    "lead_paragraph": "String"
    "print_section": "String"
    "print_page": "Integer"
    "source": "String"
    "multimedia": "Array of multimedia objects (*ignored)"
    "headline": {
        "main": "String"
        "print_headline": "String"
    },
    "keywords": "Array of keyword objects (*ignored)"
    "pub_date": "Date/time string"
    "document_type": "String"
    "news_desk":  "String"
    "section_name": "String"
    "byline": {
        "original": "String"
        "organization": "String"
    },
    "type_of_material": "String"
    "_id": "ObjectId"
    "word_count": "Integer"
    "uri": "String"
}
```
<br>

## 2. **Flattening**  Data - MongoDB Database
The obtained data (many JSON files with nested fields) is flattened with MongoDB and then saved into one plain CSV file.  


| Raw Data |  To | Data Flattened  |
| -------- |:---: |--------------- |
| <pre lang="text">"headline": {<br>    "main": "String"<br>    "print_headline": "String"<br>}</pre>| &rarr; | <pre lang="text">"headline_main" : "String"<br>"headline_print_headline" : "String"</pre>|
| <pre lang="text">"byline": {<br>    "original": "String"<br>    "organization": "String"<br>}</pre>| &rarr; | <pre lang="text">"byline_original" : "String"<br>"byline_organization" : "String"</pre>|



### Flattened Data - JSON Schema
```json
{
	"_id": "ObjectId"
	"abstract" : "String"
	"web_url" : "String"
	"snippet" : "String"
	"lead_paragraph" : "String"
	"print_section" : "String"
	"print_page" : "Integer"
	"a_source" : "String"
	"headline_main" : "String"
	"headline_print_headline" : "String"
	"pub_date" : "Date/time string"
	"document_type" : "String"
	"news_desk" : "String"
	"section_name" : "String"
	"byline_original" : "String"
	"byline_organization" : "String"
	"type_of_material" : "String"
	"word_count" : "Integer"                
}
```
<br>

## 3. **Cleaning**, **Normalizing** and **Storing** Data - Pandas & SQLite Database
The previous CSV file is read into a Dataframe. 
The `byline_original` field, which contains one or more authors separated by commas and may have several unnecessary "stop words," is cleaned and normalized.
<br>

### Data Cleaning
<kbd>
  <img src="images/data_cleaning_and_normalization.png">
</kbd>

<br>

To do the normalization a "composite-table" `article_author`  was created, and also the table `author`. This solves the problem of "many-to-many" data relation.

<br>

### Normalized Data Model - Database Schema

<kbd>
  <img src="images/database_model_nyt.png">
</kbd>

<br>
<br>

## 4. Data Consumption - FastAPI
At this moment, the API consists of 12 api-endpoints.

<kbd>
  <img src="images/fastapi_endpoints.png">
</kbd>


## 5. Data Consumption - MongoDB (Standalone script)
With this Jupyter Notebook you can explore the data retrieved from the JSON files that is in a MongoDB collection.  
There are some MongoDB queries.
[a link](https://github.com/cperead/nyt_data_etl_pipeline/blob/main/etl/query_mongodb.ipynb)


### API endpoints

1. **Status** : Checks API Status
    - **/** : Check if the api is working.  

2. **Database Statistics** : Database Statistics
    - **/rows_per_table** : Total number of rows per table.  

3. **Query Authors** : Query, search author defined by the user
    - **/author** : Retrieve the names of authors that contain the [search string].  

    - **/articles_count_with_word_in_headline_by_author** : Visualize the count of articles authored by [author name] that include the [exact word] in the 'headline_main' field.

    - **/articles_count_by_section_by_author** : Visualize the count of articles authored by [author name] in each section.

    - **/articles_count_by_author** : Visualize the count of articles written by authors whose name contains the [search string].

    - **/articles_count_by_author_per_year_month** : Visualize the count of articles authored by [author name] string, grouped by year and month.

4. **Info Authors** : Retrieve predefined info-queries about authors

    - **top_authors_by_section** : Rank the authors in each section by the count of their articles and visualize the top author in each section.

    - **/most_prolific_authors_by_section** : Rank authors in each section by word count and visualize the author with the highest word count in each section.

    - **/count_pairs_authors_collaboration** : Identify pairs of authors and visualize the count of articles they co-authored.

5. **Test Insert Author** : Test the DB-Normalization inserting one article with a new author in the DB, and after checking the author table

    - **test_inserted_author** : Show the articles written by an author ordered latest first. You can check the inserted one.

    - **/insert_new_article_with_new_author** : Insert a new article with a new author. Check if the author exists or not, taking care of the DB integrity.

<br><br>

******

# How to run this Pipeline **Automatically** <br> with **docker-compose**

1. **Clone this project**

2. **Clean up** the folders `input_data/` and `output_data`  
    - To do this you can: `rm -r output_data/* input_data/*`

3. **Optional**: In case you want to clean up all your docker:    
    - The following instructions will remove ALL containers, images, networks and volumes.  
        ```
        docker compose down; \
        docker kill $(docker ps -q); \
        docker rm -f $(docker ps -a -q); \
        docker rmi $(docker images -q); \
        docker volume rm $(docker volume ls -q); \
        docker system prune --volumes
        ```
      ► *Use them at your own risk!* ◄

4. **Run the docker-compose.yml**
    - **Description**: 
    - **docker-compose file**: `docker-compose.yaml`
    - **Location**: `./`
    - **Input**:
        - `etl/config_vars.py`
        - `etl/requirements_files/convert_data_mongo_requirements.txt`
        - `etl/requirements_files/create_db_sqlite_requirements.txt`
        - `etl/requirements_files/fastapi_requirements.txt`
    - **Output**:
        - Many files `input_data/NYT_YYYY_MM.json ... NYT_YYYY_MM.json...`
        - `output_data/extracted_data.csv`
        - `output_data/nyt_db.db`
        - `output_data/extracted_data_clean.csv` **Not used in the moment*
    - **Usage**:
        * Obtain your New York Times Archive API Key from [https://developer.nytimes.com/docs/archive-product/1/overview](https://developer.nytimes.com/docs/archive-product/1/overview) and set the value of the variable NYT_API_KEY in the configuration file `etl/config_vars.py`.
        * Define the period of years and months you want to extract and set those values in the `etl/config_vars.py` configuration file.
        * Config MongoDB URL in `etl/config_vars.py`. Leave this IP.  
        `MONGODB_URL='mongodb://root:1234@172.25.0.9:27017/' `
        * Run the docker-compose.yml  
        `docker compose up`
        * Go to your browser  
        [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  
        You can query the database based in the authors, and then insert articles with new or old authors. So you can test the integrity and normalization of the database.

<br>

****

# How to run this Pipeline **Manually** with Docker <br> (**Container-by-Container**)

1. **Clone this project**

2. **Clean up** the folders `input_data/` and `output_data`  
    - To do this you can: `rm -r output_data/* input_data/*`

3. **Optional**: In case you want to clean up all your docker.    
    - The following instructions will remove ALL containers, images, networks and volumes.  
        ```
        docker compose down; \
        docker kill $(docker ps -q); \
        docker rm -f $(docker ps -a -q); \
        docker rmi $(docker images -q); \
        docker volume rm $(docker volume ls -q); \
        docker system prune --volumes
        ```
      ► *Use them at your own risk!* ◄

4. **<u>STAGE A</u>**: Run the **Extraction Data Container**
    - **Description**: Extracts New York Times data from the Archive API using a  Bash Shell Script.
    - **Dockerfile**: `A.extract.Dockerfile`
    - **Location**: `./`
    - **Input**:
        - NYT Archive API
        - `etl/config_vars.py`
    - **Output**:
        - Many files `input_data/NYT_YYYY_MM.json ... NYT_YYYY_MM.json...`
    - **Usage**:
        * Obtain your New York Times Archive API Key from [https://developer.nytimes.com/docs/archive-product/1/overview](https://developer.nytimes.com/docs/archive-product/1/overview)  and set the value of the variable NYT_API_KEY in the configuration file `etl/config_vars.py`.
        * Define the period of years and months you want to extract and set those values in the `etl/config_vars.py` configuration file.
        * Build the container.  
        `docker build -f A.extract.Dockerfile -t extract_app/ubuntu:1.0 . `
        * Start the container  
        `docker run --rm -v $(pwd)/input_data:/input_data extract_app/ubuntu:1.0`
        * After running the container, the downloaded data will be saved as JSON files in the input_data directory with filenames like NYT_2023_1.json, NYT_2023_12.json, etc. You can verify the successful download by checking the presence of these files in the input_data directory.
        <br><br>

5. **<u>STAGE B</u>**: Run the **Conversion Data Container**
    - **Script Name**: `B.convert.Dockerfile`
    - **Location**: `./`
    - **Input**:  
        - Many files `input_data/NYT_YYYY_MM.json ... NYT_YYYY_MM.json...`
	    - `etl/config_vars.py`
        - `etl/requirements_files/convert_data_mongo_requirements.txt`
    - **Output**: <br>
        - `output_data/extracted_data.csv`
    - **Description**: Converts and extracts data from JSON files to a CSV file using MongoDB.
    - **Usage**:
        * Make sure you have the JSON files to be converted in the `input_data/ directory`
        * Config MongoDB URL in `etl/config_vars.py`. Leave this IP.  
        `MONGODB_URL='mongodb://root:1234@172.25.0.9:27017/' `
        * Create a docker network  
        `docker network create --subnet=172.25.0.0/16 service-net`
        * Build the container.  
        `docker build -f B.convert.Dockerfile -t convert_data_app/mongo:1.0 .`
        * Run MongoDB with a static IP.  
        ```
        docker run -d \  
        --net service-net \  
        --ip 172.25.0.9 \  
        -e MONGO_INITDB_ROOT_USERNAME=root \  
        -e MONGO_INITDB_ROOT_PASSWORD=1234 \  
        -p 27017:27017 \  
        convert_data_app/mongo:1.0
        ```
        * Run the script in a container
        ```
        docker run --rm \
        --net service-net \
        --ip 172.25.0.15 \
        -v $(pwd)/input_data:/input_data \
        -v $(pwd)/output_data:/output_data \
        --name convert_data_mongo \
        convert_data_app/mongo:1.0 \
        python3 /etl/convert_data.py
        ```
        This will read the JSON files from the `input_data/` directory, extract relevant data, and store it in a CSV file `output_data/extracted_data.csv`.
        * Optional: You can access the converted data saved to `extracted_data.csv` for further analysis or processing.

6. **<u>STAGE C</u>**: Run the container to **Normalize Author's Data, Create Normalized Database and Load Data**
    - **Script Name**: `C.db-create.Dockerfile`
    - **Location**: `./`
    - **Input**:
        - `output_data/extracted_data.csv`
	    - `etl/config_vars.py`
        - `etl/requirements_files/create_db_sqlite_requirements.txt`
    - **Output**:
        - `output_data/nyt_db.db`
        - `output_data/extracted_data_clean.csv`  *(Not used at the moment)*
    - **Description**: <br> 
        * **Cleans** the data using Python to address issues in the authors column.
        * **Normalizes** the relation between articles and authors. One article can have none or multiple authors. One author could have written one or more articles.
        * **Stores** the data in a SQLite database with three tables: <br>
        `article`, `author` and `article_author` *(this last is a composite_table)*
    - **Usage**:
        * Make sure you have the file `output_data/extract_data.csv` file
        * Build the container.  
        `docker build -f C.db-create.Dockerfile -t create_db_app/ubuntu:1.0 ${PWD}`
        * Run the container.  
        `docker run --rm -v ${PWD}/output_data:/output_data create_db_app/ubuntu:1.0`
        This will reads the CSV file from the `output_data/` directory, extracts the relevant data, normalizes it and stores it in a SQLite Database `output_data/nyt_db.db`
        * You can view the SQLite Database with https://sqlitebrowser.org/ or https://sqlitestudio.pl/ 
        <br><br>

7. **<u>STAGE D</u>**: Run and Check the API **Data Consumption**
    - **Script Name**: `D.api.Dockerfile`
    - **Location**: `./`
    - **Input**:
        - `output_data/nyt_db.db` 
	    - `etl/config_vars.py`
        - `etl/requirements_files/fastapi_requirements.txt`
    - **Output**:
        - `output_data/nyt_db.db` 
    - **Description**: Creates an API using FastAPI for querying or modifying(insert) the SQLite database.
    - **Usage**:
        * Make sure you have a the Database on `output_data/ny_db.db`
        * Build the container  
        `docker build -f D.api.Dockerfile -t api/ubuntu:1.0 ${PWD}`
        * Start the container
        ```
        docker run --rm \
        --net service-net \
        --ip 172.25.0.20 \
        -p 8000:8000 \
        -v $(pwd)/output_data:/output_data \
        --name nty_api_db \
        api/ubuntu:1.0 \
        uvicorn main:app --host 0.0.0.0
        ```
        * Go to your browser  
        [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  
        You can query the database based in the authors, and then insert articles with new or old authors. So you can test the integrity and normalization of the database.

<br>

****

# How to run this Pipeline **Manually** <br>  (**script-by-script**)

1. **Clone this project**

2. Create the **Virtual environments** for every stage (unless you like messing your python installation)

3. **<u>STAGE A</u>**: Run the **Extraction Data Script**
    - **Description**: Extracts New York Times data from the Archive API using a  Bash Shell Script.
    - **Script Name**: `extract_data.sh`
    - **Location**: `etl/`
    - **Input**:
        - NYT Article API
        - `etl/config_vars.py`
    - **Output**:
        - Many files `input_data/NYT_YYYY_MM.json ... NYT_YYYY_MM.json...`
    - **Dependencies**:
        - Bash
        - Curl
    - **Usage**:
        * Obtain your New York Times Archive API Key from [https://developer.nytimes.com/docs/archive-product/1/overview](https://developer.nytimes.com/docs/archive-product/1/overview)  and set the value of the variable NYT_API_KEY in the configuration file `etl/config_vars.py`.
        * Define the period of years and months you want to extract and set those values in the `etl/config_vars.py` configuration file.
        * Make sure you have curl installed on your system. If not, install them using the following commands:  
            `sudo apt-get update && sudo apt-get install curl`
        * Inside the directory `etl/` , run the script using  
         `extract_data.sh`  
         **(don't forget to RUN `chmod +x extract_data.sh`)*
        * After running the extract_data.sh script, the downloaded data will be saved as JSON files in the input_data directory with filenames like NYT_2023_1.json, NYT_2023_12.json, etc. You can verify the successful download by checking the presence of these files in the input_data directory.  

4. **<u>STAGE B</u>**: Run the **Conversion Data Script**
    - **Script Name**: `convert_data.py`
    - **Location**: `etl/`
    - **Input**:
        - Many files `input_data/NYT_YYYY_MM.json ... NYT_YYYY_MM.json...`
	    - `etl/config_vars.py`
    - **Output**:
        - `output_data/extracted_data.csv`
    - **Description**: This script converts and extracts data from JSON files to a CSV file using MongoDB.
    - **Dependencies**:
        - Python 3
        - Docker: MongoDB Image
        - Python libraries:  pandas, pymongo
    - **Usage**:
        * Make sure you have the JSON files to be converted in the `input_data/` directory.
        * Run the MongoDB Docker in the background with: <br>
        `docker run -d -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=root -p 27017:27017 --name my_mongo_daemon mongo` <br>
        So you don’t need to install MongoDB locally.
        * Install pandas and pymongo in your virtual environment <br>
        `pip install pandas` <br>
        `pip install pymongo`
        * Run the script using the following command: <br>
        `python3 convert_data.py` <br>
        This script will read the JSON files from the `input_data/` directory, extract relevant data, and store it in a CSV file named extracted_data.csv in the `output_data/` directory.
        * You can access the converted data saved to `extracted_data.csv` for further analysis or processing.

5. **<u>STAGE C</u>**: Run the script to **Normalize Author's Data, Create Normalized Database and Load Data**
    - **Script Name**: `create_db.py`
    - **Location**: `etl/`
    - **Input**:
        - `output_data/extracted_data.csv`
	    - `etl/config_vars.py`
    - **Output**:
        - `output_data/nyt_db.db`
        - `output_data/extracted_data_clean.csv`  *(Not used at the moment)*
    - **Description**: <br> 
        * **Cleans** the data using Python to address issues in the authors column.
        * **Normalizes** the relation between articles and authors. One article can have none or multiple authors. One author could have written one or more articles.
        * **Stores** the data in a SQLite database with three tables: <br>
        `article`, `author` and `article_author` *(this last is a composite_table)*
   - **Dependencies**:
        - Python 3
        - SQLite
        - Python libraries: pandas
    - **Usage**:
        * Make sure you have the file `output_data/extract_data.csv` file
        * Install pandas in your virtual environment. *(SQLite is included in python)* <br>
        `pip install pandas`
        * Run the script using the following command: <br>
        `python3 create_db.py` <br>
        This script will read the CSV file from the `output_data/` directory, extract the relevant data, normalize it and store it in a SQLite Database named nyt_db.db in the `output_data/` directory.
        * You can view the SQLite Database with https://sqlitebrowser.org/ or https://sqlitestudio.pl/ 

6. **<u>STAGE D</u>**: Run and Check the API **Data Consumption**
    - **Script Name**: `main.py`
    - **Location**: `api/`
    - **Input**:
        - `output_data/nyt_db.db` 
	    - `etl/config_vars.py`
    - **Output**:
        - `output_data/nyt_db.db` 
    - **Description**: Creates an API using FastAPI for querying or modifying(insert) the SQLite database.
    - **Dependencies**:
        - Python 3
        - SQLite
        - Python libraries:  fastapi, uvicorn, databases, aiosqlite
    - **Usage**:
        * Make sure you have a the Database on `output_data/ny_db.db`
        * Install the modules fastapi, uvicorn, databases, aiosqlite in your virtual environment  
        `pip install fastapi`  
        `pip install uvicorn`  
        `pip install databases`  
        `pip install aiosqlite`  
        * Run the script using the following command:
        `python -m uvicorn main:app --reload` (if you close your terminal the process is finished). You can try this if problems arises `uvicorn main:app --host 0.0.0.0 --reload`
        * Go to your browser  
        [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  
        You can query the database based in the authors and insert articles with new or old authors. So you can test the integrity and normalization of the database
        * In case you want to interrupt the api:  `CTRL + C`

<br>

****

# How To Test this Pipeline

Currently, I have these two tests with the goal of testing the integrity of the pipeline and the normalization of the database.  
Additional tests/information can be viewed on the running API through different endpoints

## 1. Test if all articles retrieved from the NYT API, were inserted in the SQLite Database

| Step |    Tool   |         ToDo                 |
|------|-----------|----------------------------- |
|1.| Editor | Config years and months to request from NYT API in `etl/config_vars.py` |
|2.| Terminal | Run `docker compose up` |
|3.| Terminal | Run this command `grep -roh '{"abstract":' . \| wc -w ` on the `input_data/` folder. This will count the number of articles in all JSON files. |
|4.| SQLite DB Viewer | Check the last row of the DB, check the number of rows of `article` table. |

<br>

## 2. Test the **Data Consistency** of the Authors in the Database <br> (Querying or Inserting )

### Follw these instructions

| Step |    Tool   |         ToDo                 |
|------|-----------|----------------------------- |
|1.| Editor | Config years and months to request from NYT API in `etl/config_vars.py` |
|2.| Terminal | Check no containers are running `docker ps -a` |
|3.| Browser| 127.0.0.1:8000 (Shouldn't work) |
|4.| Terminal | Run `docker compose up` |
|5.| Browser| 127.0.0.1:8000 (Should work)|
|6.| Editor | Open last JSON file. Check the last article's byline author(s) |
|7.| SQLite DB Viewer | Check the last row of the DB, check that  this author is in the `article` and `author` tables |
|8.| Browser| Search this author using the endpoint `/test_inserted_author` |
|9.| Browser| In the endpoint `/test_inserted_author` check if, for example, the author "Max Mustermann" exists|
|10.| SQLite DB Viewer| Check if, for example, the author "Max Mustermann" exists in the `article` and `author` tables |
|11.| Browser| Insert one dummy article written by "Max Mustermann" using the endpoint `/insert_new_article_with_new_author`|
|12.| Browser| In the endpoint `/test_inserted_author` check if the author "Max Mustermann" exists with the dummy info entered |
|13.| SQLite DB Viewer| Check if the inserted author "Max Mustermann" exists in the `article` and `author` tables, normally at the end. |

### This Demo Video shows the previous instructions.  
Take a look for guidance.  
\* *The two endpoints names were modified and they are together in the api-enpoint `Test Insert Author` group.*

[![Test NYT Data ETL Pipeline Demo Video](./images/test_author_consistency_video_button.jpg)](https://youtu.be/OaLhPuH9rxM "Test NYT Data ETL Pipeline Demo Video")

<br>

****

## Known issues (Work in progess)

1. There is a NYT API call limit.

    > *There are two rate limits per API:*  
    > * ***500 requests per day*** *and*  
    > * ***5 requests per minute***.
    > *You should sleep 12 seconds between calls to avoid hitting the per minute rate limit. https://developer.nytimes.com/faq*

   In case you exceed those limits, you will get JSON files without relevant information, but with the error message inside.

2. This project is still ongoing.  :wink:


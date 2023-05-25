from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from databases import Database
from datetime import datetime

# --------------------------------------------
# Initialization, groups
app = FastAPI(
                title="Read NYT Archive API Results from a SQLite Database",
                description="► Powered by FastAPI and SQLite.",
                version="0.1",
                openapi_tags=[
                                {
                                    'name': 'Status',
                                    'description': 'Checks API Status'
                                },
                                {
                                    'name': 'Database Statistics',
                                    'description': 'Database Statistics'
                                },
                                {
                                    'name': 'Query Authors',
                                    'description': 'Query, search author defined by the user'
                                },
                                {
                                    'name': 'Info Authors',
                                    'description': 'Retrieve predefined info-queries about authors'
                                },
                                {
                                    'name': 'Test Insert Author',
                                    'description': 'Test the DB-Normalization inserting one article with a new author in the DB, and after checking the author table'
                                }
                            ]
)

database = Database("sqlite:///../output_data/nyt_db.db")

# --------------------------------------------
@app.on_event("startup")
async def database_connect():
    await database.connect()

# --------------------------------------------
@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()

    
# ============================================
#  STATUS
# ============================================    
# Check thif if the api is working
@app.get(
    "/",
    name = "Check thif if the api is working",
    tags = ['Status']        
)
async def get_index():
    return { 'API is running!': 'More info at: http://127.0.0.1:8000/docs' }


# ============================================
#  DB statistics
# ============================================

# --------------------------------------------
# Retrieve the total number of rows per table
@app.get(
    "/rows_per_table",
    name = "Total number of rows per table",
    tags = ['Database Statistics']        
)
async def fetch_data():
    query = '''
            SELECT 
                'article' AS 'table_name',
                COUNT(*) AS 'total_rows'
            FROM 
                article

            UNION

            SELECT 
                'author' AS 'table_name',
                COUNT(*) AS 'total_rows'
            FROM 
                author
            
            UNION

            SELECT
                'article_author' AS 'table_name', 
                COUNT(*) AS 'total_rows'
            FROM 
        	    article_author         
            '''
    query = query.format()
    results = await database.fetch_all(query=query)
    return  results


# ============================================
#  Query Authors
# ============================================

# --------------------------------------------
# Retrieve the name of the authors containing the string entered
@app.get(
    "/author",
    name = "Retrieve the names of authors that contain the [search string]",
    tags = ['Query Authors']        
)
async def fetch_data(author: str):
    query = '''
            SELECT 
                * 
            FROM 
                author
            WHERE 
                author_name LIKE '%{author}%'
            LIMIT 5          
            '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Visualize the count of articles authored by [author name]
# that include the [exact word] in the 'headline_main' field
@app.get(
    "/articles_count_with_word_in_headline_by_author",
    name = "Visualize the count of articles authored by [author name] that include the [exact word] in the 'headline_main' field",
    tags = ['Query Authors']        
)
async def fetch_data(author: str, word: str):
    query = '''
            SELECT 
                au.author_name, 
                COUNT(ar.article_id) AS total_articles, 
                '{word}' AS word_in_headline
            FROM 
                article ar
                JOIN 
                    article_author arau ON ar.article_id  = arau.article_id
                JOIN 
                    author au           ON arau.author_id = au.author_id
            WHERE 
                ar.headline_main LIKE '%{word}%'
                AND 
                au.author_name LIKE '%{author}%'
            GROUP BY 
                au.author_name
            ORDER BY 
                total_articles DESC, 
                au.author_name ASC
            LIMIT 20;         
            '''
    query = query.format(word=word, author=author)
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Visualize the count of articles authored by [author name] in each section
@app.get(
"/articles_count_by_section_by_author",
    name = "Visualize the count of articles authored by [author name] in each section",
    tags = ['Query Authors']        
)
async def fetch_data(author: str):
    query = '''
            SELECT 
                ar.section_name, 
                COUNT(*) AS total_articles_in_section
            FROM 
                article ar
                JOIN 
                    article_author arau ON ar.article_id  = arau.article_id
                JOIN 
                    author au           ON arau.author_id = au.author_id
            WHERE 
                au.author_name LIKE '%{author}%'
            GROUP BY 
                ar.section_name
            ORDER BY 
                total_articles_in_section DESC;         
            '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Visualize the count of articles written by authors
# whose name contains the [search string]
@app.get(
    "/articles_count_by_author",
    name = "Visualize the count of articles written by authors whose name contains the [search string]",
    tags = ['Query Authors']        
)
async def fetch_data(author: str):
    query = '''
            SELECT 
                au.author_name, 
                COUNT(ar.article_id) AS total_articles
            FROM 
                article ar
                JOIN 
                    article_author arau ON ar.article_id  = arau.article_id
                JOIN 
                    author au           ON arau.author_id = au.author_id
            WHERE 
                au.author_name LIKE '%{author}%'
            GROUP BY 
                au.author_name
            ORDER BY 
                total_articles DESC, au.author_name ASC
            LIMIT 20;         
            '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Visualize the count of articles authored by each author,
# grouped by year and month.
@app.get(
    "/articles_count_by_author_per_year_month",
    name = "Visualize the count of articles authored by [author name] string, grouped by year and month",
    tags = ['Query Authors']        
)
async def fetch_data(author: str):
    query = '''
            SELECT 
            	au.author_name,
                STRFTIME('%Y', a.a_date) AS year,
            	STRFTIME('%m', a.a_date) AS month,
                COUNT(*) as articles_written
            FROM 
                article a
                JOIN 
                    article_author aa ON a.article_id = aa.article_id
                JOIN 
                    author au ON aa.author_id = au.author_id
            WHERE
            	au.author_name LIKE "%{author}%"
            GROUP BY 
                au.author_name, 
                year
            ORDER BY 
            	au.author_name,
            	year,
            	month;
            '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results


# ============================================
#  Info Authors
# ============================================

# --------------------------------------------
# Rank the authors in each section by the count of their articles
# and visualize the top author in each section.
@app.get(
    "/top_authors_by_section",
    name = "Rank the authors in each section by the count of their articles and visualize the top author in each section",
    tags = ['Info Authors']        
)
async def fetch_data():
    query = '''
            WITH section_author_count AS (
                SELECT 
                    a.section_name, 
                    au.author_name, 
                    COUNT(*) as article_count
                FROM 
                    article a
                    JOIN 
                        article_author aa ON a.article_id = aa.article_id
                    JOIN 
                        author au ON aa.author_id = au.author_id
                GROUP BY 
                    a.section_name, 
                    au.author_name
            )

            SELECT 
                s1.section_name, 
                s1.author_name AS top_author_name, 
                s1.article_count
            FROM 
                section_author_count s1
                JOIN 
                    section_author_count s2 ON s1.section_name = s2.section_name AND s1.article_count <= s2.article_count
            GROUP BY 
                s1.section_name, 
                s1.author_name
            HAVING 
                COUNT(*) <= 1
            ORDER BY 
                s1.section_name, 
                s1.article_count DESC;         
            '''
    query = query.format()
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Rank authors in each section by word count
# and visualize the author with the highest word count in each section.
@app.get(
    "/most_prolific_authors_by_section",
    name = "Rank authors in each section by word count and visualize the author with the highest word count in each section",
    tags = ['Info Authors']        
)
async def fetch_data():
    query = '''
            WITH section_author_wordcount AS (
                SELECT 
                    a.section_name, 
                    au.author_name, 
                    SUM(a.word_count) as total_word_count
                FROM 
                    article a
                    JOIN 
                        article_author aa ON a.article_id = aa.article_id
                    JOIN 
                        author au ON aa.author_id = au.author_id
                GROUP BY 
                    a.section_name, 
                    au.author_name
            )

            SELECT 
                s1.section_name, 
                s1.author_name, 
                s1.total_word_count
            FROM 
                section_author_wordcount s1
                JOIN 
                  section_author_wordcount s2 ON s1.section_name = s2.section_name AND s1.total_word_count <= s2.total_word_count
            GROUP BY 
                s1.section_name, 
                s1.author_name
            HAVING 
                COUNT(*) <= 1
            ORDER BY 
                s1.total_word_count DESC;         
            '''
    query = query.format()
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
# Identify pairs of authors 
# and visualize the count of articles they co-authored.
@app.get(
    "/count_pairs_authors_collaboration",
    name = "Identify pairs of authors and visualize the count of articles they co-authored",
    tags = ['Info Authors']
)
async def fetch_data():
    query = '''
            SELECT 
                aa1.author_id AS author1_id,
                au1.author_name AS author1_name,
                aa2.author_id AS author2_id,
                au2.author_name AS author2_name,
                COUNT(*) AS coauthored_articles_count
            FROM 
                article_author aa1
                JOIN 
                    article_author aa2 ON aa1.article_id = aa2.article_id AND aa1.author_id < aa2.author_id
                JOIN 
                    author au1 ON aa1.author_id = au1.author_id
                JOIN 
                    author au2 ON aa2.author_id = au2.author_id
            GROUP BY 
                author1_id, 
                author2_id
            ORDER BY 
                coauthored_articles_count DESC;         
            '''
    query = query.format()
    results = await database.fetch_all(query=query)
    return  results


# ============================================
#  Test Insert Author
# ============================================

# --------------------------------------------
# Show the articles written by an author ordered latest first.
# Can check the inserted one
@app.get(
    "/test_inserted_author",
    name = "Show the articles written by an author ordered latest first. You can check the inserted one",
    tags = ['Test Insert Author']        
)
async def test_inserted(author: str):
    query = '''
            SELECT 
                ar.article_id, 
                ar.abstract, 
                ar.headline_main, 
                a_date || ' ' || a_time, 
                au.author_id, 
                au.author_name
            FROM 
                article ar
                JOIN 
                    article_author arau ON ar.article_id  = arau.article_id
                JOIN
                    author au           ON arau.author_id = au.author_id
            WHERE  
                au.author_name LIKE '%{author}%'
            ORDER BY 
                ar.article_id DESC
            LIMIT  20;         
          '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results

# --------------------------------------------
#  INSERT
# --------------------------------------------
# Insert a new article with its one author.
# Check if the author exists or not, taking care of the DB integrity
@app.post(
    "/insert_new_article_with_new_author",
    name = "Insert a new article with a new author. Check if the author exists or not, taking care of the DB integrity",
    tags = ['Test Insert Author']        
)
async def insert_article(
                abstract: str,
                section_name: str,
                headline_main: str,
                authors: str
):
    
    # ====== INSERT into TABLE 'articles'
    now = datetime.now()

    a_date = now.strftime("%Y-%m-%d")
    a_time = now.strftime("%H:%M:%S")
    
    query = '''
            INSERT OR IGNORE INTO article (abstract, section_name, headline_main, a_date, a_time, authors) 
            VALUES (:abstract, :section_name, :headline_main, :a_date, :a_time, :authors)
            '''
    values = {"abstract": abstract,
              "section_name": section_name,
              "headline_main": headline_main,
              "a_date": a_date,
              "a_time": a_time, 
              "authors": authors }
        
    await database.execute(query=query, values=values)

    query = '''
            SELECT 
                MAX(article_id) AS max_article_id 
            FROM 
                article
            '''
    result = await database.fetch_all(query=query)
    
    last_article_id = result[0].max_article_id
    
    
    # ======= INSERT into TABLE 'author'

    # Search author to be inserted if exist or not in the table 'author'
    query = ''' 
                SELECT 
                    COUNT(author_id) AS count_author_id
                FROM 
                    author 
                WHERE 
                    author_name LIKE '{authors}';
            '''
    query = query.format(authors=authors)
    result = await database.fetch_all(query=query)
    author_exits = result[0].count_author_id
    
    # If the author does NOT exists, insert it and then retrieve his author_id
    author_id = ''
    if author_exits == 0:
        query = '''
                    INSERT OR IGNORE INTO author (author_name) 
                    VALUES (:authors)
                '''
        values = {"authors": authors}
        
        await database.execute(query=query, values=values)

        query = '''
                    SELECT 
                        MAX(author_id) AS max_author_id 
                    FROM 
                        author
                '''
        result = await database.fetch_all(query=query)
    
        author_id = result[0].max_author_id
    # If the author exists, retrieve his author_id
    else:
        query = ''' 
                    SELECT 
                        author_id
                    FROM 
                        author 
                    WHERE 
                        author_name LIKE '{authors}';
                '''
        query = query.format(authors=authors)
        result = await database.fetch_all(query=query)
        author_id = result[0].author_id        
    
    
    # ======= INSERT into TABLE 'article_author'
    # Insert ids on composite table 'article_author'   
    query = '''
                INSERT OR IGNORE INTO article_author (article_id, author_id) 
                VALUES (:article_id, :author_id)
            '''
    values = {
                "article_id": last_article_id,
                "author_id" : author_id
             }
        
    await database.execute(query=query, values=values)

    # End result
    return  {"Status": "Inserted OK"}
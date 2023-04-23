from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from databases import Database
from datetime import datetime

# ------------------------------
# Initialization
app = FastAPI(
                title="Read NYT Archive API Results",
                description="► Powered by FastAPI.",
                version="0.1",
                openapi_tags=[
                                {
                                    'name': 'Status',
                                    'description': 'Checks API Status'
                                },
                                {
                                    'name': 'Queries',
                                    'description': 'Query DB'
                                },
                                {
                                    'name': 'Insert',
                                    'description': 'Insert one article in the DB. Check if author exists or not to save it.'
                                }
                            ]
)

database = Database("sqlite:///../output_data/nyt_db.db")


# ------------------------------
@app.on_event("startup")
async def database_connect():
    await database.connect()

# ------------------------------
@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()

    
# ============================================
#  STATUS
# ============================================    
# Check that the api is working
@app.get("/",
        name = "Check status",
        tags = ['Status']        
        )
async def get_index():
    return { 'API is running!': 'More info at: http://127.0.0.1:8000/docs' }



# ============================================
#  QUERIES
# ============================================

# ------------------------------
# Retrieve the authors containing the string entered
@app.get("/author",
        name = "Retrieve the authors containing the string entered",
        tags = ['Queries']        
        )
async def fetch_data(author: str):
    query = '''
            SELECT * 
            FROM author
            WHERE author_name LIKE '%{author}%'
            LIMIT 5          
          '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results

# ------------------------------
# Show the nummer of articles that an author
# has written about a topic (exact word in headline_main)
@app.get("/articles_author_word",
        name = "Show the nummer of articles that one author has written about a topic (exact word in the headline_main)",
        tags = ['Queries']        
        )
async def fetch_data(author: str, word: str):
    query = '''
            SELECT au.author_name, COUNT(ar.article_id) AS total_articles, '{word}' AS with_word
            FROM article ar
              JOIN article_author arau ON ar.article_id  = arau.article_id
              JOIN author au           ON arau.author_id = au.author_id
            WHERE ar.headline_main LIKE '%{word}%'
              AND au.author_name LIKE '%{author}%'
            GROUP BY au.author_name
            ORDER BY total_articles DESC, au.author_name ASC
            LIMIT 20;         
          '''
    query = query.format(word=word, author=author)
    results = await database.fetch_all(query=query)
    return  results


# ------------------------------
# Show the articles written by an author ordered latest first.
# Can check the inserted one
@app.get("/test_inserted",
        name = "Show the articles written by an author ordered latest first. You can check the inserted one",
        tags = ['Queries']        
        )
async def test_inserted(author: str):
    query = '''
            SELECT ar.article_id, ar.abstract, ar.headline_main, a_date || ' ' || a_time, au.author_id, au.author_name
            FROM   article ar
              JOIN article_author arau ON ar.article_id  = arau.article_id
              JOIN author au           ON arau.author_id = au.author_id
            WHERE  au.author_name LIKE '%{author}%'
            ORDER BY ar.article_id DESC
            LIMIT  20;         
          '''
    query = query.format(author=author)
    results = await database.fetch_all(query=query)
    return  results


# ============================================
#  INSERT
# ============================================
# Insert a new article with its one author.
# Check if the author exists or not, taking care of the DB integrity
@app.post("/insert_article",
        name = "Insert a new article with his one author. Check if the author exists or not, taking care of the DB integrity",
        tags = ['Insert']        
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
    
    query = '''INSERT OR IGNORE INTO article (abstract, section_name, headline_main, a_date, a_time, authors) 
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
            SELECT MAX(article_id) AS max_article_id 
            FROM article
            '''
    result = await database.fetch_all(query=query)
    
    last_article_id = result[0].max_article_id
    
    
    # ======= INSERT into TABLE 'author'

    # Search author to be inserted if exist or not in the table 'author'
    query = ''' 
                SELECT COUNT(author_id) AS count_author_id
                FROM author 
                WHERE author_name LIKE '{authors}';
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
                    SELECT MAX(author_id) AS max_author_id 
                    FROM author
                '''
        result = await database.fetch_all(query=query)
    
        author_id = result[0].max_author_id
    # If the author exists, retrieve his author_id
    else:
        query = ''' 
                    SELECT author_id
                    FROM author 
                    WHERE author_name LIKE '{authors}';
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
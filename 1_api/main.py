from typing import List, Optional, Dict
from pydantic import BaseModel, HttpUrl
from fastapi import FastAPI, HTTPException
from bson import ObjectId
import pymongo

# --------------------------------------------
# Initialization, groups
app = FastAPI(
                title="Read NYT Archive API Results Raw from downloaded JSON files",
                description="► Powered by FastAPI and MongoDB.",
                version="0.1",
                openapi_tags=[
                                {
                                    'name': 'Status',
                                    'description': 'Checks API Status'
                                },
                                {
                                    'name': 'Collection Statistics',
                                    'description': 'Collection Statistics'
                                },
                                {
                                    'name': 'Query Authors',
                                    'description': 'Query, search author defined by the user'
                                },
                                {
                                    'name': 'Info Authors',
                                    'description': 'Retrieve predefined info-queries about authors'
                                }
                            ]
)

# ============================================
#  Database connection
# ============================================    
myclient = pymongo.MongoClient("mongodb://root:1234@localhost:27017/")
nyt_db = myclient["nyt_db_mongo"]
nyt_articles_coll = nyt_db["nyt_articles_coll"]

# ============================================
#  Classes
# ============================================    
class Article(BaseModel):
    _id: str
    abstract: str
    byline: dict
    document_type: str
    headline: dict
    #keywords: List[dict]       # ignored
    lead_paragraph: str
    #multimedia: List[dict]     # ignored
    news_desk: str
    print_page: Optional[int] = None
    print_section: Optional[str] = None
    pub_date: str
    section_name: str
    snippet: str
    source: str
    type_of_material: str
    uri: str
    web_url: str
    word_count: int

class CountArticlesByAuthor(BaseModel):
   _id: Dict[str, Optional[str]]
   firstname: str
   lastname: str
   count: int

class TopAuthorBySectionName(BaseModel):
    _id: str
    top_author: dict
    count: int

    # FastAPI itself does not have an ORM (Object-Relational Mapping)
    class Config:
        orm_mode = True



# ============================================
#  STATUS
# ============================================    
@app.get(
    "/",
    name = "Check thif if the api is working",
    tags = ['Status']
)
async def get_index():
    """
    Endpoint to check if the API is running.
    Returns a JSON response indicating that the API is running and provides additional information about accessing the API documentation.
    """
    
    return { 'API is running!': 'More info at: http://127.0.0.1:8000/docs' }


# ============================================
#  Collection Statistics
# ============================================

# --------------------------------------------
@app.get(
    "/count_total_documents",
    name = "Retrieve the total number of documents in the collection",
    tags = ['Collection Statistics']
)
async def count_total_documents():
    """
    Endpoint to retrieve the total number of documents in the collection.

    Returns the total number of documents as a JSON response.
    """

    total_documents = nyt_articles_coll.count_documents({})

    return {"Total documents":total_documents}

# --------------------------------------------
@app.get(
    "/articles_count_by_section",
    name = "Count the total number of documents/articles by section name",
    tags = ['Collection Statistics']
)
async def articles_count_by_section():
    """
    Endpoint to count the total number of documents/articles by section name.

    Returns a list of section names along with the count of articles for each section, sorted in descending order of the count.
    """

    pipeline = [
        { 
            "$group": 
                    { 
                        "_id": "$section_name", 
                        "count_articles": { "$sum": 1 } 
                    }
        },
        {
            "$sort": { "count_articles": -1 }
        }
    ]

    query_results = nyt_articles_coll.aggregate(pipeline)

    return list(query_results)


# --------------------------------------------
@app.get(
    "/articles_count_by_type_of_material",
    name = "Count the total number of documents/articles by type of material",
    tags = ['Collection Statistics']
)
async def articles_count_by_type_of_material():
    """
    Endpoint to count the total number of documents/articles by type of material.

    Returns a list of material types along with the count of articles for each type, sorted in descending order of the count.
    """

    pipeline = [
        { 
            "$group": 
                    { 
                        "_id": "$type_of_material", 
                        "count_articles": { "$sum": 1 } 
                    }
        },
        {
            "$sort": { "count_articles": -1 }
        }
    ]

    query_results = nyt_articles_coll.aggregate(pipeline)

    return list(query_results)


# --------------------------------------------
@app.get(
    "/articles_count_by_document_type",
    name = "Count the total number of documents/articles by document type",
    tags = ['Collection Statistics']
)
async def articles_count_by_document_type():
    """
    Endpoint to count the total number of documents/articles by document type.

    Returns a list of document types along with the count of articles for each type, sorted in descending order of the count.
    """

    pipeline = [
        { 
            "$group": 
                    { 
                        "_id": "$document_type", 
                        "count_articles": { "$sum": 1 } 
                    }
        },
        {
            "$sort": { "count_articles": -1 }
        }
    ]

    query_results = nyt_articles_coll.aggregate(pipeline)

    return list(query_results)


# --------------------------------------------
@app.get(
    "/automatic_distribution_count_articles_per_word_count_range",
    name = "Automatic Distribution: Number of Articles per Word Count Range. Histogram",
    tags = ['Collection Statistics']
)
async def automatic_distribution_count_articles_per_word_count_range():
    """
    Endpoint to create an automatic distribution, similar to a histogram,
    for the number of articles per word count range.

    Returns a list of dictionaries containing the automatically generated word count ranges
    and the count of articles falling within each range.
    """

    pipeline = [
        {
            '$bucketAuto' : {
                 'groupBy': '$word_count',
                 'buckets': 10
             }
        }
    ]   

    query_results = nyt_articles_coll.aggregate(pipeline)

    return list(query_results)


# --------------------------------------------
@app.get(
    "/count_articles_by_range_word_count",
    name = "Count articles by a range of word count",
    tags = ['Collection Statistics']
)
async def count_articles_by_range_word_count():
    """
    Endpoint to count the number of articles based on different ranges of word count.
    The articles are grouped into buckets based on their word_count field.

    Returns a list of dictionaries containing the range boundaries, count of articles, and range labels.
    """

    pipeline = [
        {
            '$bucket' : {
                'groupBy' : '$word_count',
                'boundaries' : [ 0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500 ],
                'default' : 'Above 5500',
                'output' : {
                    'count' : { '$sum' : 1 }
                }
            }
        },
        {
            '$project': {
                'from': '$_id',
                'to': {
                    '$cond': {
                        'if': {'$eq': ['$_id', 'Above 5500']},
                        'then': 'Infinity',
                        'else': {'$add': ['$_id', 499]}
                    }
                },
                'total_articles': '$count',
                '_id': 0
            }
        }
    ]

    query_results = nyt_articles_coll.aggregate(pipeline)

    return list(query_results)


# ============================================
#  Info Authors
# ============================================

# --------------------------------------------
@app.get(
    "/list_articles_author", 
    name = "Retrive the articles written by an author [search string]. Not grouped or ordered",
    tags = ['Query Authors'],
    response_model = List[Article]
)
async def search_author(author: str):
    """
    Endpoint to retrieve the articles written by an author based on a search string. The articles
    are not grouped or ordered.

    Parameters:
    - author: The search string for the author's first name.

    Returns a list of articles written by the specified author.
    """

    articles = nyt_articles_coll.find(
                    {
                        "byline.person.firstname": author
                    }
                )
    
    return list(articles)


# --------------------------------------------
@app.get(
    "/count_articles_by_author",
    name = "Show total number of articles/documents by author [search string]",
    tags = ['Query Authors'],
    response_model = List[CountArticlesByAuthor]
)
async def count_articles_by_author(author: str):
    """
    Endpoint to show the total number of articles/documents by an author based on a search string.

    Parameters:
    - author: The search string for the author's name.

    Returns a list of dictionaries containing the author's first name, last name, and the count
    of articles/documents associated with the author.
    """

    pipeline = [
        {
            "$unwind": "$byline.person"
        },
        {
            "$match": {
                "$or": [
                    {"byline.person.firstname": {"$regex": author, "$options": "i"}},
                    {"byline.person.lastname": {"$regex": author, "$options": "i"}}
                ]
            }
        },
        {
            "$group": {
                "_id": {
                    "firstname": "$byline.person.firstname",
                    "lastname": "$byline.person.lastname"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "firstname": "$_id.firstname",
                "lastname": "$_id.lastname",
                "count": 1
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]

    query_results = list(nyt_articles_coll.aggregate(pipeline))
    
    return query_results



# ============================================
#  Info Authors
# ============================================

# --------------------------------------------
@app.get(
    "/top_authors_by_section_name",
    name = "Top Authors by Section: Most Articles Written",
    tags = ['Info Authors'],
    response_model=List[TopAuthorBySectionName]
)
async def top_authors_by_section_name():
    """
    Endpoint to show the top authors with the most articles written, grouped by section.

    Returns a list of dictionaries containing the section name, top author details (first name, last name),
    and the count of articles written by the top author in each section.
    """

    pipeline = [
       {
           "$unwind": "$byline.person"
       },
       {
           "$group": {
               "_id": {
                   "section_name": "$section_name",
                   "firstname": "$byline.person.firstname",
                   "lastname": "$byline.person.lastname"
               },
               "count": {"$sum": 1}
           }
       },
       {
           "$sort": {"count": -1}
       },
       {
           "$group": {
               "_id": "$_id.section_name",
               "top_author": {
                   "$first": {
                       "section_name": "$_id.section_name",
                       "firstname": "$_id.firstname",
                       "lastname": "$_id.lastname"
                   }
               },
               "count": {"$first": "$count"}
           }
       },
       {
            "$project": {
                "_id": 0,
                "top_author": "$top_author",
                "count": 1
            }
        },
       {
           "$sort": {"_id": 1}
       }
    ]

    query_results = list(nyt_articles_coll.aggregate(pipeline))
    
    return query_results

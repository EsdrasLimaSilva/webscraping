# airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# other tools
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from typing import List, Tuple

# time
from datetime import timedelta
from datetime import datetime

# database
import psycopg2 as db


# Url to webscrap
PAGE_URL = 'http://books.toscrape.com/'

class Book:
    title: str
    price: float
    availability: int
    rating: int
    upc: str

# ====================================

def get_numeric_rating(str_rating: str):
    str_rating = str_rating.lower()

    match str_rating:
        case 'one':
            return 1
        case 'two':
            return 2
        case 'three':
            return 3
        case 'four':
            return 4
        case 'five':
            return 5

    return -1

def parse_page(url):
    # sending a request
    response = requests.get(url)
    # parsing the HTML
    soup = bs(response.content, 'html.parser')
    return soup

# gets the book page url
def get_book_info(book_url):
    soup = parse_page(book_url)
    
    book = {}

    # getting book data
    book["title"] = soup.find("div", class_="product_main").find("h1").get_text().replace("'", "`")

    book["price"] = soup.find("div", class_="product_main").find("p", class_="price_color").get_text()
    book["price"] = float(re.sub(r'[^0-9|\.]', "", book["price"]))

    # getting availability
    book["availability"] = soup.find("div", class_="product_main").find("p", class_="availability").get_text()
    book["availability"] = int(re.sub(r'[^0-9]', "", book["availability"]))

    # getting ratin
    book["rating"] = int(get_numeric_rating(soup.find("p", class_="star-rating").get_attribute_list("class")[1]))

    book["upc"] = soup.find_all("td")[0].get_text()

    return book
    
# =================================
# =================================
# ============ Tasks ==============


def put_into_db(ti):
    books: List[Book] = ti.xcom_pull(key="books", task_ids="get_data_tsk")

    # creating a connection
    conn = db.connect('dbname=webscrap user=postgres password=postgres host=localhost port=5432')

    # opening the cursor
    cur = conn.cursor()

    try:
        # creating the table
        cur.execute("CREATE TABLE Books (title VARCHAR(200), price REAL, availability INT, rating INT, upc VARCHAR(150) PRIMARY KEY)")
        conn.commit()
    except:
        print("Relation already exists")
        conn.rollback()

    # tuples to be inserted
    tuples = tuple(tuple(book.values()) for book in books)

    # inserting into db
    cur.executemany("INSERT INTO Books(title, price, availability, rating, upc) VALUES(%s, %s, %s, %s, %s)", tuples)
    conn.commit()

    # closing the cursor connection
    cur.close()
    conn.close()

# Get data
def get_data(ti):
    # retrieving current page
    current_page = Variable.get("current_page")

    if(not current_page):
        current_page = 1
    
    current_page=int(current_page)

    # #getting the url
    url = PAGE_URL

    # # updating the page if it is greater than 1
    if(current_page > 1):
        url += f"catalogue/page-{current_page}.html"

    # getting the page
    soup = parse_page(url)

    # getting the books
    books = []
    
    i = 0
    # inserting data
    for row in soup.select("article.product_pod"):
        book_url = PAGE_URL + "catalogue/" + row.find('h3').find('a').get_attribute_list('href')[0]
        book = get_book_info(book_url)
        books.append(book)
    
    
    # updating books
    ti.xcom_push(key="books", value=books)
    # updating the current page
    Variable.set("current_page", current_page+1)


# =======================================
# =======================================
# =======================================
# ============    DAG    ================

# creating the DAG
with DAG(
    "webscraping",
    start_date=datetime.now(),
    description='Webscraping a web page',
    schedule=timedelta(minutes=5), # we get 20 books each 15 minutes
    catchup=False,
    default_args={
        'depends_on_past': True,
        'retires': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    # defining the tasks
    get_data_tsk = PythonOperator(task_id="get_data_tsk", python_callable=get_data)
    put_into_db_tsk = PythonOperator(task_id="put_into_db_tsk", python_callable=put_into_db)

    # setting up downstream and upstream
    get_data_tsk >> put_into_db_tsk
    put_into_db_tsk << get_data_tsk
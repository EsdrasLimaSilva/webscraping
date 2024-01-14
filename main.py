# airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# other tools
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs

# time
from datetime import timedelta
from datetime import datetime


# Url to webscrap
PAGE_URL = 'http://books.toscrape.com/'

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
    book["title"] = soup.find("div", class_="product_main").find("h1").get_text()

    book["price"] = soup.find("div", class_="product_main").find("p", class_="price_color").get_text()
    book["price"] = re.sub(r'[^0-9|\.]', "", book["price"])

    # getting availability
    book["availability"] = soup.find("div", class_="product_main").find("p", class_="availability").get_text()
    book["availability"] = re.sub(r'[^0-9]', "", book["availability"])

    # getting ratin
    book["rating"] = get_numeric_rating(soup.find("p", class_="star-rating").get_attribute_list("class")[1])

    book["upc"] = soup.find_all("td")[0].get_text()

    return book

# Get data
def get_data(ti):
    # retrieving current page
    current_page = ti.xcom_pull(key="current_page", task_ids="get_data_tsk", default_arg=1)

    #getting the url
    url = PAGE_URL

    # updating the page if it is greater than 1
    if(current_page > 1):
        url += f"catalogue/page-{current_page}.html"

    # getting the page
    soup = parse_page(url)

    # getting the books
    books = []
    
    i = 0
    # inserting data
    for row in soup.select("article.product_pod"):
        book_url = url + row.find('h3').find('a').get_attribute_list('href')[0]
        book = get_book_info(book_url)
        books.append(book)
    
    # updating the current page
    ti.xcom_push(key="current_page", value=current_page+1)


# =======================================
# =======================================
# =======================================
# ============    DAG    ================

# # creating the DAG
# with DAG(
#     "webscraping",
#     description='Webscraping a web page',
#     schedule=timedelta(minutes=15), # we get 20 books each 15 minutes
#     catchup=False,
#     default_args={
#         'retires': 1,
#         'retry_delay': timedelta(minutes=5)
#     }
# ) as dag:
#     # defining the tasks
#     get_data_tsk = PythonOperator(task_id="get_data_tsk", python_callable=get_data)
# Webscraping

This is a simple project to pratice webscraping with Python

## Technologies

- Python 3.10
- Airflow
- Anaconda

## Objective

This project was intended for webscraping the [Books Page](http://books.toscrape.com/) retrieving the books data. The current book schema is the following:

| attribute | type | description |
|-|-| - |
|title | string | The book's title |
|price | float | Book's price |
|rating | int| Book's rating from 1 to 5
availability | int | How many are avaiable
| upc | string | Identificator |

So the goal is reach the page, and get this data from the HTML. To parse the html the **BeaufitulSoup** was used, and to get the data the **Requests** module was used. Basically it is needed to reach the data, treat and convert and then put into a dictonary.

As a bonus this project uses Airflow to schedule and go into every page using **xcom** and also to put the data into a postgesql database instance

## Requirements

- Python installed
- Airflow up and running
- Postgesql up and running
- Anaconda installed

## How to run

**All you have to do is copy and pase the webs.py file and put into the Dags folder of airflow. If you want to edit it go ahead.**

Clone this repository and change to it. Next, create the environment based on the [environment.yml]("environment.yml") file with the following:

```
conda env create --prefix ./env --file environment.yml
```

Then activate the environment

```
conda activate ./env
```

To run copy the webs.py file into your Dags folder and open the airflow.

## Details
THis project gets the data from the website, transform it and then put the data into a postgresql instance. Do not forget to put the right info on the webs.py connection string, otherwise it won't work.

The schedule is 15 min to not overload the site with a bunch of requests, and each cycle go trough a page, updating the page number each task running, starting from 1. Each interaction gets 20 books.
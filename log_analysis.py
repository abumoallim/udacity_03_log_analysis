#!/usr/bin/env python3
from tabulate import tabulate
import psycopg2

DBNAME = "news"

database_connection = psycopg2.connect(database=DBNAME)

database_cursor = database_connection.cursor()

"""executing views"""
create_views = open("create_views.sql").read()
database_cursor.execute(create_views)

print('The most popular three articles of all time : \n')
headers = ["Article", "Views"]
most_viewed_article = """SELECT *
                       FROM all_articles_view
                       LIMIT 3"""
database_cursor.execute(most_viewed_article)
posts = database_cursor.fetchall()
print tabulate(posts, headers)
print("\n")

print('The most popular article authors of all time : \n')
headers = ["Author", "Views"]
most_author_views = """SELECT authors.name,SUM(all_articles_view.views)
                       AS total_views
                       FROM authors join articles
                       ON authors.id=articles.author
                       JOIN all_articles_view
                       ON articles.slug = all_articles_view.articles
                       GROUP BY articles.author,authors.name
                       ORDER BY total_views DESC"""
database_cursor.execute(most_author_views)
data = database_cursor.fetchall()
print tabulate(data, headers)
print("\n")

print('This day more than 1% request lead to error : \n')
headers = ["Date", "Error Percentage"]
error_value = """(tot_err_each_day_view.errors*100.0
                    /tot_req_each_day_view.errors)::float"""
error_percentage = """round((tot_err_each_day_view.errors*100.0
                        /tot_req_each_day_view.errors)::numeric,2)"""
most_error = """SELECT tot_req_each_day_view.time,{0}
                FROM tot_req_each_day_view
                LEFT JOIN tot_err_each_day_view
                ON tot_req_each_day_view.time = tot_err_each_day_view.time
                WHERE {1} > 1 ;""".format(error_percentage, error_value)
database_cursor.execute(most_error)
errors = database_cursor.fetchall()
print tabulate(errors, headers)
print("\n")


database_connection.close()

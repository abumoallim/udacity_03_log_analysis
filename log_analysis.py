#!/usr/bin/env python3
from tabulate import tabulate
import psycopg2

DBNAME = "news"

"""View for article name"""
article_name = "split_part(log.path,'/',3) as articles"

"""View for total number of views for each article"""
all_articles_view = """CREATE VIEW all_articles_view as
                       SELECT {},count(*) as views
                       FROM articles
                       JOIN log
                       ON log.path = '/article/' || articles.slug
                       GROUP BY path
                       ORDER BY views DESC""".format(article_name)

"""View for total number of request for each article"""
tot_req_each_day_view = """CREATE VIEW tot_req_each_day_view as
                           SELECT CAST(time as Date),count(*) as errors
                           FROM log
                           GROUP BY CAST(time as Date)
                           ORDER BY CAST(time as Date) DESC"""

"""View for total number of unsuccessful request for each article"""
tot_err_each_day_view = """CREATE VIEW tot_err_each_day_view as
                           SELECT CAST(time as Date),count(*) as errors
                           FROM log
                           WHERE status!='200 OK'
                           GROUP BY CAST(time as Date)"""

database_connection = psycopg2.connect(database=DBNAME)

database_cursor = database_connection.cursor()

print ('The most popular three articles of all time : \n')
headers = ["Article", "Views"]
database_cursor.execute(all_articles_view)
most_viewed_article = """SELECT *
                       FROM all_articles_view
                       LIMIT 3"""
database_cursor.execute(most_viewed_article)
posts = database_cursor.fetchall()
print tabulate(posts, headers)
print("\n")

print('The most popular article authors of all time : \n')
headers = ["Author", "Views"]
most_author_views = """SELECT authors.name,SUM(all_articles_view.views) as total_views
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

print ('This day more than 1% request lead to error : \n')
headers = ["Date", "Error Percentage"]
error_value = """(tot_err_each_day_view.errors*100.0
                    /tot_req_each_day_view.errors)::float"""
error_percentage = """round((tot_err_each_day_view.errors*100.0
                        /tot_req_each_day_view.errors)::numeric,2)"""
database_cursor.execute(tot_req_each_day_view)
database_cursor.execute(tot_err_each_day_view)
most_error = """EXPLAIN ANALYZE
                SELECT tot_req_each_day_view.time,{0}
                FROM tot_req_each_day_view
                LEFT JOIN tot_err_each_day_view
                ON tot_req_each_day_view.time = tot_err_each_day_view.time
                WHERE {1} > 1 ;""".format(error_percentage, error_value)
database_cursor.execute(most_error)
errors = database_cursor.fetchall()
print tabulate(errors, headers)
print("\n")


database_connection.close()

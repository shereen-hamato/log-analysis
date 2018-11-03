
# !Python 3.6.5
import datetime
import psycopg2
import bleach


def getPopularArticles():
    """ What are the most popular three articles of all time? """
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(" select count (*) as views, title  from articles "
              + "left join "
              + "log on concat('/article/', articles.slug) = log.path "
              + "group by title order by views desc limit 3")
    views = c.fetchall()
    db.close()
    return views


def getPopualrAuthors():
    """ Who are the most popular article authors of all time?"""
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(" select count(*) as views ,  authors.name from articles "
              + " inner join  "
              + "log on concat('/article/', articles.slug) = log.path "
              + " inner join authors on articles.author = authors.id "
              + "group by name order by views desc; ")
    authors = c.fetchall()
    db.close()
    return authors


def getWorstDays():
    """ Who are the most popular article authors of all time?"""
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(" select c.* from"
              + "(select a.* , b.* , "
              + "(cast( b.total as decimal(16,4))/a.total)*100 as percent from"
              + " (select count(*) total , time::timestamp::date as timea "
              + "from log group by timea order by timea) as a, "
              + "(select count(*) total , time::timestamp::date as timea "
              + "from log where status <> '200 OK'"
              + "group by timea order by timea ) as b   "
              + "where a.timea = b.timea) as c where c.percent > 1;")
    days = c.fetchall()
    db.close()
    return days


print "\nWhat are the most popular three articles of all time"
for a in getPopularArticles():
    print a[1], " - ", a[0], " views"
print " \nWho are the most popular article authors of all time"
for a in getPopualrAuthors():
    print a[1], " - ", a[0], " views"
print "\nOn which days did more than 1% of requests lead to errors"
for a in getWorstDays():
    print a[1], " - ", round(a[4], 2), "% errors"

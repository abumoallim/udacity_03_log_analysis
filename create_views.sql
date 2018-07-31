
-- View for article name
CREATE OR REPLACE VIEW all_articles_view as
                       SELECT split_part(log.path,'/',3) as articles,count(*) as views
                       FROM articles
                       JOIN log
                       ON log.path = '/article/' || articles.slug
                       GROUP BY path
                       ORDER BY views DESC;

-- View for total number of views for each article
CREATE OR REPLACE VIEW tot_req_each_day_view as
                           SELECT CAST(time as Date),count(*) as errors
                           FROM log
                           GROUP BY CAST(time as Date)
                           ORDER BY CAST(time as Date) DESC;

-- View for total number of unsuccessful request for each article
CREATE OR REPLACE VIEW tot_err_each_day_view as
                           SELECT CAST(time as Date),count(*) as errors
                           FROM log
                           WHERE status!='200 OK'
                           GROUP BY CAST(time as Date);

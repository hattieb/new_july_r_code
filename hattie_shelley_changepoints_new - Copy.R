####### correlation dist plot / correleation matrix ##################

library(tidyverse)
library(tidytext)
library(changepoint)
library(changepoint.np)
library(RJDBC)
library(ocp)
library(bcp)
library(magrittr)
library(dplyr) 
library(ggplot2)
library(reshape)
library("tibble")
library(stringi)
library(purrr)
library(tidyr)
library(spatstat)
library(lubridate)


###################################################################################################################################

Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_231')

# specify driver
# specify driver
jdbcDriver <- JDBC(driverClass="com.snowflake.client.jdbc.SnowflakeDriver", 
                   classPath="C:\\Users\\hatti\\Downloads\\snowflake-jdbc-3.9.2.jar") #


# Pulling data from Snowflake
jdbcConnection <- dbConnect(jdbcDriver, "jdbc:snowflake://mumsnet.us-east-1.snowflakecomputing.com", 
                            rstudioapi::askForPassword("Database user"),
                            rstudioapi::askForPassword("Database password"))

result <- dbGetQuery(jdbcConnection, 'USE WAREHOUSE "HATTIE_ANALYSIS"')

top_level <- dbGetQuery(jdbcConnection, 'SELECT TO_DATE(COLLECTOR_TSTAMP) as date,  sum(TIME_ENGAGED_IN_S) as total_seconds, COUNT(DISTINCT NETWORK_USERID) as top_level_users, 
                        count(page_url) as top_level_pageviews, count(distinct session_id) as top_levelsessions
                        FROM "SNOWPLOW"."DERIVED"."PAGE_VIEWS" where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                        and PAGE_URLHOST = \'www.mumsnet.com\' GROUP BY 1 ')


traffic_sources <- dbGetQuery(jdbcConnection, 'select TO_DATE(COLLECTOR_TSTAMP) as date, refr_medium, count(distinct session_id) as traffic_sources_sessions
                              from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                              where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                              and PAGE_URLHOST = \'www.mumsnet.com\' GROUP BY 1,2')

new_returning_users <- dbGetQuery(jdbcConnection, 'select to_date(COLLECTOR_TSTAMP) as date, 
                                  case when session_index = \'1\' then \'new_user\' else \'returning_user\' end as new_or_returning_user,
                                  count(distinct NETWORK_USERID) AS new_returning_users_users
                                  from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                  where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                  and PAGE_URLHOST = \'www.mumsnet.com\' 
                                  group by 1,2')


active <- dbGetQuery(jdbcConnection, 'select to_date(COLLECTOR_TSTAMP) as date, 
                                  count(distinct NETWORK_USERID) AS active_users,
                                  count(page_url) as active_pageviews
                                  from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                  where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                  and PAGE_URLHOST = \'www.mumsnet.com\' 
                                  and PAGE_REFERRER like \'%Talk/active%\'
                                  group by 1')

ghkjdhf
land_on_talk_thread <- dbGetQuery(jdbcConnection, 'select to_date(COLLECTOR_TSTAMP) as date, 
                                  count(distinct NETWORK_USERID) AS land_on_talk_thread_users,
                                  count(page_url) as land_on_talk_thread_pageviews
                                  from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                  where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                  and PAGE_URLHOST = \'www.mumsnet.com\' 
                                  and PAGE_URL like \'%/Talk%\'
                                  AND PAGE_VIEW_IN_SESSION_INDEX =1
                                  group by 1')

land_on_content <- dbGetQuery(jdbcConnection, 'select to_date(COLLECTOR_TSTAMP) as date, 
                                  count(distinct NETWORK_USERID) AS land_on_content_users,
                                  count(page_url) as land_on_content_pageviews
                                  from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                  where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                  and PAGE_URLHOST = \'www.mumsnet.com\' 
                                  and PAGE_URL not like \'%/Talk%\'
                                  AND PAGE_VIEW_IN_SESSION_INDEX =1
                                  group by 1')


land_on_home_page <- dbGetQuery(jdbcConnection, 'select to_date(COLLECTOR_TSTAMP) as date,
                                count(distinct NETWORK_USERID) AS land_on_homepage_users,
                                count(page_url) as land_on_homepage_pageviews
                                from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                and PAGE_URLHOST = \'www.mumsnet.com\' 
                                and PAGE_URL = \'https://www.mumsnet.com/\'
                                AND PAGE_VIEW_IN_SESSION_INDEX =1
                                group by 1')



top_n_talk_categories <- dbGetQuery(jdbcConnection, 'select date,talk_category_b,pageviews_b as top_n_talk_categories_pageviews from (
                                                      select 
                                                      split_part(page_urlpath, \'/\', 3) as talk_category_a,
                                                      count(distinct NETWORK_USERID) AS users_a,
                                                      count(page_url) as pageviews_a
                                                      from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                                      where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                                      and PAGE_URLHOST = \'www.mumsnet.com\' 
                                                      and page_url like \'%Talk%\'
                                                      group by 1 order by 3 desc limit 25) as a 
                                                      join
                                                      (select to_date(COLLECTOR_TSTAMP) as date,
                                                      split_part(page_urlpath, \'/\', 3) as talk_category_b,
                                                      count(distinct NETWORK_USERID) AS users_b,
                                                      count(page_url) as pageviews_b
                                                      from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                                      where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                                      and PAGE_URLHOST = \'www.mumsnet.com\' 
                                                      and page_url like \'%Talk%\'
                                                      group by 1,2) as b
                                                      on a.talk_category_a = b.talk_category_b')
# this is only 2 weeks worth of data

engaged_users_2_weeks <- dbGetQuery(jdbcConnection, 'select date_b as date, sum(pageviews) as engaged_users_2_weeks_pageviews,count(distinct NETWORK_USERID_B) AS engaged_users_2_weeks_users from (
                                                    select 
                                                    NETWORK_USERID as NETWORK_USERID_a,
                                                    count(distinct to_date(COLLECTOR_TSTAMP)) as days_active
                                                    from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                                    where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                                    and PAGE_URLHOST = \'www.mumsnet.com\' 
                                                    group by 1 
                                                    having days_active >7) as a 
                                                    join
                                                    (select 
                                                    to_date(COLLECTOR_TSTAMP) as date_b,
                                                    NETWORK_USERID as NETWORK_USERID_b,
                                                    count(page_url) as pageviews
                                                    from "SNOWPLOW"."DERIVED"."PAGE_VIEWS"
                                                    where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-05-10\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-26\'
                                                    and PAGE_URLHOST = \'www.mumsnet.com\' group by 1,2 ) as b
                                                    on a.NETWORK_USERID_a = b.NETWORK_USERID_b
                                                    group by 1')
                                      





# ggplot(data=engaged_users_2_weeks, aes(x=DATE, y=ENGAGED_USERS, group = 1)) +
#   geom_line()


# CONVERT DF TO TIBBLES

top_level <-  top_level %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

traffic_sources <-  traffic_sources %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

new_returning_users <-  new_returning_users %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

active <-  active %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

land_on_talk_thread <-  land_on_talk_thread %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

land_on_content <-  land_on_content %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

land_on_home_page <-  land_on_home_page %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

top_n_talk_categories <-  top_n_talk_categories %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))

engaged_users_2_weeks <-  engaged_users_2_weeks %>% as_tibble %>% mutate(DATE = ymd(DATE)) %>% arrange(desc(DATE))


head(top_n_talk_categories)

# PIVOT THE TABLES

traffic_sources <- cast(traffic_sources, DATE ~ REFR_MEDIUM)
head(traffic_sources)

new_returning_users <- cast(new_returning_users, DATE ~ NEW_OR_RETURNING_USER, value = "NEW_RETURNING_USERS_USERS")
head(new_returning_users)

top_n_talk_categories <- cast(top_n_talk_categories, DATE ~ TALK_CATEGORY_B, value = "TOP_N_TALK_CATEGORIES_PAGEVIEWS")

head(top_n_talk_categories,5)
View(top_n_talk_categories)

head(top_n_talk_categories,5)


#UPPER CASE COLUMN TITLES

names(top_n_talk_categories)[1:3] <- toupper(names(top_n_talk_categories)[1:3])
names(engaged_users_2_weeks)[1:3] <- toupper(names(engaged_users_2_weeks)[1:3])
names(new_returning_users)[1:3] <- toupper(names(new_returning_users)[1:3])
names(traffic_sources)[1:7] <- toupper(names(traffic_sources)[1:7])
names(land_on_home_page)[1:3] <- toupper(names(land_on_home_page)[1:3])
names(land_on_content)[1:3] <- toupper(names(land_on_content)[1:3])
names(land_on_talk_thread)[1:3] <- toupper(names(land_on_talk_thread)[1:3])
names(active)[1:3] <- toupper(names(active)[1:3])
names(top_level)[1:5] <- toupper(names(top_level)[1:5])

# CHANGE COLUMN NAME
names(traffic_sources)[names(traffic_sources)=='na'] <- "direct"

# REMOVING DATAFRAME COLUMNS
traffic_sources$paid <- NULL
top_n_talk_categories$v1 <- NULL
head(top_n_talk_categories)



# Joining all the dataframes together

#do.call("rbind", list(top_level,traffic_sources,new_returning_users,active,land_on_talk_thread,land_on_content,land_on_home_page,top_n_talk_categories,engaged_users_2_weeks))
full_join <-  list(top_level,traffic_sources,new_returning_users,active,land_on_talk_thread,land_on_content,land_on_home_page,engaged_users_2_weeks,top_n_talk_categories) %>% reduce(left_join, by = "DATE")
head(full_join)

correlations_against_total_time <- cor(full_join[-1], full_join$TOTAL_SECONDS) 
correlations_against_total_time
df <-  as.data.frame(t(correlations_against_total_time))
df <-  sort(df, decreasing = TRUE)
df <- melt(df, id=(c("TOTAL_SECONDS")))
colnames(df)
names(df)[names(df)=='variable'] <- "x_var"
df$TOTAL_SECONDS <- NULL
df <-  df %>% as_tibble 
df$total_seconds <- NULL
ggplot(df, aes(x=x_var, y=value)) + geom_bar(stat="identity") + theme(axis.text.x = element_text(angle = 60, hjust = 1))



############################################ CHANGE POINTS ##########################################################################


# Shelley's stuff! --------------------------------------------------


# Accepts a tibble with a DATE and y columns where y is the timeseries
find_changepoints <- function(timeseries) {
  # Bauesian Online Changepoints
  changepoint_model <- onlineCPD(timeseries %>% pull(y), getR=TRUE, optionalOutputs = TRUE,
                                 hazard_func= function(x, lambda){const_hazard(x, lambda=100)},
                                 cpthreshold = 0.3)
  
  changepoints <- changepoint_model$threshcps[changepoint_model$threshcps > 1]
  
  timeseries <- timeseries %>% mutate(r = row_number(), 
                                      changepoint = ifelse(r %in% changepoints, "bayesian", NA))
  
  # Control chart
  timeseries <- timeseries %>% 
    mutate(m = mean(y), cl = 3*sd(y), upper = m+cl, lower = m - cl, cp = (y > upper) | (y < lower)) %>% 
    mutate(changepoint = ifelse(cp, "control_chart", changepoint))
  
  
  # Bayesian changepoint detection
  bcp_results <- bcp(timeseries %>% pull(y))
  bcp_cpts <- which(bcp_results$posterior.prob > 0.5)
  
  timeseries %>% 
    mutate(changepoint = ifelse(r %in% bcp_cpts, "bcp", changepoint))
}

plot_changepoints <- function(timeseries) {
  dates <- timeseries %>% filter(!is.na(changepoint)) %>% pull(DATE)
  timeseries %>% ggplot(aes(DATE, y)) + geom_line() +
    geom_vline(xintercept = dates, color = "red", size = 0.5)
}





# Running for USERS
timeseries_with_changepoints <- find_changepoints(full_join %>% select(DATE, y = TOP_LEVEL_USERS))
plot_changepoints(timeseries_with_changepoints)

# # Running for TOTAL_SECONDS
# timeseries_with_changepoints <- find_changepoints(full_join %>% select(DATE, y = TOTAL_SECONDS))
# plot_changepoints(timeseries_with_changepoints)








################ HATTIE TEST (ignore) ###################################

# names(full_join)[1:47] <- toupper(names(full_join)[1:47])
# 
# head(full_join)
# 
# 
# 
# # Running for USERS
# timeseries_with_changepoints <- find_changepoints(full_join %>% select(DATE, y = TOP_LEVEL_USERS))
# plot_changepoints(timeseries_with_changepoints)
# 
# # Running for USERS
# timeseries_with_changepoints <- find_changepoints(full_join %>% select(DATE, y = SEARCH))
# plot_changepoints(timeseries_with_changepoints)
# 
# cpts(timeseries_with_changepoints)
#
# 
# # Running for TOTAL_SECONDS
# timeseries_with_changepoints <- find_changepoints(pageviews %>% select(DATE, y = TOTAL_SECONDS))
# plot_changepoints(timeseries_with_changepoints)
# 
# 
# ??BCP



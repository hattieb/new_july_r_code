# Installing
#install.packages("tibble")
# Loading
library("tibble")
library(tidyverse)
library(stringi)
library(tidytext)
library(purrr)
library(tidyr)
library(spatstat)
library(ggplot2)
#install.packages("magrittr") # package installations are only needed the first time you use it
#install.packages("dplyr")    # alternative installation of the %>%
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)  
library(lubridate)
if(!require(changepoint)){
  install.packages('changepoint')
}
library(changepoint)
if(!require(changepoint.np)){
  install.packages('changepoint.np')
}

library(changepoint.np)



###################################################################################################################################

Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_231')

library(RJDBC)

# specify driver
jdbcDriver <- JDBC(driverClass="com.snowflake.client.jdbc.SnowflakeDriver", 
                   classPath="C:\\Users\\hatti\\Downloads\\snowflake-jdbc-3.9.2.jar") #


# Pulling data from Snowflake

jdbcConnection <- dbConnect(jdbcDriver, "jdbc:snowflake://mumsnet.us-east-1.snowflakecomputing.com", 
                            rstudioapi::askForPassword("Database user"),
                            rstudioapi::askForPassword("Database password"))

result <- dbGetQuery(jdbcConnection, 'USE WAREHOUSE "HATTIE_ANALYSIS"')

pageviews <-dbGetQuery(jdbcConnection, paste0('SELECT TO_DATE(COLLECTOR_TSTAMP) as date, COUNT(DISTINCT NETWORK_USERID) as users, sum(TIME_ENGAGED_IN_S) as total_seconds FROM "SNOWPLOW"."DERIVED"."PAGE_VIEWS" where TO_DATE(COLLECTOR_TSTAMP)>=\'2020-01-01\' and TO_DATE(COLLECTOR_TSTAMP)<=\'2020-06-22\'  GROUP BY 1 '))
                       
head(pageviews)                      
                       
class(pageviews)
#pageviews <- as_tibble(pageviews)


#pageviews <- pageviews %>% select(1:2)
pageviews

pageviews <-  pageviews %>%  mutate(DATE = ymd(DATE))



# Plotting the data

# plot <- ggplot()
# + geom_line(aes(y=USERS, x =DATE ), data= pageviews) + geom_line(aes(y=TOTAL_SECONDS, x =DATE ), data= pageviews) 
# 
# 
# plot

df <- ggplot(pageviews, aes(x=DATE)) + 
  geom_line(aes(y = USERS), color = "darkred") + 
  geom_line(aes(y = TOTAL_SECONDS), color="steelblue", linetype="twodash")

#df  + scale_y_continuous(sec.axis = sec_axis(~ . + 10), name = 'test')

df <- df + scale_y_log10()
df
# Filtering the dataframes

users <- pageviews %>% select(USERS)
head(users)
class(users$USERS)
class(users)

# users_vector <- as.vector(users['USERS'])
# class(users_vector)


users_vector <- users[['USERS']]
class(users_vector)


# ggplot(users, aes(x=DATE)) + 
#   geom_line(aes(y = USERS), color = "darkred") 

total_time <- pageviews %>% select(TOTAL_SECONDS)
head(total_time)


total_time_vector <- total_time[['TOTAL_SECONDS']]
class(total_time_vector)
head(total_time_vector)

# ggplot(total_time, aes(x=DATE)) + 
#   geom_line(aes(y = TOTAL_SECONDS), color = "steelblue") 



############################################## changepoints ###############################################

set.seed(1)
#v1=c(rnorm(100,0,1),rnorm(100,0,2),rnorm(100,0,10), rnorm(100,0,9))
v1 <-  users_vector
v1.man <- cpt.var(v1,method='PELT',penalty='Manual',pen.value=1)
cpts(v1.man)
param.est(v1.man)
plot(v1.man,cpt.width=2)


set.seed(1)
#v1=c(rnorm(100,0,1),rnorm(100,0,2),rnorm(100,0,10), rnorm(100,0,9))
v1 <-  total_time_vector
v1.man <- cpt.var(v1,method='PELT',penalty='Manual',pen.value=1)
cpts(v1.man)
param.est(v1.man)
plot(v1.man,cpt.width=2)


hdjshak





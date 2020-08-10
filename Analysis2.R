# Analysis2.R
#
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
#
# In this analysis the focus is on identifying the best publisher. The best
# publisher is someone who released the most number of games over the period

# Loading the necessary libraries
library(rpart)
library(rpart.plot)
library(RMySQL)

# Create a database connection using the credentials root, password. 
mydb = dbConnect(MySQL(), user='root', password='password', dbname='Sales', host='localhost')

# Formulate the query, execute and fetch the data from the object
rs = dbSendQuery(mydb, "SELECT Publisher_Name, COUNT(Game_ID) AS Published_Count
                 FROM Game 
                 JOIN Publisher ON Publisher_ID=Game.Publisher 
                 GROUP BY Publisher_Name 
                 ORDER BY Published_Count DESC 
                 LIMIT 10;")

# Fetching the data from the query result object
data = fetch(rs, n =-1)
ylim <- c(0,max(data['Published_Count'])*1.1);
barplot(
  t(data[c('Published_Count')]),
  ylim=ylim,
  col=c("darkblue","red"),
  names.arg=data$Publisher_Name,
  main = 'Top 10 Video Game Publishers',
  xlab='Publisher Name',
  ylab='Number of Games',
  las = 2,
)
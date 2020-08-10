# Analysis1.R
#
# @author: Santosh Kumar Nunna (sn7916@rit.edu)
#
# In this analysis the focus is on identifying the genre that has the most 
# number of games being released.
# Loading the necessary libraries
library(rpart)
library(rpart.plot)
library(RMySQL)

# Create a database connection using the credentials root, password. 
mydb = dbConnect(MySQL(), user='root', password='password', dbname='PUBG', host='localhost')

# Formulate the query, execute and fetch the data from the object
rs = dbSendQuery(mydb, "SELECT Genre_Name, COUNT(Game_ID) AS Game_Count 
                 FROM Game JOIN Genre ON Genre_ID=Genre 
                 GROUP BY Genre_ID 
                 ORDER BY Game_Count DESC;")

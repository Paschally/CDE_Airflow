
Task:
You have been hired by a data consulting organization,
who is looking at building a stock market prediction tool that applies sentiment analysis, called CoreSentiment. 
To perform this sentiment analysis, they plan to leverage the data about the number of Wikipedia page views a company has.

Wikipedia is one the largest public information resources on the internet. 
Besides the wiki pages, other items such as website pageview counts are also publicly available. 
To make things simple, they assume that an increase in a company’s website page views shows a positive sentiment, and the company’s stock is likely to increase. 
On the other hand, a decrease in pageviews tells us a loss in interest, and the stock price is likely to decrease.

Steps to write a DAG File for this Orchestration Task:
i Import the necessary libraries and create an instance od DAG Class.
ii. Write a python function to download the .gz file 
iii. Create a task using PythonOperator to call this function
iii. Use a BashOperator to extract the .gz file
iv. Fetch data and insert into the database
v. Write an SQL file to insert the data into the database
vi. PostgresOperator to load data into the PostgreSQL database
vii. Create Task dependencies

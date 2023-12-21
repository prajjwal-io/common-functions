import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pandas as pd
import pypyodbc
import tldextract
import json
import logging
from logging import handlers
import sys
import requests
from urllib.request import urlopen
import urllib3
import datetime as dt




load_dotenv()

user = os.environ.get("db_user_id")
password = os.environ.get("db_password")
host = os.environ.get("db_server")
database = os.environ.get("db_database")

def database_connect():
    """
    conncet Sfrogs Databse 
    """
    con = pypyodbc.connect('Driver={SQL Server};SERVER='+host+';PORT=1433;DATABASE='+database+';UID='+user+';PWD='+ password)
    pypyodbc.lowercase = False
    return con

def select_data_from_database(con,query):
    """
    function take SQL query and SQL connection  as input  and give the result of query as DataFrame
    Args:
    -------
        con :- SQL Connectivity from the DataBase
        query :- SQL select Query like 'select  * from table'
    Returns:
    -----------
        return table as DataFrame of sql query's result
    """
    table = pd.read_sql_query(query,con,chunksize=100000)
    return table

def get_input_domains():
    #Connect Program to the database
    try:
        con = database_connect()
        cur = con.cursor()
        table = select_data_from_database(con,"select Name, LinkedInURL from [sfrogs].[dbo].[Organization] where LinkedInURL is not NULL and Name is not NULL")
        table = pd.concat(table)
        # for i in table:
        #     df = pd.concat(i)
    except Exception as e:
        print(e)
    con.close()
    return table

con = database_connect()
cur = con.cursor()

table=get_input_domains()
table.to_csv('Linkedinurl.csv')

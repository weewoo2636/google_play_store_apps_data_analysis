'''
=================================================
Milestone 3

Name: Wilson
Batch: FTDS-016-HCK
Objective: This program is made to extract data from postgres, transform data until it's analysis ready, and load data into elasticsearch  
Dataset overview: Our data contains information about apps released on Google Play Store
=================================================
'''

# import libraries
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine # connection to postgres


# global variables definition
db_name = 'airflow_m3'
username = 'airflow_m3'
password = 'airflow_m3'
host = 'postgres'

# establish connection
postgres_url= f'postgresql+psycopg2://{username}:{password}@{host}/{db_name}'
engine = create_engine(postgres_url)
connection = engine.connect()

es = Elasticsearch('http://elasticsearch:9200')

# file paths
source_data_path = '/opt/airflow/dags/P2M3_wilson_data_source.csv' # local file that will be sent to postgres
raw_data_path = '/opt/airflow/dags/P2M3_wilson_data_raw.csv'
cleaned_data_path = '/opt/airflow/dags/P2M3_wilson_data_clean.csv'

default_args = {
    'owner': 'wilson',
    'start_date': dt.datetime(2024, 6, 20)
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=5),
}


def csv_to_postgres():
    '''
    this function is made to read a csv file 
    then create a table in the postgres server defined b y arguments received
    '''

    print(connection)
    # load csv from path
    df = pd.read_csv(source_data_path) # local file that will be sent to postgres

    # create a table from df in the connected server
    df.to_sql(name='table_m3', con=connection, if_exists='replace', index=False)
    print(connection)


def postgres_to_csv():
    '''
    this function is made to pull a table from the postgres server defined by arguments received
    then saving it in csv file format
    '''

    # load table from connected server
    df = pd.read_sql_query('select * from table_m3', connection)

    # save loaded table into csv format
    df.to_csv(raw_data_path, sep=',', index=False)


def suffix_to_number(x:str):
    '''
    this function is made to transform number suffix during data cleaning like
    'M' to '000000', and 'k' to '000', and adjusting accordingly if there's decimal value
    '''

    new_x = x
    if 'k' in x:
        if '.' in x:
            new_x = x.replace('.', '')
            new_x= new_x.replace('k', '00')
        else:
            new_x = x.replace('k', '000')
    elif 'M' in x:
        if '.' in x:
            new_x = x.replace('.', '')
            new_x = new_x.replace('M', '00000')
        else:
            new_x = x.replace('M', '000000')
    return new_x


def clean_data():
    '''
    this function is made to clean data that's loaded from specified path 
    and saving it in csv file format
    '''
    
    # load csv from path
    df = pd.read_csv(raw_data_path)

    # remove unnecessary column ['Unnamed: 0']
    # because it represents the same thing as index
    df.drop(columns=['Unnamed: 0'], inplace=True)

    # change column names to follow snake_case format
    # for easier query and to use uniformed format
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')

    # remove duplicated rows
    # to avoid double information
    df.drop_duplicates(inplace=True) # the whole row
    df.drop_duplicates(subset=['app'], keep='last', inplace=True) # only keep the latest app information

    # drop missing values
    # because we couldn't find any pattern within their own column 
    # nor any relation to other columns
    index_w_na = df[df['rating'].isnull() | df['type'].isnull() | 
                            df['content_rating'].isnull() | df['current_ver'].isnull() |
                            df['android_ver'].isnull()].index
    df.drop(index=index_w_na, inplace=True)
    df.reset_index(inplace=True, drop=True)

    # turn multiple values in column 'genres' into list format
    df['genres'] = df['genres'].str.split(';')

    # add column 'primary_genre' and 'secondary_genre' as a temporary solution to column with multiple values 
    # for easier query and visualization
    primary_genre_data = []
    secondary_genre_data = []

    for g in df['genres']:
        primary_genre_data.append(g[0])
        if len(g) > 1:
            secondary_genre_data.append(g[1])
        else:
            secondary_genre_data.append('-')

    df.insert(9, 'primary_genre', primary_genre_data)
    df.insert(10, 'secondary_genre', secondary_genre_data)

    # remove unnecessary characters/words and
    # turn necessary characters/words into their numerical form
    # on numeric columns
    # to prevent error during data type conversion
    df['reviews'] = df['reviews'].apply(suffix_to_number)
    df['size'] = df['size'].apply(suffix_to_number)
    df['size'] = df['size'].str.replace(r'[,+]', '', regex=True)
    df['size'] = df['size'].str.replace('Varies with device', '-1')
    df['installs'] = df['installs'].str.replace(r'[,+]', '', regex=True)
    df['price'] = df['price'].str.replace('$', '')
    df['price'] = df['price'].str.replace('Free', '0')

    # add 0 padding to column 'last_updated'
    # to prevent error during data type conversion
    df['last_updated'] = df['last_updated'].str.zfill(9)

    # convert columns' datatypes to be more suitable to their values
    # for better manipulation and query capability
    df['reviews'] = pd.to_numeric(df['reviews'], downcast='integer')
    df['size'] = df['size'].astype(float).round(1)
    df['installs'] = pd.to_numeric(df['installs'], downcast='integer')
    df['price'] = df['price'].astype(float).round(2)
    df['last_updated'] = pd.to_datetime(df['last_updated'], format='%d-%b-%y')

    # save cleaned data to csv format
    df.to_csv(cleaned_data_path, sep=',', index=False)


def csv_to_es():
    # load csv from path
    df = pd.read_csv(cleaned_data_path)

    # turn df into dictionary then 
    # create a new index in elastic search and fill the body with said dictionary 
    for i, r in df.iterrows():
        doc = r.to_dict()
        res = es.index(index='index_m3', id=i+1, body=doc)
        #print(f'response from elasticsearch: {res}')


# dag defintion
with DAG('dag_m3',
         default_args=default_args,
         description='dag for milestone 3',
         schedule_interval='30 6 * * *', # scheduled to run every 06:30
         catchup=False
         ) as dag:

    # dag's tasks definition
    csv_to_pg_task = PythonOperator(task_id='csv_to_pg',
                             python_callable=csv_to_postgres)
    csv_from_pg_task = PythonOperator(task_id='csv_from_pg',
                             python_callable=postgres_to_csv)
    clean_data_task = PythonOperator(task_id='clean_data',
                             python_callable=clean_data)
    csv_to_es_task = PythonOperator(task_id='csv_to_es',
                             python_callable=csv_to_es)


# dag's tasks' execution order 
csv_to_pg_task >> csv_from_pg_task >> clean_data_task >> csv_to_es_task
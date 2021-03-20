import pyexcel as p
import pymysql
from datetime import datetime
from dateutil import parser
import dateparser

def extract():
    csv_file = p.iget_records(file_name='netflix_titles.csv')
    return csv_file


db = extract()

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='09112002',
                             db='netflix',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()

#import script to Dim table


def load_data_into_dim_table(id, data, table_name,field):
    cursor.execute(f'''
        INSERT INTO netflix.{table_name}(id, {field})
        VALUES ('{id}', '{data}',  SELECT id FROM netflix.movie_rating WHERE rating = 'TV-PG')
    ''')

# ETL into DIm table
def etl_dim_table_without_nomalize(table):
    i = 0
    duplicate_value = []
    for data in db:
        data = data[table]
        data_transform = data.split(", ")
        if len(data_transform) > 0:
            for d in data_transform:
                if d != "" and d not in duplicate_value:
                    i = i + 1
                    load_data_into_dim_table(i, d, f'''movie_{table}''', table)
                    # table_rating(i,data)
                    duplicate_value.append(d)
    connection.commit()



# cursor.execute(f'''
#     SELECT * FROM netflix.movie_rating WHERE rating = 
# ''')

# cursor.execute("SELECT ID FROM NETFLIX_RATING")
# myresult = cursor.fetchone()
# print(myresult)


def load_data_into_fact_movie_table(id, title, date_added, release_year, duration, rating_id, type_id, description):
    cursor.execute(f'''
        INSERT INTO netflix.netflix_movie(id, title, date_added, release_year, duration, rating_id, type_id, description)
        VALUES (
            '{id}', 
            '{title}', 
            '{date_added}', 
            '{release_year}', 
            '{duration}', 
            (SELECT id FROM netflix.movie_rating WHERE rating = '{rating_id}'), 
            (SELECT id FROM netflix.movie_type WHERE type = '{type_id}'),
            '{description}'
        )
    ''')

def exec_fact_movie_table():
    index = 0
    for data in db:
        index += 1
        date_added = dateparser.parse(data['date_added'])
        print(index, data['title'], date_added, data['release_year'], data['duration'], data['rating'], data['type'], data['description'])
        load_data_into_fact_movie_table(index, data['title'], date_added, data['release_year'], data['duration'], data['rating'], data['type'], data['description'])

    connection.commit()
# exec_fact_movie_table()

def load_data_into_other_fact_table(id, table_name, movie_title, movie_description, record):
    cursor.execute(f'''
            INSERT INTO netflix.netflix_movie_{table_name}(id, netflix_movie_id, netflix_movie_{table_name}_id)
            VALUES (
                '{id}'
                ,(SELECT id FROM netflix.netflix_movie WHERE title = '{movie_title}' AND description = '{movie_description}')
                ,(SELECT id FROM netflix.movie_{table_name} WHERE {table_name} = '{record}')
            )
        ''')

def exec_fact_other_table(table_name):
    index = 0
    for data in db:
        
        row = data[table_name]
        data_transform = row.split(", ")
        movie_title = data['title']
        movie_description = data['description']
        for record in data_transform:
            index += 1
            print(index, table_name, movie_title, record)
            load_data_into_other_fact_table(index, table_name, movie_title, movie_description, record)


        # print(data)
    connection.commit()


exec_fact_other_table('listed_in')




import os
from tokenize import Number
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String
# from data_process.data_completion.base import Base
from sqlalchemy import MetaData
from sqlalchemy import text
import random
import math

metadata_obj = MetaData()
data_path = os.environ.get("DATA_DIR")
db_location = os.path.join(data_path, "completed_data.db")
engine = create_engine("sqlite+pysqlite:///" + str(db_location))

# CREATE TABLE cs (
#     "id"	varchar,
#     "venue"	varchar,
#     "year"	INTEGER,
#     "n_citations"	INTEGER
# )
def create_table(cate_name, topic_name):
    tbo = Table(
        cate_name +  "_" + topic_name,
        metadata_obj,
        Column('id', String, primary_key=True),
        Column('venue', String),
        Column('year', Integer),
        Column('n_citations', Integer),
        Column('partition', Integer, default=0),
        Column('filled', Integer, default=0)
    )
    metadata_obj.create_all(engine)
    return tbo


def insert_index(tbo, index):
    with engine.connect() as conn:
        sql = f'''
            select count(1) from {tbo}
        '''
        result = conn.execute(text(sql))
    
        for row in result:
            if row[0] == 30000:
                print(f"already have 30000 records indexed in table {tbo}, no insertion is executed")
                return 

    part = partition(index)

    p_index = 0
    with engine.connect() as conn:
        for p in part:
            for id in p:
                sql = f'''
                    insert or ignore into {tbo} (id, partition, filled) values ('{id}', {p_index}, 0)
                '''
                conn.execute(text(sql))
            p_index += 1
    
    print(f'total {len(index)} data were stored')

def partition(index): 
    individual_duty = int((len(index) / 3))
    random.shuffle(index)

    part_1 = index[:individual_duty]
    part_2 = index[individual_duty:individual_duty * 2]
    part_3 = index[individual_duty * 2:]

    return [
        part_1, part_2, part_3
    ]

def get_incompleted(table_name, partition):
    index = []
    if partition == -1:
        partition = '0,1,2'
    with engine.connect() as conn:
        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row.id)

    return index

def get_status(table_name, partition):
    index = []
    if partition == -1:
        partition = '0,1,2'
    with engine.connect() as conn:
        sql = f'''
            select count(1) as a from {table_name} where partition in ({partition}) and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row[0])

        sql = f'''
            select count(1) as a from {table_name} where partition in ({partition}) and filled = 1
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row[0])

        sql = f'''
            select count(1) as a from {table_name} where partition in ({partition}) and filled = 2
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row[0])

    return index

def error_record(table_name, id):
    with engine.connect() as conn:
        sql = f'''
            update {table_name} 
            set filled = 2
            where id = '{id}'
        '''
        conn.execute(text(sql))


def fill_data(table_name, data, id):
    with engine.connect() as conn:
        sql = f'''
            update {table_name}
            set year = :year, venue = :venue, n_citations = :n_citations, filled = 1 
            where id = :id
        '''
        conn.execute(
            text(sql), 
            {
                'id': id,
                'year': data['year'],
                'venue': data['venue'],
                'n_citations': data['citationCount']
            }
        )

def clear_partition(table_name, keeping_partition):
    with engine.connect() as conn:
        sql = f'''
            update {table_name}
            set year = null, venue = null, n_citations = null, filled = 0 
            where partition is not :keeping_partition
        '''
        conn.execute(
            text(sql), 
            {
                'keeping_partition': keeping_partition,
            }
        )

def get_all_data(table_name):
    data = {}
    with engine.connect() as conn:
        sql = f'''
            select * from {table_name} where filled = 1 and venue is not ''
        '''
        result = conn.execute(text(sql))
        for row in result:
            data[row.id] = row

    return data
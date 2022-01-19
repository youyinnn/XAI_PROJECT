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
    part = partition(index)

    p_index = 0
    for p in part:
        with engine.connect() as conn:
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
    with engine.connect() as conn:
        sql = f'''
            select id from {table_name} where partition = {partition} and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row.id)

    return index

def get_status(table_name, partition):
    index = []
    with engine.connect() as conn:
        sql = f'''
            select count(1) as a from {table_name} where partition = {partition} and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row[0])

        sql = f'''
            select count(1) as a from {table_name} where partition = {partition} and filled = 1
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append(row[0])

        sql = f'''
            select count(1) as a from {table_name} where partition = {partition} and filled = 2
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
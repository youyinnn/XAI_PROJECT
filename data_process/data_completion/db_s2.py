from curses import echo
import os, zlib
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, BLOB
# from data_process.data_completion.base import Base
from sqlalchemy import MetaData
from sqlalchemy import text
from sqlalchemy.orm import Session

metadata_obj = MetaData()
data_path = os.environ.get("DATA_DIR")
db_location = os.path.join(data_path, "completed_cs_data_s2.db")
engine = create_engine("sqlite+pysqlite:///" + str(db_location))

def create_table():
    Table(
        'cs',
        metadata_obj,
        # Column('id', Integer, primary_key=True),
        Column('s2_id', String, primary_key=True),
        Column('title', BLOB),
        Column('abstract', BLOB),
        Column('authors', BLOB),
        Column('venue', String),
        Column('year', Integer),
        Column('n_citations', Integer),
        Column('partition', String),
    )
    metadata_obj.create_all(engine)

def create_partition_check_table():
    Table(
        'partition_check',
        metadata_obj,
        Column('partition', String, primary_key=True),
        Column('is_checked', Integer, default=0),
    )
    metadata_obj.create_all(engine)

def partition_check_init():
    mainfast_location = os.path.join(data_path, "s2-corpus-manifast.txt")
    if not os.path.isfile(str(mainfast_location)):
        print(f"no manifast file from s2 {mainfast_location}")
    else:
        create_partition_check_table()
        create_table()
        partition_list = []
        with open(mainfast_location) as f:
            Lines = f.readlines()
            for line in Lines:
                line_strip = line.strip().split('.')[0]
                # print(line_strip)
                partition_list.append({
                    'partition': line_strip,
                    'is_checked': 0
                })
        with engine.connect() as conn:
            sql = f'''
                INSERT or ignore INTO partition_check (partition, is_checked) VALUES (:partition, :is_checked)
            '''
            conn.execute(text(sql), partition_list)
                
def get_unchecked_partition():
    unchecked_partition = []
    with engine.connect() as conn:
            sql = f'''
                select partition from partition_check where is_checked is 0
            '''
            result = conn.execute(text(sql))
            for row in result:
                unchecked_partition.append(row[0])

    return unchecked_partition

def insert_data(dataset, partition):
    # print(dataset[:1])
    # dataset = dataset[:1]
    with Session(engine) as session:
        session.begin()
        try:
            # session.add(some_object)
            # session.add(some_other_object)
            sql = f'''
                INSERT or ignore INTO cs (s2_id, title, abstract, authors, venue, year, n_citations, partition) 
                VALUES (:s2_id, :title, :abstract, :authors, :venue, :year, :n_citations, '{partition}')
            '''
            session.execute(text(sql), dataset)
        except:
            session.rollback()
            raise
        else:
            session.commit()
    # with engine.connect() as conn:
        # conn.execute(text(sql), dataset)

def check_partition(partition):
    with Session(engine) as session:
        session.begin()
        try:
            # session.add(some_object)
            # session.add(some_other_object)            
            sql = f'''
                update partition_check
                set is_checked = 1
                where partition is '{partition}'
            '''
            session.execute(text(sql))
        except:
            session.rollback()
            raise
        else:
            session.commit()

def get_data():
    data = []
    with engine.connect() as conn:
        sql = f'''
            select * from cs
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            data.append({
                'title': zlib.decompress(row[1]).decode('utf-8'),
                'abstract': zlib.decompress(row[2]).decode('utf-8'),
                'venue': row[4],
                'authors': zlib.decompress(row[3]).decode('utf-8'),
                'year': row[5],
                'n_citations': row[6],
            })
    return data
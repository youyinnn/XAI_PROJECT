import multiprocessing
import os, zlib, time,re, threading
from unittest import result

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, BLOB
# from data_process.data_completion.base import Base
from sqlalchemy import MetaData
from sqlalchemy import text
from sqlalchemy.orm import Session
from multiprocessing import Pool

metadata_obj = MetaData()
data_path = os.environ.get("DATA_DIR")
db_location = os.path.join(data_path, "completed_cs_data_s2.db")
engine = create_engine("sqlite+pysqlite:///" + str(db_location))

def create_table():
    Table(
        'cs',
        metadata_obj,
        Column('id', Integer, primary_key=True),
        Column('s2_id', String),
        Column('title', BLOB),
        Column('abstract', BLOB),
        Column('authors', BLOB),
        Column('venue', String),
        Column('year', Integer),
        # Column('doi', String),
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

def get_checked_partition():
    checked_partition = []
    with engine.connect() as conn:
            sql = f'''
                select partition from partition_check where is_checked is 1
            '''
            result = conn.execute(text(sql))
            for row in result:
                checked_partition.append(row[0])

    return checked_partition

def insert_data(dataset, partition):
    with Session(engine) as session:
        session.begin()
        try:
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

def get_data(start, amount):
    data = []
    with engine.connect() as conn:
        sql = f'''
            select * from cs limit {start}, {amount}
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            data.append({
                'id': row[0],
                's2_id': row[1],
                'title': zlib.decompress(row[2]).decode('utf-8'),
                'abstract': zlib.decompress(row[3]).decode('utf-8'),
                'venue': row[5],
                'authors': zlib.decompress(row[4]).decode('utf-8'),
                'year': row[6],
                'n_citations': row[7],
            })
    return data

def get_data_with_ids(ids):
    data = []
    with engine.connect() as conn:
        sql = f'''
            select * from cs where id in ({','.join(ids)})
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            data.append({
                'id': row[0],
                's2_id': row[1],
                'title': zlib.decompress(row[2]).decode('utf-8'),
                'abstract': zlib.decompress(row[3]).decode('utf-8'),
                'venue': row[5],
                'authors': zlib.decompress(row[4]).decode('utf-8'),
                'year': row[6],
                'n_citations': row[7],
            })
    return data

def get_partition_data(partition):
    data = []
    with engine.connect() as conn:
        sql = f'''
            select * from cs where partition is '{partition}'
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            data.append({
                'id': row[0],
                's2_id': row[1],
                'title': zlib.decompress(row[2]).decode('utf-8'),
                'abstract': zlib.decompress(row[3]).decode('utf-8'),
                'venue': row[5],
                'authors': zlib.decompress(row[4]).decode('utf-8'),
                'year': row[6],
                'n_citations': row[7],
            })
    return data

def get_data_count():
    with engine.connect() as conn:
        sql = f'''
            select count(1) from cs
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            return row[0]

def search_data_in_partition(list_and_topic):
    partition_list = list_and_topic[0]
    topic = list_and_topic[1]
    flags = (re.I | re.M)
    data_out = []
    for p in partition_list:
        data = get_partition_data(p)
        for d in data:
            if (re.search(topic['regex'], d["title"], flags) != None) or (re.search(topic['regex'], d["abstract"], flags) != None):
                data_out.append(d)
        # print(threading.get_ident(), f'searched {p}')

    return data_out

def get_count_by_regex_on_title_and_abstract(topic):
    partition = get_checked_partition()
    
    start_time = time.time()
    p_number = multiprocessing.cpu_count()
    process_map_arg = []
    i = 1
    worker_number = p_number
    duty_number_for_worker = int(6000 / worker_number)
    current_partition_start_index = 0
    while i <= worker_number:
        process_map_arg.append(
            [
                partition[
                    current_partition_start_index: 
                    current_partition_start_index + duty_number_for_worker
                ], 
            topic]
        )
        current_partition_start_index += duty_number_for_worker
        i += 1
    with Pool(p_number) as pool:
        rs = pool.map_async(search_data_in_partition, process_map_arg) 
        print(len(rs.get()))

    print(f'time: {time.time() - start_time}')

def get_all_doi_by_partitions(partition_name_list):
    partition_name_list = list(map(lambda n: "'" + n + "'", partition_name_list))
    titles = []
    with engine.connect() as conn:
        sql = f'''
            select id, doi from cs where partition in ({','.join(partition_name_list)})
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            titles.append({
                'id': row[0],
                'doi': row[1],
            })
    return titles


def get_all_titles_by_partitions(partition_name_list):
    partition_name_list = list(map(lambda n: "'" + n + "'", partition_name_list))
    titles = []
    with engine.connect() as conn:
        sql = f'''
            select id, title from cs where partition in ({','.join(partition_name_list)})
        '''
        rs = conn.execute(text(sql))
        for row in rs:
            titles.append({
                'id': row[0],
                'title': zlib.decompress(row[1]).decode('utf-8'),
            })
    return titles

def create_index():
    with engine.connect() as conn:
        sql = f'''
            CREATE UNIQUE INDEX IF NOT EXISTS "id_idx" 
            ON "cs" (
                "id"	ASC
            )
        '''
        conn.execute(text(sql))
        sql = f'''
            CREATE INDEX IF NOT EXISTS "p_idx" 
            ON "cs" (
                "partition"	ASC
            )
        '''
        conn.execute(text(sql))
    print(f'index created')
import os
from tokenize import Number
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String
# from data_process.data_completion.base import Base
from sqlalchemy import MetaData
from sqlalchemy import text
import random

metadata_obj = MetaData()
data_path = os.environ.get("DATA_DIR")
db_location = os.path.join(data_path, "completed_data.db")
engine = create_engine("sqlite+pysqlite:///" + str(db_location))
tmp_engine_map = {}

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

def get_temp_db_engine(table_name):
    existed_tmp_engine = tmp_engine_map.get(table_name)
    if existed_tmp_engine != None:
        return existed_tmp_engine
    tmp_db_location = os.path.join(data_path, f"{table_name}.tmp.db")
    tmp_engine = create_engine("sqlite+pysqlite:///" + str(tmp_db_location))
    Table(
        table_name,
        metadata_obj,
        Column('id', String, primary_key=True),
        Column('venue', String),
        Column('year', Integer),
        Column('n_citations', Integer),
        Column('partition', Integer, default=0),
        Column('filled', Integer, default=0)
    )
    metadata_obj.create_all(tmp_engine)
    tmp_engine_map[table_name] = tmp_engine
    return tmp_engine

def create_initial_records(tbo, index):
    with engine.connect() as conn:
        sql = f'''
            select count(1) from {tbo}
        '''
        result = conn.execute(text(sql))
    
        for row in result:
            if row[0] == 30000:
                print(f"already have 30000 records indexed in table {tbo}, no insertion is executed")
                return 

    part = record_partitioning(index)

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

def record_partitioning(index): 
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
            select id,partition from {table_name} where partition in ({partition}) and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            index.append({
                'id': row.id,
                'partition': row.partition
            })

    tmp_engine = get_temp_db_engine(table_name)
    filled_in_tmp = {}
    with tmp_engine.connect() as conn:
        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled is not 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            filled_in_tmp[row.id] = ''

    incompleted_id_p = []
    for idp in index:
        id =  idp['id']
        if filled_in_tmp.get(id) == None:
            incompleted_id_p.append(idp)

    return tmp_engine, incompleted_id_p

def get_filling_status(table_name, partition):
    
    total = 0    
    if partition == -1:
        partition = '0,1,2'
    filled_records = {}
    unfilled_records = {}
    error_records = {}
    with engine.connect() as conn:
        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 0
        '''
        result = conn.execute(text(sql))
        for row in result:
            unfilled_records[row[0]] = ''
            total += 1

        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 1
        '''
        result = conn.execute(text(sql))
        for row in result:
            filled_records[row[0]] = ''
            total += 1

        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 2
        '''
        result = conn.execute(text(sql))
        for row in result:
            error_records[row[0]] = ''
            total += 1

    tmp_engine = get_temp_db_engine(table_name)
    with tmp_engine.connect() as conn:
        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 1
        '''
        result = conn.execute(text(sql))
        for row in result:
            filled_records[row[0]] = ''

        sql = f'''
            select id from {table_name} where partition in ({partition}) and filled = 2
        '''
        result = conn.execute(text(sql))
        for row in result:
            error_records[row[0]] = ''

    filled = len(filled_records.keys())
    unfilled = len(unfilled_records.keys())
    error = len(error_records.keys())

    return filled, unfilled, error, total

def mark_as_error_record(table_name, id_p, eng):
    with eng.connect() as conn:
        # sql = f'''
        #     update {table_name} 
        #     set filled = 2
        #     where id = '{id}'
        # '''        
        sql = f'''
            insert into {table_name} (id, partition, filled)
            values ('{id_p['id']}', {id_p['partition']}, 2)
        '''
        conn.execute(text(sql))

def fill_data(table_name, data, id_p, eng):
    with eng.connect() as conn:
        sql = f'''
            insert into {table_name} (id, year, venue, n_citations, partition, filled)
            values ('{id_p['id']}', :year, :venue, :n_citations, {id_p['partition']}, 1)
        '''
        conn.execute(
            text(sql), 
            {
                # 'id': id,
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

def get_all_data_from_tmp(table_name):
    data = {}
    tmp_engine = get_temp_db_engine(table_name)
    with tmp_engine.connect() as conn:
        sql = f'''
            select * from {table_name} where filled in (0, 1, 2)
        '''
        result = conn.execute(text(sql))
        for row in result:
            data[row.id] = row

    return data

def merge_data_from_temp(table_name, tmp_data):
    count = 0
    with engine.connect() as conn:
        for d in tmp_data:
            tmp_records= tmp_data[d]
            if tmp_records[5] != 0:
                count += 1
            sql = f'''
                update {table_name}
                set venue = :venue, year = :year, n_citations = :n_citations, filled = :filled
                where id = :id and filled is 0
            '''
            conn.execute(text(sql),
                {
                    'id': d,
                    'year': tmp_records[2],
                    'venue': tmp_records[1],
                    'n_citations': tmp_records[3],
                    'filled': tmp_records[5]
                }
            )
            # print(d)
            # break
    print(f'{count} records are merged into {table_name}')
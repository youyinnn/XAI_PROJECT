import os
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String
# from data_process.data_completion.base import Base
from sqlalchemy import MetaData
from sqlalchemy import text

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
        Column('n_citations', Integer)
    )
    metadata_obj.create_all(engine)
    return tbo


def insert_index(tbo, index):
    with engine.connect() as conn:
        for id in index:
            sql = f'''
                INSERT OR IGNORE INTO {tbo} (id) VALUES ('{id}')
            '''
            conn.execute(text(sql))
    
    print(f'total {len(index)} data were stored')

import requests, json, time
from data_process.data_completion.db import get_incompleted, engine
from sqlalchemy import text

# fill one record every 3 seconds
def fill(table_name, partition):
    print(f'filling data for table {table_name} from partition {partition}')

    index = get_incompleted(table_name, partition)

    for id in index:
        # print(id)
        r = requests.get(f'https://api.semanticscholar.org/graph/v1/paper/URL:https://arxiv.org/abs/{id}?fields=venue,year,citationCount')
        data = json.loads((r.text))
        if r.status_code == 403:
            print('api forbidden, sleep 60 seconds')
            time.sleep(60)
        else:
            # print(f'api worked filling {id} and sleep 3.1 seconds')
            with engine.connect() as conn:
                sql = f'''
                    update {table_name} 
                    set year = {data['year']}, venue = '{data['venue']}', n_citations = {data['citationCount']}, filled = 1 
                    where id = '{id}'
                '''
                conn.execute(text(sql))
                time.sleep(3.1)

        

import requests, json, time, sys
from data_process.data_completion.db import get_incompleted, get_status, fill_data, error_record
from sqlalchemy import text

# fill one record every 3 seconds
def fill(table_name, partition):
    print(f'filling data for table {table_name} from partition {partition}')
    status(table_name, partition)

    index = get_incompleted(table_name, partition)

    count = 0
    for id in index:
        # print(id)
        count += 1

        # print status every 150 seconds
        if (count % 50 == 0):
            status(table_name, partition)

        # try filling
        try:
            r = requests.get(f'https://api.semanticscholar.org/graph/v1/paper/URL:https://arxiv.org/abs/{id}?fields=venue,year,citationCount')
            data = json.loads((r.text))
            if r.status_code == 403:
                print('api forbidden, sleep 60 seconds')
                time.sleep(60)
            elif data.get('error') != None:
                print(data)
                error_record(table_name, id)
            else:
                # print(f'api worked filling {id} and sleep 3.1 seconds')
                fill_data(table_name, data, id)
                time.sleep(3.1)
        except KeyboardInterrupt:
            print(f"KeyboardInterrupt on id {id}")
            sys.exit()
        except:
            print(f"An exception occurred on id {id}")
            error_record(table_name, id)
            time.sleep(3.1)
        # break

def status(table_name, partition):
    count = get_status(table_name, partition)
    estimated_finish_time = time.strftime('%H hrs %M min %S sec', time.gmtime(int(count[0] * 3)))
    print(f'({count[1]}/{count[2]}/{count[0] + count[1] + count[2]}) records are completed in partition {partition}, {estimated_finish_time} left')


def export(table_name):
    print()

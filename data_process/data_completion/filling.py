import requests, json, time, sys, os
from data_process.data_completion.db import get_incompleted, get_status, fill_data, error_record,get_all_data
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
    data_src = os.path.join(os.environ.get("DATA_DIR"), f"raw_{table_name}.data")
    # print(data_src)
    all_data = get_all_data(table_name)
    completed_data = []
    with open(data_src) as f:
        Lines = f.readlines()
        for line in Lines:
            line_strip = line.strip()
            jso = json.loads(line_strip)
            db_data = all_data.get(jso['id'])
            if db_data != None:
                authors = []
                for parsed in jso['authors_parsed']:
                    authors.append(f'{parsed[1]} {parsed[0]}')
                del jso['authors_parsed']
                jso['authors'] = authors
                jso['year'] = db_data['year']
                jso['venue'] = db_data['venue']
                jso['n_citations'] = db_data['n_citations']
                completed_data.append(json.dumps(jso))

    output_topic_data_file_name = os.path.join(os.environ.get("DATA_DIR"), f"completed_{table_name}.data")
    with open(output_topic_data_file_name, "w") as leaned_raw_topic_data:
        leaned_raw_topic_data.write("\n".join(completed_data))
        print(output_topic_data_file_name + f" saved with {len(completed_data)} completed data")

    # print(len(completed_data))

import requests, json, time, sys, os
from data_process.data_completion.db import get_incompleted, get_filling_status, fill_data, mark_as_error_record,get_all_data, get_all_data_from_tmp, merge_data_from_temp
import datetime

# fill one record every 3 seconds
def fill(table_name, partition):
    print(f'filling data for table {table_name} from partition {partition}')
    status(table_name, partition)

    # return 
    tmp_engine, index_partition = get_incompleted(table_name, partition)

    count = 0
    for id_p in index_partition:
        # print(id)
        count += 1

        # print status every 150 seconds
        if (count % 50 == 0):
            status(table_name, partition)

        id = id_p['id']
        # try filling
        try:
            r = requests.get(f'https://api.semanticscholar.org/graph/v1/paper/URL:https://arxiv.org/abs/{id}?fields=venue,year,citationCount')
            data = json.loads((r.text))
            if r.status_code == 403:
                print('api forbidden, sleep 60 seconds')
                time.sleep(60)
            elif data.get('error') != None:
                print(data)
                mark_as_error_record(table_name, id_p, tmp_engine)
            else:
                # print(f'api worked filling {id} and sleep 3.1 seconds')
                fill_data(table_name, data, id_p, tmp_engine)
                time.sleep(3.1)
        except KeyboardInterrupt:
            print(f"KeyboardInterrupt on id {id}")
            sys.exit()
        except:
            print(f"An exception occurred on id {id}")
            mark_as_error_record(table_name, id_p, tmp_engine)
            time.sleep(3.1)

def status(table_name, partition):
    filled, unfilled, error, total = get_filling_status(table_name, partition)
    m, s = divmod(int(unfilled * 3), 60)
    h, m = divmod(m, 60)
    # estimated_finish_time = time.strftime('%H hrs %M min %S sec', time.gmtime(int(count[0] * 3)))
    estimated_finish_time = "%d hrs %02d min %02d sec" % (h, m, s)
    time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f'[{time_str}] ({filled}/{error}/{total}) records are completed in partition {partition}, {estimated_finish_time} left')

def merge_from_tmp_db(table_name):
    tmp_data = get_all_data_from_tmp(table_name)
    merge_data_from_temp(table_name, tmp_data)

def export(table_name):
    cate_name = table_name.split("_")[0]
    data_src = os.path.join(os.environ.get("DATA_DIR"), f"raw_{cate_name}.data.json")
    # print(data_src)
    all_data = get_all_data(table_name)
    completed_data = []
    with open(data_src) as f:
        # Lines = f.readlines()
        # for line in Lines:
        #     line_strip = line.strip()
        #     jso = json.loads(line_strip)
        data_json = json.load(f)
        for jso in data_json:
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

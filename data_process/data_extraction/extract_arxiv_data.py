from cmath import exp
import json
import re
import time
import os
import random, multiprocessing
from multiprocessing import Pool
from functools import reduce

from numpy import partition
from data_process.data_completion.db_s2 import get_all_titles_by_partitions, get_checked_partition

def extract_by_cate(cate, src):
    print("extracting data from " + src + "\nby categories: " + cate['name'])
    count = 0
    data_out = []
    start_time_1 = time.time()

    with open(src) as f:
    # with open('../data/arxiv-metadata-oai-snapshot.data') as f:
        Lines = f.readlines()

        for line in Lines:
            line_strip = line.strip()
            jso = json.loads(line_strip)
            if (re.search(cate['regex'], jso["categories"]) != None):
                count += 1

                # del unused properties
                del jso["doi"]
                del jso["submitter"]
                del jso["comments"]
                del jso["authors"]
                # del jso["authors_parsed"]
                del jso["versions"]
                del jso["license"]
                if (jso.get('journal-ref') != None):
                    del jso["journal-ref"]
                if (jso.get('categories') != None):
                    del jso["categories"]
                if (jso.get('update_date') != None):
                    del jso["update_date"]

                keys = jso.keys()
                keys_arr = []

                for key in keys:
                    keys_arr.append(key)

                # del None properties
                for key in keys_arr:
                    if (jso[key] == None):
                        del jso[key]

                data_out.append(json.dumps(jso))
            # print("Line {}: {} - {} - {} - {}".format(count, jso["title"], jso["categories"], jso["doi"], jso['id']))

    print("Total Count: " + str(count))
    print("--- Processing data with %s seconds ---" % (time.time() - start_time_1))


    start_time_2 = time.time()
    output_data_file_name = os.path.join(os.environ.get("DATA_DIR"), "raw_" + cate['name'] + ".data.json")
    with open(output_data_file_name, "w") as leaned_raw_data:
        leaned_raw_data.write("[\n" + ",\n".join(data_out) + "\n]")
        print(output_data_file_name + " saved")

    print("--- Writing data with %s seconds ---" % (time.time() - start_time_2))

    # with open("../data/raw_data_format_sample.json", "w") as raw_sample:
    #     json.dump(json.loads(data[0]), raw_sample, ensure_ascii=False, indent=4)

def extract_by_topic(cate_name, topic, src):
    print("extracting data from " + src + "\nby topic regx: " + topic['regex'])

    count = 0
    total_count = 0
    data_index_out = []
    # data_out = []
    matched_jso_data = []
    with open(src) as f:
        data_json = json.load(f)
        flags = (re.I | re.M)
        for jso in data_json:
            total_count += 1
            if (re.search(topic['regex'], jso["title"], flags) != None) or (re.search(topic['regex'], jso["abstract"], flags) != None):
                count += 1
                # data_index_out.append(jso['id'])
                # if (jso.get('journal-ref') != None):
                #     del jso["journal-ref"]
                # if (jso.get('categories') != None):
                #     del jso["categories"]
                # if (jso.get('update_date') != None):
                #     del jso["update_date"]
                # data_out.append(json.dumps(jso))
                matched_jso_data.append(jso)

    if (len(matched_jso_data) > 30000):
        random.shuffle(matched_jso_data)
        matched_jso_data = matched_jso_data[:30000]

    for jso in matched_jso_data:
        data_index_out.append(jso['id'])
        # data_out.append(json.dumps(jso))

    print(f'({count}/{total_count}) records are matched the topic with {len(data_index_out)} of them are picked')

    # output_topic_data_file_name = os.path.join(os.environ.get("DATA_DIR"), f"raw_{cate_name}_{topic['name']}.data")
    # with open(output_topic_data_file_name, "w") as leaned_raw_topic_data:
    #     leaned_raw_topic_data.write("\n".join(data_out))
    #     print(output_topic_data_file_name + " saved")

    return data_index_out

def topic_count(topic, src):
    print("searching data from " + src + "\nby topic regx: " + topic['regex'])

    count = 0
    total_count = 0
    try:
        with open(src) as f:
            data_json = json.load(f)
            flags = (re.I | re.M)
            for jso in data_json:
                total_count += 1
                if (re.search(topic['regex'], jso["title"], flags) != None) or (re.search(topic['regex'], jso["abstract"], flags) != None):
                    count += 1
    except FileNotFoundError:
        print(f'No file {src} extract it first')

    print(f'total {count} indecial out of {total_count}')

def search_matching_result_with_title_in_s2_by_partition(partition_name_list_and_matched_jso_data):
    partition_name_list = partition_name_list_and_matched_jso_data[0]
    matched_jso_data = partition_name_list_and_matched_jso_data[1]
    final_matched_result = []
    titles_by_partition = get_all_titles_by_partitions(partition_name_list)
    for data in titles_by_partition:
        title = data['title'].replace(' ', '').lower()
        if matched_jso_data.get(title) != None:
            final_matched_result.append(data)
    
    print(f'finished searching from: {partition_name_list[0]} to {partition_name_list[len(partition_name_list) - 1]} in {len(titles_by_partition)} s2 data')

    return final_matched_result

def extract_all_by_topic(cate, src):
    print("extracting data from " + src + "\nby cate regx: " + cate['regex'])

    count = 0
    total_count = 0
    matched_jso_data = {}
    with open(src) as f:
        line = f.readline()
        while line:
            line_strip = line.strip()
            jso = json.loads(line_strip)
            total_count += 1
            if (re.search(cate['regex'], jso["categories"]) != None):
                count += 1
                matched_jso_data[jso['title'].replace(' ', '').lower()] = jso
            line = f.readline()

    print(f'({count}/{total_count}) records are matched the topic')

    partition = get_checked_partition()

    start_time = time.time()
    p_number = multiprocessing.cpu_count()
    process_map_arg = []
    i = 1
    worker_number = p_number
    duty_number_for_worker = int(len(partition) / worker_number)
    current_partition_start_index = 0
    while i <= worker_number:
        process_map_arg.append(
            [
                partition[
                    current_partition_start_index: 
                    current_partition_start_index + duty_number_for_worker
                ], 
                matched_jso_data
            ]
        )
        current_partition_start_index += duty_number_for_worker
        i += 1

    with Pool(p_number) as pool:
        rs = pool.map_async(search_matching_result_with_title_in_s2_by_partition, process_map_arg)
        data = reduce(lambda a, b: a+b, rs.get())
        print(len(data))


    return matched_jso_data
from cmath import exp
import json
import re
import time
import os
import random

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

import json
import re
import time
import os

def extract_by_cate(top_cate, src):
    print("extracting data from " + src + "\nby categories: " + top_cate)
    count = 0
    data = []
    start_time_1 = time.time()

    with open(src) as f:
    # with open('../data/arxiv-metadata-oai-snapshot.data') as f:
        Lines = f.readlines()

        for line in Lines:
            line_strip = line.strip()
            jso = json.loads(line_strip)
            if (re.search("(^" + top_cate + "\.)|( " + top_cate + "\.)", jso["categories"]) != None):
                count += 1

                # del unused properties
                del jso["doi"]
                del jso["submitter"]
                del jso["comments"]
                del jso["authors_parsed"]
                # del jso["versions"]
                del jso["license"]

                keys = jso.keys()
                keys_arr = []

                for key in keys:
                    keys_arr.append(key)

                # del None properties
                for key in keys_arr:
                    if (jso[key] == None):
                        del jso[key]

                data.append(json.dumps(jso))
            # print("Line {}: {} - {} - {} - {}".format(count, jso["title"], jso["categories"], jso["doi"], jso['id']))

    print("Total Count: " + str(count))
    print("--- Processing data with %s seconds ---" % (time.time() - start_time_1))


    start_time_2 = time.time()
    output_data_file_name = os.path.join(os.environ.get("DATA_DIR"), "raw_" + top_cate + ".data.json")
    with open(output_data_file_name, "w") as leaned_raw_data:
        leaned_raw_data.write("[\n" + ",\n".join(data) + "\n]")
        print(output_data_file_name + " saved")

    print("--- Writing data with %s seconds ---" % (time.time() - start_time_2))

    # with open("../data/raw_data_format_sample.json", "w") as raw_sample:
    #     json.dump(json.loads(data[0]), raw_sample, ensure_ascii=False, indent=4)
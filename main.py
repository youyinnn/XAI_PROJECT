#!/usr/bin/env python
import os
import sys
from pathlib import Path

base_dir = str(Path(__file__).resolve().parent)
data_dir = os.path.join(base_dir, "data_process", "data")
os.environ.setdefault("BASE_DIR", base_dir)
os.environ.setdefault("DATA_DIR", data_dir)

from data_process.data_extraction.extract_arxiv_data import extract_by_cate as ebc
from data_process.data_extraction.extract_arxiv_data import extract_by_topic as ebt
from data_process.data_extraction.extract_arxiv_data import topic_count
from data_process.data_completion.db import create_table, insert_index, clear_partition
from data_process.data_completion.filling import fill, status, export
from data_process.conf import cates


def main():
    argv_len = len(sys.argv)
    if argv_len > 1:
        cmd = sys.argv[1]
        if (cmd == 'extract-cate' and argv_len >= 3):
            cate_name = sys.argv[2]
            cate = cates.get(cate_name)
            if (cate != None):
                ebc(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))
            else:
                print("No config cate for: " + cate_name)

        if (cmd == 'extract-topic' and argv_len >= 4):
            cate_name = sys.argv[2]
            topic_name = sys.argv[3]
            cate = cates.get(cate_name)
            if (cate != None):
                cate_data_file = os.path.join(data_dir, "raw_" + cate['name'] + ".data.json")
                # ebc(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))
                topic = cate['topic'].get(topic_name)
                if (topic != None):
                    # print(topic)
                    if not os.path.isfile(str(cate_data_file)):
                        print(cate['name'] + " cate data not exist, extract it first")
                        ebc(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))

                    data_index = ebt(cate['name'], topic, str(cate_data_file))
                    val = input("store those index into database?(type yes if you want): ")
                    if val == 'yes':
                        tbo = create_table(cate['name'], topic['name'])
                        insert_index(tbo, data_index)
                else:
                    print("No config topic: " + topic_name + " on cate: " + cate_name)
            else:
                print("No config cate for: " + cate_name)

        if (cmd == 'fill-data' and argv_len >= 4):
            table_name = sys.argv[2]
            partition = int(sys.argv[3])
            fill(table_name, partition)

        if (cmd == 'fill-data-status' and argv_len >= 4):
            table_name = sys.argv[2]
            partition = int(sys.argv[3])
            status(table_name, partition)

        if (cmd == 'clear-part' and argv_len >= 4):
            table_name = sys.argv[2]
            partition = int(sys.argv[3])
            clear_partition(table_name, partition)

        if (cmd == 'export-data' and argv_len >= 3):
            table_name = sys.argv[2]
            export(table_name)        
            
        if (cmd == 'topic-count' and argv_len >= 3):
            cate_name = sys.argv[2]
            topic_name = sys.argv[3]
            cate = cates.get(cate_name)
            if (cate != None):
                cate_data_file = os.path.join(data_dir, "raw_" + cate['name'] + ".data.json")
                topic = cate['topic'].get(topic_name)
                if (topic != None): 
                    topic_count(topic, str(cate_data_file))
                else:
                    print("No config topic: " + topic_name + " on cate: "+ cate_name)
            else:
                print("No config cate for: " + cate_name)

if __name__ == '__main__':
    main()
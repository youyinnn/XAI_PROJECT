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
from data_process.data_completion.db import create_table, create_initial_records, clear_partition
from data_process.data_completion.filling import fill, status, export, merge_from_tmp_db
from data_process.conf import cates

from data_process.data_completion.db_s2 import partition_check_init,get_data_count, get_count_by_regex_on_title_and_abstract as get_s2_topic_count
from data_process.data_completion.db_s2 import create_index
from data_process.data_extraction.extract_s2_data import extract, export as s2_export, export_rand as s2_export_rand
from data_process.data_extraction.extract_s2_data import matching_arxiv_data_by_topic as madbt
from data_process.data_extraction.extract_s2_data import extract_from_arxiv_cate_id_list

def main():
    argv_len = len(sys.argv)
    if argv_len > 1:
        cmd = sys.argv[1]
        arxiv_data_cmd(cmd, argv_len)
        s2_data_cmd(cmd, argv_len)

def arxiv_data_cmd(cmd, argv_len):
    # if (cmd == 'extract-cate' and argv_len >= 3):
    #         cate_name = sys.argv[2]
    #         cate = cates.get(cate_name)
    #         if (cate != None):
    #             ebc(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))
    #         else:
    #             print("No config cate for: " + cate_name)

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
                    create_initial_records(tbo, data_index)
            else:
                print("No config topic: " + topic_name + " on cate: " + cate_name)
        else:
            print("No config cate for: " + cate_name)

    if (cmd == 'fill-data' and argv_len >= 4):
        table_name = sys.argv[2]
        partition = int(sys.argv[3])
        fill(table_name, partition)

    # if (cmd == 'merge-data' and argv_len >= 3):
    #     table_name = sys.argv[2]
    #     merge_from_tmp_db(table_name)   

    if (cmd == 'fill-data-status' and argv_len >= 4):
        table_name = sys.argv[2]
        partition = int(sys.argv[3])
        status(table_name, partition)

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

def s2_data_cmd(cmd, argv_len):
    if (cmd == 'init-s2-database'):
        partition_check_init()    

    if (cmd == 'create-s2-index'):
        create_index()    
    
    if (cmd == 'extract-s2-cs-data'):
        extract()

    if (cmd == 's2-cs-data-count'):
        print(get_data_count())

    if (cmd == 'export-s2-cs-data'):
        start, amount = None, None
        if argv_len >= 4:
            start = int(sys.argv[2]) - 1
            amount = sys.argv[3]
        s2_export(start, amount)

    if (cmd == 'export-s2-cs-data-rand'):
        s2_export_rand()

    if (cmd == 'count-s2-cs-topic'):
        topic_name = sys.argv[2]
        cate = cates.get('cs')
        topic = cate['topic'].get(topic_name)
        get_s2_topic_count(topic)

    if(cmd == 'arxiv-s2-data-match' and argv_len >= 3):
        cate_name = sys.argv[2]
        cate = cates.get(cate_name)
        madbt(cate, os.path.join(data_dir, "arxiv-metadata-oai-snapshot.data"))

    if(cmd == 'arxiv-s2-data-extract' and argv_len >= 3):
        cate_name = sys.argv[2]
        extract_from_arxiv_cate_id_list(cate_name)

    if(cmd == 'arxiv-s2-data-extract-rand' and argv_len >= 3):
        cate_name = sys.argv[2]
        amount = int(sys.argv[3])
        extract_from_arxiv_cate_id_list(cate_name, amount)

if __name__ == '__main__':
    main()
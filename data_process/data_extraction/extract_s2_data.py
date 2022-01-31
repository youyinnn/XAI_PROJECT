from data_process.data_completion.db_s2 import get_unchecked_partition, insert_data, check_partition,get_data
from data_process.data_completion.db_s2 import get_data_count, get_data_with_ids
import urllib.request
import shutil, os, gzip,json,zlib, time, random, multiprocessing,re
from multiprocessing import Pool
from functools import reduce
from data_process.conf import cates

from data_process.data_completion.db_s2 import get_all_titles_by_partitions, get_checked_partition
data_loading_line_limit = 100000

def extract():
    unchecked_partition = get_unchecked_partition()
    # print(unchecked_partition)
    total_sec = 0.0
    count = 0
    if len(unchecked_partition) == 0:
        print('all s2 data partitions have been checked and extracted')
    for partition in unchecked_partition:
        count += 1
        gz_name = f'{partition}.gz'
        url = f"https://s3-us-west-2.amazonaws.com/ai2-s2-research-public/open-corpus/2022-01-01/{gz_name}"
        file_store_path = os.path.join(os.environ.get("DATA_DIR"), f"{gz_name}")
        file_store_path_str = str(file_store_path)

        print(f'downloading.. {gz_name} to {file_store_path_str}')
        start = time.time()
        with urllib.request.urlopen(url) as response, open(file_store_path_str, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        sec = time.time() - start
        total_sec += sec
        end = format((sec), '.2f')
        avarage_sec = format((total_sec / count), '.2f')
        print(f'{gz_name} downloaded in {end} sec, avarage time: {avarage_sec} sec')

        print(f'reading {gz_name}..')
        cs_data = []
        with gzip.open(file_store_path_str, 'rb') as f:
            file_content_lines = f.readlines()
            for line in file_content_lines:
                line_strip = line.strip()
                jso = json.loads(line_strip)
                fos = jso['fieldsOfStudy']
                if ('Computer Science' in fos):
                    authors_list = []
                    for author in jso['authors']:
                        authors_list.append(author['name'])
                    
                    jso['abstract'] = jso['paperAbstract']
                    jso['s2_id'] = jso['id']
                    jso['authors'] = json.dumps(authors_list)
                    jso['n_citations'] = len(jso['inCitations'])

                    del jso['paperAbstract']
                    del jso['inCitations']
                    del jso['outCitations']
                    del jso['s2Url']
                    del jso['sources']
                    del jso['pdfUrls']
                    del jso['journalName']
                    del jso['journalVolume']
                    del jso['journalPages']
                    del jso['doi']
                    del jso['doiUrl']
                    del jso['pmid']
                    del jso['magId']
                    del jso['s2PdfUrl']
                    del jso['entities']
                    # del jso['fieldsOfStudy']

                    if jso['venue'] != '' and jso['abstract'] != '' and jso['year'] != None:
                        jso['title'] = zlib.compress(str.encode(jso['title'], encoding='utf-8'), 9)
                        jso['abstract'] = zlib.compress(str.encode(jso['abstract'], encoding='utf-8'), 9)
                        jso['authors'] = zlib.compress(str.encode(jso['authors'], encoding='utf-8'), 9)
                        cs_data.append(jso)

        # print(len(cs_data))
        # print(len(cs_data[0]['abstract']))
        insert_data(cs_data, partition)
        print(f'{len(cs_data)} records from {partition} have inserted')
        check_partition(partition)
        if os.path.exists(file_store_path_str):
            print(f'removeing gz file {file_store_path_str}')
            os.remove(file_store_path_str)

        print('\n')
        # break

def export(start, amount):
    amount_left = int(amount)
    start_for_this_round = int(start)
    output_s2_incomplete_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_{int(start) + 1}_to_{int(start) + int(amount)}#incomplete.data")
    if os.path.exists(output_s2_incomplete_data_file_name):
        os.remove(output_s2_incomplete_data_file_name)

    output_s2_complete_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_{int(start) + 1}_to_{int(start) + int(amount)}.data")
    while amount_left > 0:
        data_out = []
        amount_for_this_round = data_loading_line_limit if amount_left > data_loading_line_limit else amount_left
        data = get_data(start_for_this_round, amount_for_this_round)

        amount_left -= data_loading_line_limit
        print(f'{amount_left} left')
        start_for_this_round += amount_for_this_round
        for d in data:
            d['authors'] = json.loads(d['authors'])
            data_out.append(json.dumps(d))

        with open(output_s2_incomplete_data_file_name, "a+") as f:
            f.write("\n".join(data_out) + '\n')
        data = []
        
    
    os.rename(output_s2_incomplete_data_file_name, output_s2_complete_data_file_name)
    print(output_s2_complete_data_file_name + f" saved with {amount} completed data")


# this will take 600M more memories
def export_rand():
    record_count = get_data_count()
    id_arr = []
    i = 1
    # this will take 300M memories
    while (i <= record_count):
        id_arr.append(str(i))
        i += 1

    random.shuffle(id_arr)

    rand_ids = id_arr[:30000]
    data = get_data_with_ids(rand_ids)
    data_out = []
    for d in data:
        d['authors'] = json.loads(d['authors'])
        data_out.append(json.dumps(d))

    rand_sequence = hex(zlib.crc32(str(time.time()).encode('utf-8')))
    output_s2_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_rand.{rand_sequence}.data")
    with open(output_s2_data_file_name, "w") as leaned_raw_topic_data:
        leaned_raw_topic_data.write("\n".join(data_out))
        print(output_s2_data_file_name + f" saved with {len(data_out)} completed data")

def export_with_id(ids_file):
    ids = []
    with open(ids_file) as f:
        for id in f:
            ids.append(id)

    head, tail = os.path.split(ids_file)
    output_s2_incomplete_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_with_id.{tail}#incomplete.data")
    if os.path.exists(output_s2_incomplete_data_file_name):
        os.remove(output_s2_incomplete_data_file_name)
    output_s2_complete_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_with_id.{tail}.data")

    start_idx = 0
    ids_left = len(ids)
    while ids_left > 0:
        end_idx = start_idx + data_loading_line_limit if ids_left > data_loading_line_limit else start_idx + ids_left
        data = get_data_with_ids(ids[start_idx: end_idx])
        ids_left -= data_loading_line_limit
        print(f'{ids_left} left')
        start_idx = end_idx

        data_out = []
        for d in data:
            d['authors'] = json.loads(d['authors'])
            data_out.append(json.dumps(d))

        with open(output_s2_incomplete_data_file_name, "a+") as f:
            f.write("\n".join(data_out) + '\n')
    
    os.rename(output_s2_incomplete_data_file_name, output_s2_complete_data_file_name)
    print(output_s2_complete_data_file_name + f" saved with {len(ids)} completed data")

def g_title(title):
    new_s = []
    for c in title:
        if c.isalpha():
            new_s.append(c)
    new_s = ''.join(new_s).lower()
    return new_s

def search_matching_result_with_title_in_s2_by_partition(partition_name_list_and_matched_jso_data):
    partition_name_list = partition_name_list_and_matched_jso_data[0]
    matched_jso_data = partition_name_list_and_matched_jso_data[1]
    final_matched_result = []
    titles_by_partition = get_all_titles_by_partitions(partition_name_list)
    for data in titles_by_partition:
        title = g_title(data['title'])
        arxiv_data = matched_jso_data.get(title)
        if arxiv_data != None:
            final_matched_result.append({
                'arxiv': {
                    'id': arxiv_data['id'],
                    'title': arxiv_data['title'],
                    'categories': arxiv_data['categories'],
                },
                's2': data
            })
    
    print(f'finished searching from: {partition_name_list[0]} to {partition_name_list[len(partition_name_list) - 1]} in {len(titles_by_partition)} s2 data')

    return final_matched_result

def matching_arxiv_data_by_topic(cate, src):
    print("extracting data from " + src + "\nby cate regx: " + cate['regex'])

    st = time.time()
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
                matched_jso_data[g_title(jso['title'])] = jso
            line = f.readline()

    print(f'({len(matched_jso_data)}/{total_count}) arxiv records are matched')

    # partition = get_checked_partition()[:8]
    partition = get_checked_partition()

    # p_number = int(multiprocessing.cpu_count() / 2)
    p_number = int(multiprocessing.cpu_count())
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

    data_out = []
    data_v_out = []

    for d in data:
        data_out.append(str(d['s2']['id']))
        data_v_out.append(str(d['s2']['id']) + ' ' + d['arxiv']['categories'])
        data_v_out.append('a:' + d['arxiv']['title'].replace('\n', '').replace('  ', ' '))
        data_v_out.append('s:' + d['s2']['title'].replace('\n', ''))
        data_v_out.append('\n')

    percentage = round(len(data_out)/len(matched_jso_data) * 100, 2)
    print(f'({len(data_out)}/{len(matched_jso_data)}) ({percentage}%) records are also found in s2 dataset')

    output_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 'index', "arxiv_" + cate['name'] + "_data_in_s2id.txt")
    with open(output_data_file_name, "w") as leaned_raw_data:
        leaned_raw_data.write("\n".join(data_out))
        print(output_data_file_name + " saved")

    output_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 'index', "arxiv_" + cate['name'] + "_data_in_s2id_title_compare.txt")
    with open(output_data_file_name, "w") as leaned_raw_data:
        leaned_raw_data.write("\n".join(data_v_out))
        print(output_data_file_name + " saved")

    spend_t = round(time.time() - st, 4)
    print(f'cost {spend_t} sec')

def extract_from_arxiv_cate_id_list(cate_name, amount=None):
    src = os.path.join(os.environ.get("DATA_DIR"), 'index',  "arxiv_" + cate_name + "_data_in_s2id.txt")
    id_list = []
    with open(src) as f:
        lines = f.readlines()
        for id in lines:
            id = id.strip()
            id_list.append(id)

    sufix = 'all'
    if amount != None:
        print(f'randonly pick {amount} data from {len(id_list)}')
        if (amount < len(id_list)):
            random.shuffle(id_list)
            id_list = id_list[:amount]
            rand_sequence = hex(zlib.crc32(str(time.time()).encode('utf-8')))
            sufix = f'rand_{amount}_{rand_sequence}'

    data = get_data_with_ids(id_list)
    data_out = []
    for d in data:
        d['authors'] = json.loads(d['authors'])
        data_out.append(json.dumps(d))

    
    output_s2_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_arxiv_{cate_name}_{sufix}_.data")
    with open(output_s2_data_file_name, "w") as leaned_raw_topic_data:
        leaned_raw_topic_data.write("\n".join(data_out))
        print(output_s2_data_file_name + f" saved with {len(data_out)} completed data")

def count_arxiv_data_by_cate():
    count = []
    data_count = 0
    cate_count = 0
    for c in cates:
        if c != 'cs':
            cate_count += 1
            src = os.path.join(os.environ.get("DATA_DIR"), 'index',  "arxiv_" + c + "_data_in_s2id.txt")
            with open(src) as f:
                lines = f.readlines()
                data_count += len(lines)
                count.append({
                    'name': c,
                    'count': len(lines)
                })

    count = sorted(count, key=lambda c: -c['count'])

    print(f'total {data_count} labeled testable data in {cate_count} categories')
    for l in count:
        print(l)
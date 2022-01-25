from data_process.data_completion.db_s2 import get_unchecked_partition, insert_data, check_partition,get_data
from data_process.data_completion.db_s2 import get_data_count, get_data_with_ids
import urllib.request
import shutil, os, gzip,json,zlib, time, random

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
    data = get_data(start, amount)
    data_out = []
    for d in data:
        d['authors'] = json.loads(d['authors'])
        data_out.append(json.dumps(d))

    output_s2_data_file_name = os.path.join(os.environ.get("DATA_DIR"), 's2_sample', f"completed_s2_{int(start) + 1}_to_{int(start) + int(amount)}.data")
    with open(output_s2_data_file_name, "w") as leaned_raw_topic_data:
        leaned_raw_topic_data.write("\n".join(data_out))
        print(output_s2_data_file_name + f" saved with {len(data_out)} completed data")

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
    
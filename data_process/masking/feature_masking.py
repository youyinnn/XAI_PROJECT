import json
import os
from pathlib import Path

# example_src = 'expect_feature_data_example.json'
example_src = '/Users/ayuee/Documents/GitHub/XAI_PROJECT/data_process/data/completed_cs_ml.data'


# example_src = 'test.data.json'

def save_masking_json(data, masking_feature):
    base_dir = str(Path(__file__).resolve().parent)
    # data_dir = os.path.join(base_dir, "data_process", "data")
    data_dir = os.path.join(base_dir)
    os.environ.setdefault("DATA_DIR", data_dir)
    output_data_file_name = os.path.join(os.environ.get("DATA_DIR"), "masking_" + masking_feature + ".data.json")
    with open(output_data_file_name, "w") as leaned_raw_data:
        for dict_data in data:
            leaned_raw_data.write(dict_data + "\n")
        print(output_data_file_name + " saved")


def masking_features(src, masking_feature='title'):
    data = []
    count = 0
    print(masking_feature)
    with open(src) as f:
        Lines = f.readlines()
        for line in Lines:
            line_strip = line.strip()
            count = count + 1
            jso = json.loads(line_strip)
            if masking_feature == 'authors':
                jso['authors'] = []
            elif masking_feature == 'n_citations':
                jso['n_citations'] = 0
            elif masking_feature == 'full':
                pass
            else:
                jso[masking_feature] = " "

            del jso["id"]
            keys = jso.keys()
            keys_arr = []
            for key in keys:
                keys_arr.append(key)
            # del None properties
            for key in keys_arr:
                if (jso[key] == None):
                    del jso[key]
            data.append(json.dumps(jso))
    save_masking_json(data, masking_feature)


def masking_processing_features():
    pass


def mask_all_features(src):
    features = ['title', 'abstract', 'venue', 'authors', 'year', 'n_citations', 'full']
    for feature in features:
        masking_features(src, feature)


mask_all_features(example_src)

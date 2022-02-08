import json, os

def write_file(data, output_file_path):
    with open(output_file_path, "w") as leaned_raw_data:
        leaned_raw_data.write("\n".join(data))

def varient(seed_list_path, source_data_path):
    source_file_name = os.path.basename(source_data_path).replace('.data', '').replace('completed_s2_arxiv_', '').replace('_all_', '')
    source_data = []
    with open(source_data_path) as f:
        Lines = f.readlines()
        for line in Lines:
            src_paper = json.loads(line.strip())
            source_data.append(src_paper)
    
    with open(seed_list_path) as f:
        Lines = f.readlines()
        for line in Lines:
            seed = json.loads(line.strip())
            
            f_list = ['title', 'abstract', 'venue', 'authors']
            
            for f in f_list:
                varient_data = []
                for src in source_data:
                    v = {**seed}
                    v[f] = src[f]
                    varient_data.append(json.dumps(v))
                    
                write_file(varient_data, 
                        os.path.join('data_process/data/variant', f'{seed["id"]}-{source_file_name}-variant-in-{f}.data'))

            varient_data = []
            for i in range(2050):
                v = {**seed}
                v['year'] = i
                varient_data.append(json.dumps(v))
                
            write_file(varient_data, 
                    os.path.join('data_process/data/variant', f'{seed["id"]}-{source_file_name}-variant-in-year.data'))
            
            varient_data = []
            for i in range(10000):
                v = {**seed}
                v['n_citations'] = i
                varient_data.append(json.dumps(v))
                
            write_file(varient_data, 
                    os.path.join('data_process/data/variant', f'{seed["id"]}-{source_file_name}-variant-in-citation.data'))
            
                
            
### Data extraction

Extract data from the original data set: https://www.kaggle.com/Cornell-University/arxiv

Put the data in: `data_process/data/arxiv-metadata-oai-snapshot.data`.

#### Extract by arxiv top categories - cate level data

Cmd format:

```bash
python main.py extract-cate [category_name]
```

The value of the top categoreis can be referred to: https://arxiv.org/search/advanced

Finish the config file `data_process/conf.py`:

```python
cates = {
    'cs': {
        'name': 'cs',
        'regex': '(^cs\.)|( cs\.)',
      	# ...
    }
}
```

Then:

```bash
python main.py extract-cate cs
```

This will match the regex on `categories` property and extract data which contain 'cs' on their categories properties to location: `data_process/data/raw_cs.data.json`

#### Extract by topic from categories data - topic level data

Cmd format:

```bash
python main.py extract-topic [category_name] [topic_name]
```

By configuring `data_process/conf.py`:

```python
cates = {
    'cs': {
        'name': 'cs',
        'regex': '(^cs\.)|( cs\.)',
        'topic': {
            'ml': {
                'name': 'ml',
                'regex': '(machine( |-)learning)'
            },
            'nlp': {
                'name': 'nlp',
                'regex': 'nlp'
            },
        }
    }
}
```

where there are topics defined under `cs` categories.

And:

```bash
python main.py extract-topic cs ml
```

This will find the matching records with the regex on `title` and `abstract` properties and then there is a prompt:

```bash
extracting data from .../XAI_PROJECT/data_process/data/raw_cs.data.json
by topic regx: (machine( |-)learning)
total 24435 indecial
store those index into database?(type yes if you want):
```

If typing yes, a sqlite database will be created at `data_process/data/completed_data.db`, and a table named `cs_ml` will be also created with the matched records inserted.

### Data completion

Cmd format:

```bash
python main.py fill-data [table_name] [partition_number]
```

Filling the properties which are needed by s2search tool.

Once we have the database and the table `cs_ml`, we can complete the data by calling semantic scholar api.

Every topic level data is divided into 3 partitions: 0, 1 ,2 with the original order shuffled randomly.

Then:

```bash
python main.py fill-data cs_ml 0
```

This will fill the un-filled data from partition 0 of the table `cs_ml`.

**If you want to fill all partitions just use -1 partition.**

#### Filling status check

```bash
python main.py fill-data-status [table_name] [partition_number]
```

e.g.

```bash
> python main.py fill-data-status csai_all -1
> (8/0/30000) records are completed in partition -1, 24 hrs 59 min 36 sec left
```

### How to get completed data

Cmd format:

```bash
python main.py export-data [table_name]
```

E.g

```bash
python main.py export-data cs_ml
```

\*Make sure you have the following files:

1. `data_process/data/raw_cs.data.json`
2. `data_process/data/completed_data.db` with completed data in table `cs_ml`

### Feature Masking

Input: completed_cs_ml.data

Change the example_src in feature_masking.py to the input file.

Go into ./masking

```bash
python feature_masking.py 
```
Output: json files under ./masking


### S2 Scoring

Go to this Repo https://github.com/DingLi23/s2search

Install requirments, Run the following to test if you have the correct Model.

```bash
python s2search_example.py 
```

Change the root_dir to your local filepath of ./masking

```bash
python s2search_score.py 
```
The data by numpy file are generated in s2search file folder.

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

``` python
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

``` python
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

``` bash
python main.py extract-topic cs ml
```

This will find the matching records with the regex on `title` and `abstract` properties and then there is a prompt:

``` bash
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

``` bash
python main.py fill-data cs_ml 0 
```

This will fill the un-filled data from partition 0 of the table `cs_ml`.


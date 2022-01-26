## Data Processing

### Arxiv data source

<details>
 <summary>Extracting data from <strong>Arxiv</strong> dataset and semantic api(outdated !!!)</summary>

#### Data extraction

Extract data from the original data set: https://www.kaggle.com/Cornell-University/arxiv

Put the data in: `data_process/data/arxiv-metadata-oai-snapshot.data`.

##### Extract by arxiv top categories - cate level data

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

##### Extract by topic from categories data - topic level data

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

#### Data completion

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

##### Filling status check

```bash
python main.py fill-data-status [table_name] [partition_number]
```

e.g.

```bash
> python main.py fill-data-status csai_all -1
> (8/0/30000) records are completed in partition -1, 24 hrs 59 min 36 sec left
```

#### How to get completed data

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

</details>

### S2AG data source

#### Download the pre-processed s2ag data

Get the pre-processed s2 data from: [Google Drive](https://drive.google.com/file/d/1mIK7cVGGVJFCv5Cied-X_Z12Uv-lmFk2/view?usp=sharing)

Put the `completed_cs_data_s2.db` file in `data_process/data/` .

Check the amount of data with:

```bash
python main.py s2-cs-data-count
```

#### Extract the sample data

Then you can extract the data with:

```bash
python main.py export-s2-cs-data [start_number] [amount]
```

or

```bash
python main.py export-s2-cs-data-rand
```

The first command will extract the `amount` of data started from `start_number` and store the data into `data_process/data/completed_s2_{start_number + 1}_to_{start_number + amount}.data`.

E.g:

```bash
python main.py export-s2-cs-data 1 30
```

will get the `data_process/data/s2_sample/completed_s2_1_to_30.data` file.

The second command will extract 30000 randomly shuffled data from all the data(out of 6.6 million data).

#### Extract data which labeled by arxiv

Semantic scholar only give the top categories about the very base subject of the paper:

- Computer Science
- Physics
- ...

and there are almost 6.6 millions testable records related to "Computer Science".

Say we have 10000 data and a query "machine learning" as input to the s2search. A more likely outcome is that only 3000 scores are valid scores.

For better s2search score computing, we should **narrow the topic** of the input data to the same topic of the input query.

Hence we use the second level categories defined by arxiv: https://arxiv.org/category_taxonomy.

Extract the data by the following steps:

1. **Create database index**

   Create database index first!

   Use command:

   ```bash
   python main.py create-s2-index
   ```

2. **Extract all data or randomly pick amount of data**

   1. To extract all:

      ```bash
      python main.py arxiv-s2-data-extract [second_cate_name]
      ```

      E.g.

      ```bash
      python main.py arxiv-s2-data-extract csai
      ```

      will then extract all data which have `cs.AI` label on arxiv to location `data_process/data/s2_sample/completed_s2_arxiv_csai_all_.data`.

   1. To extract certain amount of data randomly:

      ```bash
      python main.py arxiv-s2-data-extract-rand [second_cate_name] [amount]
      ```

      E.g.

      ```bash
      python main.py arxiv-s2-data-extract-rand csai 1000
      ```

      will then extract 1000 data randomly which have `cs.AI` label on arxiv to location `data_process/data/s2_sample/completed_s2_arxiv_csai_rand_1000_0x2b68d8c1_.data`.

#### Data count of arxiv labeled data:

<details>
	<summary>Show</summary>

```bash
>>> python main.py arxiv-s2-data-count
total 542877 labeled testable data in 40 categories
{'name': 'cslg', 'count': 92938}
{'name': 'cscv', 'count': 64531}
{'name': 'csai', 'count': 39344}
{'name': 'csit', 'count': 35936}
{'name': 'cscl', 'count': 30750}
{'name': 'cscr', 'count': 19374}
{'name': 'csds', 'count': 19017}
{'name': 'cssy', 'count': 17812}
{'name': 'csni', 'count': 16422}
{'name': 'csro', 'count': 16088}
{'name': 'csdc', 'count': 14754}
{'name': 'cssi', 'count': 14279}
{'name': 'cslo', 'count': 11918}
{'name': 'csna', 'count': 11581}
{'name': 'cscy', 'count': 10994}
{'name': 'csdm', 'count': 10444}
{'name': 'csir', 'count': 10273}
{'name': 'csne', 'count': 9834}
{'name': 'csse', 'count': 9761}
{'name': 'cscc', 'count': 8752}
{'name': 'cshc', 'count': 8608}
{'name': 'csgt', 'count': 8030}
{'name': 'cssd', 'count': 7046}
{'name': 'csdb', 'count': 6230}
{'name': 'cscg', 'count': 5529}
{'name': 'cspl', 'count': 5471}
{'name': 'csma', 'count': 4562}
{'name': 'csce', 'count': 4436}
{'name': 'csfl', 'count': 3657}
{'name': 'csdl', 'count': 3603}
{'name': 'csmm', 'count': 3580}
{'name': 'csgr', 'count': 3233}
{'name': 'cspf', 'count': 2795}
{'name': 'csar', 'count': 2563}
{'name': 'cset', 'count': 2550}
{'name': 'csoh', 'count': 1944}
{'name': 'cssc', 'count': 1786}
{'name': 'csms', 'count': 1670}
{'name': 'csos', 'count': 633}
```

</details>

## Feature Masking

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

### Data extraction

Extract data from the original data set: https://www.kaggle.com/Cornell-University/arxiv

Put the data in: `data_process/data/arxiv-metadata-oai-snapshot.data`.

#### Extract by arxiv top categories

The value of the top categoreis can be referred to: https://arxiv.org/search/advanced

Just call:

```python
python main.py extract-cate cs
```

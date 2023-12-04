# Explore Danbooru

Since [Danbooru2021](https://gwern.net/danbooru2021) haven't been updated for a while, I decided to extract the latest (by 2023-11-30) dataset from the [public cloud storage of Danbooru](https://console.cloud.google.com/storage/browser/danbooru_public/data?project=danbooru1) and work on the data processing pipeline with [PostgresSQL](https://www.postgresql.org/).

You could download the compressed dataset from [huggingface](https://huggingface.co/datasets/Crosstyan/danbooru-public).

## TODO

- [ ] provide a [parquet](https://parquet.apache.org/) version of the dataset

## See also

- [fire-eggs/Danbooru2021](https://github.com/fire-eggs/Danbooru2021)
- [Danbooru Tags Explorer](https://nsk.sh/tools/danbooru-tags-explorer/)
- [an analysis of Danbooru tags and metadata](https://nsk.sh/posts/an-analysis-of-danbooru-tags-and-metadata/)
- [Danbooru Public (requester pays)](https://console.cloud.google.com/storage/browser/danbooru_public/data?project=danbooru1)
- [isek-ai/danbooru-tags-2016-2023](https://huggingface.co/datasets/isek-ai/danbooru-tags-2016-2023)

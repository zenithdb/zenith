# Revisiting the Inverted Indices for Billion-Scale Approximate Nearest Neighbors

This ANN extension of Postgres is based
on [ivf-hnsw](https://github.com/dbaranchuk/ivf-hnsw.git) implementation of [HNSW](https://www.pinecone.io/learn/hnsw),
the code for the current state-of-the-art billion-scale nearest neighbor search system presented in the paper:

[Revisiting the Inverted Indices for Billion-Scale Approximate Nearest Neighbors](http://openaccess.thecvf.com/content_ECCV_2018/html/Dmitry_Baranchuk_Revisiting_the_Inverted_ECCV_2018_paper.html),
<br>
Dmitry Baranchuk, Artem Babenko, Yury Malkov

# Postgres extension

HNSW index is hold in memory (built on demand) and it's maxial size is limited
by `maxelements` index parameter. Another required parameter is number of dimensions (if it is not specified in column type).
Optional parameter `ef` specifies number of neighbors which are considered during index construction and search (corresponds `efConstruction` and `efSearch` parameters
described in the article).

# Example of usage:

```
create extension hnsw;
create table embeddings(id integer primary key, payload real[]);
create index on embeddings using hnsw(payload) with (maxelements=1000000, dims=100, m=32);
select id from embeddings order by payload <-> array[1.0, 2.0,...] limit 100;
```

# hes-dimport
hes-dimport is a elasticsearch plugin which generate shards files of index in mapreduce.

This plugin is applied to the offline import index, each import will generate an index, when used to define the template to organize these indexes together

## Compared to bulk's load API elasticsearch, its advantages are below:
* In a node that improves the write performance of 30%, when there is a large scale of Hadoop clusters, the overall write throughput is linearly increased.
* The throughput is linear increase when scale-out hadoop clusters.
* The index is generated once, and can be pushed to shard primary and multiple shards replica.
* Make the elasticsearch read and write separate,do not affect the performance of the read.
* When there is a large volume of data written, you can use the relatively inexpensive hardware to extend the cluster, when you need a higher query qps and low latency, you can use a better HW.
* Based on available of MapReduce job, the write is with HA.

# How to use
## package plugin
```
wget https://github.com/kewn21/eagle-hes.git
cd eagle-hes
mvn package
```
## install to elasticsearch
```./bin/plugin install eagle-hes.zip```
## mapreduce job
```hadoop jar HesDimportJob {-Dindex.name={index_name} -Dindex.type={index_type} [-Dautogenerate.id={true|false}]} [-Dfile={meta_file_path}] {input_file_path} [delimiter]```
### You can describe the data format in the original data, or define the data in the metafile
#### original data with format
* Each column is separated by the delimiter,when autogenerate.id is set to false, the first is id.
* The others be separated by comma, the column name and value and type be together with a colon.

#### meta file format
* The definition of each column be separated by comma, the column name and type be together with a colon.



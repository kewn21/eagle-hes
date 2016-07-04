# hes-dimport
hes-dimport is a elasticsearch plugin which generate shards files of index in mapreduce.

This plugin is applied to offline import index, each time will generate an index, so that you have to define the template to organize these indexes together.

Support `Integer` and `Long` and `String` and `Double` and `Float` and `Short` data types.

Compared to bulk's load API elasticsearch, it's advantages are below:
* In a node that improves the write performance of 30%, when there is a large scale of Hadoop clusters, the overall write throughput is linearly increased.
* The throughput is linear increase when scale-out hadoop clusters.
* The index is generated once, and can be pushed to shard primary and multiple shards replicas.
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
```./bin/plugin install eagle-hes-dimport-plugin-0.0.1.zip```
## mapreduce job
```hadoop jar HesDimportJob {-Dindex.name={index_name} -Dindex.type={index_type} [-Dautogenerate.id={true|false}]} [-Dfile={meta_file_path}] {input_file_path} [delimiter]```
### metadata 
You can describe the data format in the original data, or define in the metafile
#### original data with metadata
* Each column is separated by the delimiter, when property `autogenerate.id` is set to false, the first is for id.
* Other columns be separated by `,`, the column name and value and type be together with `:`.

#### metadata format
* The definition of each column be separated by `,`, the column name and type be together with `:`.



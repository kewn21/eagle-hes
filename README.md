# hes-dimport
hes-dimport is a elasticsearch plugin which create shards files of index in mapreduce.

##Constrast elasticsearch bulk load api, it's advantage is below:
* In a node that improves the write performance of 30%, when there is a large scale of Hadoop clusters, the overall write throughput is linearly increased;
* The throughput is linear increase when scale-out hadoop clusters;
* The index is generated once, and can be pushed to shard primary and multiple shards replica;
* Make the elasticsearch read and write separate,do not affect the performance of the read;
* When there is a large volume of data written, you can use the relatively inexpensive hardware to extend the cluster, when you need a higher query QPS and low latency, you can use a better HW;
* Based on available of MapReduce job, the write is with HA.


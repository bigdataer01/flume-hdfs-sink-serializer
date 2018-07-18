# flume-hdfs-sink-serializer
custom flume hdfs sink serializer which used to write binary data to hdfs

# usage
a1.sinks.sink1.hdfs.serializer=net.bigdataer.demo.flume.sink.serializer.CustomAvroToHdfsSerializer

view src/main/resources/avro-hdfs-sink.conf and see how to confige

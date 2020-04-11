---
layout: post
title: "Flink exactly-once 实战笔记"
categories: ["Apache Flink"]
date: 2019-04-12T14:59:28+08:00
lastmod: 2019-04-12
draft: false
---

## Flink-Kafka
众所周知，Flink在很早的时候就通过Checkpointing提供了exactly-once的semantic，不过仅限于自身或者是从KafkaConsumer中消费数据。而在Flink 1.4版本的时候加入了赫赫有名的TwoPhaseCommitSinkFunction，提供了End-to-End的exatcly-once语言，当然是在需要下游支持回滚的情况下，具体的概念和设计方式官网已经写的比较清楚，就不多加赘述。而对于KafkaProducer，Kafka在0.11版本之后支持transaction，也就意味着支持对写入数据的commit和rollback，在通过Flink写入到Kafka的应用程序中可以达到exactly-once的效果。

接下来展示一下如何在Flink应用程序中激活exactly-once语义。对于SourceFunction大家随意采用一种即可，文件，kafka topic等皆可。而主要部分是在于对FlinkKafkaProducer的初始化。我使用的是Flink1.7版本使用的Producer类为FlinkKafkaProducer011，观察它的构造函数，很容易发现有的构造函数中需要你传入一个枚举变量semantic, 有三种可选值**NONE**, **AT_LEAST_ONCE**,**EXACTLY_ONCE**，而默认值为**AT_LEAST_ONCE**，很显然我们在这里需要使用**EXACTLY_ONCE**。不过在此之前，我们需要仔细阅读一下Flink官网[Flink-kafka-connector](https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html)的内容，其中提到，Kafka broker的transaction.max.timeout.ms默认为15分钟，而FlinkKafkaProducer011默认的transaction.timeout.ms为1个小时，远远超出了broker的最大超时时间，这种情况下如果你的服务挂了超过15分钟，就会造成数据丢失。所以如果需要你的producer支持的更长的事务时间就需要提高kafka broker transaction.max.timeout.ms的值。下面是一个简单的实例去使用Exactly-once语义的FlinkKafkaProducer。
``` java
FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
    topics,
    new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
    properties,
    FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
)
```
这么做的话Flink sink到Kafka中在大部分情况下就都能保证Exactly-once。值得注意的是，所有通过事务写入的Kafka topic, 在消费他们的时候，必须给消费者加上参数isolation.level=read_committed，这是因为Kafka的事务支持是给写入的数据分为committed和uncomitted，如果使用默认配置的consumer，读取的时候依然会读取所有数据而不是根据事务隔离。

## Flink-Hdfs
目前我们使用的cdh中hadoop版本为2.6，Hadoop在2.7版本后对Hdfs支持了truncate操作，会使得回滚机制感觉方便快捷。这里只谈一下关于低版本Hdfs flink的容错机制，以及我们自身对写入消息进行gzip压缩所遇到的一些坑。

flink-connector-filesystem的源码中提供了BucketingSink来支持文件在文件系统上的滚动写入。BucketingSink对象通过传入自定义Writer来执行写入的方式，研究下面的一些已经实现的Writer类可以发现，BucketingSink通过hadoop API的FSDataOutputStream来创建文件流和写入。而FSDataOutputStream中又包裹了一个PositionCache类来记录文件流每次运行的状态。
``` java
private static class PositionCache extends FilterOutputStream {
    private FileSystem.Statistics statistics;
    long position;

    public PositionCache(OutputStream out, 
                         FileSystem.Statistics stats,
                         long pos) throws IOException {
      super(out);
      statistics = stats;
      position = pos;
    }

    public void write(int b) throws IOException {
      out.write(b);
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public void close() throws IOException {
      out.close();
    }
  }
```
从该类中可以看到，文件流每次执行write操作的时候，PosistionCache都会刷新他本身的position变量。而BucketingSink的BucketState则会通过这个变量来更新currentFileValiedLength成员来记录文件的有效长度。当Job因为某种原因down了之后，checkpoint会记录bucketState的信息，在任务恢复的时候，会在文件系统上生成一个valid-length文件来表明该文件的有效长度（单位：Byte）是多少（Hadoop2.7后的truncate()功能可以直接帮你truncate掉多余的内容，但是低版本就需要自己处理了）。

在我们的业务环境中，需要对写入的文本文件进行gzip压缩，Flink目前只提供了SequenceFile和Avro格式的Writer，并没有提供普通的原生文本压缩支持。所以需要我们自己编写Writer。在这值得注意的是对于压缩流库的选择，我们选择了java.util.zip下的GzipOutpuStream而不是org.apache.hadoop.io下的CompressOutputStream，原因是后者不支持对压缩数据流设置syncFlush，因此在调用flush()方法的时候只会flush outputStream而前者会先flush底层的compressor。后者在使用中会导致PositionCache的position不正常从而导致valid-length不可用而无法达到hdfs的exactly-once语义。

``` java
public class HdfsCompressStringWriter extends StreamWriterBase<JSONObject> {

    private static final long serialVersionUID = 2L;

    /**
     * The {@code CompressFSDataOutputStream} for the current part file.
     */
    private transient GZIPOutputStream compressionOutputStream;

    public HdfsCompressStringWriter() {}

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        super.open(fs, path);
        this.setSyncOnFlush(true);
        compressionOutputStream = new GZIPOutputStream(this.getStream(), true);
    }

    public void close() throws IOException {
        if (compressionOutputStream != null) {
            compressionOutputStream.close();
            compressionOutputStream = null;
        }
        /** 
        --此处对StreamWriterBase类进行了修改添加了resetStream方法来将内部的FSDataOutputStream置空，
        不然在close的时候如果已经通过compressionOutputStream关闭流则FSDataOutputStream对象没有置空
        会导致再下一次open的时候报Stream already open的错误。
        */
        resetStream();
    }

    @Override
    public void write(JSONObject element) throws IOException {
        if (element == null || !element.containsKey("body")) {
            return;
        }
        String content = element.getString("body") + "\n";
        compressionOutputStream.write(content.getBytes());
        compressionOutputStream.flush();
    }

    @Override
    public Writer<JSONObject> duplicate() {
        return new HdfsCompressStringWriter();
    }

}
```

通过自定义的GzipWriter，如果任务遇到异常，checkpoint会记录valid-length来让我们恢复成准确无重复的数据。但是由于我们是2.6版本的Hadoop，只能将压缩文件从Hdfs上get下来处理。而由于gzip非文本文件，而且在文件尾部有一个4字节的滚动更新的CRC32编码，和另外一个4字节的ISIZE代表原始非压缩文件的长度对$2^32$求模，简单的truncate会导致gzip文件损坏而无法通过正常的解压缩读取。不过Gzip本身的压缩文本是以chunk形式连续存在的，zcat命令可以在不压缩的情况下读取有效的内容。所以如果我们需要修复原始的文件，则大致必须通过以下方式。
``` shell
length=$(hdfs dfs -text /path/to/my/file.valid-length) # 有时候获取的length还有一些奇怪的空字符和特殊字符要处理比如('\x0' or ^M^H等)
hdfs dfs -get /path/to/my/file.gz myfile.gz
truncate myfile.gz -s $length
zcat myfile.gz > myfixedfile
gzip myfixedfile
```
现在的问题是，如果文件数较多且大小不小的时候，通过脚本逐步执行这些效率会非常低，所以目前也在寻找更完善的方式去达成这个问题。

## Flink-Elasticsearch & Hbase
对于Es，我们用了整条数据的哈希作为uuid，对于Hbase每条数据也同样有固定的rowkey，因此只需要AT_LEAST_ONCE语义就可以保证数据不缺失不重复。

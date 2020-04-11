---
layout: post
title: "Hadoop-Streaming批量通过Flink-HDFS生成的valid-length文件修复压缩的part文件"
categories: ["Apache Flink"]
date: 2019-05-23T14:59:28+08:00
lastmod: 2019-05-23
draft: false
---

使用过Flink-connector-hdfs做Flink sink到HDFS的朋友都知道，对于低于2.7版本的hadoop来说，HDFS并不支持文件系统的truncate的操作，所以Flink应用无法对已经写入的HDFS文件进行修改，再遇到需要从Checkpoints回滚的情况就会生成一个valid-length文件来表明对应的part文件的有效长度是多少。特别是在我们的case，文件写入是Gzip压缩的情况，及时支持truncate也无法轻松恢复（由于流式压缩写入的缘故，简单的truncate将造成Gzip压缩文件损坏）。

## 流程
对于Gzip压缩文件，每修复一个需要的流程是：
1. `Hdfs dfs -get [path] file.gz`
2. `Hdfs dfs -text [_path.valid-length]`
3. `truncate file.gz -s [valid-length]` 
4. `zcat file.gz > file`
5. `gzip -f file`
6. `Hdfs dfs -put file.gz [path]`

## 并行处理
因此如果有大量的文件要修复，一个一个去执行显然效率会非常的低。但是我们可以想到，这些每个文件的处理都可以独立进行，而且除了压缩其他大部分操作都是I/O密集型，所以很自然可以想到开多进程去处理，Bash脚本或者Python multiprocessing都可以做到。但是我在尝试使用Python多进程去处理的时候发现，只要并行度稍稍开高，对机器的CPU压力还是相对比较大的，资源无法被有效的管理和控制。而且多进程异步处理还需要考虑一套比较完善的异常处理，不然很有可能子进程的报错无法被感知。

## Hadoop Streaming
单机的要求无法满足我们自然考虑分布式的方法去解决这个问题，能够更好的平衡资源。而其实我们的任务只是根据文件名来分配任务，其实是一个非常简单的分布式计算任务，只需要维护一个文件名的队列，让每个节点去取然后执行相关命令即可。目前也有许多框架如Celery也可以很简单的达到我们的目的。不过发现Hadoop光法有一个Hadoop streaming的jar，可以用任意语言来定义自己的mapper和reducer来跑MR任务。

首先因为我们的任务很明显是按文件名进行切分，所以首先需要获得所有需要处理的valid-length文件，并将文件上传至HDFS可以作为Hadoop streaming的Input。
``` bash
hdfs dfs -ls -C /path/to/your/file/*valid-length > filenames.txt
hdfs dfs -put -f /path/to/input/filenames.txt
```

``` bash
#!/bin/bash -e
game=l12
FIX_PATH=/your/data/path
DOWNLOAD_PATH=$FIX_PATH/download
UPLOAD_PATH=$FIX_PATH/upload

mkdir -p $DOWNLOAD_PATH
# mkdir -p $UPLOAD_PATH

while read dummy filename ; do
    echo "Processing $filename"
    # 通过valid-length文件名获取对应的part文件名
    part=${filename//_part/part}
    part=${part//.valid-length/}
    # 获取valid-length的数值
    valid_length=$(hdfs dfs -text "$filename")
    valid_length=${valid_length//[!0-9]/}
    download_gz=${part//\//\_}
    download_gz=$DOWNLOAD_PATH/${download_gz:1}
    # 下载gz文件
    hdfs dfs -get $part $download_gz
    # 截取有效长度
    truncate $download_gz -s $valid_length
    decompressed=${download_gz::-3}
    # 重新生成压缩文件
    zcat $download_gz > $decompressed 2>/dev/null || true
    rm $download_gz
    gzip $decompressed
    hdfs dfs -put -f $download_gz $part
    hdfs dfs -rm -r $filename
    rm $download_gz
done
```
我们编写一个可执行的脚本文件fix.sh用作mapper，文件的输入应该是"key value"的形式，我们不需要key只需要value，就是我们在之前生成的valid-length文件路径。注意需要确保该文件被赋予了执行权限。

在新建好对应的输入文件和输出文件之后，就可以使用Hadoop streaming的命令行工具来执行我们的修复过程了。

``` bash
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -Dmapred.reduce.tasks=0 \
    -mapper ./fix.sh \
    -input /path/to/your/input/filenames.txt \
    -output /path/to/your/output/ \
    -verbose \
    -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
    -file fix.sh
```
其中NLineINputFormat告诉Job对每一个输入分配一个maptask，能够让Hadoop集群最大的利用计算资源。

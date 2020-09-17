---
layout: post
title: "基于Flink与Kafka再分区的一种日志检索方式"
date: 2020-07-19T16:00:01+08:00
categories: ["Apache Flink","Apache Kafka"]
draft: false
---

## 背景

在当前大数据的场景之下，高速，实时的日志数据检索已经成为非常常见的需求，有助于用户可以快速定位业务或是程序的问题。

目前我们在工作中通常使用Kafka消息队列接收各游戏和服务产生的数据日志，并将其进行结构化处理并落盘到HDFS上，每天通过批处理将HDFS上数据导入数仓，给用户提供T+1级别的查询。而通常对于有高实时性查询需求的日志，则将日志数据处理后写入到某些分布式存储引擎或是搜索引擎（如Hbase, Elasticsearch和Kudu）。

由于某些日志数据数量较大，且用户查询次数较为低频，但是在需要之时又需要能够相对快速，实时的查询数据，采用原有方法会导致在不同的存储中冗余多份数据，造成硬件资源的长期占用。因此，我们设计了一种通过Flink计算引擎根据时间段查询Kafka和HDFS数据的方式，为用户提供低频需求下的实时日志检索服务。

## 技术设计

### 原有方案

![img](https://gitee.com/ysn2233/imgurls/raw/master/img/clip_image002.png)

上图为原有大数据检索方式。将发送的Kafka的数据经处理后以一定格式写入到HDFS中，通过定时任务使用Hive按天处理HDFS数据作为数据仓库。

对于需要实时检索的需求的数据，则另起任务将数据以必要的格式写入Elasticsearch/Kudu/Hbase等下游存储引擎。

对于数据量特别大的日志数据，额外写入一份进入Elasticsearch等引擎会占用大量的存储资源。若在不频繁使用的情况下性价比极低。

### 改进方案

![img](https://gitee.com/ysn2233/imgurls/raw/master/img/clip_image001.png)

如上图所示，本发明提出的实时日志检索方案只基于Kafka和Hdfs，与其余数据服务共用相同的存储，避免了新的存储引擎介入。而现有基于Kafka的数据查询方式并发量高度依赖于Kafka topic的分区数，如果Kafka topic的分区数不高，则无法在查询时充分利用分布式计算资源的优势，查询效率不佳。因此本发明又对从Kafka中查询历史数据的方式进行改善，以支持更高的并行度来提升效率。

### Kafka再分区

![kafka_partitions](https://gitee.com/ysn2233/imgurls/raw/master/img/kafka_partitions.png)

通常情况下，Kafka最高支持的并发量与Kafka topic的总分区数想同，如果一个Kafka topic的分区数为N，对于官方的Kafka客户端，至多只有N个线程同时消费该topic，由于需要查询的是Kafka中的历史数据，本发明提出的方案为，将每个分区在时间维度上再切分成M份，每一个线程去消费一个子分区的内容，则一共可以支持N×M个线程去并行消费Kafka，大大提高了Kafka批处理的吞吐量和速度。其中，子分区可设定最小记录值，防止过度切分影响效率。

### 最终实现

该方案使用Spark或者是Flink批处理API都可以实现想要的效果，由于我对Flink比较熟悉，就使用了Flink DataSet API实现了对Kafka在时间维度上的再分区查询。最后的查询结果将会结合HDFS上的T+1离线部分和Kafka中的当天实时部分进行联合过滤而给出最终结果。



## 总结

该方案主要对已使用的存储Kafka和HDFS进行复用，使用Flink直接对其中的数据进行检索，防止冗余多份数据到Elasticsearch, Kudu等查询引擎。尽管在查询速度上不及建好索引的存储引擎，但是对于低频查询操作来说，避免了大量数据长时间占用硬件资源。另外对于查询Kafka中离线数据，使用了再分区的方案突破了并行度的限制，大大加速了对于Kafka的离线查询。




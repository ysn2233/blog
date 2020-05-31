---
layout: post
title: "剖析Flink-on-Yarn-Taskmanagers分配过于集中问题"
categories: ["Apache Flink"]
draft: true
---

## 问题

我们拥有一个超过150个节点的CDH5.15集群，自带的Hadoop版本为2.6.0。

在工作中我们使用Flink将Kafka中的数据写入到HDFS，并根据日期和某一个字段进行分区。我们在Yarn上跑这些Flink Job的时候发现任务经常失败，查看日志发现在写入HDFS的时候和Datanode连接发生了错误，并且无法对任务做Checkpoint。

## 原因

根据我们的排查，发现问题产生主要产生原因如下

- CDH本身默认对每个进程打开的文件描述符数量有限制
- 由于分区字段在不同数据中的取值较多，加上数据量很大，在写入时每个并行度都会同时打开非常多的文件
- Flink on Yarn中Yarn将所有Taskmanager都调度到了一到两个容器中，这样大部分甚至所有子任务都集中到一台物理机器上运行
- 由于Flink默认写入HDFS也是优先向本地的DataNode写入的
- **由于上面几条原因的共同作用，导致TM集中的那个物理节点上HDFS的DataNode会遭遇多个并行度同时并发写入，并且同时打开大量的文件导致文件描述符大量飙升而引发报错**

## 方案

### 调整CDH文件描述符限制

CDH默认对于每个进程打开的文件描述符数量有限制，默认为32768。我们首先调高该限制提升几倍，但是发现结果依旧报错，原来是Datanode的并发进程数也达到了上限，因此我们继续提高`dfs.datanode.max.transfer.threads`这个参数(低版本Hadoop为`dfs.datanode.max.xcievers`)。但是我们发现即使把这两个参数到增大到可以支撑写入的并发量，Datanode依然会因为线程太多处理不过来而严重影响性能甚至依然失败。

### Rocksdb Statebackend打开大量小文件

阿里的工程师提出使用Rocksdb状态后端可能引起checkpoint时打开大量小文件，增加对HDFS的压力，并且提出了运用合并sst文件方式的解决方案[^1]。然而我们无论是使用了最新分支的Flink或是直接换用FsStatebackend都无法解决问题，这表明我们遇到的困难主要还是由于Yarn将TM都调度到同一台物理机并且Flink任务的每个并行度在同一时刻都会打开非常多的文件这两个因素作用而成。

### 调整Yarn的调度方式



[^1]: https://issues.apache.org/jira/browse/FLINK-11937


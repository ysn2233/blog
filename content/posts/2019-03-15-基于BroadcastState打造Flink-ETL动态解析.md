---
layout: post
title: "基于BroadcastState打造Flink ETL动态解析"
categories: ["Apache Flink"]
date: 2019-03-15T14:59:28+08:00
lastmod: 2019-03-15
draft: false
---

ETL是每个公司大数据部门最重要的部分之一，一个稳定，高效，高质量的ETL流程是任何对大数据有要求的公司所追求的。多年以来ETL的技术也在光速的进行发展和更新，包括从Mapreduce等离线数据处理方式到后来的Flume,Logstash,Filebeat等数据采集工具和现在火热的Spark Streaming以及Flink的出现。本文讲述了使用Apache Flink来实现ETL流程并利用它的BroadcastState来实现对数据处理方式的动态更新。

Apache Flink最大的优势是通过Checkpointing和两段协议可以实现大部分情况下数据的Exactly-once语义，提供高质量的数据采集。同时它是一个真正的流处理框架，也是今后的大势所趋，方便之后的技术扩展和转型。

##  ETL流程

<div class="mermaid">
graph LR
A[Kafka] --> B(Flink)
X["Kafka(Parse pattern)"] --> B
B -- Parse --> C["Kafka(Formatted)"]
C --> D(Flink)
D --> E[ElasticSearch]
D --> F[Hdfs]
D --> G[Hbase]
D --> H[...]
</div>

以上是一个Flink ETL方案的流程图。

我们对获取的原始数据使用Flink的算子进行解析，转换成需要的格式并存储到新的Kafka Topic中去，然后继续利用Flink将预处理过的数据以各种需要的方案写入到不同的存储中去。其中Kafka(Formatted)这部的临时存储视需求而定可以省略直接将数据写入到Es, Hdfs, Hbase中。

## 配置更新

显而易见，对原始数据做处理通常有一个解析的方案，而这个方案很有可能会根据业务的需求进行不断的修改和迭代。"Hard code"显然是一个最直接但是却非常不优雅的方案，开发人员更多的会想把解析的逻辑抽取出来进行解耦，以配置文件或者Pattern的形式来表达数据处理的方案，比如Properties文件或者JSON，然后每次读取这个逻辑来进行解析。

但是如何让配置的更新被数据处理框架感知到呢？最简单的方式是通过文件或其他方式存储然后通过重启任务进行重新读取，或者是像Flume/Logstash那样，另起一个专门的线程对配置文件轮询内容是否变化。不过Flink不像Flume和Logstash那样通过在服务器是起多个实例来达成分布式处理，而是有一个自己的或者是yarn的集群，通过JobManager来对分布式任务做资源调度，本地文件显然不是一个很好的解决方案，而运用分布式文件系统或者数据库可能是一个解决的方式。

不过Flink在1.5版本引入了BroadcastState概念，它允许Flink建立另外一个stream并将其中的内容广播到下游的所有operator中来进行state的更新并用这些state来处理主要数据流的数据。因此，在我们的case中，数据处理的配置就可以用BroadcastState来维护。每当我们需要更新自己的解析逻辑，只需要往Kafka(Parse Pattern)中写入新的逻辑就可以做到快速更新。具体的实现原理可以参考Fabian Hueske这篇[a Practical Guide to Broadcast State in Apache Flink](https://www.ververica.com/blog/a-practical-guide-to-broadcast-state-in-apache-flink)。

## 实现

```java
public abstract class AbstractParseFunction extends BroadcastProcessFunction<String, JSONObject, String> {

    private static Logger logger = LoggerFactory.getLogger(AbstractParseFunction.class);

    protected ParseConfig parseConfig;

    protected MapStateDescriptor<Void, ParseConfig> configState = new MapStateDescriptor<>(this.getClass().getName(), Types.VOID, Types.POJO(ParseConfig.class));

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parseConfig = 	loadParseConfig(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("initialConfigJson"));
    }
    
    @Override
    public void processBroadcastElement(JSONObject config, Context context, Collector<String> out) throws Exception {
    	ParseConfig parseConfig = context.getBroadcastState(getConfigState()).get(null);
        if (parseConfig == null) {
            parseConfig = this.parseConfig;
            ...
        }
    }

    @Override
    public void processElement(String event, ReadOnlyContext context, Collector<String> out) throws Exception {
        ParseConfig parseConfig = context.getBroadcastState(getConfigState()).get(null);
        if (parseConfig == null) {
            parseConfig = this.parseConfig;
        }
        ...
    }

    


```

如上述代码所示，Flink中使用BroadcastProcessFunction可以达到我们的目的。我们创建一个类继承BroadcastProcessFunction，然后设置两个成员变量。一个是Flink的state（有多种类型），我们使用MapStateDescriptor来包含一个ParseConifg的pojo类作为内容，这个ParseConifg类就代表了我们的解析逻辑信息。在processElement方法中可以发现，我们每次执行处理首先获取这个state来得到解析逻辑。另一个成员变量是一个ParseConifg对象，这个对象我们用于job刚运行conifg流中还没有数据的时候作为解析逻辑的初始化。我们在外部创建任务的时候传入一个初始化的配置（可以通过文件，参数或者硬编码各种方式），然后在open()方法中初始化这个对象。之后我们在处理数据的时候，如果发现获取的state为空，就先把state赋值为初始化的值。

## 结论

通过2个Kafka数据流，1个维护真实数据，1个维护配置逻辑，我们通过Flink的BroadcastState实现了真正的数据动态解析，不过目前初始化数据的方法还是有些不优雅，也在寻找更为完善的方式。

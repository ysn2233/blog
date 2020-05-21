---
layout: post
title: "Flink均匀KeyBy处理窗口任务"
categories: ["Apache Flink"]
date: 2019-03-21T19:59:28+08:00
lastmod: 2019-03-21
draft: false
---

## 场景

组内的监控平台有这样一个需求，不停的有程序日志会被发到Kafka中，需要实时统计一定时间内某种错误日志的数量。这个任务看上去是一个非常常规的Flink Window场景，直接使用Window API配合Event time就能很容易的达到目的。

## 局限

Flink的Window API只提供allWindow和keyedWindow，因此当使用窗口时，要么将所有数据都发往一个window算子进行处理，这样的话该算子的并行度就只有1，失去了分布式计算的意义。因此想要进行高并行度的计算，就必须对数据进行分key处理，但是很多情况下原始数据中并没有很适合用于分key的元素，或者说即使可以分key，也可能造成每个key分到的数据量差异很大，产生数据倾斜的问题，严重影响任务效率。

## 方法

为了防止数据倾斜，可以手动为每条数据分配人工生成的key，以用于保证数据可以被均匀分配到每个window。简单使用原始数据中已经有的元素，很难保证均匀分配，因此我们采用为数据去构造一个key。

``` java
DataStream<Tuple2<String, Integer>> streamWithkey = stream.map(
    
	new MapFunction(String, Tuple2<String, Integer>)) {
    	private int counter = 0;
   		@Override
    	public Tuple2<String, String> map(String msg) {
            int key = (counter++) % parallelism;
            return new Tuple2<>(msg, key);
        }
	}
);
KeyedStream<Tuple2<String, Integer>, Integer> keyedStream = streamWithKey.keyBy(1);
```

我们使用一个counter去不断累加，并对并行度取模，然后使用这个Integer作为key，但是在实际任务中我们发现，数据仅仅被发送到了其中一部分subtask，很多subtask中并没有数据。这个原因是因为Flink在使用key进行分组的时候，会对key的hashcode()再进行一次murmurhash算法，目的是为了在实际情况中尽量打散数据，减少碰撞。但是对于我们这种使用数字手工生成的key来说，比如在1-32的数字中，可能会有不少murmurhash()结果是相同，所以导致了部分subtask分配不到数据。

所以如果要解决这个问题，只能手动跑一些murmurhash()的结果，找出n个不同的key值，然后把他们用作Flink stream的key。
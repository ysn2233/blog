---
layout: post
title: "Flink均匀KeyBy处理窗口任务"
categories: ["Apache Flink"]
date: 2019-03-21T19:59:28+08:00
lastmod: 2019-03-21
draft: false
---

## 场景

##场景

组内的监控平台有这样一个需求，不停的有程序日志会被发到Kafka中，需要实时统计一定时间内某种错误日志的数量。这个任务看上去是一个非常常规的Flink Window场景
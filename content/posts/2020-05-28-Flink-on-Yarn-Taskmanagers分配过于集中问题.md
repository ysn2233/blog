---
layout: post
title: "剖析Flink-on-Yarn-Taskmanagers分配过于集中问题"
categories: ["Apache Flink"]
draft: false
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

其实很容易看出，解决该问题最好的方式还是让Yarn能够将TaskManagers均匀调度到不同的NodeManager上，这样可以较好的均摊对HDFS Datanode造成的IO压力。

#### 限制

1. 在Flink端无法控制Yarn对Taskmanager的容器分配，算法由Yarn决定
2. 当前Yarn依旧不支持Vcore和Memory以外的资源，尽管社区已经针对支持硬盘资源，IO资源进行了一定的分析和讨论，但是目前资源的分配依旧是以CPU和内存为主

#### Fair Scheduler

目前我们在集群使用的调度器为Fair scheduler，我们准备从调度器的源码(2.6.5版本)来探究解决这个问题的方法。

##### 心跳分配

调度器对于容器的分配是通过NodeManager和ResourceManager的心跳来进行的，在每次心跳时，NM会向RM发送一个`NodeUpdateSchedulerEvent`事件，并调用`nodeUpdate()`方法进行处理。

```java
// ...
case NODE_UPDATE:
      if (!(event instanceof NodeUpdateSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
      break;
// ...
```

而在`nodeUpdate()`方法中，RM会从NM中拉去Container的信息并进行处理。首先处理刚创建的Container，随后处理已经完成的，最后再调用`attempScheduling()`方法来进行Container的分配。

```java
  
```

##### Continious Scheduling

在上边的源码中我们可以看到，在最后分配容器的时候，如果启用了`continusSchedulingEnabled`(持续调度部署)，那么根据心跳分配容器的行为只会发生在存在已经完成的容器的情况下。那么如果启用了持续调度部署，新容器是怎么被分配的呢？

在FairScheduler的初始化函数中，我们可以看到如果启用了持续调度部署，那调度器就会在后台启动一个新的线程来管理Container的分配。启用方式由Yarn的参数`yarn.scheduler.fair.continuous-scheduling-enabled`来进行设置。

``` java
//...
if (continuousSchedulingEnabled) {
   // start continuous scheduling thread
   schedulingThread = new ContinuousSchedulingThread();
   schedulingThread.setName("FairSchedulerContinuousScheduling");
   schedulingThread.setDaemon(true);
}
//...
```

``` java
private synchronized void nodeUpdate(RMNode nm) {
    long start = getClock().getTime();
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " cluster capacity: " + clusterResource);
    }
    eventLog.log("HEARTBEAT", nm.getHostName());
    FSSchedulerNode node = getFSSchedulerNode(nm.getNodeID());
    
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    } 
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId),
          completedContainer, RMContainerEventType.FINISHED);
    }

    if (continuousSchedulingEnabled) {
      if (!completedContainers.isEmpty()) {
        attemptScheduling(node);
      }
    } else {
      attemptScheduling(node);
    }

    long duration = getClock().getTime() - start;
    fsOpDurations.addNodeUpdateDuration(duration);
}
```

后台线程调度的间隔由`yarn.scheduler.fair.continuous-scheduling-sleep-ms`来设置。`continousSchedulingAttempt()`方法中描述了后台持续调度的算法。

``` java
void continuousSchedulingAttempt() throws InterruptedException {
    long start = getClock().getTime();
    List<NodeId> nodeIdList = new ArrayList<NodeId>(nodes.keySet());
    // Sort the nodes by space available on them, so that we offer
    // containers on emptier nodes first, facilitating an even spread. This
    // requires holding the scheduler lock, so that the space available on a
    // node doesn't change during the sort.
    synchronized (this) {
      Collections.sort(nodeIdList, nodeAvailableResourceComparator);
    }
 
    // iterate all nodes
    for (NodeId nodeId : nodeIdList) {
      FSSchedulerNode node = getFSSchedulerNode(nodeId);
      try {
        if (node != null && Resources.fitsIn(minimumAllocation,
            node.getAvailableResource())) {
          attemptScheduling(node);
        }
      } catch (Throwable ex) {
        LOG.error("Error while attempting scheduling for node " + node +
            ": " + ex.toString(), ex);
        if ((ex instanceof YarnRuntimeException) &&
            (ex.getCause() instanceof InterruptedException)) {
          // AsyncDispatcher translates InterruptedException to
          // YarnRuntimeException with cause InterruptedException.
          // Need to throw InterruptedException to stop schedulingThread.
          throw (InterruptedException)ex.getCause();
        }
      }
    }
 
    long duration = getClock().getTime() - start;
    fsOpDurations.addContinuousSchedulingRunDuration(duration);
}
```

在该方法中我们发现，该算法会先将所有NodeManager按照可用资源进行排序，然后再按空闲程度进行容器的分配，因此启用持续调度部署后，RM会倾向于把容器分配到相对比较空的节点上去，而不是像心跳部署那样随机。

##### 分配过程

下面我再看看`attempScheduling()`这个方法，表示了Container分配的具体过程。

``` java
@VisibleForTesting
synchronized void attemptScheduling(FSSchedulerNode node) {
  if (rmContext.isWorkPreservingRecoveryEnabled()
      && !rmContext.isSchedulerReadyForAllocatingContainers()) {
    return;
  }

  // Assign new containers...
  // 1. Check for reserved applications
  // 2. Schedule if there are no reservations
  FSAppAttempt reservedAppSchedulable = node.getReservedAppSchedulable();
  if (reservedAppSchedulable != null) {
    Priority reservedPriority = node.getReservedContainer().getReservedPriority();
    if (!reservedAppSchedulable.hasContainerForNode(reservedPriority, node)) {
      // Don't hold the reservation if app can no longer use it
      LOG.info("Releasing reservation that cannot be satisfied for application "
          + reservedAppSchedulable.getApplicationAttemptId()
          + " on node " + node);
      reservedAppSchedulable.unreserve(reservedPriority, node);
      reservedAppSchedulable = null;
    } else {
      // Reservation exists; try to fulfill the reservation
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to fulfill reservation for application "
            + reservedAppSchedulable.getApplicationAttemptId()
            + " on node: " + node);
      }
      node.getReservedAppSchedulable().assignReservedContainer(node);
    }
  }
  if (reservedAppSchedulable == null) {
    // No reservation, schedule at queue which is farthest below fair share
    int assignedContainers = 0;
    while (node.getReservedContainer() == null) {
      boolean assignedContainer = false;
      if (!queueMgr.getRootQueue().assignContainer(node).equals(
          Resources.none())) {
        assignedContainers++;
        assignedContainer = true;
      }
      if (!assignedContainer) { break; }
      if (!assignMultiple) { break; }
      if ((assignedContainers >= maxAssign) && (maxAssign > 0)) { break; }
    }
  }
  updateRootQueueMetrics();
}
```

在这个方法中，RM会首先Check当前NM是否有为Application预留的资源，如果预留的资源可以满足App的需求，则进行分配。如果并没有资源预留的情况，那就会在Yarn的Queue中进行容器分配。

当我们仔细观察最后的while循环可以发现一个`assignMultiple`的参数，该参数对应Yarn的配置项为`yarn.scheduler.fair.assignmultiple`。这个参数如果为true，那么在进行一次容器分配之后这个行为不会停止，会继续尝试分配更多的Container在这个节点上。

同时下面还有一个`maxAssign`参数，对应`yarn.scheduler.fair.max.assign`，表示在允许`assginMulitple`的情况下可最多连续分配的次数。

#### 解决方案

从上述源码分析我们可以看到，对于Fair Scheduler，持续调度部署和`assignMultiple`相关的参数都可能会影响到Yarn对于容器的分配。

我们查看了CDH集群(5.15)的参数设置，发现`yarn.scheduler.fair.continuous-scheduling-enabled`被设为false，而`yarn.scheduler.fair.assignmultiple`被设为true，`yarn.scheduler.fair.max.assign`被设为-1。这表明在当前集群状况下，Yarn会根据每个节点的心跳进行容器分配(NM的心跳到达是随机的)，并且在资源足够的情况下，会尽可能分配多的Container。然而Yarn的资源考量是不包含IO和磁盘资源的，所以很显而易见的会把Flink大量的TaskManager调度同一个物理节点上，引起了HDFS Datanode的崩溃。

因此为了避免这个问题，最好的方式看起来就是打开`continueSchedulingEnabled`并且禁止`assignMutliple`。这样Yarn就会根据NM的空闲程度，把容器尽量分配到最空的节点上去。然而这么操作的问题也很明显，我们上面已经提到，资源只包括CPU和内存，对于我们大量IO密集型的任务来说，这个资源大小排序对我们的帮助较小，而持续调度的后台进程需要不断的遍历集群所有的NM，对于我们目前拥有相对比较庞大的集群(>150 nodes)来说，势必会影响到RM的性能和响应速度，Cloudera在官方文档也提到了这一点[^2]

因此，我们根据自身的场景进行考量，认为依旧关闭持续调度更为合适，但是同时也禁用了一次性分配多个容器的选项。在这样的配置之下，每一个Flink TM都会随机的分配在某一个NM之上，这样就成功避免了TM在物理节点上过于集中，进而大大减轻了HDFS的压力。

## 总结

我们根据对Flink和Yarn源码的剖析，对于Flink TM分配过去集中而导致的HDFS写入问题，提出了目前最为妥当的解决方案，便是禁止`assignMultiple`，使得Yarn的公平调度器不再一次性分配多个TM到同一个物理节点，而是随机化分配。尽管这种方式依旧无法完美的管控IO，磁盘或是文件描述符资源，但是在Hadoop本身功能的限制之下，这已经是一个相对完美的解决方案，能够快速的解决线上的问题。

[^1]: https://issues.apache.org/jira/browse/FLINK-11937
[^2]: https://docs.cloudera.com/documentation/enterprise/5-14-x/topics/cdh_ig_yarn_tuning.html
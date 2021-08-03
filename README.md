# Apache Spark Tutorial.《跟老卫学Apache Spark开发》

![](images/spark-logo-trademark.png)

*Apache Spark Tutorial*, is a book about how to develop Apache Spark applications.



《跟老卫学Apache Spark开发》是一本 Apache Spark 应用开发的开源学习教程，主要介绍如何从0开始开发 Apache Spark 应用。本书包括最新版本 Apache Spark 3.x 中的新特性。图文并茂，并通过大量实例带你走近 Apache Spark 的世界！

本书业余时间所著，水平有限、时间紧张，难免疏漏，欢迎指正，

## Summary 目录

* [Spark下载、安装](https://developer.huawei.com/consumer/cn/forum/topic/0202568822299090741?fid=23)
* [Spark应用初探](https://developer.huawei.com/consumer/cn/forum/topic/0201568823403320732?fid=23)
* [Spark导出数据到CSV文件](https://developer.huawei.com/consumer/cn/forum/topic/0202620883150950010?fid=23)
* [Spark累加器LongAccumulator的使用](https://developer.huawei.com/consumer/cn/forum/topic/0202622461925310080?fid=23)
* [Spark累加器DoubleAccumulator的使用](https://developer.huawei.com/consumer/cn/forum/topic/0202622590853530085?fid=23)
* [Spark累加器CollectionAccumulator的使用](https://developer.huawei.com/consumer/cn/forum/topic/0202622591182960086?fid=23)
* [启动Spark应用的方式](https://developer.huawei.com/consumer/cn/forum/topic/0202623507783170122?fid=23)
* [Spark广播变量](https://developer.huawei.com/consumer/cn/forum/topic/0202624224916630149?fid=23)
* [Spark RDD入门](https://developer.huawei.com/consumer/cn/forum/topic/0201624386890690172?fid=23)
* [Spark RDD基本操作](https://developer.huawei.com/consumer/cn/forum/topic/0201627152644060234?fid=23)
* [Spark RDD Shuffle操作](https://developer.huawei.com/consumer/cn/forum/topic/0202627152820110215?fid=23)
* [深入理解Spark RDD原理](https://developer.huawei.com/consumer/cn/forum/topic/0202628556358740265?fid=23)
* [Spark调度管理之资源分配](https://developer.huawei.com/consumer/cn/forum/topic/0202629577348060308?fid=23)
* [Spark调度管理之作业调度](https://developer.huawei.com/consumer/cn/forum/topic/0201629622395410333?fid=23)
* [Spark SQL概述](https://developer.huawei.com/consumer/cn/forum/topic/0202630480491580330?fid=23)
* [Spark SQL之Dataset与DataFrame](https://developer.huawei.com/consumer/cn/forum/topic/0202630480727520331?fid=23)
* [Spark SQL之DataFrame入门操作](https://developer.huawei.com/consumer/cn/forum/topic/0201633012983700432?fid=23)
* [Spark SQL之Dataset入门操作](https://developer.huawei.com/consumer/cn/forum/topic/0201633040938970437?fid=23)
* [Spark SQL之基于DataFrame创建临时视图](https://developer.huawei.com/consumer/cn/forum/topic/0202633194774890394?fid=23)
* [Spark SQL之RDD转为Dataset](https://developer.huawei.com/consumer/cn/forum/topic/0201633208926640450?fid=23)
* [Spark SQL之Apache Parquet数据源的读取和写入](https://developer.huawei.com/consumer/cn/forum/topic/0202634018676920418?fid=23)
* [Apache Parquet列式存储格式介绍](https://waylau.com/about-apache-parquet/)
* 未完待续...

## Samples 示例


* [Spark导出数据到CSV文件](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/WriteCVSExample.java)
* [Spark累加器LongAccumulator的使用](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/util/LongAccumulatorSample.java)
* [Spark累加器DoubleAccumulator的使用](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/util/DoubleAccumulatorSample.java)
* [Spark累加器CollectionAccumulator的使用](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/util/CollectionAccumulatorSample.java)
* [SparkLauncher示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/launcher/SparkLauncherSample.java)
* [InProcessLauncherSample示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/launcher/InProcessLauncher.java)
* [Broadcast 示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/broadcast/BroadcastSample.java)
* [RDD基本操作示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/rdd/JavaRddBasicSample.java)
* [RDD Transformation和Action基本操作示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/rdd/JavaRddBasicOperationSample.java)
* [DataFrame基本操作示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/DataFrameBasicExample.java)
* [Dataset基本操作示例](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/DatasetBasicExample.java)
* [基于DataFrame创建临时视图](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/DataFrameTempViewExample.java)
* [RDD转为Dataset](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/DatasetSchemaExample.java)
* [Apache Parquet数据源的读取和写入](samples/spark-java-samples/src/main/java/com/waylau/spark/java/samples/sql/DataSourceParquetExample.java)
* 未完待续...





## Get start 如何开始阅读

选择下面入口之一：

* <https://github.com/waylau/apache-spark-tutorial>
* <https://gitee.com/waylau/apache-spark-tutorial>


## Code 源码

书中所有示例源码，移步至<https://github.com/waylau/apache-spark-tutorial>的 `samples` 目录下，代码遵循《[Java 编码规范](<http://waylau.com/java-code-conventions>)》

## Issue 意见、建议

如有勘误、意见或建议欢迎拍砖 <https://github.com/waylau/apache-spark-tutorial/issues>

## Contact 联系作者

* Blog: [waylau.com](http://waylau.com)
* Gmail: [waylau521(at)gmail.com](mailto:waylau521@gmail.com)
* Weibo: [waylau521](http://weibo.com/waylau521)
* Twitter: [waylau521](https://twitter.com/waylau521)
* Github : [waylau](https://github.com/waylau)


## Support Me 请老卫喝一杯

![开源捐赠](https://waylau.com/images/showmethemoney-sm.jpg)

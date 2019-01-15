# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

此版本为bestpay分支.增加以下功能:

1, oracle directly read mode(like oraoop)
2, clickhouse reader/writer
3, phoenix writer


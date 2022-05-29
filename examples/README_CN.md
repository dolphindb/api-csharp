## DolphinDB C# API例子

### 1. 概述
目前已有2个C# API的例子，如下表所示：

| 例子主题        | 文件名称          |
|:-------------- |:-------------|
|数据库写入|DFSTableWriting.cs|
|流数据写入和订阅|StreamingData.cs|

本文下面对每个例子分别进行简单的说明，包括运行和使用帮助等。
### 2. 数据库写入例子

本例实现了用C# API往分布式数据库写入数据的功能。例子中的目标数据库是一个按日期分区的分布式数据库。

#### 2.1 代码说明
主要有2个函数：
* createBasicTable函数 : 定义写入的数据，该函数创建了一个本地的表对象BasicTable。
* writeDfsTable函数 : 通过API在DolphinDB创建待写入的分布式表，并用`run("tableInsert",args)`函数将C#端的BasicTable上传和写入分布式表。
#### 2.2 运行
将代码编译成xxx.exe 执行 
```
xxx.exe [serverIP] [serverPort]
```
若不传入serverIP和serverPort参数，默认serverIP="localhost"，serverPort==8848

### 3. 流数据写入和订阅例子
C# API提供了ThreadedClient、ThreadPooledClient和PollingClient三种订阅模式订阅流表的数据。三种模式的主要区别在于收取数据的方式：

ThreadedClient单线程执行，并且对收到的消息直接执行用户定义的handler函数进行处理；

ThreadPooledClient多线程执行，对收到的消息进行多线程并行调用用户定义的handler函数进行处理；

PollingClient返回一个消息队列，用户可以通过轮询队列的方式获取和处理数据。

在本例流数据订阅的源代码中，选择用ThreadedClient，PollingClient两种方式。ThreadPooledClient可以参照ThreadedClient使用方式。

#### 3.1 代码说明
本例实现了流数据表的写入和流数据订阅的功能，订阅服务端发布的数据并在C#应用端打印出来，主要有5个函数和类如下：
* createStreamTable函数 ：用于在DolphinDB中创建流数据表。
* pollingClient类 ：用PollingClient订阅模式订阅流表的数据，并在主线程中获取的数据展示出来。
* ThreadedClient类 ：用ThreadedClient订阅模式订阅流表的数据。
* SampleMessageHandler类 ： 处理ThreadedClient订阅的流表数据，将获取的数据展示出来。

#### 3.2 运行
将代码编译成xxx.exe 执行 
```
xxx.exe [serverIP] [serverPort] [subscribePort] [subscribeMethod]
```
subscribeMethod有2个选项：
* 'T'，用ThreadedClient开启订阅。
* 'P'，用PollingClient开启订阅。

若不传入参数，默认serverIP="localhost"，serverPort==8848，subscribePort=8892，subscribeMethod='P'.

程序运行之后，Server端流表被创建，客户端处于等待流数据的状态。在DolphinDB 服务端运行以下脚本发布数据(持续100秒)：
```
for(x in 0:1000){
    time =time(now())
    sym= rand(`S`M`MS`GO, 1)
    qty= rand(1000..2000,1)
    price = rand(2335.34,1)
    insert into Trades values(x, time,sym, qty, price)
    sleep(100)
}
```
在PollingClient订阅时，若需要中途停止订阅，可以向流表写入 id = -1的记录，示例程序会停止运行
```
insert into Trades values(-1,time(now()),`a, 0, 0)
```
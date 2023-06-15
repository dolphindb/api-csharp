# DolphinDB C# API

### 1. C# API 概述

C# API本质上实现了.Net程序和DolphinDB服务器之间的消息传递和数据转换协议。C# API可运行在.Net Framework 4.0 及以上环境。

C# API遵循面向接口编程的原则。C# API使用接口类IEntity来表示DolphinDB返回的所有数据类型。在IEntity接口类的基础上，根据DolphinDB的数据类型，C# API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在com.xxdb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector，matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set， dictionary和table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

“Basic”表示基本的数据类型接口，\<DataType\>表示DolphinDB数据类型名称，\<DataForm\>是一个DolphinDB数据形式名称。

DolphinDB C# API 提供的最核心的对象是DBConnection，它主要的功能就是让C#应用可以通过它调用DolphinDB的脚本和函数，在C#应用和DolphinDB服务器之间双向传递数据。
DBConnection类提供如下主要方法：


| 方法名                                      | 详情                     |
| ---------------------------------------- | ---------------------- |
| DBConnection([asynchronousTask=false], [useSSL=false], [compress=false], [usePython=false]) | 构造函数，表示是否开启异步、ssl、压缩功能 |
| connect(hostName, port, [userId=""], [password=""], [startup=""], [highAvailability=false], [highAvailabilitySites], [reconnect=false]) | 将会话连接到DolphinDB服务器     |
| login(userId, password, enableEncryption) | 登陆服务器                  |
| run(script, [listener], [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | 将脚本在DolphinDB服务器同步运行   |
| runAsync(script, [priority = 4], [parallelism=2],  [fetchSize=0], [clearMemory = false]) | 将脚本在DolphinDB服务器异步运行   |
| run(functionName, arguments, [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | 同步调用DolphinDB服务器上的函数   |
| runAsync(functionName, arguments, [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | 异步调用DolphinDB服务器上的函数   |
| upload(variableObjectMap)                | 将本地数据对象上传到DolphinDB服务器 |
| isBusy()                                 | 判断当前会话是否正忙             |
| close()                                  | 关闭当前会话  .若当前会话不再使用，会自动被释放，但存在释放延时，可以调用 close() 立即关闭会话。否则可能出现因连接数过多，导致其它会话无法连接服务器的问题。               |

### 2. 建立DolphinDB连接

C# API通过TCP/IP协议连接到DolphinDB服务器。在以下例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```cs
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}
```
声明connection变量的时候，有三个可选参数：asynchronousTask（支持一部分），useSSL（支持SSL），compress（是否压缩传输）。这三个参数默认值为false。 目前只支持linux, 稳定版>=1.10.17，最新版>=1.20.6。  

下面例子是，建立支持SSL而非支持异步的connection，要求数据进行压缩。服务器端应该添加参数enableHTTPS=true(单节点部署，需要添加到dolphindb.cfg;集群部署需要添加到cluster.cfg)。

```cs
DBConnection conn = new DBConnection(false,true,true)
```

下面建立不支持SSL，但支持异步的connection。异步情况下，只能执行DolphinDB脚本和函数， 且不再有返回值。该功能适用于异步写入数据。

```cs
DBConnection conn = new DBConnection(true,false)
```
输入用户名和密码建立连接：

```cs
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

当需要在应用程序里定义和使用自定义函数时，可以使用 initialScript 参数传入函数定义脚本。这样做的好处是：一、无需每次运行`run`函数的时候重复定义这些函数。二、API提供自动重连机制，断线之后重连时会产生新的会话。如果 initialScript 参数不为空，API会在新的会话中自动执行初始化脚本重新注册函数。在一些网络不是很稳定但是应用程序需要持续运行的场景里，这个参数会非常有用。
```cs
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```

ExclusiveDBConnectionPool可以复用多个DBConnection。可以直接使用ExclusiveDBConnectionPool.run执行命令，也可以通过execute方法执行任务，然后使用BasicDBTask的getResults方法获取该任务的执行结果。

| 方法名                                      | 详情                                       |
| ---------------------------------------- | ---------------------------------------- |
| ExclusiveDBConnectionPoolExclusiveDBConnectionPool(host, port, uid, pwd, count, loadBalance,  highAvaliability, [haSites], [startup=””], [compress= false], [useSSL=false], [usePython=false]) | 构造函数，参数count为连接数，loadBalance为true会连接不同的节点 |
| run(script, [priority=4], [parallelism=2], [clearMemory=false]) | 将脚本在DolphinDB服务器同步运行                     |
| runAsync(script, [priority=4], [parallelism=2], [clearMemory=false]) | 将脚本在DolphinDB服务器异步运行                     |
| run(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | 同步调用DolphinDB服务器上的函数                     |
| runAsync(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | 异步调用DolphinDB服务器上的函数                     |
| execute(task)                            | 执行任务                                     |
| execute(tasks)                           | 执行批量任务                                   |
| getConnectionCount()                     | 获取连接数                                    |
| shutdown                                 | 关闭连接池请注意，若当前 ExclusiveDBConnectionPool 线程池不再使用，会自动被释放，但存在释放延时，可以通过调用 shutdown() 等待线程任务执行结束后立即释放连接。    |

BasicDBTask包装了需要执行的脚本和参数。

| 方法名                                      | 详情                      |
| ---------------------------------------- | ----------------------- |
| BasicDBTask(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | functionName为需要执行的函数，arguments为参数。 |
| BasicDBTask(script, [priority=4], [parallelism=2], [clearMemory=false]) | 需要执行的脚本                 |
| isSuccessful()                           | 任务是否执行成功                |
| getResults()                             | 获取脚本运行结果                |
| getErrorMsg()                            | 获取任务运行时发生的异常信息          |

建立一个DBConnection连接数为10的连接池。

```cs
ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("192.168.1.38", 8902, "admin", "123456", 10, false, true);

//运行脚本
IEntity ret = pool.run("1 + 1");
Console.Out.WriteLine(ret.getString());

//运行指定的函数。
ret = pool.run("abs", new List<IEntity> { new BasicInt(-3) });
Console.Out.WriteLine(ret.getString());
```

创建一个任务。

```cs
BasicDBTask task = new BasicDBTask("1..10");
pool.execute(task);
```

检查任务是否执行成功。如果执行成功，获取相应结果；如果失败，获取异常信息。
```cs
BasicIntVector data = null;
if (task.isSuccessful())
{
      data = (BasicIntVector)task.getResults();
}
else
{
      throw new Exception(task.getErrorMsg());
}
System.Console.Out.WriteLine(data.getString());
```

输出
```
[1,2,3,4,5,6,7,8,9,10]
```

创建多个任务，在ExclusiveDBConnectionPool上并行调用。

```cs
List<IDBTask> tasks = new List<IDBTask>();
for (int i = 0; i < 10; ++i){
      //调用函数log。
      tasks.Add(new BasicDBTask("log", new List<IEntity> { data.get(i) }));
}
pool.execute(tasks);
```

检查任务是否都执行成功。如果执行成功，获取相应结果；如果失败，获取异常信息。

```cs
for (int i = 0; i < 10; ++i)
{
      if (tasks[i].isSuccessful())
      {
            logData.append((IScalar)tasks[i].getResults());
      }
      else
      {
            throw new Exception(tasks[i].getErrorMsg());
      }
}
System.Console.Out.WriteLine(logData.getString());
```

输出

```cs
[0,0.693147,1.098612,1.386294,1.609438,1.791759,1.94591,2.079442,2.197225,2.302585]
```

### 3. 运行脚本

在C#中运行DolphinDB脚本的基础语法如下：
```cs
conn.run("script");
conn.runAsync("script")
```

run 表示同步执行脚本，runAsync 表示异步执行。

如果脚本只包含一条语句，如表达式，DolphinDB会返回该语句计算结果。如果脚本包含多条语句，将返回最后一条语句的结果。如果脚本含有错误或者出现网络问题，会抛出IOException。

### 4. 调用DolphinDB函数

调用的函数可以是内置函数或用户自定义函数。 下面的示例将一个double类型向量传递给服务器，并调用`sum`函数。

```cs
public void testFunction(){
      List<IEntity> args = new List<IEntity>(1);
      BasicDoubleVector vec = new BasicDoubleVector(3);
      vec.setDouble(0, 1.5);
      vec.setDouble(1, 2.5);
      vec.setDouble(2, 7);            
      args.Add(vec);
      BasicDouble result = (BasicDouble)conn.run("sum", args);
      Console.WriteLine(result.getValue());
}
```
### 5. 上传本地对象到DolphinDB服务器

我们可以将二进制数据对象上传到DolphinDB服务器，并将其分配给一个变量以备将来使用。 变量名称可以使用三种类型的字符：字母，数字或下划线。 第一个字符必须是字母。

```cs
public void testUpload(){

      BasicTable tb = (BasicTable)conn.run("table(1..100 as id,take(`aaa,100) as name)");
      Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
      upObj.Add("table_uploaded", (IEntity)tb);
      db.upload(upObj);
      BasicIntVector v = (BasicIntVector)conn.run("table_uploaded.id");
      Console.WriteLine(v.rows());
}
```

### 6. 下载DolphinDB服务器对象到本地

下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

首先导入DolphinDB数据类型包：

```cs
using dolphindb.data;
```

注意，下面的代码需要在建立连接后才能运行。

- 向量

在下面的示例中，DolphinDB语句返回C#对象BasicStringVector。
```cs
rand(`IBM`MSFT`GOOG`BIDU,10)
```
vector.rows()方法能够获取向量的大小。我们可以使用vector.getString(i)方法按照索引访问向量元素。

```cs
public void testStringVector(){
      IVector v = (BasicStringVector)conn.run("take(`IBM`MSFT`GOOG`BIDU, 10)");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(((BasicString)v.get(1)).getValue());
}
```

类似的，也可以处理双精度浮点类型的向量或者元组。
```cs
public void testDoubleVector(){
      IVector v = (BasicDoubleVector)conn.run("1.123 2.2234 3.4567");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
}
```
```cs
public void testAnyVector(){
      BasicAnyVector v = (BasicAnyVector)conn.run("[1 2 3,3.4 3.5 3.6]");
      Console.WriteLine(v.rows());
      Console.WriteLine(v.columns());
      Console.WriteLine(((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
}
```
- 集合
```cs
public void testSet(){
      BasicSet s = (BasicSet)conn.run("set(1 3 5)");
      Console.WriteLine(s.rows());
      Console.WriteLine(s.columns());
}
```

- 矩阵

要从矩阵中检索一个元素，我们可以使用get(row,col)。 要获取行数和列数，我们可以使用函数`rows`和`columns`。

```cs
public void testIntMatrix(){
      IMatrix m = (BasicIntMatrix)conn.run("matrix(45 47 48,56 65 67)");
      Console.WriteLine(m.isMatrix());
      Console.WriteLine(m.rows());
      Console.WriteLine(m.columns());
      Console.WriteLine(((BasicInt)m.get(0, 1)).getValue());
}
```

- 字典

用函数`keys`和`values`可以从字典取得所有的键和值。要从一个键里取得它的值，可以调用`get`。

```cs
public void testDictionary(){
      BasicDictionary tb = (BasicDictionary)conn.run("dict(1 2 3 4,5 6 7 8)");
      foreach (var key in tb.keys())
      {
            BasicInt val = (BasicInt)tb.get(key);
            Console.WriteLine(val);
      }
}
```

- 表

要获取表的列，可以调用table.getColumn(index)；要获取列名，可以调用table.getColumnName(index)。 对于列和行的数量，我们可以分别调用table.columns()和table.rows()。

```cs
public void testTable(){
	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Console.WriteLine(dt.Rows.Count);
}
```
- NULL对象

要描述一个NULL对象，我们可以调用函数obj.getDataType()。
```cs
public void testVoid(){
      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}
```

### 7. 读写DolphinDB数据表

使用C# API的一个重要场景是，用户从其他数据库系统或是第三方WebAPI中取到数据，将数据进行清洗后存入DolphinDB数据库中，本节将介绍通过C# API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为两种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

#### 7.1 将数据保存到DolphinDB内存表

DolphinDB提供三种方式将数据新增到内存表：
- 通过`insert into`保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

一般不建议通过`append!`函数保存数据，因为`append!`函数会返回表的schema，产生不必要的通信量。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是string, int, timestamp, double类型，列名分别为cstring, cint, ctimestamp, cdouble。
```cs
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以该内存表只有当前会话可见。如果需要在其它会话中访问，需要通过`share`在会话间共享内存表。

##### 7.1.1 使用 `insert into` 保存单条数据

若将单条数据记录保存到DolphinDB内存表，那么可以通过类似SQL语句 insert into。
```cs
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

##### 7.1.2 使用`tableInsert`函数批量保存数组对象

`tableInsert`函数比较适合用来批量保存数据，它可将多个数组追加到DolphinDB内存表中。若C#程序获取的数据可以组织成List方式，可使用`tableInsert`函数保存。

```cs
public void test_save_TableInsert(string[] strArray, int[] intArray, long[] tsArray, double[] dblArray)
{
      //用数组构造参数
      List<IEntity> args = new List<IEntity>() { new BasicStringVector(strArray), new BasicIntVector(intArray), new BasicTimestampVector(tsArray), new BasicDoubleVector(dblArray) };
      conn.run("tableInsert{sharedTable}", args);
}
```

在本例中，使用了DolphinDB 中的“部分应用”这一特性，将服务端表名以tableInsert{sharedTable}的方式固化到`tableInsert`中，作为一个独立函数来使用。具体文档请参考[部分应用文档](https://www.dolphindb.cn/cn/help/Functionalprogramming/PartialApplication.html)。

##### 7.1.3 使用`tableInsert`函数保存BasicTable对象

若C#程序获取的数据处理后组织成BasicTable对象，`tableInsert`函数也可以接受一个表对象作为参数，批量添加数据。

```cs
public void test_save_table(BasicTable table1)
{
      List<IEntity> args = new  List<IEntity>(){ table1};
      conn.run("tableInsert{shareTable}", args);
}
```
#### 7.2 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

#### 7.2.1 使用`tableInsert`函数保存BasicTable对象

本例中涉及到的数据表可以通过如下脚本构建：

```cs
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供`loadTable`方法可以加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```cs
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
    List<IEntity> args = new List<IEntity>() { table1 };
    conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```

C#程序中的数组或列表，也可以很方便的构造出BasicTable用于追加数据。例如若有 boolArray, intArray, dblArray, dateArray, strArray 这5个列表对象(List<T>)，可以通过以下语句构造BasicTable对象：

```cs
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```
#### 7.2.2 分布式表的并发写入


DolphinDB的分布式表支持并发读写，下面展示如何在C#客户端中将数据并发写入DolphinDB的分布式表。

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。C# API提供了自动按分区分流数据并行写入的简便方法，其基本原理是设计一个连接池用于多线程写入，然后利用server的schema函数获取分布式表的分区信息，按指定的分区列将用户写入的数据进行分类分别交给不同的连接来并行写入。函数定义如下

```cs
public PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, IDBConnectionPool pool)
```
* dbUrl: 必填，分布式数据库地址
* tableName: 必填，分布式表名
* partitionColName: 必填，分区字段
* appendFunction: 可选，自定义写入函数名，不填此参数则调用内置tableInsert函数。
* pool: 连接池，并行写入数据。


首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://DolphinDBUUID"和分布式表"device_status"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

```cs
t = table(timestamp(1..10)  as date,string(1..10) as sym)
db1=database(\"\",HASH,[DATETIME,10])
db2=database(\"\",HASH,[STRING,5])
if(existsDatabase(\"dfs://demohash\")){
    dropDatabase(\"dfs://demohash\")
}
db =database(\"dfs://demohash\",COMPO,[db2,db1])
pt = db.createPartitionedTable(t,`pt,`sym`date)
```

> 请注意： 使用最新的1.30版本以上的server，可以使用C# api中的 PartitionedTableAppender类来写入分布式表。具体可以参考examples/DFSTableWritingMultiThread.cs
使用示例脚本如下：

```cs
IDBConnectionPool conn = new ExclusiveDBConnectionPool(host, port, "admin", "123456",threadCount, false, false);

PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "gid", "saveGridData{'" + dbPath + "','" + tableName + "'}", conn);
BasicTable table1 = createTable();
appender.append(table1);            
```

<!--不推荐使用磁盘表，删除
#### 7.3 保存数据到本地磁盘表

通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不持支并发读写，不保证运行中的数据一致性，所以不建议在生产环境中使用。

```cs
//
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
通过`tableInsert`追加数据:
```cs
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```
-->
#### 7.3 读取和使用数据表

在C# API中，数据表保存为BasicTable对象。由于BasicTable是列式存储，所以若要在C# API中读取行数据需要先取出需要的列，再取出行。

以下例子中参数BasicTable的有4个列，列名分别为cstring, cint, ctimestamp, cdouble，数据类型分别是STRING, INT, TIMESTAMP, DOUBLE。
```cs
public void test_loop_basicTable(BasicTable table1)
{
      BasicStringVector stringv = (BasicStringVector) table1.getColumn("cstring");
      BasicIntVector intv = (BasicIntVector)table1.getColumn("cint");
      BasicTimestampVector timestampv = (BasicTimestampVector)table1.getColumn("ctimestamp");
      BasicDoubleVector doublev = (BasicDoubleVector)table1.getColumn("cdouble");
      for(int ri=0; ri<table1.rows(); ri++){
            Console.WriteLine(stringv.getString(ri));
            Console.WriteLine(intv.getInt(ri));
            DateTime timestamp = timestampv.getTimestamp(ri);
            Console.WriteLine(timestamp);
            Console.WriteLine(doublev.getDouble(ri));
      }
}
```

#### 7.4 批量异步追加数据

DolphinDB C# API 提供 `MultithreadedTableWriter` 类对象支持多线程的并发写入。目前，`MultithreadedTableWriter` 支持批量写入数据到内存表、分区表和维度表。

`MultithreadedTableWriter` 对象及主要方法介绍如下：

```cs
MultithreadedTableWriter(string hostName, int port, string userId, string password,string dbName, string tableName, bool useSSL, bool enableHighAvailability = false, string[] pHighAvailabilitySites = null,int batchSize = 1, float throttle = 0.01f, int threadCount = 5, string partitionCol = "", int[] pCompressMethods = null, Mode mode = Mode.M_Append, string[] pModeOption = null, Callback callbackHandler = null);
```

参数说明：

* **hostName** 字符串，表示所连接的服务器的地址
* **port** 整数，表示服务器端口。 
* **userId** / **password**: 字符串，登录时的用户名和密码。
* **dbPath** 字符串，表示分布式数据库地址。内存表时该参数为空。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需填写内存表表名。
* **tableName** 字符串，表示分布式表或内存表的表名。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需为空。
* **useSSL** 布尔值，默认值为 False。表示是否启用加密通讯。
* **enableHighAvailability** 布尔值，默认为 False。若要开启 API 高可用，则需要指定 *enableHighAvailability* 参数为 True。
* **pHighAvailabilitySites** 列表类型，表示所有可用节点的 ip:port 构成的 list。
* **batchSize** 整数，表示批处理的消息的数量，默认值是 1，表示客户端写入数据后就立即发送给服务器。如果该参数大于 1，表示数据量达到 *batchSize* 时，*客户端*才会将数据发送给服务器。
* **throttle** 大于 0 的数，单位为秒。若客户端有数据写入，但数据量不足 batchSize，则等待 throttle的时间再发送数据。
* **threadCount** 整数，表示创建的工作线程数量，默认为 1，表示单线程。对于维度表，其值必须为1。
* **partitionCol** 字符串类型，默认为空，仅在 threadCount 大于1时起效。对于分区表，必须指定为分区字段名；如果是流表，必须指定为表的字段名；对于维度表，该参数不起效。
* **pCompressMethods** 列表类型，用于指定每一列采用的压缩传输方式，为空表示不压缩。每一列可选的压缩方式包括：
  * Vector_Fields.COMPRESS_LZ4: LZ4 压缩
  * Vector_Fields.COMPRESS_DELTA: DELTAOFDELTA 压缩
* **mode** 写入模式，用于指定 MultithreadedTableWriter 对象写入数据的方式，包括两种：
   * Mode.M_Append：表示以 [tableInsert](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/t/tableInsert.html) 的方式向追加数据。
   * Mode.M_Upsert：表示以 [upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert!.html) 方式更新（或追加）数据。
* **pModeOption**：字符串数组，表示不同模式下的扩展选项，目前，仅当 mode 指定为 Mode.M_Upsert 时有效，表示由 upsert! 可选参数组成的字符串数组。如：
  ```new String[] { "ignoreNull=false", "keyColNames=`volume" }```。
* **callbackHandler**：回调类（Callback），默认为空，表示不使用回调。开启回调后，将继承回调接口 Callback 并重载回调方法 writeCompletion。

若 MultithreadedTableWriter 指定了 *callbackHandler*，则注意以下事项：

* insert 的第一个参数必须是 STRING 类型，表示这一行数据的 id。
* getUnwrittenData 方法将不可用。

以下是 `MultithreadedTableWriter` 对象包含的函数方法介绍：

(1) insert

```cs
ErrorCodeInfo insert(params Object[] args)
```

函数说明：

插入单行数据。返回一个ErrorCodeInfo，表示是否插入成功。


参数说明：

* **args**：是变长参数，代表插入的一行数据。

返回值：返回一个ErrorCodeInfo对象，包含 errorCode 和 errorInfo，分别表示错误代码和错误信息。当 errorCode 不为空时，表示 MTW 写入失败，此时，errorInfo 会显示失败的详细信息。之后的版本中会对错误信息进行详细说明，给出错误信息的代码、错误原因及解决办法。

(2) getUnwrittenData

```cs
List<List<IEntity>> getUnwrittenData();
```

函数说明：

返回一个嵌套列表，表示未写入服务器的数据。

注意：该方法获取到数据资源后， `MultithreadedTableWriter` 将释放这些数据资源。

(3) insertUnwrittenData

```cs
ErrorCodeInfo insertUnwrittenData(List<List<IEntity>> data);
```

函数说明：

将数据插入数据表。返回值同 insert 方法。与 insert 方法的区别在于，insert 只能插入单行数据，而 insertUnwrittenData 可以同时插入多行数据。

参数说明：

* **data**：需要再次写入的数据。可以通过方法 getUnwrittenData 获取该对象。

```cs
Status getStatus()
```

函数说明：

获取 `MultithreadedTableWriter` 对象当前的运行状态。

参数说明：

* **status**：是MultithreadedTableWriter::Status 类，具有以下属性和方法

属性：

* isExiting：写入线程是否正在退出。
* errorCode：错误码。
* errorInfo：错误信息。
* sentRows：成功发送的总记录数。
* unsentRows：待发送的总记录数。
* sendFailedRows：发送失败的总记录数。
* threadStatus：写入线程状态列表。
  - threadId：线程 Id。
  - sentRows：该线程成功发送的记录数。
  - unsentRows：该线程待发送的记录数。
  - sendFailedRows：该线程发送失败的记录数。

(5) waitForThreadCompletion

```cs
waitForThreadCompletion()
```

函数说明：

调用此方法后，MTW 会进入等待状态，待后台工作线程全部完成后退出等待状态。

`MultithreadedTableWriter` 常规处理流程如下：

```cs
//创建连接，并初始化测试环境
string HOST = "192.168.1.38";
int PORT = 18848;
string USER = "admin";
string PASSWD = "123456";
DBConnection dBConnection = new DBConnection();
dBConnection.connect(HOST, PORT, USER, PASSWD);
Random random = new Random();
string script =
"dbName = 'dfs://valuedb3'" +
"if (exists(dbName))" +
"{" +
      "dropDatabase(dbName);" +
"}" +
"datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);" +
"db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);" +
"pt=db.createPartitionedTable(datetest,'pdatetest','id');";
dBConnection.run(script);

ErrorCodeInfo ret;
MultithreadedTableWriter.Status writeStatus;
MultithreadedTableWriter writer = new MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://valuedb3", "pdatetest", false, false, null, 10000, 1, 5, "id", new int[] { Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_DELTA });
try
{
      //插入100行正确数据 （类型和列数都正确），MTW正常运行
      for (int i = 0; i < 100; ++i)
      {
            ret = writer.insert(new DateTime(2022, 3, 23), "AAAAAAAB", i);
            //此处不会执行到
            if (ret.errorCode != "")
            Console.WriteLine(string.Format("insert wrong format data: {0}\n", ret.ToString()));
      }
      Thread.Sleep(2000);

      //插入1行数据，类型不匹配，MTW立刻发现
      //MTW立刻返回错误信息
      ret = writer.insert(new DateTime(2022, 3, 23), random.Next() % 10000, random.Next() % 10000);
      if (ret.errorCode != "")
            Console.WriteLine("insert wrong format data: {0}\n", ret.ToString());
      /*
      insert wrong format data: code = A1 info = Failed to insert data. Cannot convert int to DT_SYMBOL
      */

      //如果发生了连接断开的情况，mtw将会在下一次向服务器写数据时发生失败。
      //先写一行数据，触发error
      ret = writer.insert(new DateTime(2022, 3, 23), "AAAAAAAB", 1);

      Thread.Sleep(1000);

      //再插入9行正确数据，MTW会因为工作线程终止而抛出异常，且该行数据不会被写入MTW
      //这里会直接抛出异常
      for (int i = 0; i < 9; ++i)
      {
            ret = writer.insert(new DateTime(2022, 3, 23), "AAAAAAAB", random.Next() % 10000);
      }

}
catch (Exception e)
{
      Console.WriteLine(e.Message);
      //Thread is exiting.
}
writer.waitForThreadCompletion();
writeStatus = writer.getStatus();
if (writeStatus.errorCode != "")
      //写入发生错误
      Console.WriteLine(string.Format("error in writing:\n {0}", writeStatus.ToString()));
Console.WriteLine(((BasicLong)dBConnection.run("exec count(*) from pt")).getLong());

/*
      error in writing: Cause of write failure
sentRows: 100
unsentRows: 3
sendFailedRows: 7
threadId : 3 sentRows : 20 unsentRows : 0 sendFailedRows : 5
threadId : 4 sentRows : 20 unsentRows : 2 sendFailedRows : 1
threadId : 5 sentRows : 20 unsentRows : 1 sendFailedRows : 0
threadId : 6 sentRows : 20 unsentRows : 0 sendFailedRows : 0
threadId : 7 sentRows : 20 unsentRows : 0 sendFailedRows : 1

100
      */

for (int i = 0; i < 30; ++i)
      Console.Write('-');
Console.WriteLine();

List<List<IEntity>> unwriterdata = new List<List<IEntity>>();
if (writeStatus.sentRows != 110)
{
      Console.WriteLine("error after write complete:" + writeStatus.errorInfo);
      unwriterdata = writer.getUnwrittenData();
      Console.WriteLine("unwriterdata {0}", unwriterdata.Count);

      //重新写入这些数据，原有的MTW因为异常退出已经不能用了，需要创建新的MTW
      MultithreadedTableWriter newWriter = new MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://valuedb3", "pdatetest", false, false, null, 10000, 1, 5, "id", new int[] { Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_DELTA });
      try
      {
            //插入未写入的数据
            if (newWriter.insertUnwrittenData(unwriterdata).errorCode != "")
            {
            //等待写入完成后检查状态
            newWriter.waitForThreadCompletion();
            writeStatus = newWriter.getStatus();
            if (writeStatus.errorCode != "")
            {
                  System.Console.Out.WriteLine("error in write again:" + writeStatus.errorInfo);
            }
            }

      }
      finally
      {
            newWriter.waitForThreadCompletion();
            writeStatus = newWriter.getStatus();
            Console.WriteLine(string.Format("write again result:\n{0}", writeStatus));
      }
}
else
      Console.WriteLine("write complete : \n {0}", writeStatus.ToString());
//检查最后写入结果
Console.WriteLine(((BasicLong)dBConnection.run("exec count(*) from pt")).getLong());
/*
      unwriterdata 10
write again result:

isExiting: True
sentRows: 10
unsentRows: 0
sendFailedRows: 0
threadId : 8 sentRows : 5 unsentRows : 0 sendFailedRows : 0
threadId : 9 sentRows : 3 unsentRows : 0 sendFailedRows : 0
threadId : 10 sentRows : 1 unsentRows : 0 sendFailedRows : 0
threadId : 11 sentRows : 0 unsentRows : 0 sendFailedRows : 0
threadId : 12 sentRows : 1 unsentRows : 0 sendFailedRows : 0

110
      */
```

MultithreadedTableWriter 回调的使用

`MultithreadedTableWriter` 在开启回调后，用户会在回调的方法中获取到一个 BasicTable 类型的回调表，该表由两列构成：
第一列（String类型），存放的是调用 `MultithreadedTableWriter.insert` 时增加的每一行的 id；第二列（布尔值），表示每一行写入成功与否，true 表示写入成功，false 表示写入失败。

-继承 Callback 接口并重载 writeCompletion 方法用于获取回调数据

示例：

```cs
public class CallbackHandler : Callback
{
    public void writeCompletion(ITable callbackTable)
    {
        List<String> failedIdList = new List<string>();
        BasicStringVector idVec = (BasicStringVector)callbackTable.getColumn(0);
        BasicBooleanVector successVec = (BasicBooleanVector)callbackTable.getColumn(1);
        for (int i = 0; i < successVec.rows(); i++)
        {
            if (!successVec.getBoolean(i))
            {
                failedIdList.Add(idVec.getString(i));
            }
        }
    }
}
```

示例：

```cs
MultithreadedTableWriter mtw = new MultithreadedTableWriter(host, port, userName, password, dbName, tbName, useSSL,
        enableHighAvailability, null, 10000, 1, 1, "price", null,MultithreadedTableWriter.Mode.M_Append,null, new CallbackHandler());
```

-调用 `MultithreadedTableWriter` 的 `insert` 方法并在第一列中为每一行写入 id

```cs
String theme = "theme1";
for (int id = 0; id < 1000000; id++){
    mtw.insert(theme + id, code, price); //theme+id 为每一行对应的 id，将在回调时返回
}
```

### 7.5 更新并写入DolphinDB的数据表

DolphinDB CSHARP API 提供 `AutoFitTableUpsert` 类对象来更新并写入 DolphinDB 的表。`AutoFitTableUpsert` 同 `MultithreadedTableWriter` 指定 mode 为 Mode.M_Upsert 时更新表数据的功能一样，区别在于 `AutoFitTableUpsert` 为单线程写入，而 `MultithreadedTableWriter` 为多线程写入。

-AutoFitTableUpsert的主要方法如下：

-构造方法：

```cs
AutoFitTableUpsert(string dbUrl, string tableName, DBConnection connection, bool ignoreNull, string[] pkeyColNames, string[] psortColumns)
```

参数说明：

* dbUrl 字符串，表示分布式数据库地址。内存表时该参数为空。
* tableName 字符串，表示分布式表或内存表的表名。
* connection DBConnection 对象，用于连接 server 并 upsert 数据。注意：创建用于 AutoFitTableUpsert 的 DBConnection 对象时，asynchronousTask 必须为 false。
* ignoreNull 布尔值，表示 [upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert!.html) 的一个参数，其含义为若 upsert! 的新数据表中某元素为 NULL 值，是否对目标表中的相应数据进行更新。
* pkeyColNames 字符串数组，用于指定 upsert! 的 keyColNames 参数，即指定 DFS 表（目标表）的键值列。
* psortColumns 字符串数组，用于指定 upsert! 的 sortColumns 参数，设置该参数后，更新的分区内的所有数据会根据指定的列进行排序。排序在每个分区内部进行，不会跨分区排序。

-写入并更新数据的方法：

```cs
int upsert(BasicTable table)
```

函数说明：

将一个 BasicTable 对象更新到目标表中，返回一个 int 类型，表示更新了多少行数据。

`AutoFitTableUpsert` 使用示例如下：

```cs
DBConnection conn = new DBConnection(false, false, false);
conn.connect("192.168.1.116", 18999, "admin", "123456");
String dbName = "dfs://upsertTable";
String tableName = "pt";
String script = "dbName = \"dfs://upsertTable\"\n" +
"if(exists(dbName)){\n" +
"\tdropDatabase(dbName)\t\n" +
"}\n" +
"db  = database(dbName, RANGE,1 10000,,'TSDB')\n" +
"t = table(1000:0, `id`value,[ INT, INT[]])\n" +
"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
conn.run(script);

BasicIntVector v1 = new BasicIntVector(3);
v1.setInt(0, 1);
v1.setInt(1, 100);
v1.setInt(2, 9999);

BasicArrayVector ba = new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY);
ba.append(v1);
ba.append(v1);
ba.append(v1);

List<String> colNames = new List<string>();
colNames.Add("id");
colNames.Add("value");
List<IVector> cols = new List<IVector>();
cols.Add(v1);
cols.Add(ba);
BasicTable bt = new BasicTable(colNames, cols);
String[] keyColName = new String[] { "id" };
AutoFitTableUpsert aftu = new AutoFitTableUpsert(dbName, tableName, conn, false, keyColName, null);
aftu.upsert(bt);
BasicTable res = (BasicTable)conn.run("select * from pt;");
System.Console.Out.WriteLine(res.getString());
```

### 8. C#原生类型转换为DolphinDB数据类型

C# API提供了一组以Basic+\<DataType\>方式命名的类，分别对应DolphinDB的数据类型，比如BasicInt，BasicDate等等。

大部分DolphinDB数据类型可以由对应的C#数据类型构建，例如new BasicInt(4)对应integer，new BasicDouble(1.23)对应double，等等。但是也有一些DolphinDB数据类型，并不能由上述方法构建：

- CHAR类型：DolphinDB中的CHAR类型保存为一个byte，所以在C# API中用 BasicByte 类型来构造 CHAR，例如 new BasicByte((byte)'c')。
- SYMBOL类型：DolphinDB中的SYMBOL类型将字符串存储为整形，可以提高DolphinDB对字符串数据存储和查询的效率，但是C#中并没有这种类型，所以C# API不提供 BasicSymbol这种对象，直接用BasicString来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp九种类型的时间类型，最高精度可以是纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.cn/cn/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html)。由于C#也提供了DateTime及TimeSpan等数据类型，所以C# API在Utils类里提供了所有C#时间类型和int或long之间的转换函数。

以下脚本展示C# API中DolphinDB时间类型与C#原生时间类型之间的对应关系：
```cs
//Date:2018.11.12
BasicDate bd = new BasicDate(new DateTime(2018, 11, 12));

//Month:2018.11M
BasicMonth bm = new BasicMonth(new DateTime(2018, 11,1));

//Time:20:08:01.123
BasicTime bt = new BasicTime(new TimeSpan(0,20, 8, 1, 123));

//Minute:20:08m
BasicMinute bmn = new BasicMinute(new TimeSpan(20, 8,0));

//Second:20:08:01
BasicSecond bs = new BasicSecond(new TimeSpan(0,20, 8, 1));

//DateTime: 2018.11.12T08:01:01
BasicDateTime bdt = new BasicDateTime(new DateTime(2018, 11, 12, 8, 1, 1));

//Timestamp: 2018.11.12T08:01:01.123
BasicTimestamp bts = new BasicTimestamp(new DateTime(2018, 11, 12, 8, 1, 1, 123));
```
如果在第三方系统中时间以时间戳的方式存储，DolphinDB时间对象也可以用时间戳来实例化。
C# API中的Utils类提供了各种时间类型与标准时间戳的转换算法，比如将毫秒级的时间戳转换为DolphinDB的BasicTimestamp对象:
```cs
DateTime dt = Utils.parseTimestamp(154349485400L);
BasicTimestamp ts = new BasicTimestamp(dt);
```
也可以将DolphinDB对象转换为整形或长整形的时间戳，比如：
```cs
DateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```
如果时间戳以其他精度保存，Utils类还中提供如下方法，可以适应各种不同的精度：
- Utils.countMonths：计算给定时间到1970.01之间的月份差，返回int
- Utils.countDays：计算给定时间到1970.01。01之间的天数差，返回int
- Utils.countMinutes：计算给定时间到1970.01.01T00:00之间的分钟差，返回int
- Utils.countSeconds：计算给定时间到1970.01.01T00:00:00之间的秒数差，返回int
- Utils.countMilliseconds：计算给定时间到1970.01.01T00:00:00之间的毫秒数差，返回long

需要注意，由于C#的DateTime和TimeSpan在精度上达不到纳秒级别，所以如果在对纳秒精度的时间数据进行操作并且需要保留纳秒精度时，可以通过 NanoTimestamp.getInternalValue()来获取内部保存的long值，不要通过DateTime和TimeSpan转换，否则会造成精度损失。

### 9. C#流数据 API

C#程序可以通过API订阅流数据。C# API有三种获取流数据的方式：单线程回调（ThreadedClient），多线程回调（ThreadPooledClient）和通过 PollingClient 返回的对象获取消息队列。

#### 9.1 接口说明
三种方法对应的 subscribe 接口如下：
1. 通过 ThreadedClient 方式订阅的接口：
```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, float throttle = 0.01f, StreamDeserializer deserializer = null, string user = "", string password = "")
```
- **host** 是发布端节点的 IP 地址。
- **port** 是发布端节点的端口号。
- **tableName** 是发布表的名称。
- **actionName** 是订阅任务的名称。
- **handler** 是用户自定义的回调函数，用于处理每次流入的数据。
- **offset** 是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定 *offset*，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。*offset* 与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。
- **reconnect** 是布尔值，表示订阅中断后，是否会自动重订阅。
- **filter** 是一个向量，表示过滤条件。流数据表过滤列在 *filter* 中的数据才会发布到订阅端，不在 *filter* 中的数据不会发布。
- **batchSize** 是一个整数，表示批处理的消息的数量。如果它是正数，直到消息的数量达到 *batchSize* 时，*handler* 才会处理进来的消息。如果它没有指定或者是非正数，消息到达之后，*handler* 就会马上处理消息。
- **throttle** 是一个浮点数，表示 *handler* 处理到达的消息之前等待的时间，以秒为单位。默认值为 1。如果没有指定 *batchSize*，*throttle* 将不会起作用。
- **deserializer** 是订阅的异构流表对应的反序列化器。
- **user** 是一个字符串，表示 API 所连接服务器的登录用户名。
- **password** 是一个字符串，表示 API 所连接服务器的登录密码。

2. 通过 ThreadPooledClient 方式订阅的接口：

```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```
3. 通过 PollingClient 方式订阅的接口：
```cs
subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```
#### 9.2 示例代码
下面分别介绍如何通过3种方法订阅流数据。  
- 通过客户机上的应用程序定期去流数据表查询是否有新增数据，推荐使用 PollingClient。
```cs
PollingClient client = new PollingClient(subscribeHost, subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true)
{
      List<IMessage> msgs = poller1.poll(1000);

      if (msgs.Count > 0)
      {
            foreach(IMessage msg in msgs)
            System.Console.Out.WriteLine(string.Format("receive: {0}, {1}, {2}", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString()));
      }
      /*
      Successfully subscribed table 192.168.1.38:18848:local8848/Trades/csharpStreamingApi
      receive: 1, 2022.05.26T10:39:22.105, 1.5
      */
}
```
*  使用 MessageHandler 回调的方式获取新数据。

首先需要调用者定义handler。需要实现 dolphindb.streaming.MessageHandler接 口。
```cs
public class MyHandler : MessageHandler
{
      public void doEvent(IMessage msg)
      {
            System.Console.Out.WriteLine(string.Format("receive: {0}, {1}, {2}", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString()));
      }

      public void batchHandler(List<IMessage> msgs)
      {
            throw new NotImplementedException();
      }
}
```
在启动订阅时，把 handler 实例作为参数传入订阅函数。包括单线程回调或多线程回调两种方式：

1. 单线程回调 ThreadedClient
```cs
ThreadedClient client = new ThreadedClient(subscribeHost, subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler());
Thread.Sleep(10000);
//如果需要停止订阅，可以使用close函数。
client.close();
```

2. 多线程回调(ThreadPollingClient)：handler 模式客户端(线程池处理任务)
```cs
ThreadPooledClient client = new ThreadPooledClient(subscribeHost, subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler());
//如果需要停止订阅，可以使用close函数。
Thread.Sleep(10000);
client.close();
```

#### 9.3 断线重连

reconnect参数是一个布尔值，表示订阅意外中断后，是否会自动重新订阅。默认值为false。如果reconnect=true，有以下三种情况：

- 如果发布端与订阅端处于正常状态，但是网络中断，那么订阅端会在网络正常时，自动从中断位置重新订阅。
- 如果发布端崩溃，订阅端会在发布端重启后不断尝试重新订阅。
    - 如果发布端对流数据表启动了持久化，发布端重启后会首先读取硬盘上的数据，直到发布端读取到订阅中断位置的数据，订阅端才能成功重新订阅。
    - 如果发布端没有对流数据表启用持久化，那么订阅端将自动重新订阅失败。
- 如果订阅端崩溃，订阅端重启后不会自动重新订阅，需要重新执行`subscribe`函数。

以下例子在订阅时，设置 reconnect 为 true：

```cs
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

#### 9.4 启用filter

filter参数是一个向量。该参数需要发布端配合`setStreamTableFilterColumn`函数一起使用。使用`setStreamTableFilterColumn`指定流数据表的过滤列，流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为`subscribe`的filter参数：

```cs
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```

#### 9.5 订阅异构流表

DolphinDB server 自 1.30.17 及 2.00.5 版本开始，支持通过 [replay](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/r/replay.html) 函数将多个结构不同的流数据表，回放（序列化）到一个流表里，这个流表被称为异构流表。Python API 自 1.30.19 版本开始，新增 `StreamDeserializer` 类，用于构造异构流表反序列化器，以实现对异构流表的订阅和反序列化操作。

C# API 通过 `StreamDeserializer` 类来构造异构流表反序列化器，语法如下：
1. 通过指定表的schema进行构造，包含以下两种方式，指定表的schema信息或指定表的各列类型 ：

指定表的schema信息：
```cs
StreamDeserializer(Dictionary<string, BasicDictionary> filters)
```
指定表的各列类型：
```cs
StreamDeserializer(Dictionary<string, List<DATA_TYPE>> filters)
```
2. 通过指定表进行构造：
```cs
StreamDeserializer(Dictionary<string, Tuple<string, string>> tableNames, DBConnection conn = null)
```
订阅示例：
```cs
//假设异构流表回放时inputTables如下：
//d = dict(['msg1', 'msg2'], [table1, table2]); \
//replay(inputTables = d, outputTables = `outTables, dateColumn = `timestampv, timeColumn = `timestampv)";
//异构流表解析器的创建方法如下：

{//指定schema的方式
      BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
      BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
      Dictionary<string, BasicDictionary> schemas = new Dictionary<string, BasicDictionary>();
      schemas["msg1"] = outSharedTables1Schema;
      schemas["msg2"] = outSharedTables2Schema;
      StreamDeserializer streamFilter = new StreamDeserializer(schemas);
}
{//指定表的各列类型
	Dictionary<string, List<DATA_TYPE>> colTypes = new Dictionary<string, List<DATA_TYPE>>();
	List<DATA_TYPE> table1ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_DOUBLE };
	colTypes["msg1"] = table1ColTypes;
	List<DATA_TYPE> table2ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE };
	colTypes["msg2"] = table2ColTypes;
	StreamDeserializer streamFilter = new StreamDeserializer(colTypes);
}
{//指定表的方式
      Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
      tables["msg1"] = new Tuple<string, string>("", "table1");
      tables["msg2"] = new Tuple<string, string>("", "table2");
      //conn是可选参数，如果不传入，在订阅的时候会自动使用订阅的conn进行构造
      StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
}
```
下面分别介绍如何通过 ThreadedClient, ThreadPooledClient 和 PollingClient 三种方式订阅异构流表：
1. 通过 ThreadedClient 订阅异构流表：通过两种方式完成订阅时对异构流表的解析操作。
* 通过指定 `subscribe` 函数的 *deserialize* 参数，实现在订阅时直接解析异构流表：
```cs
ThreadedClient client = new ThreadedClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, -1, (float)0.01, streamFilter);
```
* 异构流表（streamFilter）也可以写入客户自定义的 Handler 中，在回调时被解析：
```cs
public class Handler6 : MessageHandler
      {
      private StreamDeserializer deserializer_;
      private List<BasicMessage> msg1 = new List<BasicMessage>();
      private List<BasicMessage> msg2 = new List<BasicMessage>();

      public Handler6(StreamDeserializer deserializer)
      {
            deserializer_ = deserializer;
      }

      public void batchHandler(List<IMessage> msgs)
      {
            throw new NotImplementedException();
      }

      public void doEvent(IMessage msg)
      {
            try
            {
                  BasicMessage message = deserializer_.parse(msg);
                  if (message.getSym() == "msg1")
                  {
                  msg1.Add(message);
                  }
                  else if (message.getSym() == "msg2")
                  {
                  msg2.Add(message);
                  }
            }
            catch (Exception e)
            {
                  System.Console.Out.WriteLine(e.StackTrace);
            }
      }

      public List<BasicMessage> getMsg1()
      {
            return msg1;
      }

      public List<BasicMessage> getMsg2()
      {
            return msg2;
      }
      };

Handler6 handler = new Handler6(streamFilter);
ThreadedClient client = new ThreadedClient(listenport);
client.subscribe(SERVER, PORT, tableName, actionName, handler, 0, true);
```

2. 通过 ThreadPooledClient 订阅异构流表的方法和 ThreadedClient 一致。
* 指定 `subscribe` 函数的 *deserialize* 参数：
```cs
ThreadPooledClient client = new ThreadPooledClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter);
```
* 异构流表（streamFilter）也可以写入客户自定义的 Handler 中，在回调时被解析：
```cs
Handler6 handler = new Handler6(streamFilter);
ThreadPooledClient client = new ThreadPooledClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true);
```

由于 PollingClient 没有回调函数，只能通过为 `subscirbe` 的 *deserialize* 参数传入 streamFilter 的方式进行解析：
```cs
PollingClient client = new PollingClient(listenport);
TopicPoller poller = client.subscribe(hostName, port, tableName, actionName, 0, true, null, streamFilter);
```
#### 9.6 取消订阅
每一个订阅都有一个订阅主题topic作为唯一标识。如果订阅时topic已经存在，那么会订阅失败。这时需要通过unsubscribeTable函数取消订阅才能再次订阅。
```cs
client.unsubscribe(serverIP, serverPort, tableName,actionName);
```
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

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password],[initialScript])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

### 2. 建立DolphinDB连接

C# API通过TCP/IP协议连接到DolphinDB服务器。在以下例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}
```
输入用户名和密码建立连接：

```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

当需要在应用程序里定义和使用自定义函数时，可以使用 initialScript 参数传入函数定义脚本。这样做的好处是：一、无需每次运行`run`函数的时候重复定义这些函数。二、API提供自动重连机制，断线之后重连时会产生新的会话。如果 initialScript 参数不为空，API会在新的会话中自动执行初始化脚本重新注册函数。在一些网络不是很稳定但是应用程序需要持续运行的场景里，这个参数会非常有用。
```
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```

### 3. 运行脚本

在C#中运行DolphinDB脚本的语法如下：
```
conn.run("script");
```

如果脚本只包含一条语句，如表达式，DolphinDB会返回该语句计算结果。如果脚本包含多条语句，将返回最后一条语句的结果。如果脚本含有错误或者出现网络问题，会抛出IOException。

### 4. 调用DolphinDB函数

调用的函数可以是内置函数或用户自定义函数。 下面的示例将一个double类型向量传递给服务器，并调用`sum`函数。

```
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

```
public void testUpload(){

      BasicTable tb = (BasicTable)conn.run("table(1..100 as id,take(`aaa,100) as name)");
      Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
      upObj.Add("table_uploaded", (IEntity)tb);
      db.upload(upObj);
      BasicIntVector v = (BasicIntVector)conn.run("table_uploaded.id");
      Console.WriteLine(v.rows());
}
```

### 6. 读取数据示例

下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

首先导入DolphinDB数据类型包：

```
using dolphindb.data;
```

注意，下面的代码需要在建立连接后才能运行。

- 向量

在下面的示例中，DolphinDB语句
```
rand(`IBM`MSFT`GOOG`BIDU,10)
```
返回C#对象BasicStringVector。vector.rows()方法能够获取向量的大小。我们可以使用vector.getString(i)方法按照索引访问向量元素。

```
public void testStringVector(){
      IVector v = (BasicStringVector)conn.run("take(`IBM`MSFT`GOOG`BIDU, 10)");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(((BasicString)v.get(1)).getValue());
}
```

类似的，也可以处理双精度浮点类型的向量或者元组。
```
public void testDoubleVector(){
      IVector v = (BasicDoubleVector)conn.run("1.123 2.2234 3.4567");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
}
```
```
public void testAnyVector(){
      BasicAnyVector v = (BasicAnyVector)conn.run("[1 2 3,3.4 3.5 3.6]");
      Console.WriteLine(v.rows());
      Console.WriteLine(v.columns());
      Console.WriteLine(((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
}
```
- 集合
```
public void testSet(){
      BasicSet s = (BasicSet)conn.run("set(1 3 5)");
      Console.WriteLine(s.rows());
      Console.WriteLine(s.columns());
}
```

- 矩阵

要从矩阵中检索一个元素，我们可以使用get(row,col)。 要获取行数和列数，我们可以使用函数`rows`和`columns`。

```
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

```
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

```
public void testTable(){
	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Console.WriteLine(dt.Rows.Count);
}
```
- NULL对象

要描述一个NULL对象，我们可以调用函数obj.getDataType()。
```
public void testVoid(){
      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}
```

### 7. 读写DolphinDB数据表

使用C# API的一个重要场景是，用户从其他数据库系统或是第三方WebAPI中取到数据，将数据进行清洗后存入DolphinDB数据库中，本节将介绍通过C# API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

#### 7.1 将数据保存到DolphinDB内存表

DolphinDB提供三种方式将数据新增到内存表：
- 通过`insert into`保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

一般不建议通过`append!`函数保存数据，因为`append!`函数会返回表的schema，产生不必要的通信量。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是string, int, timestamp, double类型，列名分别为cstring, cint, ctimestamp, cdouble。
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以该内存表只有当前会话可见。如果需要在其它会话中访问，需要通过`share`在会话间共享内存表。

##### 7.1.1 使用 `insert into` 保存单条数据

若将单条数据记录保存到DolphinDB内存表，那么可以通过类似SQL语句 insert into。
```
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

##### 7.1.2 使用`tableInsert`函数批量保存数组对象

`tableInsert`函数比较适合用来批量保存数据，它可将多个数组追加到DolphinDB内存表中。若C#程序获取的数据可以组织成List方式，可使用`tableInsert`函数保存。

```
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

```
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

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供`loadTable`方法可以加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
    List<IEntity> args = new List<IEntity>() { table1 };
    conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```

C#程序中的数组或列表，也可以很方便的构造出BasicTable用于追加数据。例如若有 boolArray, intArray, dblArray, dateArray, strArray 这5个列表对象(List<T>)，可以通过以下语句构造BasicTable对象：

```
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```
#### 7.2.2 分布式表的并发写入


DolphinDB的分布式表支持并发读写，下面展示如何在C#客户端中将数据并发写入DolphinDB的分布式表。

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。C# API提供了自动按分区分流数据并行写入的简便方法，其基本原理是设计一个连接池用于多线程写入，然后利用server的schema函数获取分布式表的分区信息，按指定的分区列将用户写入的数据进行分类分别交给不同的连接来并行写入。函数定义如下

```
public PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, IDBConnectionPool pool)
```
* dbUrl: 必填，分布式数据库地址
* tableName: 必填，分布式表名
* partitionColName: 必填，分区字段
* appendFunction: 可选，自定义写入函数名，不填此参数则调用内置tableInsert函数。
* pool: 连接池，并行写入数据。


首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://DolphinDBUUID"和分布式表"device_status"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

```
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

```
IDBConnectionPool conn = new ExclusiveDBConnectionPool(host, port, "admin", "123456",threadCount, false, false);

PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "gid", "saveGridData{'" + dbPath + "','" + tableName + "'}", conn);
BasicTable table1 = createTable();
appender.append(table1);            
```


#### 7.3 保存数据到本地磁盘表

通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不持支并发读写，不保证运行中的数据一致性，所以不建议在生产环境中使用。

```
//
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
通过`tableInsert`追加数据:
```
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```
#### 7.4 读取和使用数据表

在C# API中，数据表保存为BasicTable对象。由于BasicTable是列式存储，所以若要在C# API中读取行数据需要先取出需要的列，再取出行。

以下例子中参数BasicTable的有4个列，列名分别为cstring, cint, ctimestamp, cdouble，数据类型分别是STRING, INT, TIMESTAMP, DOUBLE。
```
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

### 8. C#原生类型转换为DolphinDB数据类型

C# API提供了一组以Basic+\<DataType\>方式命名的类，分别对应DolphinDB的数据类型，比如BasicInt，BasicDate等等。

大部分DolphinDB数据类型可以由对应的Java数据类型构建，例如new BasicInt(4)对应integer，new BasicDouble(1.23)对应double，等等。但是也有一些DolphinDB数据类型，并不能由上述方法构建：

- CHAR类型：DolphinDB中的CHAR类型保存为一个byte，所以在C# API中用 BasicByte 类型来构造 CHAR，例如 new BasicByte((byte)'c')。
- SYMBOL类型：DolphinDB中的SYMBOL类型将字符串存储为整形，可以提高DolphinDB对字符串数据存储和查询的效率，但是C#中并没有这种类型，所以C# API不提供 BasicSymbol这种对象，直接用BasicString来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp九种类型的时间类型，最高精度可以是纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.cn/cn/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html)。由于C#也提供了DateTime及TimeSpan等数据类型，所以C# API在Utils类里提供了所有C#时间类型和int或long之间的转换函数。

以下脚本展示C# API中DolphinDB时间类型与C#原生时间类型之间的对应关系：
```
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
```
DateTime dt = Utils.parseTimestamp(154349485400L);
BasicTimestamp ts = new BasicTimestamp(dt);
```
也可以将DolphinDB对象转换为整形或长整形的时间戳，比如：
```
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

C#程序可以通过API订阅流数据。C# API有两种获取数据的方式：

- 客户机上的应用程序定期去流数据表查询是否有新增数据，若有，应用程序会获取新增数据。
```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);

   if (msgs.size() > 0) {
       BasicInt value = msgs.get(0).getValue<BasicInt>(2);  //取表中第一行第二个字段
   }
}
```
* C# API使用MessageHandler获取新数据。

首先需要调用者先定义handler。需要实现 dolphindb.streaming.MessageHandler接口。
```
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue<BasicInt>(2);
               //..处理数据
       }
}
```
在启动订阅时，把handler实例作为参数传入订阅函数。

```
ThreadedClient client = new ThreadedClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

**handler模式客户端(多线程)(ThreadPollingClient)**
```
ThreadPooledClient client = new ThreadPooledClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

#### 断线重连

reconnect参数是一个布尔值，表示订阅意外中断后，是否会自动重新订阅。默认值为false。如果reconnect=true，有以下三种情况：

- 如果发布端与订阅端处于正常状态，但是网络中断，那么订阅端会在网络正常时，自动从中断位置重新订阅。
- 如果发布端崩溃，订阅端会在发布端重启后不断尝试重新订阅。
    - 如果发布端对流数据表启动了持久化，发布端重启后会首先读取硬盘上的数据，直到发布端读取到订阅中断位置的数据，订阅端才能成功重新订阅。
    - 如果发布端没有对流数据表启用持久化，那么订阅端将自动重新订阅失败。
- 如果订阅端崩溃，订阅端重启后不会自动重新订阅，需要重新执行`subscribe`函数。

以下例子在订阅时，设置 reconnect 为 true：

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

#### 启用filter

filter参数是一个向量。该参数需要发布端配合`setStreamTableFilterColumn`函数一起使用。使用`setStreamTableFilterColumn`指定流数据表的过滤列，流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为`subscribe`的filter参数：

```
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```

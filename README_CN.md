### 1. C# API 概念
C# API本质上实现了.Net程序和DolphinDB服务器之间的消息传递和数据转换协议。
C# API运行在.Net Framework 4.0 及以上环境

C# API遵循面向接口编程的原则。C# API使用接口类Entity来表示DolphinDB返回的所有数据类型。在Entity接口类的基础上，根据DolphinDB的数据类型，C# API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在 dolphindb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|`Basic<DataType>`|BasicInt, BasicDouble, BasicDate, etc.
vector，matrix|`Basic<DataType><DataForm>`|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set， dictionary和table|`Basic<DataForm>`|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

“Basic”表示基本的数据类型接口，`<DataType>`表示DolphinDB数据类型名称，`<DataForm>`是一个DolphinDB数据形式名称。

详细接口和类描述请参考[C# API手册](https://www.dolphindb.com/C#api/)

DolphinDB C# API 提供的最核心的对象是DBConnection，它主要的功能就是让C#应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。
DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

### 2. 建立DolphinDB连接

C# API通过TCP/IP协议连接到DolphinDB服务器。 在下列例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}

```
使用用户名和密码建立连接：

```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```
当不带用户名密码连接成功后，脚本在Guest权限下运行，后续运行中若需要提升权限，可以通过调用 `conn.login('admin','123456',true)` 登录获取权限。

### 3.运行脚本

在C#中运行DolphinDB脚本的语法如下：
```
conn.run("<SCRIPT>");
```
其中，脚本的最大长度为65,535字节。

如果脚本只包含一条语句，如表达式，DolphinDB会返回一个数据对象；否则返回NULL对象。如果脚本包含多条语句，将返回最后一个对象。如果脚本含有错误或者出现网络问题，它会抛出IOException。

### 4. 运行函数
当一段逻辑需要被服务端脚本反复调用时，可以用DolphinDB脚本将逻辑封装成自定义函数，类似于存储过程，然后在C#程序中通过函数方式调用。

下面的示例展示C#程序调用DolhinDB的add函数的方式，add函数有两个参数，参数的存储位置不同，也会导致调用方式的不同，下面会分三种情况来展示示例代码：

* 所有参数都在DolphinDB Server端

变量 x, y 已经通过C#程序提前在服务器端生成。
```
conn.run("x = [1,3,5];y = [2,4,6]")
```
那么在C#端要对这两个向量做加法运算，只需要直接使用`run(script)`的方式即可
```
public void testFunction()
{
      IVector result = (IVector) conn.run("add(x,y)");
      Console.WriteLine(result.getString());
}

```

* 部分参数在DolphinDB Server端存在

变量 x 已经通过C#程序提前在服务器端生成，参数 y 要在C#客户端生成
```
conn.run("x = [1,3,5]")
```
这时就需要使用`部分应用`方式，把参数 x 固化在add函数内，具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

```
public void testFunction()
{
      List<IEntity> args = new List<IEntity>(1);
      BasicDoubleVector y = new BasicDoubleVector(3);
      y.setDouble(0, 2.5);
      y.setDouble(1, 3.5);
      y.setDouble(2, 5);
      args.Add(y);
      IVector result = (IVector)conn.run("add{x}", args);
      Console.WriteLine(result.getString());
}
```
* 两个参数都在C#客户端
```
public void testFunction()
{
      List<IEntity> args = new List<IEntity>(1);
      BasicDoubleVector x = new BasicDoubleVector(3);
      x.setDouble(0, 1.5);
      x.setDouble(1, 2.5);
      x.setDouble(2, 7);
      BasicDoubleVector y = new BasicDoubleVector(3);
      y.setDouble(0, 2.5);
      y.setDouble(1, 3.5);
      y.setDouble(2, 5);
      args.Add(x);
      args.Add(y);
      IVector result = (IVector)conn.run("add", args);
      Console.WriteLine(result.getString());
}
```

### 5. 上传数据对象
当C#中的一些数据需要被服务端频繁的用到，那么每次调用的时候都上传一次肯定不是一个好的做法，这个时候可以使用upload方法，将数据上传到服务器并分配给一个变量，在Server端就可以重复使用这个变量。

我们可以将二进制数据对象上传到DolphinDB服务器，并将其分配给一个变量以备将来使用。 变量名称可以使用三种类型的字符：字母，数字或下划线。 第一个字符必须是字母。

```
public void testUpload()
{
      Dictionary<string, IEntity> vars = new Dictionary<string, IEntity>();
      BasicDoubleVector vec = new BasicDoubleVector(3);
      vec.setDouble(0, 1.5);
      vec.setDouble(1, 2.5);
      vec.setDouble(2, 7);
      vars.Add("a",vec);
      conn.upload(vars);
      IEntity result = (IEntity)conn.run("accumulate(+,a)");
      Console.WriteLine(result.getString());
}
```
### 6. 读取数据示例

下面介绍建立DolphinDB连接后，在C#环境中，对不同DolphinDB数据类型进行操作，运行结果显示在Console窗口。

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

要从整数矩阵中检索一个元素，我们可以使用getInt(row,)。 要获取行数和列数，我们可以使用函数rows()和columns()。

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

用函数keys()和values()可以从字典取得所有的键和值。要从一个键里取得它的值，可以调用get(key)。

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

要获取表的列，我们可以调用table.getColumn(index)；同样，我们可以调用table.getColumnName(index)获取列名。 对于列和行的数量，我们可以分别调用table.columns()和table.rows()。

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

- 内存表: 数据仅保存在本节点内存，存取速度最快，但是节点关闭数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上，即使节点关闭，通过脚本就可以方便的从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据：
- 通过 insert into 保存单条数据；
- 通过 tableInsert 函数批量保存多条数据；
- 通过 append! 函数保存表对象。


这几种方式的区别是接收的参数类型不同，具体业务场景中，可能从数据源取到的是单点数据，也可能是多个数组或者表的方式组成的数据集。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是`string,int,timestamp,double`类型，列名分别为`cstring,cint,ctimestamp,cdouble`，构建脚本如下：
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以GUI中创建的内存表只有当前GUI会话可见，如果需要在C#程序或者其他终端访问，需要通过share关键字在会话间共享内存表。
### 7.1.1. 保存单点数据
若C#程序是每次获取单条数据记录保存到DolphinDB，那么可以通过SQL语句（insert into）保存数据。
```
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

### 7.1.2 使用tableInsert函数批量保存数据

若C#程序获取的数据可以组织成List方式，使用tableInsert函数比较适合，这个函数可以接受多个数组作为参数，将数组追加到数据表中。

```
public void test_save_TableInsert(string[] strArray, int[] intArray, long[] tsArray, double[] dblArray)
{
      //用数组构造参数
      List<IEntity> args = new List<IEntity>() { new BasicStringVector(strArray), new BasicIntVector(intArray), new BasicTimestampVector(tsArray), new BasicDoubleVector(dblArray) };
      conn.run("tableInsert{sharedTable}", args);
}
```
实际运用的场景中，通常是C#程序往服务端已经存在的表中写入数据，在服务端可以用 `tableInsert(sharedTable,vec1,vec2,vec3...)` 这样的脚本，但是在C#里用 `conn.run(functionName,args)` 方式调用时，args里是无法传入服务端表的对象引用的。所以常规的做法是在预先在服务端定义一个函数，把sharedTable固化到函数体内，比如
```
def saveData(v1,v2,v3,v4){tableInsert(sharedTable,v1,v2,v3,v4)}
```
然后再通过`conn.run("saveData",args)`运行函数，虽然这样也能实现目标，但是对Java程序来说要多一次服务端的调用，多消耗了网络资源。
在本例中，使用了DolphinDB 中的`部分应用`这一特性，将服务端表名以`tableInsert{sharedTable}`这样的方式固化到tableInsert中，作为一个独立函数来使用。这样就不需要再使用自定义函数的方式实现。
具体的文档请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

### 7.1.3 使用append！函数批量保存数据
若C#程序是从DolphinDB的服务端获取表数据做处理后保存到分布式表，那么使用append!函数会更加方便，append!函数接受一个表对象作为参数，将数据追加到数据表中。

```
public void test_save_table(BasicTable table1)
{
      List<IEntity> args = new  List<IEntity>(){ table1};
      conn.run("append!{shareTable}", args);
}
```
#### 7.2 保存数据到分布式表
分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性; 分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

本例中涉及到的数据表可以通过如下脚本构建 ：

*请注意只有启用 `enableDFS=1` 的集群环境才能使用分布式表。*

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供loadTable方法可以加载分布式表，通过append!方式追加数据，具体的脚本示例如下：

```
public void test_save_table(String dbPath, BasicTable table1)
{
    List<IEntity> args = new List<IEntity>() { table1 };
    conn.run(String.Format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```

当用户在C#程序中取到的值是数组或列表时，也可以很方便的构造出BasicTable用于追加数据，比如现在有 `boolArray, intArray, dblArray, dateArray, strArray` 5个列表对象(List<T>),可以通过以下语句构造BasicTable对象：

```
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```

### 7.3 保存数据到本地磁盘表
通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不保证运行中的数据一致性，所以不建议在生产环境中使用。

```
//使用DolphinDB脚本创建一个数据表
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供loadTable方法可以加载本地磁盘表和分布式表，对于本地磁盘表而言，追加数据都是通过append!方式进行。
```
public void test_save_table(String dbPath, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```
#### 7.4 读取和使用表数据
在C# API中，表数据保存为BasicTable对象，由于BasicTable是列式存储，所以要读取和使用所有desultory需要通过先取出列，再循环取出行的方式。

例子中参数BasicTable的有4个列，分别是`STRING,INT,TIMESTAMP,DOUBLE`类型，列名分别为`cstring,cint,ctimestamp,cdouble`。

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

### 8. DolphinDB和C#之间的数据类型转换
C# API提供了与DolphinDB内部数据类型对应的对象，通常是以Basic+<DataType>这种方式命名，比如BasicInt，BasicDate等等。
一些C#的基础类型，可以通过构造函数直接创建对应的DOlphinDB数据结构，比如`new BasicInt(4)`，`new BasicDouble(1.23)`，但是也有一些类型需要做一些转换，下面列出需要做简单转换的类型：
- `CHAR`类型：DolphinDB中的`CHAR`类型以Byte形式保存，所以在C# API中用`BasicByte`类型来构造`CHAR`，例如`new BasicByte((byte)'c')`
- `SYMBOL`类型：DolphinDB中的`SYMBOL`类型是对字符串的优化，可以提高DolphinDB对字符串数据存储和查询的效率，但是C#中并不需要这种类型，所以C# API不提供`BasicSymbol`这种对象，直接用`BasicString`来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供`date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp`九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html)。由于C#也提供了`DateTime,TimeSpan`等数据类型，所以C# API在Utils类里提供了所有C#时间类型和int或long之间的转换函数。

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
C# API中的Utils类提供了各种时间类型与标准时间戳的转换算法，比如将毫秒级的时间戳转换为DolphinDB的`BasicTimestamp`对象:
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

需要注意，由于C#的DateTime和TimeSpan在精度上达不到纳秒级别，所以如果要操作纳秒精度的时间数据时，可以通过 `NanoTimestamp.getInternalValue()`来获取内部保存的long值，不要通过DateTime和TimeSpan转换，否则会造成精度损失。
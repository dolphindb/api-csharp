# DolphinDB C# API

### 1. C# API Introduction


The C# API implements messaging and data conversion between .Net program and DolphinDB server. C# API runs on .Net Framework 4.0 and above.

The C# API adopts interface-oriented programming. It uses the interface class "IEntity" to represent all data types returned by DolphinDB. Based on the "IEntity" interface class and DolphinDB data forms, the C# API provides 7 extension interfaces: scalar, vector, matrix, set, dictionary, table and chart. These interface classes are included in the package of com.xxdb.data.

Extended Interface Classes | Naming Rules | Examples
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary and table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart| |BasicChart|

"Basic" indicates the basic data type interface, \<DataType\> indicates a DolphinDB data type, and \<DataForm\> indicates a DolphinDB data form.

The most important object provided by the DolphinDB C# API is DBConnection. It allows C# applications to execute script and functions on DolphinDB servers and transfer data between C# applications and DolphinDB servers. DBConnection provides the following methods：

| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to DolphinDB server|
|login(username,password,enableEncryption)|Log in to the server|
|run(script)|Run script on DolphinDB server|
|run(functionName,args)|Call a function on DolphinDB server|
|upload(variableObjectMap)|Upload local data to DolphinDB server|
|isBusy()|Determine if the current session is busy|
|close()|Close the current session|

### 2. Establish DolphinDB connection

The C# API connects to the DolphinDB server via TCP/IP protocol. To connect to a local DolphinDB server with port number 8848:

```
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}
```
Establish a connection with a username and password:
```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

If an application calls user-defined functions, we can pass the script with function definitions to the parameter 'initialScript'. The advantages of this are: First, we do not need to repeatedly define these functions each time we call function `run`. Second, the API provides an automatic reconnection mechanism, which generates a new session when it is reconnected after a network disruption. If the parameter 'initialScript' is specified, the API will automatically execute the script to redefine these functions in the new session. This parameter can be very useful when network connection is not very stable but the application needs to run continuously.
```
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```

### 3.Run DolphinDB script

To run DolphinDB script in C#:

```
conn.run("<SCRIPT>");
```

If the script contains only one statement, such as an expression, DolphinDB returns the result of the statement. If the script contains more than one statement, the result of the last statement will be returned. If the script contains an error or there is a network problem, it throws an IOException.

### 4. Call DolphinDB functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. The following example passes a double vector to the server and calls function `sum`.

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

### 5. Upload data to DolphinDB server

We can upload a data object to DolphinDB server and assign it to a variable for future use. Variable names can use 3 types of characters: letters, numbers and underscores. The first character must be a letter.

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

### 6. Read data

This section introduces how to read different data forms in DolphinDB with the DBConnection object.

We need to import the DolphinDB data type package:
```
using dolphindb.data;
```

- Vector

In the following example, the DolphinDB statement returns the C# object BasicStringVector. The `rows` method returns the size of the vector. We can access vector elements by index with the `getString` method.

```
rand(`IBM`MSFT`GOOG`BIDU,10)
```

```
public void testStringVector(){
      IVector v = (BasicStringVector)conn.run("take(`IBM`MSFT`GOOG`BIDU, 10)");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(((BasicString)v.get(1)).getValue());
}
```
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

- Set
```
public void testSet(){
      BasicSet s = (BasicSet)conn.run("set(1 3 5)");
      Console.WriteLine(s.rows());
      Console.WriteLine(s.columns());
}
```

- Matrix

To retrieve an element from a matrix, we can use `get`. To get the number of rows and columns, we can use the functions `rows` and `columns`, respectively.

```
public void testIntMatrix(){
      IMatrix m = (BasicIntMatrix)conn.run("matrix(45 47 48,56 65 67)");
      Console.WriteLine(m.isMatrix());
      Console.WriteLine(m.rows());
      Console.WriteLine(m.columns());
      Console.WriteLine(((BasicInt)m.get(0, 1)).getValue());
}
```

- Dictionary

The keys and values of a dictionary can be retrieved with functions `keys` and `values`, respectively. To get the value for a key, use `get`.

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

- Table

To get a column of a table, use `getColumn`; to get a column name, use `getColumnName`. To get the number of columns and rows of a table, use `columns` and `rows`, respectively.

```
public void testTable(){
	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Console.WriteLine(dt.Rows.Count);
}
```
- NULL object

To determine if an object is NULL, use `getDataType`.

```
public void testVoid(){
      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}
```

### 7. Read from and write to DolphinDB tables

There are 3 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- Local disk table: data are saved on the local disk and can be loaded into memory.
- Distributed table: data are distributed across disks of multiple nodes. Users can query the table as if it is a local disk table.

#### 9.1. Save data to DolphinDB in-memory table

DolphinDB offers several ways to save data to an in-memory table:
- Save a single row of data with `insert into`
- Save multiple rows of data in bulk with function `tableInsert`
- Save a table object with function `tableInsert`

The table in the following examples has 4 columns. Their data types are string, int, timestamp and double. The column names are cstring, cint, ctimestamp and cdouble, respectively.

```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```

By default, an in-memory table is not shared among sessions. To access it in a different session, we need to share it among sessions with `share`.

##### 7.1.1 Save a single record to an in-memory table with 'insert into' 

```
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

##### 7.1.2 Save data in batches with `tableInsert`

Function `tableInsert` can save records in batches. If data in Java can be organized as a List, it can be saved with function `tableInsert`.

```
public void test_save_TableInsert(string[] strArray, int[] intArray, long[] tsArray, double[] dblArray)
{
      // Constructing parameters with arrays
      List<IEntity> args = new List<IEntity>() { new BasicStringVector(strArray), new BasicIntVector(intArray), new BasicTimestampVector(tsArray), new BasicDoubleVector(dblArray) };
      conn.run("tableInsert{sharedTable}", args);
}
```

The example above uses partial application in DolphinDB to embed a table in `tableInsert{sharedTable}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html).

##### 7.1.3 Use function `tableInsert` to save a table object
```
public void test_save_table(BasicTable table1)
{
      List<IEntity> args = new  List<IEntity>(){ table1};
      conn.run("tableInsert{shareTable}", args);
}
```

#### 7.2 Save data to a distributed table

Distributed table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. With data replication, Distributed tables offers fault tolerance and load balancing.

Use the following script in DolphinDB to create a distributed table. Function `database` creates a database. The path of a distributed database must start with "dfs". Function `createPartitionedTable` creates a distributed table. 

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

Use function `loadTable` to load a distributed table. Use function `tableInsert` to append data to the table.

```
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```

With an array or a list in the C# program, it is convenient to construct a BasicTable for appending data. For example, with the 5 list objects boolArray, intArray, dblArray, dateArray, strArray (List< T>), we can construct a BasicTable object with the following statements:

```
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```

#### 7.3 Save data to a local disk table

Local disk tables can be used for data analysis on historical data sets. They do not support transactions, nor do they support concurrent read and write.

Use the following script in DolphinDB to create a local disk table.

```
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

Next, use `tableInsert` to to append data to a local disk table. 

```
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```

### 7.4 Read and use a database table

In the C# API, a table is saved as a BasicTable object. Since BasicTable is column based, to retrieve rows we need to get the necessary columns first and then get the rows.

In the example below, the BasicTable has 4 columns with data types STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble.

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

### 8. Convert C# data types into DolphinDB data types

C# API provides objects that correspond to DolphinDB data types. They are usually named as Basic+ \<DataType\>, such as BasicInt, BasicDate, etc.

The majority of DolphinDB data types can be constructed from corresponding C# data types. For examples, INT in DolphinDB from 'new BasicInt(4)', DOUBLE in DolphinDB from 'new BasicDouble(1.23)'. The following DolphinDB data types, however, need to be constructed in different ways: 
- CHAR type: as the CHAR type in DolphinDB is stored as a byte, we can use the BasicByte type to construct CHAR in C# API, for example 'new BasicByte((byte)'c')'.
- SYMBOL type: the SYMBOL type in DolphinDB is stored as INT to improve the efficiency of storage and query of strings. C# doesn't have this data type, so C# API does not provide BasicSymbol. SYMBOL type can be processed directly with BasicString. 
- Temporal types: temporal data types are stored as INT or LONG in DolphinDB. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime and nanotimestamp. For detailed description, please refer to [DolphinDB Temporal Type and Conversion](https://www.dolphindb.com/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html). Since C# also provides data types such as LocalDate, LocalTime, LocalDateTime and YearMonth, C# API provides conversion functions in the Utils class between all C# temporal types and INT or LONG.

The following script shows the correspondence between DolphinDB temporal types and C# native temporal types:

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
If a temporal variable is stored as timestamp in a third-party system, DolphinDB temporal object can also be instantiated with a timestamp. The Utils class in the C# API provides conversion algorithms for various temporal types and standard timestamps, such as converting millisecond timestamps to DolphinDB's BasicTimestamp objects:

```
DateTime dt = Utils.parseTimestamp(154349485400L);
BasicTimestamp ts = new BasicTimestamp(dt);
```

We can also convert a DolphinDB object to a timestamp of an integer or long integer, such as:

```
DateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```

The Utils class provides the following methods to handle a variety of timestamp precisions:
- Utils.countMonths: Calculate the monthly difference between a given time and 1970.01, returning an int
- Utils.countDays: Calculate the difference in the number of days between the given time and 1970.01.01, return int
- Utils.countMinutes: Calculate the minute difference between the given time and 1970.01.01T00:00, return int
- Utils.countSeconds: Calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning int
- Utils.countMilliseconds: Calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, return long
- Utils.countNanoseconds: Calculate the difference in nanoseconds between a given time and 1970.01.01T00:00:00.000, return long

需要注意，由于C#的DateTime和TimeSpan在精度上达不到纳秒级别，所以如果要操作纳秒精度的时间数据时，可以通过 NanoTimestamp.getInternalValue()来获取内部保存的long值，不要通过DateTime和TimeSpan转换，否则会造成精度损失。

As the precision of DateTime and TimeSpan in C# is not up to nanosecond, to keep nanosecond precision when working with temporal data, we can use NanoTimestamp.getInternalValue() to get the LONG values corresponding to the nanosecond precision data. 

### 9. C# streaming API

A C# program can subscribe to streaming data via API. C# API can acquire streaming data in the following 2 ways:

- The application on the client periodically checks if new data has been added to the streaming table. If yes, the application will acquire and consume the new data. 

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);

   if (msgs.size() > 0) {
       BasicInt value = msgs.get(0).getValue<BasicInt>(2);  //  Take the second field in the first row of the table
   }
}
```
* The API uses MessageHandler to get new data

First we need to define the message handler, which needs to implement dolphindb.streaming.MessageHandler interface. 

```
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue<BasicInt>(2);
               //..Processing data
       }
}
```

The handler instance is passed into function `subscribe` as a parameter. 

```
ThreadedClient client = new ThreadedClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

**Handler mode client (multithreading) (ThreadPollingClient)**

```
ThreadPooledClient client = new ThreadPooledClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

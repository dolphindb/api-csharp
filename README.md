# DolphinDB C# API

- [DolphinDB C# API](#dolphindb-c-api)
    - [1. C# API Introduction](#1-c-api-introduction)
    - [2. Establish DolphinDB Connection](#2-establish-dolphindb-connection)
      - [2.1 DBConnection](#21-dbconnection)
      - [2.2 ExclusiveDBConnectionPool](#22-exclusivedbconnectionpool)
    - [3. Run DolphinDB Scripts](#3-run-dolphindb-scripts)
    - [4. Call DolphinDB Functions](#4-call-dolphindb-functions)
    - [5. Upload Data to DolphinDB Server](#5-upload-data-to-dolphindb-server)
    - [6. Read Data](#6-read-data)
    - [7. Read From and Write to DolphinDB Tables](#7-read-from-and-write-to-dolphindb-tables)
      - [7.1 Write to an In-Memory Table](#71-write-to-an-in-memory-table)
      - [7.2 Write to a DFS Table](#72-write-to-a-dfs-table)
      - [7.3 Load and Query Tables](#73-load-and-query-tables)
      - [7.4 Append Data Asynchronously](#74-append-data-asynchronously)
      - [7.5 Update and Write to DolphinDB Tables](#75-update-and-write-to-dolphindb-tables)
    - [8. Data Type Conversion](#8-data-type-conversion)
    - [9. C# Streaming API](#9-c-streaming-api)
      - [9.1 Interfaces](#91-interfaces)
      - [9.2 Code Examples](#92-code-examples)
      - [9.3 Reconnect](#93-reconnect)
      - [9.4 Filter](#94-filter)
      - [9.5 Subscribe to a Heterogeneous Table](#95-subscribe-to-a-heterogeneous-table)
      - [9.6 Unsubscribe](#96-unsubscribe)



### 1. C# API Introduction

The C# API implements messaging and data conversion between .Net program and DolphinDB server, which runs on .Net Framework 4.0 and above.

The C# API adopts interface-oriented programming. It uses the interface class "IEntity" to represent all data types returned by DolphinDB. Based on the "IEntity" interface class and DolphinDB data forms, the C# API provides the following extension interfaces which are included in the com.xxdb.data package:

Extended Interface Classes|Naming Rules|Examples
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, and table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

* Basic: basic data type interface
* \<DataType>: DolphinDB data type
* \<DataForm>: DolphinDB data form

The most important object provided by the DolphinDB C# API is `DBConnection`. It provides the C# applications with the following methods:


| Method Name                                      | Details                    |
| ---------------------------------------- | ---------------------- |
| DBConnection([asynchronousTask=false], [useSSL=false], [compress=false], [usePython=false]) | Construct an object, indicating whether to enable asynchronous tasks, ssl, and compression |
| connect(hostName, port, [userId=””], [password=””], [startup=””], [highAvailability=false], [highAvailabilitySites], [reconnect=false]) | Connect the session to DolphinDB server     |
| login(userId, password, enableEncryption) | Log in to the server                  |
| run(script, [listener], [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | Run scripts on DolphinDB server synchronously   |
| runAsync(script, [priority = 4], [parallelism=2],  [fetchSize=0], [clearMemory = false]) | Run scripts on DolphinDB server asynchronously    |
| run(functionName, arguments, [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | Call a function on DolphinDB server synchronously   |
| runAsync(functionName, arguments, [priority=4], [parallelism=2], [fetchSize=0], [clearMemory=false]) | Call a function on DolphinDB server asynchronously   |
| upload(variableObjectMap)                | Upload local data to DolphinDB server |
| isBusy()                                 | Determine if the current session is busy             |
| close()                                  | Close the current session                |

**Note**: If the current session is idle, C# API will automatically close the connection after a while. You can close the session by calling `close()` to release the connection. Otherwise, other sessions may be unable to connect to the server due to too many connections.

### 2. Establish DolphinDB Connection

#### 2.1 DBConnection

The C# API connects to the DolphinDB server via TCP/IP protocol. To connect to a local DolphinDB server with port number 8848:

```cs
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}
```
Starting from API 1.30.17, the following optional parameters can be specified for connection: *asynchronousTask*, *useSSL*, *compress*, and *usePython*. The default values of these parameters are false. Currently, only linux stable version >= 1.10.17, and latest version >= 1.20.6 are supported.  

The following example establishes a connection to the server. It enables SSL and compression but disables asynchronous communication. Note that the configuration parameter *enableHTTPS=true* must be specified on the server side.

```cs
DBConnection conn = new DBConnection(false,true,true)
```

In the following example, the connection disables SSL and enables asynchronous communication. In this case, only DolphinDB scripts and functions can be executed and no values are returned. This feature is for asynchronous writes.

```cs
DBConnection conn = new DBConnection(true,false)
```
Establish a connection with a username and password:

```cs
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

To define and use user-defined functions in a C# program, you can pass in the user-defined scripts to the parameter *startup*. The advantages are: (1) These functions don't need to be defined repeatedly every time run is called; (2) The API client can automatically connect to the server after disconnection. If the parameter *startup* is specified, the C# API will automatically execute the script and register the functions. The parameter can be very useful for scenarios where the network is not stable but the program needs to run continuously.

```cs
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```

#### 2.2 ExclusiveDBConnectionPool

Multiple `DBconnection` objects can be reused by `ExclusiveDBConnectionPool`. You can either execute command `ExclusiveDBConnectionPool.run`, or execute a task with `execute` and then obtain the results with `getResults` method of `BasicDBTask`.

| Method Name                              | Details                                       |
| ---------------------------------------- | ---------------------------------------- |
| ExclusiveDBConnectionPoolExclusiveDBConnectionPool(host, port, uid, pwd, count, loadBalance,  highAvaliability, [haSites], [startup=””], [compress= false], [useSSL=false], [usePython=false]) | Constructor. The parameter *count* indicates the number of connections to be used. If *loadBalance* is set to true, different nodes are connected. |
| run(script, [priority=4], [parallelism=2], [clearMemory=false]) | Run scripts on DolphinDB server synchronously                     |
| runAsync(script, [priority=4], [parallelism=2], [clearMemory=false]) | Run scripts on DolphinDB server asynchronously                      |
| run(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | Call a function on DolphinDB server synchronously                     |
| runAsync(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | Call a function on DolphinDB server asynchronously                     |
| execute(task)                            | Execute the task.                                     |
| execute(tasks)                           | Execute tasks in batches.                                   |
| getConnectionCount()                     | Get the number of connections.                                   |
| shutdown                                 | Shut down the connection pool.                                    |

**Note**: If the current `ExclusiveDBConnectionPool` is idle, C# API will automatically close the connection after a while. To release the connection resources, call `shutdown()` upon the completion of thread tasks.

`BasicDBTask` encapsulates the functions and arguments to be executed.

| Method Name                                      | Details                      |
| ---------------------------------------- | ----------------------- |
| BasicDBTask(functionName, arguments, [priority=4], [parallelism=2], [clearMemory=false]) | functionName: the function to be executed; arguments: the arguments passed to the *functionName*. |
| BasicDBTask(script, [priority=4], [parallelism=2], [clearMemory=false]) | The script to be executed.                 |
| isSuccessful()                           | Check whether the task is executed successfully.                |
| getResults()                             | Get the execution results.                |
| getErrorMsg()                            | Get the error messages.          |

Build a connection pool with 10 connections.

```cs
ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("192.168.1.38", 8902, "admin", "123456", 10, false, true);

//run the script
IEntity ret = pool.run("1 + 1");
Console.Out.WriteLine(ret.getString());

//run the specified function
ret = pool.run("abs", new List<IEntity> { new BasicInt(-3) });
Console.Out.WriteLine(ret.getString());
```

Create a task.

```cs
BasicDBTask task = new BasicDBTask("1..10");
pool.execute(task);
```

Check whether the task is executed successfully. If successful, returns the results; otherwise returns the error messages.

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

Output:

```
[1,2,3,4,5,6,7,8,9,10]
```

Create multiple tasks and call these tasks concurrently in ExclusiveDBConnectionPool.

```cs
List<IDBTask> tasks = new List<IDBTask>();
for (int i = 0; i < 10; ++i){
      //call function log
      tasks.Add(new BasicDBTask("log", new List<IEntity> { data.get(i) }));
}
pool.execute(tasks);
```

Check whether the task is executed successfully. If successful, returns the results; otherwise returns the error messages.

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

Output:

```cs
[0,0.693147,1.098612,1.386294,1.609438,1.791759,1.94591,2.079442,2.197225,2.302585]
```

### 3. Run DolphinDB Scripts

To run DolphinDB script in C#:

```cs
conn.run("script");
conn.runAsync("script")
```

`run` indicates synchronous execution of the script. `runAsync` indicates asynchronous execution.

If the script contains only one statement, such as an expression, DolphinDB returns the result of the statement. If the script contains more than one statements, the result of the last statement is returned. If the script contains an error or there is a network problem, an exception is thrown.

### 4. Call DolphinDB Functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. 

The following example passes a double vector to the server and calls function `sum`.

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
### 5. Upload Data to DolphinDB Server

You can upload a data object to DolphinDB server and assign it to a variable for future use. You can specify the variable names with 3 types of characters: letters, numbers and underscores. The first character must be a letter.

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

### 6. Read Data

This section introduces how to read data of different data forms in DolphinDB with the `DBConnection` object.

Import the DolphinDB data type package:

```cs
using dolphindb.data;
```

- Vector

The following DolphinDB statement returns the C# object `BasicStringVector`. 

```cs
rand(`IBM`MSFT`GOOG`BIDU,10)
```

The `rows` method returns the size of the vector. You can access vector elements by index with the `getString` method.

```cs
public void testStringVector(){
      IVector v = (BasicStringVector)conn.run("take(`IBM`MSFT`GOOG`BIDU, 10)");
      Console.WriteLine(v.isVector());
      Console.WriteLine(v.rows());
      Console.WriteLine(((BasicString)v.get(1)).getValue());
}
```

Similarly, you can work with vectors or tuples of DOUBLE type.

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
- Set

```cs
public void testSet(){
      BasicSet s = (BasicSet)conn.run("set(1 3 5)");
      Console.WriteLine(s.rows());
      Console.WriteLine(s.columns());
}
```

- Matrix

To retrieve an element from a matrix, use `get`. To get the number of rows and columns, use the functions `rows` and `columns`, respectively.

```cs
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

- Table

To get a column of a table, use `getColumn`; To get a column name, use `getColumnName`; To get the number of columns and rows of a table, use `columns` and `rows`, respectively.

```cs
public void testTable(){
	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Console.WriteLine(dt.Rows.Count);
}
```
- NULL object

To determine if an object is NULL, use `getDataType`.

```cs
public void testVoid(){
      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}
```

### 7. Read From and Write to DolphinDB Tables

This section introduces how to write data from other databases or third-party Web APIs to DolphinDB table using the C# API.

There are 2 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- DFS table: data are distributed across disks of multiple nodes.

#### 7.1 Write to an In-Memory Table

DolphinDB offers several ways to write to an in-memory table:
- Insert a single row of data with `insert into`
- Insert multiple rows of data in bulk with function `tableInsert`
- Insert a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns the schema of a table and unnecessarily increases the network traffic.

The table in the following examples has 4 columns. Their data types are STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble, respectively.

```cs
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```

By default, an in-memory table is not shared among sessions. To access it in a different session, share it among sessions with `share`.

##### 7.1.1 Insert a Single Record with `insert into` <!-- omit in toc -->

```cs
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

##### 7.1.2 Insert Multiple Records in Bulk with `tableInsert` <!-- omit in toc -->

Function `tableInsert` can save records in batches. If data in C# can be organized as a List, it can be saved with function `tableInsert`.

```cs
public void test_save_TableInsert(string[] strArray, int[] intArray, long[] tsArray, double[] dblArray)
{
      //Constructing parameters with arrays
      List<IEntity> args = new List<IEntity>() { new BasicStringVector(strArray), new BasicIntVector(intArray), new BasicTimestampVector(tsArray), new BasicDoubleVector(dblArray) };
      conn.run("tableInsert{sharedTable}", args);
}
```

The example above uses partial application in DolphinDB to embed a table in `tableInsert{sharedTable}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/help200/Functionalprogramming/PartialApplication.html).

##### 7.1.3 Save BasicTable Objects With Function `tableInsert` <!-- omit in toc -->

Function `tableInsert` can also accept a `BasicTable` object in C# as a parameter to append data to a table in batches.

```cs
public void test_save_table(BasicTable table1)
{
      List<IEntity> args = new  List<IEntity>(){ table1};
      conn.run("tableInsert{shareTable}", args);
}
```
#### 7.2 Write to a DFS Table

DFS table is recommended in production environment. It supports snapshot isolation and ensures data consistency. With data replication, DFS tables offer fault tolerance and load balancing.

##### 7.2.1 Save BasicTable Objects With Function `tableInsert` <!-- omit in toc -->

Use the following script in DolphinDB to create a DFS table.

```cs
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides function `loadTable` to load DFS tables, and function `tableInsert` to append data.

```cs
public void test_save_table(string dbPath, string tableName, BasicTable table1)
{
    List<IEntity> args = new List<IEntity>() { table1 };
    conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", dbPath,tableName), args);
}
```

In C#, you can easily create a BasicTable object using arrays or lists, which can then be appended to DFS tables. For example, the following 5 list objects(List<T>) boolArray, intArray, dblArray, dateArray and strArray are used to construct a BasicTable object:

```cs
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```
##### 7.2.2 Append to DFS Tables <!-- omit in toc -->

DolphinDB DFS tables support concurrent reads and writes. This section introduces how to write data concurrently to DolphinDB DFS tables in C#.

> Note that multiple writers are not allowed to write to one partition at the same time in DolphinDB. Therefore, make sure that each thread writes to a different partition separately when the client uses multiple writer threads. The user needs to first specify a connection pool, and the system obtains information about partitions before assigning the partitions to the connection pool for concurrent writes. A partition can only be written by one thread at a time.

DolphinDB C# API offers a convenient way to separate data by partition and write concurrently:

```cs
public PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, IDBConnectionPool pool)
```

**Parameters**:

* dbUrl: DFS database path
* tableName: DFS table name
* partitionColName: partitioning column
* appendFunction: (optional) a user-defined function. `tableInsert` is called by default.
* pool: connection pool for concurrent writes


The following script first creates a DFS database "dfs://DolphinDBUUID" and a partitioned table "device_status". The database uses a COMPO domain of VALUE-HASH-HASH.

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

With DolphinDB server version 1.30 or higher, you can write to DFS tables with the `PartitionedTableAppender` object in C# API. For example:

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
#### 7.3 Load and Query Tables

In the C# API, a table is saved as a BasicTable object. Since BasicTable is column-based, to retrieve rows, you need to get the necessary columns first and then get the rows.

In the example below, the BasicTable has 4 columns with data types STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble.

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

#### 7.4 Append Data Asynchronously

DolphinDB C# API provides `MultithreadedTableWriter` that supports concurrent writes in multiple threads. You can use methods of `MultithreadedTableWriter` class to asynchronously append data to a DolphinDB in-memory table, dimension table, or a DFS table.

The methods of `MultithreadedTableWriter` object are introduced as follows:

```cs
MultithreadedTableWriter(string hostName, int port, string userId, string password,string dbName, string tableName, bool useSSL, bool enableHighAvailability = false, string[] pHighAvailabilitySites = null,int batchSize = 1, float throttle = 0.01f, int threadCount = 5, string partitionCol = "", int[] pCompressMethods = null, Mode mode = Mode.M_Append, string[] pModeOption = null, Callback callbackHandler = null);
```

**Parameters**:

* **hostName**: a string indicating host name
* **port**: an integer indicating port number 
* **userId** / **password**: a string indicating username and password
* **dbPath**: a string indicating the DFS database path. Leave it unspecified for an in-memory table.
* **tableName**: a string indicating the DFS database path. Leave it unspecified for an in-memory table. Note: For API 1.30.17 or lower versions, when writing to an in-memory table, please specify the in-memory table name for *dbPath* and leave *tableName* empty.
* **useSSL**: a Boolean value indicating whether to enable SSL. The default value is false.
* **enableHighAvailability**: a Boolean value indicating whether to enable high availability. The default value is false.
* **pHighAvailabilitySites**: a list of ip:port of all available nodes
* **batchSize**: an integer indicating the number of messages in batch processing. The default value is 1, indicating the server processes the data as soon as they are written. If it is greater than 1, only when the number of data reaches batchSize, the client will send the data to the server.
* **throttle**: a positive floating-point number indicating the waiting time (in seconds) before the server processes the incoming data if the number of data written from the client does not reach *batchSize*.
* **threadCount**: an integer indicating the number of working threads to be created. The default value is 1, indicating single-threaded process. It must be 1 for a dimension table.
* **partitionCol**: a string indicating the partitioning column. It is None by default, and only takes effect when *threadCount* is greater than 1. For a partitioned table, it must be the partitioning column; for a stream table, it must be a column name; for a dimension table, the parameter does not take effect.
* **pCompressMethods**: an array of the compression methods used for each column. If unspecified, the columns are not compressed. The compression methods (case-insensitive) include:
      * "Vector_Fields.COMPRESS_LZ4": LZ4 algorithm
      * "Vector_Fields.COMPRESS_DELTA": Delta-of-delta encoding
* **mode**: indicates how data is written. It can be specified as Mode.M_Append or Mode.M_Upsert.
   * Mode.M_Append: the data is appended by [tableInsert](https://www.dolphindb.com/help200/FunctionsandCommands/FunctionReferences/t/tableInsert.html).
   * Mode.M_Upsert: the data is updated by [upsert!](https://www.dolphindb.com/help200/FunctionsandCommands/FunctionReferences/u/upsert%21.html).
* **pModeOption**:  Currently, it takes effect only when mode is specified as M_Upsert. It is a STRING array consisting of the optional parameters of `upsert!`, indicating the expansion option of mode. For example, it can be specified as ```new String[] { "ignoreNull=false", "keyColNames=`volume" }```. 
* **callbackHandler**: the Callback class. The default value is None, indicating that no callback is used. With callback enabled, the Callback class is inherited and method writeCompletion is reloaded. If parameter *callbackHandler* is specified, note that:
- The first parameter of `insert` must be of type STRING, indicating the id of the row.
- The `getUnwrittenData` method will not be available.

The following part introduces methods of `MultithreadedTableWriter` object.

(1) insert

```cs
ErrorCodeInfo insert(params Object[] args)
```

**Details**:

Insert a single record. Return a class `ErrorCodeInfo` containing *errorCode* and *errorInfo*. If *errorCode* is not "", `MultithreadedTableWriter` has failed to insert the data, and *errorInfo* displays the error message.

The class `ErrorCodeInfo` provides methods `hasError()` and `succeed()` to check whether the data is written properly. `hasError()` returns true if an error occurred, otherwise false. `succeed()` returns true if the data is written successfully, otherwise false.

**Parameters**:

* **args**: a variable-length argument (varargs) indicating the record to be inserted.

(2) getUnwrittenData

```cs
List<List<IEntity>> getUnwrittenData();
```

**Details**:

Return a nested list of data that has not been written to the server.

**Note**: Data obtained by this method will be released by `MultithreadedTableWriter`.

(3) insertUnwrittenData

```cs
ErrorCodeInfo insertUnwrittenData(List<List<IEntity>> data);
```

**Details**:

Insert unwritten data. The result is in the same format as `insert`. The difference is that `insertUnwrittenData` can insert multiple records at a time.

**Parameters**:

* **data**: the data that has not been written to the server. You can obtain the object with method getUnwrittenData.

(4) getStatus

```cs
Status getStatus()
```

**Details**:

Get the current status of the `MultithreadedTableWriter` object.

**Parameters**:

* **status**: the MultithreadedTableWriter.Status class with the following attributes and methods:

Attributes:

* isExiting: whether the threads are exiting
* errorCode: error code
* errorInfo: error message
* sentRows: number of sent rows
* unsentRows: number of rows to be sent
* sendFailedRows: number of rows failed to be sent
* threadStatus: a list of the thread status
  - threadId: thread ID
  - sentRows: number of rows sent by the thread
  - unsentRows: number of rows to be sent by the thread
  - sendFailedRows: number of rows failed to be sent by the thread

(5) waitForThreadCompletion

```cs
waitForThreadCompletion()
```

**Details**:

After calling the method, `MultithreadedTableWriter` will wait until all working threads complete their tasks.

The methods of `MultithreadedTableWriter` are usually used in the following way:

```cs
//build a connection and initialize the environment
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
      //insert 100 rows of records with correct data types and column count
      for (int i = 0; i < 100; ++i)
      {
            ret = writer.insert(new DateTime(2022, 3, 23), "AAAAAAAB", i);
            //this line will not be executed
            if (ret.errorCode != "")
            Console.WriteLine(string.Format("insert wrong format data: {0}\n", ret.ToString()));
      }
      Thread.Sleep(2000);

      //An error message is returned at once if insert one row with wrong data type.
      ret = writer.insert(new DateTime(2022, 3, 23), random.Next() % 10000, random.Next() % 10000);
      if (ret.errorCode != "")
            Console.WriteLine("insert wrong format data: {0}\n", ret.ToString());
      /*
      insert wrong format data: code = A1 info = Failed to insert data. Cannot convert int to DT_SYMBOL
      */

      //If a disconnection occurs, MTW will fail the next time it writes data to the server.
      //Write one row of data first to trigger an error.
      ret = writer.insert(new DateTime(2022, 3, 23), "AAAAAAAB", 1);

      Thread.Sleep(1000);

      //Insert 9 more rows of correct data types, MTW raises an exception because the work thread terminates and the row will not be written to MTW.
      //an exception will be thrown directly here
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
      //an error occurred when writing
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

      //To rewrite this data, a new MTW needs to be created because the original MTW is no longer available due to an exception.
      MultithreadedTableWriter newWriter = new MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://valuedb3", "pdatetest", false, false, null, 10000, 1, 5, "id", new int[] { Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_DELTA });
      try
      {
            //insert unwritten data
            if (newWriter.insertUnwrittenData(unwriterdata).errorCode != "")
            {
            //wait until MTW finishes writing
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
//check the status
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

Enable the Callback class for `MultithreadedTableWriter` <!-- omit in toc -->

With callback enabled, the Callback class is inherited and method `writeCompletion` is reloaded to obtain the callback data.

A table of type BasicTable will be returned using the callback method, which consists of two columns: 
- The first column (of String type) holds the id of each row added with `MultithreadedTableWriter.insert`. 
- The second column (of Boolean type) indicates whether each row was written successfully or not. True means it was written successfully; False means it failed.

Example:

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

Example:

```cs
MultithreadedTableWriter mtw = new MultithreadedTableWriter(host, port, userName, password, dbName, tbName, useSSL,
        enableHighAvailability, null, 10000, 1, 1, "price", null,MultithreadedTableWriter.Mode.M_Append,null, new CallbackHandler());
```

Use the `insert` method of `MultithreadedTableWriter` and write the id for each row in the first column.

```cs
String theme = "theme1";
for (int id = 0; id < 1000000; id++){
    mtw.insert(theme + id, code, price); //theme+id is the id of each row, which will be returned in the callback
}
```



#### 7.5 Update and Write to DolphinDB Tables

DolphinDB C# API provides `AutoFitTableUpsert` class to update and write to DolphinDB tables. `AutoFitTableUpsert` has the same functions as `MultithreadedTableWriter` when its parameter *mode* is specified as Mode.M_Upsert. The difference is that `AutoFitTableUpsert` is single-threaded and `MultithreadedTableWriter` writes with multiple threads.

The methods of `AutoFitTableUpsert` object are introduced as follows:

```cs
AutoFitTableUpsert(string dbUrl, string tableName, DBConnection connection, bool ignoreNull, string[] pkeyColNames, string[] psortColumns)
```

**Parameters**:

* dbUrl: a string indicating the path of the DFS database. It is specified as None for in-memory table.
* tableName: a STRING indicating the in-memory or DFS table name.
* connection: a DBConnection object to connect to the DolphinDB server and upsert data. Note that *asynchronousTask* must be false when creating the DBConnection object for `AutoFitTableUpsert`.
* ignoreNull: a Boolean value. It is used to specify the parameter ignoreNull of [upsert!](https://www.dolphindb.com/help200/FunctionsandCommands/FunctionReferences/u/upsert%21.html), indicating whether to update the NULL values of the target table.
* pkeyColNames: a STRING array. It is used to specify the parameter *keyColNames* of `upsert!`, i.e., specifing the key columns.
* psortColumnsa STRING array. It is used to specify the parameter *sortColumns* of `upsert!`. The updated partitions will be sorted on sortColumns (only within each partition, not across partitions).

The following introduces the `upsert` method of `AutoFitTableUpsert` object.

```cs
int upsert(BasicTable table)
```

**Details**:

Update a BasicTable object to the target table, and return an integer indicating the number of rows that have been updated.

**Example**:

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

### 8. Data Type Conversion

The C# API provides objects that correspond to DolphinDB data types. They are usually named as Basic+ <DataType>, such as BasicInt, BasicDate, etc.

The majority of DolphinDB data types can be constructed from corresponding C# data types. For examples, INT in DolphinDB from 'new BasicInt(4)', DOUBLE in DolphinDB from 'new BasicDouble(1.23)'. The following DolphinDB data types, however, need to be constructed in different ways:

- CHAR type: as the CHAR type in DolphinDB is stored as a byte, we can use the BasicByte type to construct CHAR in C# API, for example 'new BasicByte((byte)'c')'.
- SYMBOL type: the SYMBOL type in DolphinDB is stored as INT to improve the efficiency of storage and query of strings. C# doesn't have this data type, so C# API does not provide BasicSymbol. SYMBOL type can be processed directly with BasicString.
- Temporal types: temporal data types are stored as INT or LONG in DolphinDB. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime and nanotimestamp. For detailed description, please refer to [DolphinDB Temporal Type and Conversion](https://www.dolphindb.com/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html). Since C# also provides data types such as LocalDate, LocalTime, LocalDateTime and YearMonth, C# API provides conversion functions in the Utils class between all C# temporal types and INT or LONG.

The following script shows the correspondence between DolphinDB temporal types and C# primitive temporal types:

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

If a temporal variable is stored as timestamp in a third-party system, DolphinDB temporal object can also be instantiated with a timestamp. The Utils class in the C# API provides conversion algorithms for various temporal types and standard timestamps, such as converting millisecond timestamps to DolphinDB's BasicTimestamp objects:

```cs
DateTime dt = Utils.parseTimestamp(154349485400L);
BasicTimestamp ts = new BasicTimestamp(dt);
```

You can also convert a DolphinDB object to a timestamp of an integer or long integer, such as:

```cs
DateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```

The Utils class provides the following methods to handle a variety of timestamp precisions:
- Utils.countMonths: Calculate the monthly difference between a given time and 1970.01, returning INT
- Utils.countDays: Calculate the difference in the number of days between the given time and 1970.01.01, returning INT
- Utils.countMinutes: Calculate the minute difference between the given time and 1970.01.01T00:00, returning INT
- Utils.countSeconds: Calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning INT
- Utils.countMilliseconds: Calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, returning LONG

As the precision of DateTime and TimeSpan in C# is not up to nanosecond, to keep nanosecond precision when working with temporal data, we can use NanoTimestamp.getInternalValue() to get the LONG values corresponding to the nanosecond precision data.

### 9. C# Streaming API

A C# program can subscribe to streaming data via API. C# API can acquire streaming data in the following 3 ways: ThreadedClient, ThreadPooledClient, and PollingClient.

#### 9.1 Interfaces

The corresponding interfaces of `subscribe` are:

(1) Subscribe using ThreadedClient:

```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, float throttle = 0.01f, StreamDeserializer deserializer = null, string user = "", string password = "")
```

**Parameters**:

- **host**: the IP address of the publisher node.
- **port**: the port number of the publisher node.
- **tableName**: a string indicating the name of the publishing stream table.
- **actionName**: a string indicating the name of the subscription task.
- **handler**: a user-defined function to process the subscribed data.
- **offset**: an integer indicating the position of the first message where the subscription begins. A message is a row of the stream table. If *offset* is unspecified, negative or exceeding the number of rows in the stream table, the subscription starts with the next new message. *offset* is relative to the first row of the stream table when it is created. If some rows were cleared from memory due to cache size limit, they are still considered in determining where the subscription starts.
- **reconnect**: a Boolean value indicating whether to resubscribe after network disconnection.
- **filter**: a vector indicating the filtering conditions. Only the rows with values of the filtering column in the vector specified by the parameter *filter* are published to the subscriber.
- **batchSize**: an integer indicating the number of unprocessed messages to trigger the *handler*. If it is positive, the *handler* does not process messages until the number of unprocessed messages reaches *batchSize*. If it is unspecified or non-positive, the *handler* processes incoming messages as soon as they come in.
- **throttle**: a floating-point number indicating the maximum waiting time (in seconds) before the *handler* processes the incoming messages. The default value is 1. This optional parameter has no effect if *batchSize* is not specified.
- **deserializer**: the deserializer for the subscribed heterogeneous stream table.
- **user**: a string indicating the username used to connect to the server.
- **password**: a string indicating the password used to connect to the server.

(2) Subscribe using ThreadPooledClient:

```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```

(3) Subscribe using PollingClient:

```cs
subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```
#### 9.2 Code Examples

The following examples introduce how to subscribe to stream table:

* The application on the client periodically checks if new data has been added to the streaming table. If yes, the application will acquire and consume the new data.

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
* The API uses MessageHandler to get new data

First you need to define the message handler, which needs to implement dolphindb.streaming.MessageHandler interface.

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

You can pass the handler instance into function `subscribe` as a parameter with single-thread or multi-thread callbacks.

(1) ThreadedClient

```cs
ThreadedClient client = new ThreadedClient(subscribeHost, subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler());
Thread.Sleep(10000);
//To cancel the subscription, you can use function close.
client.close();
```

(2) ThreadPooledClient: Handler mode client (multithreading)

```cs
ThreadPooledClient client = new ThreadPooledClient(subscribeHost, subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler());
//To cancel the subscription, you can use function close.
Thread.Sleep(10000);
client.close();
```

#### 9.3 Reconnect

Parameter *reconnect* is a Boolean value indicating whether to automatically resubscribe after the subscription experiences an unexpected interruption. The default value is false.

When reconnect=true:

- If the publisher and the subscriber both stay on but the network connection is interrupted, then after network is restored, the subscriber resumes subscription from where the network interruption occurs.
- If the publisher crashes, the subscriber will keep attempting to resume subscription after the publisher restarts.
    - If persistence was enabled on the publisher, the publisher starts to read the persisted data on disk after restarting. Automatic resubscription would fail until the publisher has read the data for the time when the publisher crashed.
    - If persistence was not enabled on the publisher, the automatic subscription will fail.
- If the subscriber crashes, the subscriber won't automatically resume the subscription after it restarts. In this case, we need to execute function `subscribe` again.

Parameter *reconnect* is set to be true for the following example：

```cs
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

#### 9.4 Filter

Parameter *filter* is a vector. It is used together with function `setStreamTableFilterColumn` at the publisher node. Function `setStreamTableFilterColumn` specifies the filtering column in the streaming table. Only the rows with filtering column values in *filter* are published.

In the following example, parameter *filter* is assigned an INT vector [1,2]:

```cs
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```

#### 9.5 Subscribe to a Heterogeneous Table

Since DolphinDB server version 1.30.17/2.00.5, the [replay](https://www.dolphindb.com/help200/FunctionsandCommands/FunctionReferences/r/replay.html) function supports replaying (serializing) multiple stream tables with different schemata into a single stream table (known as "heterogeneous stream table"). Starting from DolphinDB C# API version 1.30.19, a new class `streamDeserializer` has been introduced for the subscription and deserialization of heterogeneous stream table.

##### 9.5.1 Construct Deserializer for Heterogeneous Stream Table <!-- omit in toc -->

You can construct a deserializer for heterogeneous table with `streamDeserializer`.

(1) With specified table schema:

* specified schema

```cs
StreamDeserializer(Dictionary<string, BasicDictionary> filters)
```
* specified column types

```cs
StreamDeserializer(Dictionary<string, List<DATA_TYPE>> filters)
```
(2) With specified table:

```cs
StreamDeserializer(Dictionary<string, Tuple<string, string>> tableNames, DBConnection conn = null)
```

Code example:

```cs
//Supposing the inputTables to be replayed is:
//d = dict(['msg1', 'msg2'], [table1, table2]); \
//replay(inputTables = d, outputTables = `outTables, dateColumn = `timestampv, timeColumn = `timestampv)";
//create a deserializer for heterogeneous table

{//specify schema
      BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
      BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
      Dictionary<string, BasicDictionary> schemas = new Dictionary<string, BasicDictionary>();
      schemas["msg1"] = outSharedTables1Schema;
      schemas["msg2"] = outSharedTables2Schema;
      StreamDeserializer streamFilter = new StreamDeserializer(schemas);
}
{//or specify column types
	Dictionary<string, List<DATA_TYPE>> colTypes = new Dictionary<string, List<DATA_TYPE>>();
	List<DATA_TYPE> table1ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_DOUBLE };
	colTypes["msg1"] = table1ColTypes;
	List<DATA_TYPE> table2ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE };
	colTypes["msg2"] = table2ColTypes;
	StreamDeserializer streamFilter = new StreamDeserializer(colTypes);
}
{//specify tables
      Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
      tables["msg1"] = new Tuple<string, string>("", "table1");
      tables["msg2"] = new Tuple<string, string>("", "table2");
      //conn is an optional parameter
      StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
}
```

##### 9.5.2 Subscribe to a Heterogeneous Table <!-- omit in toc -->

(1) subscribe to a heterogeneous table using ThreadedClient:

* specify the parameter *deserializer* of function `subscribe` to deserialize the table when data is ingested:

```cs
ThreadedClient client = new ThreadedClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, -1, (float)0.01, streamFilter);
```

* add the streamFilter to user-defined Handler:

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

(2) subscribe to a heterogeneous table using ThreadPooledClient is similar as above:

* specify the parameter *deserializer* of function `subscribe`

```cs
ThreadPooledClient client = new ThreadPooledClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter);
```
* add the streamFilter to user-defined Handler:

```cs
Handler6 handler = new Handler6(streamFilter);
ThreadPooledClient client = new ThreadPooledClient(listenport);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true);
```

(3) As PollingClient does not support callbacks, you can only pass the *deserializer* parameter to the function `subscribe`:

```cs
PollingClient client = new PollingClient(listenport);
TopicPoller poller = client.subscribe(hostName, port, tableName, actionName, 0, true, null, streamFilter);
```
#### 9.6 Unsubscribe

Each subscription is identified with a subscription topic. Subscription fails if a topic with the same name already exists. You can cancel the subscription with `unsubscribe`.

```cs
client.unsubscribe(serverIP, serverPort, tableName,actionName);
```

### 1. C# API Concepts


The C# API essentially implements the messaging and data conversion between the .Net program and the DolphinDB server. C# API runs in .Net Framework 4.0 and above.


### 2. Mapping between C# objects and DolphinDB objects

The C# API follows the principles of interface-oriented programming. The C# API uses the interface class `IEntity` to represent all the data types returned by DolphinDB. Based on the IEntity interface class, according to the data type of DolphinDB, the C# API provides seven extension interfaces, namely scalar, vector, matrix, set, dictionary, table and chart. These interface classes are included in the com.xxdb.data package.


Extended Interface Classes | Naming Rules | Examples
---|---|---
scalar|`Basic<DataType>`|BasicInt, BasicDouble, BasicDate, etc.
vector，matrix|`Basic<DataType><DataForm>`|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set， dictionary和table|`Basic<DataForm>`|BasicSet, BasicDictionary, BasicTable.
chart| |BasicChart|


"Basic" indicates the basic data type interface, `<DataType>` indicates the DolphinDB data type name, and `<DataForm>` is a DolphinDB data form name.

### 3. C# API Key Functions

The core object provided by the DolphinDB C# API is DBConnection. Its main function is to allow C# applications to call DolphinDB scripts and functions to exchange data between C# applications and DolphinDB servers.
DBConnection provides the following methods：

| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to the DolphinDB server|
|login(username,password,enableEncryption)|Log in to the server|
|run(script)|Run the script on the DolphinDB server|
|run(functionName,args)|Call the function on the DolphinDB server|
|upload(variableObjectMap)|Upload local data objects to the DolphinDB server|
|isBusy()|Determine if the current session is busy|
|close()|Close current session|

### 4. Establish a DolphinDB connection


The C# API connects to the DolphinDB server via the TCP/IP protocol. In the following example, we connect the running local DolphinDB server with port number 8848:

```
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect("localhost",8848));
}

```

Establish a connection with a username and password

```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

### 5.Run a DolphinDB script

The syntax for running the DolphinDB script in C# is as follows:

```
conn.run("<SCRIPT>");
```

Note that the maximum allowed length of the script is 65,535 bytes.


If the script contains only one statement, such as an expression, DolphinDB returns a data object; otherwise it returns a NULL object. If the script contains more than one statement, the last object will be returned. If the script contains an error or a network problem, it throws an IOException.

### 6.Manipulating data from the DolphinDB server


The following describes the establishment of the DolphinDB connection, in the C# environment, the operation of different DolphinDB data types, the results are displayed in the Console window.



First import the DolphinDB data type package:

```
using dolphindb.data;
```


Note that the code below can be run only if the connection is established.

- Vector


In the following example, the DolphinDB statement returns the C# object BasicStringVector. The vector.rows() method gets the size of the vector. We can access vector elements by index using the vector.getString(i) method.


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


Similarly, you can also handle vectors or tuples of double or float data types.

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


To retrieve an element from an integer matrix, we can use get(row,col). To get the number of rows and columns, we can use the functions rows() and columns().


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


All keys and values ​​can be retrieved from the dictionary using the functions keys() and values(). To get its value from a key, you can call get(key).

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

To get the column of the table, we can call table.getColumn(index); again, we can call table.getColumnName(index) to get the column name. For the number of columns and rows, we can call table.columns() and table.rows() respectively.

```
public void testTable(){
	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Console.WriteLine(dt.Rows.Count);
}

```
- NULL object

To know the data type of a NULL object, we can call the function obj.getDataType().

```
public void testVoid(){
      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}

```

### 7.Call DolphinDB function


The function called can be a built-in function or a user-defined function. The following example passes a double vector to the server and calls the sum function.

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

### 8.Upload the object to the DolphinDB server

We can upload the binary data object to the DolphinDB server and assign it to a variable for future use. Variable names can use three types of characters: letters, numbers, or underscores. The first character must be a letter.

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

### 9. How to save C# data table objects to DolphinDB database

An important scenario for using the C# API is that users fetch data from other database systems or third-party WebAPIs, clean the data and store it in the DolphinDB database.
This section describes how to upload and save the data to a DolphinDB data table through the C# API.


The DolphinDB data table is divided into three types according to storage methods:

- In-memory table: The data is only stored in the memory of this node, and the access speed is the fastest, but the data will lose if the node is shut down.
- Local disk table: The data is saved on the local disk. Even if the node is closed, it can be easily loaded from the disk into the memory.
- Distributed table: Data is physically distributed across different nodes. Through DolphinDB's distributed computing engine, query a distributed table is simply like querying a local disk table.


#### 9.1. Save data to DolphinDB in-memory table


DolphinDB offers several ways to save data:

- save a single piece of data by insert into ;
- Save multiple pieces of data in bulk via the tableInsert function;
- Save the table object with the append! function.


The difference between these methods is that the types of parameters received are different. In a specific business scenario, a single data point may be obtained from the data source, or may be a data set composed of multiple arrays or tables.

The following describes three examples of saving data. The data table used in the example has four columns, namely string, int, timestamp, double, and the column names are cstring,cint,ctimestamp,cdouble. The script is as follows:

```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```

Since an in-memory table is session-isolated, the current GUI session user can only see in-memory tables created in the current GUI session. If other terminals need to access the in-memory tables, they need to be shared among the sessions through the share keyword.

##### 9.1.1. Save a single data point using SQL


If the C# program is to save a single data record to DolphinDB each time, you can save the data through the SQL statement (insert into).

```
public void test_save_Insert(String str, int i, long ts, double dbl)
{
      conn.run(String.Format("insert into sharedTable values('{0}',{1},{2},{3})",str,i,ts,dbl));
}
```

##### 9.1.2. Use the tableInsert function to save data in batches


If the data obtained by the C# program can be organized into a List mode, it is more suitable to use the tableInsert function. This function can accept multiple arrays as parameters and append the array to the data table

```
public void test_save_TableInsert(string[] strArray, int[] intArray, long[] tsArray, double[] dblArray)
{
      // Constructing parameters with arrays
      List<IEntity> args = new List<IEntity>() { new BasicStringVector(strArray), new BasicIntVector(intArray), new BasicTimestampVector(tsArray), new BasicDoubleVector(dblArray) };
      conn.run("tableInsert{sharedTable}", args);
}
```

In the actual application scenario, usually the C# program writes data to a table already existing on the server side. On the server side, a script such as `tableInsert(sharedTable, vec1, vec2, vec3...)` can be used. But in C#, when called with `conn.run("tableInsert", args)`, the first parameter of tableInsert is the object reference of the server table. It cannot be obtained in the C# program, so the conventional practice is to define a function in server to embed the sharedTable, such as

```
def saveData(v1,v2,v3,v4){tableInsert(sharedTable,v1,v2,v3,v4)}
```



Then, run the function through `conn.run("saveData", args)`. Although this achieves the goal, for the C# program, one more server cal consumes more network resources.
In this example, using the partial application feature in DolphinDB, the server table name is embeded into tableInsert in the manner of `tableInsert{sharedTable}` and used as a stand-alone function. This way you don't need to use a custom function.
For specific documentation, please refer to [Partial Application Documentation](http://www.dolphindb.com/help/PartialApplication.html).

##### 9.1.3.  Use append! Function to save data in batches

The append! function accepts a table object as a parameter and appends the data to the data table.

```
public void test_save_table(BasicTable table1)
{
      List<IEntity> args = new  List<IEntity>(){ table1};
      conn.run("append!{shareTable}", args);
}
```

#### 9.2. Save data to a distributed table

Distributed table is the data storage method recommended by DolphinDB in production environment. It supports snapshot level transaction isolation and ensures data consistency. Distributed table supports multiple copy mechanism, which provides data fault tolerance and data access. Load balancing.

The data tables involved in this example can be built with the following script:

*Please note that distributed tables can only be used in cluster environments with `enableDFS=1` enabled.*

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides the loadTable method to load distributed tables and append data via append!. The specific script examples are as follows:

```
public void test_save_table(String dbPath, BasicTable table1)
{
    List<IEntity> args = new List<IEntity>() { table1 };
    conn.run(String.Format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```


When the value retrieved by the user in the C# program is an array or a list, it is also convenient to construct a BasicTable for appending data. For example, there are now boolArray, intArray, dblArray, dateArray, strArray 5 list objects (List< T>), you can construct a BasicTable object with the following statement:

```
List<String> colNames = new List<string>() { "cbool", "cint", "cdouble", "cdate", "cstring" };
List<IVector> cols = new List<IVector>() { new BasicBooleanVector(boolArray), new BasicIntVector(intArray), new BasicDoubleVector(dblArray), new BasicDateVector(dateArray), new BasicStringVector(strArray) };
BasicTable table1 = new BasicTable(colNames, cols);
```

#### 9.3. Save data to local disk table

Local disk tables are commonly used for computational analysis of static data sets, either for data input or as a calculated output. It does not support transactions, and does not support concurrent reading and writing.

```
// Create a local-disk table using the DolphinDB script
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides the loadTable method to load local disk tables as well, and function append! to append data.

```
public void test_save_table(String dbPath, BasicTable table1)
{
      List<IEntity> args = new List<IEntity>() { table1 };
      conn.run(String.Format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```

### 10. Loop BasicTable


In the C# API, the table data is saved as a BasicTable object. Since the BasicTable is a columnar store, all the desultory needs to be read and used by retrieving the columns and retrieving the rows.

In the example, the parameter BasicTable has 4 columns, which are STRING, INT, TIMESTAMP, DOUBLE, and the column names are cstring, cint, ctimestamp, cdouble.

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

### 11. Data type conversion between DolphinDB and C#

The C# API provides objects that correspond to the internal data types of DolphinDB, usually named after `Basic+ <DataType>`, such as BasicInt, BasicDate, and so on.
Some basic C# types, you can directly create the corresponding DolphinDB data structure through the constructor, such as `new BasicInt(4)`, `new BasicDouble(1.23)`, but there are some types that need to be converted. The following list needs to be simple. Type of conversion:
- `CHAR` type: The `CHAR` type in DolphinDB is stored as a Byte, so use the `BasicByte` type to construct `CHAR` in the C# API, for example `new BasicByte((byte)'c')`
- `SYMBOL` type: The `SYMBOL` type in DolphinDB is an optimization of strings, which can improve the efficiency of DolphinDB for string data storage and query, but this type is not needed in C#, so C# API does not provide `BasicSymbol `This kind of object can be processed directly with `BasicString`.
- Temporal type: The Temporal data type is internal stored as int or long type. DolphinDB provides 9 temporal data types: `date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp`, the highest precision can be Nanoseconds. For a detailed description, refer to [DolphinDB Temporal Type and Conversion](http://www.dolphindb.com/help/TemporalTypeAndConversion.html). Since C# also provides data types such as `LocalDate, LocalTime, LocalDateTime, YearMonth`, the C# API provides all C# temporal types and conversion functions between int or long in the Utils class.



The following script shows the correspondence between the DolphinDB temporal type in the C# API and the C# native time type:

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
If the time is stored in a timestamp in a third-party system, the DolphinDB time object can also be instantiated with a timestamp. The Utils class in the C# API provides conversion algorithms for various time types and standard timestamps, such as converting millisecond timestamps to DolphinDB's BasicTimestamp objects:

```
DateTime dt = Utils.parseTimestamp(154349485400L);
BasicTimestamp ts = new BasicTimestamp(dt);
```

You can also convert a DolphinDB object to a timestamp of an integer or long integer, such as:

```
DateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```

If the timestamp is saved with other precision, the Utils class also provides the following methods to accommodate a variety of different precisions:

- Utils.countMonths: Calculate the monthly difference between a given time and 1970.01, returning an int
- Utils.countDays: Calculate the difference in the number of days between the given time and 1970.01.01, return int
- Utils.countMinutes: Calculate the minute difference between the given time and 1970.01.01T00:00, return int
- Utils.countSeconds: Calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning int
- Utils.countMilliseconds: Calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, return long
- Utils.countNanoseconds: Calculate the difference in nanoseconds between a given time and 1970.01.01T00:00:00.000, return long


Need to pay attention, due to the C# Date. Time and TimeSpan are less than nanosecond in precision, so if you want to manipulate nanosecond precision time data, you can get the internally saved long value by `NanoTimestamp.getInternalValue()`, not through DateTime and TimeSpan, otherwise will cause loss of precision.

### 12. C# streaming API


Through C# streaming API, users can subscribe streaming data from the DolphinDB server. When streaming data enters the client, there are two ways to process data in the C# API:


* The application on the client periodically checks to see if new data has been added. If new data is added, the application gets the data and uses them at work.

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
* The C# API uses new data directly using a pre-configured MessageHandler.


First, the caller needs to define the handler first, and the dolphindb.streaming.MessageHandler interface needs to be implemented.

```
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue<BasicInt>(2);
               //..Processing data
       }
}
```

When the subscription is started, the handler instance is passed as a parameter to the subscription function.

```
ThreadedClient client = new ThreadedClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```


**Handler mode client (multithreading) (ThreadPollingClient)**

```
ThreadPooledClient client = new ThreadPooledClient(subscribePort);

client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

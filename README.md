# csharp-api

#### 1. Requirement

DolphinDB C# API requires .Net Framework 4 and above.

#### 2. Use Java API

To use DolphinDB C# API, please add dolphindb.dll to your assembly reference.

#### 3. Mapping between C# Objects and DolphinDB Objects

C# API adopts interface-oriented programming. C# API uses the class interface "IEntity" to represent all data types returned from DolphinDB. C# API provides 6 types of extended interfaces: scalar, vector, matrix, set, dictionary and table based on the "IEntity" interface and DolphinDB data forms.

| Extended interfaces| Naming rules| Examples|
| :------ |:------| :-----|
| scalar      |Basic+<DataType> | BasicInt, BasicDouble, BasicDate, etc.|
| vector and matrix |Basic+<DataForm> | BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.|
| set, dictionary and table |Basic+<DataForm>  |BasicSet, BasicDictionary, BasicTable. |
       
**Basic** indicates the basic implementation of a data form interface, **DataType** indicates a DolphinDB data type, and **DataForm** indicates a DolphinDB data form.

#### 4. Setup DolphinDB connection

C# API connects to DolphinDB server through TCP/IP protocol. To establish a connection, specify the host and port of the server as illustrated by the example below.

```
using dolphindb;
using dolphindb.data;
using dolphindb.io;

public void Test_Connect(){

      private readonly string SERVER="localhost";
      private readonly int PORT=8848;
      DBConnection conn=new DBConnection();
      Assert.AreEqual(true,conn.connect(SERVER,PORT));
}

```


#### 5. Run Scripts


Use the following statement to run DolphinDB script. The maximum length of a script is 65,535 bytes.


```
conn.run("<SCRIPT>");
```

If the script contains a statement, it will return a data object. If the script contains multiple statements, it will return the last object that they generate. If the script contains errors or if network issues occur, it will throw an IOException.

#### 5.1 Vector

In the example below, the DolphinDB script **"take(`IBM`MSFT`GOOG`BIDU, 10)"**  returns the object BasicStringVector. The method vector.rows() indicates the size of the vector. To access an element in a vector, use the method vector.getString(index).

```
public void testStringVector(){

      IVector v = (BasicStringVector)conn.run("take(`IBM`MSFT`GOOG`BIDU, 10)");
	Assert.IsTrue(v.isVector());
	Assert.AreEqual(10, v.rows());
	Assert.AreEqual("MSFT", ((BasicString)v.get(1)).getValue());
}
```

Similarly, you can work with a double vector or a tuple.


```
public void testDoubleVector(){

	IVector v = (BasicDoubleVector)conn.run("1.123 2.2234 3.4567");
	Assert.IsTrue(v.isVector());
	Assert.AreEqual(3, v.rows());
	Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
}


public void testAnyVector(){

	BasicAnyVector v = (BasicAnyVector)conn.run("[1 2 3,3.4 3.5 3.6]");
	Assert.AreEqual(2, v.rows());
      Assert.AreEqual(1, v.columns());
	Assert.AreEqual(3.4, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
}
```


#### 5.2 Set


```
public void testSet(){

	BasicSet tb = (BasicSet)conn.run("set(8 9 9 5 6)");
	DataTable dt = tb.toDataTable();
	Assert.AreEqual(4, dt.Rows.Count);
}
```
       

#### 5.3 Matrix



To access an element from an integer matrix, use getInt(row, col). To get the number of rows or columns, use functions rows() or columns() respectively.


```
public void testIntMatrix(){

	BasicIntMatrix tb = (BasicIntMatrix)conn.run("1..9$3:3");
	DataTable dt = tb.toDataTable();
	Assert.AreEqual(3, dt.Rows.Count);
	Assert.AreEqual(3, dt.Columns.Count);
}
```

#### 5.4 Dictionary


To get all keys and values from a dictionary, use functions keys() and values() respectively. To look up a value in a dictionary, use the method get(key).

```
public void testDictionary(){

      BasicDictionary tb = (BasicDictionary)conn.run("dict(1 2 3 4,5 6 7 8)");
      DataTable dt = tb.toDataTable();
      Assert.AreEqual(4, dt.Rows.Count);
}
```
#### 5.5 Table

To get a table column, use method table.getColumn(index); to get column names, use method table.getColumnName(index); to get the number of columns or rows of a table, use table.columns() or table.rows(). To convert BasicTable to System.Data.DataTable, use toDataTable().

```
public void testTable(){

	BasicTable tb = (BasicTable)conn.run("table(1 as id,'a' as name)");
	DataTable dt = tb.toDataTable();
	Assert.AreEqual(1, dt.Rows.Count);
}


```
#### 5.6 Null Object

To get a "NULL" object, execute the following script and then call method obj.getDataType()

```
public void testVoid(){

      IEntity obj = conn.run("NULL");
      Assert.AreEqual(obj.getObject(), null);
}
```

#### 6. Login DolphinDB

```
 db.login("username", "password", false);
```

#### 7. Run DolphinDB Functions

We can call either a DolphinDB built-in function or a user defined function. The example below passes a double vector to the server and calls the sum function.


```
public void testFunction(){

      List<IEntity> args = new List<IEntity>(1);
      BasicDoubleVector vec = new BasicDoubleVector(3);
      vec.setDouble(0, 1.5);
      vec.setDouble(1, 2.5);
      vec.setDouble(2, 7);            
      args.Add(vec);
      BasicDouble result = (BasicDouble)conn.run("sum", args);
      Assert.AreEqual(11, result.getValue());
}
```


#### 8. Upload Objects to DolphinDB Server



We can upload a binary data object to a DolphinDB server and assign it to a variable for future use. The variable name can use 3 types of characters: letter, digit, and underscore. The first character must be a letter.


```
public void testUpload(){

      BasicTable tb = (BasicTable)conn.run("table(1..100 as id,take(`aaa,100) as name)");
      Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
      upObj.Add("table_uploaded", (IEntity)tb);
      db.upload(upObj);
      BasicIntVector v = (BasicIntVector)conn.run("table_uploaded.id");
      Assert.AreEqual(100, v.rows());
}
```

#### 9. write local basictable into dfs table

```
public void Test_WriteLocalTableToDfs()
{
    //=======================prepare data to writing into dfs database =======================
    List<string> colNames = new List<string>() { "sym", "dt", "prc", "cnt" };
    BasicStringVector symVec = new BasicStringVector(new List<string>() { "MS", "GOOG", "FB" });
    BasicDateTimeVector dtVec = new BasicDateTimeVector(new List<int?>() { Utils.countSeconds(DateTime.Now), Utils.countSeconds(DateTime.Now), Utils.countSeconds(DateTime.Now) });
    BasicDoubleVector prcVec = new BasicDoubleVector(new double[] { 101.5, 132.75, 37.96 });
    BasicIntVector cntVec = new BasicIntVector(new int[] { 50, 78, 88 });
    List<IVector> cols = new List<IVector>() { (IVector)symVec, (IVector)dtVec, (IVector)prcVec, (IVector)cntVec };
    BasicTable table1 = new BasicTable(colNames, cols);
    //======================================================================================
    DBConnection db = new DBConnection();
    db.connect(SERVER, PORT);
    db.login("admin", "123456", false);//login 
    //prepare dfs database and table  
    db.run("db = database('dfs://testDatabase',VALUE,'MS' 'GOOG' 'FB')");
    db.run("tb= table('MS' as sym,datetime(now()) as dt,1.01 as prc,1 as cnt)");
    db.run("db.createPartitionedTable(tb,'tb1','sym')");
   
    db.run("def saveQuotes(t){ loadTable('dfs://testDatabase','tb1').append!(t)}");
    List<IEntity> args = new List<IEntity>(1);
    args.Add(table1);
    db.run("saveQuotes", args);
}
```
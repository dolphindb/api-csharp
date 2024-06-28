using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
using dolphindb;
using dolphindb.route;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;

namespace dolphindb_csharp_api_test.compatibility_test.route_test
{
    [TestClass]
    public class PartitionedTableAppender_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;


        [TestMethod]
        public void createPartitionedTableAppender()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask conn = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(conn);


            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);

            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);

            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(3, res);
            pool.shutdown();

        }
        [TestMethod]//not support memory table
        public void Test_PartitionedTableAppender_memory_table()
        {
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, false, false);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = "share table(1000:0,`id`col0,[INT,STRING]) as pt1\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            String re = null;
            try
            {
                PartitionedTableAppender appender = new PartitionedTableAppender(null, "pt1", null, pool);
            }
            catch (Exception e) { 
                re = e.Message;
            }
            Assert.AreEqual("Can't find specified partition column name.", re);
            pool.shutdown();
            conn.close();
        }


        //[TestMethod]
        //public void Test_PartitionedTableAppender_CreateMethod()
        //{
        //    IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, false, false);
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    string sc1 = "if(existsDatabase(\"dfs://dataXdemo\")){dropDatabase(\"dfs://dataXdemo\")}\n";
        //    sc1 += "db = database(\"dfs://dataXdemo\", VALUE, 1..10)\n";
        //    sc1 += "t = table(take(1..10, 100) as id, take([`A, `B, `C], 100) as sym, 1..100 as qty, 100..1 as price)\n";
        //    sc1 += "pt = db.createPartitionedTable(t, `pt, `id).append!(t)\n";
        //    conn.run(sc1);
        //    string sc2 = "it = table(1000:0, `id`name, [INT, STRING]);share it as sit";
        //    conn.run(sc2);
        //    string sc3 = "n=1000\n" +
        //        "ID=rand(100, n)\n" +
        //        "dates=2017.08.07..2017.08.11\n" +
        //        "date=rand(dates, n)\n" +
        //        "x=rand(10.0, n)\n" +
        //        "t=table(ID, date, x)\n" +
        //        "\n" +
        //        "if(existsDatabase(\"dfs://compoDB\")){dropDatabase(\"dfs://compoDB\")}\n"+
        //        "dbDate = database(, partitionType=VALUE, partitionScheme=2017.08.07..2017.08.11)\n" +
        //        "dbID = database(, partitionType=RANGE, partitionScheme=0 50 100)\n" +
        //        "db = database(directory=\"dfs://compoDB\", partitionType=COMPO, partitionScheme=[dbDate, dbID])\n" +
        //        "pt = db.createPartitionedTable(t, `pt, `date`ID)\n" +
        //        "pt.append!(t)";
        //    conn.run(sc3);
        //    try
        //    {
        //        PartitionedTableAppender appender = new PartitionedTableAppender(null, "khgag", "id", "1+1", pool);
        //    }
        //    catch (Exception e)
        //    {
        //        Assert.IsNotNull(e);
        //        Console.WriteLine(e.Message);
        //    }
        //    try
        //    {
        //        PartitionedTableAppender appender = new PartitionedTableAppender("", "ajvja", "id", pool);
        //    }
        //    catch(Exception e)
        //    {
        //        Assert.IsNotNull(e);
        //        Console.WriteLine(e.Message);
        //    }

        //    try
        //    {
        //        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://dataXdemo", "pt", null, pool);
        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("Can't find specified partition column name.", e.Message);
        //        Console.WriteLine(e.Message);
        //    }

        //    try
        //    {
        //        PartitionedTableAppender appender = new PartitionedTableAppender("", "sit", "", pool);
        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("Can't find specified partition column name.", e.Message);
        //        Console.WriteLine(e.Message);
        //    }

        //    try
        //    {
        //        PartitionedTableAppender appender = new PartitionedTableAppender("dfs://compoDB", "pt", "jhbfsjg", pool);
        //    }
        //    catch(Exception e)
        //    {
        //        Assert.AreEqual("Can't find specified partition column name.", e.Message);
        //        Console.WriteLine(e.Message);
        //    }

        //    PartitionedTableAppender appender1 = new PartitionedTableAppender("dfs://compoDB", "pt", "ID", pool);
        //    BasicTable bt1 = (BasicTable)conn.run("table([1,2,3] as ints);");
        //    try
        //    {
        //        appender1.append(bt1);
        //    }
        //    catch(Exception e)
        //    {
        //        Assert.AreEqual("The input table doesn't match the schema of the target table.", e.Message);
        //        Console.WriteLine(e);
        //    }

        //    string sc4 = "nanos = nanotimestamp(48653..48667)\n" +
        //        "nano = rand(nanos, n)\n";
        //    conn.run(sc4);
        //    BasicTable bt2 = (BasicTable)conn.run("table(ID, nano, x)");
        //    try
        //    {
        //        appender1.append(bt2);
        //    }catch(Exception e)
        //    {
        //        Assert.AreEqual("column 1, temporal column must have exactly the same type, expect DT_DATE, got DT_NANOTIMESTAMP", e.Message);
        //        Console.WriteLine(e.Message);
        //    }
        //}

        [TestMethod]
        public void Test_PartitionedTableAppender_allDataType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v" };
            int rowNum = 0;
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicBooleanVector(rowNum));
            cols.Add(new BasicByteVector(rowNum));
            cols.Add(new BasicShortVector(rowNum));
            cols.Add(new BasicLongVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicFloatVector(rowNum));
            cols.Add(new BasicDateVector(rowNum));
            cols.Add(new BasicMonthVector(rowNum));
            cols.Add(new BasicTimeVector(rowNum));
            cols.Add(new BasicMinuteVector(rowNum));
            cols.Add(new BasicSecondVector(rowNum));
            cols.Add(new BasicDateTimeVector(rowNum));
            cols.Add(new BasicTimestampVector(rowNum));
            cols.Add(new BasicNanoTimeVector(rowNum));
            cols.Add(new BasicNanoTimestampVector(rowNum));
            cols.Add(new BasicSymbolVector(rowNum));
            cols.Add(new BasicStringVector(rowNum));
            cols.Add(new BasicUuidVector(rowNum));
            cols.Add(new BasicDateHourVector(rowNum));
            cols.Add(new BasicIPAddrVector(rowNum));
            cols.Add(new BasicInt128Vector(rowNum));
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2]);\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128]);\n pt=db.createPartitionedTable(t,`pt,`stringv);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://empty_table", "pt", "stringv", pool);
            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(0, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            pool.shutdown();
            conn.close();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_appendFunction()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);pt1=db.createPartitionedTable(t,`pt1,`id);");
            pool.execute(task);
            String appendScript = "tableInsert{loadTable(\"dfs://demohash\",\"pt1\")}";

            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", appendScript, pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);

            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);

            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(3, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\", `pt)");
            Assert.AreEqual(0, re.rows());
            BasicTable re1 = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\", `pt1)");
            Assert.AreEqual(3, re1.rows());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_appendFunction_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", "", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);

            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);

            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(3, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\", `pt)");
            Assert.AreEqual(3, re.rows());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_columnType_not_match()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", "", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);
            string re = null;
            try
            {
                appender.append(new BasicTable(colNames, cols));
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("the size of colNames must be equal than cols", re);
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_columnType_not_match_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", "", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);
            string re = null;
            try {
                appender.append(new BasicTable(colNames, cols));
            }
            catch (Exception ex) {
                re = ex.Message;
            }
            Assert.AreEqual("column 0, expect category LITERAL, got category INTEGRAL", re);
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_columnType_not_match_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`value,[STRING,TIME]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", "", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);
            
            BasicDateVector value = new BasicDateVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);
            
            string re = null;
            try
            {
                appender.append(new BasicTable(colNames, cols));
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("column 1, temporal column must have exactly the same type, expect DT_TIME, got DT_DATE", re);
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_partitionColName_not_exist()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);pt1=db.createPartitionedTable(t,`pt1,`id);");
            pool.execute(task);
            String appendScript = "tableInsert{loadTable(\"dfs://demohash\",\"pt1\")}";
            string re = null;
            try
            {
                PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "idd", appendScript, pool);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Can't find specified partition column name.", re);
            pool.shutdown();
        }
        [TestMethod]
        public void Test_PartitionedTableAppender_partitionColName_two()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db1 = database(, HASH,[STRING, 2]);db2 = database(, VALUE,1..100); db = database(dbPath, COMPO,[db1, db2]); t= table(100:0,`id`value,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id`value);");
            pool.execute(task);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", null, pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);

            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);

            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(3, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://demohash\", `pt)");
            Assert.AreEqual(3, re.rows());
            pool.shutdown();
        }

    }
}

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

namespace dolphindb_csharp_api_test.route_test
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
        [TestMethod] 
        public void Test_createPartitionedTableAppender_dfs_table_decimal64()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);           
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8)])\n";
            script += "db  = database(dbPath, RANGE,0 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
            int res = appender.append(bt);
            Assert.AreEqual(10001, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test\",`pt)");
            Console.Out.WriteLine(re.rows());
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getString(), bt.getColumn(i).get(j).getString());

                }
            }
            conn.close();

        }
        //[TestMethod]
        //public void Test_createPartitionedTableAppender_dfs_table_decimal32()
        //{

        //    IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
        //    DBConnection conn = new DBConnection(false);
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    string script = null;
        //    script += "dbPath = \"dfs://tableAppend_test\"\n";
        //    script += "if(existsDatabase(dbPath))\n";
        //    script += "dropDatabase(dbPath)\n";
        //    script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)])\n";
        //    script += "db  = database(dbPath, RANGE,0 50 10001)\n";
        //    script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
        //    IDBTask script1 = new BasicDBTask(script);
        //    pool.execute(script1);
        //    PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
        //    BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000,0) as col0, decimal32(0..10000,2) as col1, decimal32(0..10000,3) as col2, decimal32(0..10000,4) as col3);t2;");
        //    int res = appender.append(bt);
        //    Assert.AreEqual(10001, res);
        //    BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test\",`pt)");
        //    Console.Out.WriteLine(re.rows());
        //    for (int i = 0; i < re.columns(); i++)
        //    {
        //        for (int j = 0; j < re.rows(); j++)
        //        {
        //            Assert.AreEqual(re.getColumn(i).get(j).getString(), bt.getColumn(i).get(j).getString());

        //        }
        //    }
        //    conn.close();

        //}

        //[TestMethod]
        //public void Test_createPartitionedTableAppender_dfs_table_decimal64_compress_true()
        //{

        //    IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, false, null, null, true);
        //    DBConnection conn = new DBConnection(false);
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    string script = null;
        //    script += "dbPath = \"dfs://tableAppend_test1\"\n";
        //    script += "if(existsDatabase(dbPath))\n";
        //    script += "dropDatabase(dbPath)\n";
        //    script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8)])\n";
        //    script += "db  = database(dbPath, RANGE,0 50 10001)\n";
        //    script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
        //    IDBTask script1 = new BasicDBTask(script);
        //    pool.execute(script1);
        //    PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test1", "pt", "id", pool);
        //    BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
        //    int res = appender.append(bt);
        //    Assert.AreEqual(10001, res);
        //    BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test1\",`pt)");
        //    Console.Out.WriteLine(re.rows());
        //    for (int i = 0; i < re.columns(); i++)
        //    {
        //        for (int j = 0; j < re.rows(); j++)
        //        {
        //            Assert.AreEqual(re.getColumn(i).get(j).getString(), bt.getColumn(i).get(j).getString());

        //        }
        //    }
        //    conn.close();

        //}
        //[TestMethod]
        //public void Test_createPartitionedTableAppender_dfs_table_decimal32_compress_true()
        //{

        //    IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, false,null, null, true);
        //    DBConnection conn = new DBConnection(false);
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    string script = null;
        //    script += "dbPath = \"dfs://tableAppend_test\"\n";
        //    script += "if(existsDatabase(dbPath))\n";
        //    script += "dropDatabase(dbPath)\n";
        //    script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)])\n";
        //    script += "db  = database(dbPath, RANGE,0 50 10001)\n";
        //    script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
        //    IDBTask script1 = new BasicDBTask(script);
        //    pool.execute(script1);
        //    PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
        //    BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000,0) as col0, decimal32(0..10000,2) as col1, decimal32(0..10000,3) as col2, decimal32(0..10000,4) as col3);t2;");
        //    int res = appender.append(bt);
        //    Assert.AreEqual(10001, res);
        //    BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test\",`pt)");
        //    Console.Out.WriteLine(re.rows());
        //    for (int i = 0; i < re.columns(); i++)
        //    {
        //        for (int j = 0; j < re.rows(); j++)
        //        {
        //            Assert.AreEqual(re.getColumn(i).get(j).getString(), bt.getColumn(i).get(j).getString());

        //        }
        //    }
        //    conn.close();

        //}

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
    }
}

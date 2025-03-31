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

        static void compareBasicTable(BasicTable table, BasicTable newTable)
        {
            Assert.AreEqual(table.rows(), newTable.rows());
            Assert.AreEqual(table.columns(), newTable.columns());
            int cols = table.columns();
            for (int i = 0; i < cols; i++)
            {
                AbstractVector v1 = (AbstractVector)table.getColumn(i);
                AbstractVector v2 = (AbstractVector)newTable.getColumn(i);
                if (!v1.Equals(v2))
                {
                    for (int j = 0; j < table.rows(); j++)
                    {
                        int failCase = 0;
                        AbstractScalar e1 = (AbstractScalar)table.getColumn(i).get(j);
                        Console.WriteLine(e1.toString());

                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        Console.WriteLine(e2.toString());

                        if (e1.getString().Equals(e2.getString()) == false)
                        {
                            Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }
        }

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
            script += "db  = database(dbPath, RANGE,-1 50 10002)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
            int res = appender.append(bt);
//            Assert.AreEqual(10001, res);
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
        [TestMethod]
        public void Test_createPartitionedTableAppender_dfs_table_decimal32()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)])\n";
            script += "db  = database(dbPath, RANGE,-1 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000,0) as col0, decimal32(0..10000,2) as col1, decimal32(0..10000,3) as col2, decimal32(0..10000,4) as col3);t2;");
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

        [TestMethod]
        public void Test_createPartitionedTableAppender_dfs_table_decimal128()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL128(0),DECIMAL128(10),DECIMAL128(19),DECIMAL128(38)])\n";
            script += "db  = database(dbPath, RANGE,-1 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal128(take(\"1\" \" -1565\" \"99999999999999999999999999999999999999\" NULL,10001),0) as col0, decimal128(take(\"1.000000000001\" \" -1565.999999999999999999999999999\" \"9999999999999999999999999999.9999999999\" NULL,10001),10) as col1, decimal128(take(\"1.0000000001\" \" -15651.99999999999999\" \"9999999999999999999.9999999999999999999\" NULL,10001),19) as col2, decimal128(take(\"0.1\" \" -0.1565\" \"0.99999999999999999999999999999999999999\" NULL,10001),38) as col3);t2;");
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

        [TestMethod]
        public void Test_createPartitionedTableAppender_dfs_table_decimal64_compress_true()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, false, null, null, true);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test1\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8)])\n";
            script += "db  = database(dbPath, RANGE,0 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test1", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
            int res = appender.append(bt);
            Assert.AreEqual(10001, res);
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test1\",`pt)");
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
        [TestMethod]
        public void Test_createPartitionedTableAppender_dfs_table_decimal32_compress_true()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, false, null, null, true);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)])\n";
            script += "db  = database(dbPath, RANGE,0 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000,0) as col0, decimal32(0..10000,2) as col1, decimal32(0..10000,3) as col2, decimal32(0..10000,4) as col3);t2;");
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

        [TestMethod]
        public void Test_createPartitionedTableAppender_dfs_table_decimal128_compress_true()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, false, null, null, true,false,false);
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL128(0),DECIMAL128(10),DECIMAL128(19),DECIMAL128(38)])\n";
            script += "db  = database(dbPath, RANGE,-1 50 10001)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            IDBTask script1 = new BasicDBTask(script);
            pool.execute(script1);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppend_test", "pt", "id", pool);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal128(take(\"1\" \" -1565\" \"99999999999999999999999999999999999999\" NULL,10001),0) as col0, decimal128(take(\"1.000000000001\" \" -1565.999999999999999999999999999\" \"9999999999999999999999999999.9999999999\" NULL,10001),10) as col1, decimal128(take(\"1.0000000001\" \" -15651.99999999999999\" \"9999999999999999999.9999999999999999999\" NULL,10001),19) as col2, decimal128(take(\"0.1\" \" -0.1565\" \"0.99999999999999999999999999999999999999\" NULL,10001),38) as col3);t2;");
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
        public void test_PartitionedTableAppender_ArrayVector_decimal()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                    ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector", "pt", "cint", pool);
            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v32 = new BasicDecimal32Vector(3, 0);
            v32.set(0, new BasicDecimal32("15645.00", 0));
            v32.set(1, new BasicDecimal32("24635.00001", 0));
            v32.set(2, new BasicDecimal32("24635.00001", 0));
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0,0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v321 = new BasicDecimal32Vector(3, 4);
            v321.set(0, new BasicDecimal32("15645.00", 2));
            v321.set(1, new BasicDecimal32("24635.00001", 3));
            v321.set(2, new BasicDecimal32("24635.00001", 4));
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1,4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v640 = new BasicDecimal64Vector(3, 0);
            v640.set(0, new BasicDecimal64("15645.00", 0));
            v640.set(1, new BasicDecimal64("24635.00001", 0));
            v640.set(2, new BasicDecimal64("24635.00001", 0));
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2,0);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v641 = new BasicDecimal64Vector(3, 4);
            v641.set(0, new BasicDecimal64("15645.00", 2));
            v641.set(1, new BasicDecimal64("24635.00001", 8));
            v641.set(2, new BasicDecimal64("24635.00001", 0));
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3,4);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v642 = new BasicDecimal64Vector(3, 8);
            v642.set(0, new BasicDecimal64("15645.00", 1));
            v642.set(1, new BasicDecimal64("24635.00001", 8));
            v642.set(2, new BasicDecimal64("24635.00001", 8));
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4,8);
            cols.Add(bavcol4);

            BasicTable bt = new BasicTable(colNames, cols);
            int x = appender.append(bt);
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getSubVector(0).getString());
            Assert.AreEqual(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getSubVector(0).getString());
            Assert.AreEqual(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getSubVector(0).getString());
            Assert.AreEqual(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getSubVector(0).getString());
            Assert.AreEqual(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getSubVector(0).getString());

            pool.shutdown();
        }
        [TestMethod]
        public void test_PartitionedTableAppender_ArrayVector_decimal_compress_true()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                    ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false, null, null, true, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector", "pt", "cint", pool);
            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v32 = new BasicDecimal32Vector(3, 0);
            v32.set(0, new BasicDecimal32("15645.00", 0));
            v32.set(1, new BasicDecimal32("24635.00001", 0));
            v32.set(2, new BasicDecimal32("24635.00001", 0));
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0,0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v321 = new BasicDecimal32Vector(3, 4);
            v321.set(0, new BasicDecimal32("15645.00", 4));
            v321.set(1, new BasicDecimal32("24635.00001", 4));
            v321.set(2, new BasicDecimal32("24635.00001", 4));
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1,4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v640 = new BasicDecimal64Vector(3, 0);
            v640.set(0, new BasicDecimal64("15645.00", 0));
            v640.set(1, new BasicDecimal64("24635.00001", 0));
            v640.set(2, new BasicDecimal64("24635.00001", 0));
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2,0);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v641 = new BasicDecimal64Vector(3, 4);
            v641.set(0, new BasicDecimal64("15645.00", 4));
            v641.set(1, new BasicDecimal64("24635.00001", 4));
            v641.set(2, new BasicDecimal64("24635.00001", 4));
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3,4);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v642 = new BasicDecimal64Vector(3, 8);
            v642.set(0, new BasicDecimal64("15645.00", 8));
            v642.set(1, new BasicDecimal64("24635.00001", 8));
            v642.set(2, new BasicDecimal64("24635.00001", 8));
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4,8);
            cols.Add(bavcol4);

            BasicTable bt = new BasicTable(colNames, cols);
            int x = appender.append(bt);
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getSubVector(0).getString());
            Assert.AreEqual(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getSubVector(0).getString());
            Assert.AreEqual(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getSubVector(0).getString());
            Assert.AreEqual(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getSubVector(0).getString());
            Assert.AreEqual(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getSubVector(0).getString());

            pool.shutdown();
        }

        [TestMethod]
        public void test_PartitionedTableAppender_ArrayVector_decimal128()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                    ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector", "pt", "cint", pool);
            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v128 = new BasicDecimal128Vector(3, 0);
            v128.set(0, new BasicDecimal128("15645.00", 0));
            v128.set(1, new BasicDecimal128("24635.00001", 0));
            v128.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol0.Add(v128);
            bdvcol0.Add(v128);
            bdvcol0.Add(v128);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0,0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v1281 = new BasicDecimal128Vector(3, 4);
            v1281.set(0, new BasicDecimal128("15645.00", 2));
            v1281.set(1, new BasicDecimal128("24635.00001", 3));
            v1281.set(2, new BasicDecimal128("24635.00001", 4));
            bdvcol1.Add(v1281);
            bdvcol1.Add(v1281);
            bdvcol1.Add(v1281);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1,4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v1282 = new BasicDecimal128Vector(3, 10);
            v1282.set(0, new BasicDecimal128("15645.00", 0));
            v1282.set(1, new BasicDecimal128("24635.00001", 0));
            v1282.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol2.Add(v1282);
            bdvcol2.Add(v1282);
            bdvcol2.Add(v1282);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2,10);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v1283 = new BasicDecimal128Vector(3, 19);
            v1283.set(0, new BasicDecimal128("15645.00", 2));
            v1283.set(1, new BasicDecimal128("24635.00001", 8));
            v1283.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol3.Add(v1283);
            bdvcol3.Add(v1283);
            bdvcol3.Add(v1283);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3,19);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v1284 = new BasicDecimal128Vector(3, 37);
            v1284.set(0, new BasicDecimal128("1.00", 37));
            v1284.set(1, new BasicDecimal128("2.00001", 37));
            v1284.set(2, new BasicDecimal128("2.00001", 37));
            bdvcol4.Add(v1284);
            bdvcol4.Add(v1284);
            bdvcol4.Add(v1284);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4,37);
            cols.Add(bavcol4);

            BasicTable bt = new BasicTable(colNames, cols);
            Console.WriteLine(bt.rows());
            Console.WriteLine(bt.getString());
            Console.WriteLine("bt.getColumn(\"col0\").getDataType():" + bt.getColumn("col0").getDataType());
            
            int x = appender.append(bt);
            conn.run("sleep(500)");
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getSubVector(0).getString());
            Assert.AreEqual(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getSubVector(0).getString());
            Assert.AreEqual(v1282.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getSubVector(0).getString());
            Assert.AreEqual(v1283.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getSubVector(0).getString());
            Assert.AreEqual(v1284.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getSubVector(0).getString());
            pool.shutdown();
        }
        [TestMethod]
        public void test_PartitionedTableAppender_ArrayVector_decimal128_compress_true()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4" +
                    ",[INT,DECIMAL128(0)[],DECIMAL128(4)[],DECIMAL128(10)[],DECIMAL128(19)[],DECIMAL128(37)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false, null, null, true, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testArrayVector", "pt", "cint", pool);
            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v128 = new BasicDecimal128Vector(3, 0);
            v128.set(0, new BasicDecimal128("15645.00", 0));
            v128.set(1, new BasicDecimal128("24635.00001", 0));
            v128.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol0.Add(v128);
            bdvcol0.Add(v128);
            bdvcol0.Add(v128);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0,0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v1281 = new BasicDecimal128Vector(3, 4);
            v1281.set(0, new BasicDecimal128("15645.00", 4));
            v1281.set(1, new BasicDecimal128("24635.00001", 4));
            v1281.set(2, new BasicDecimal128("24635.00001", 4));
            bdvcol1.Add(v1281);
            bdvcol1.Add(v1281);
            bdvcol1.Add(v1281);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1,4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v1282 = new BasicDecimal128Vector(3, 10);
            v1282.set(0, new BasicDecimal128("15645.00", 10));
            v1282.set(1, new BasicDecimal128("24635.00001", 10));
            v1282.set(2, new BasicDecimal128("24635.00001", 10));
            bdvcol2.Add(v1282);
            bdvcol2.Add(v1282);
            bdvcol2.Add(v1282);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2,10);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v1283 = new BasicDecimal128Vector(3, 19);
            v1283.set(0, new BasicDecimal128("15645.00", 19));
            v1283.set(1, new BasicDecimal128("24635.00001", 19));
            v1283.set(2, new BasicDecimal128("24635.00001", 19));
            bdvcol3.Add(v1283);
            bdvcol3.Add(v1283);
            bdvcol3.Add(v1283);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3,19);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v1284 = new BasicDecimal128Vector(3, 37);
            v1284.set(0, new BasicDecimal128("1.00", 37));
            v1284.set(1, new BasicDecimal128("0.00001", 37));
            v1284.set(2, new BasicDecimal128("0.0000000000001", 37));
            bdvcol4.Add(v1284);
            bdvcol4.Add(v1284);
            bdvcol4.Add(v1284);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4,37);
            cols.Add(bavcol4);

            BasicTable bt = new BasicTable(colNames, cols);
            Console.WriteLine(bt.ToString());
            int x = appender.append(bt);
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v128.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getSubVector(0).getString());
            Assert.AreEqual(v1281.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getSubVector(0).getString());
            Assert.AreEqual(v1282.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getSubVector(0).getString());
            Assert.AreEqual(v1283.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getSubVector(0).getString());
            Assert.AreEqual(v1284.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getSubVector(0).getString());

            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_dfs_allDateType()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "csymbol = \"hello\" \"hello\" \"hello\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,csymbol,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
            script += "pt = db.createPartitionedTable(t,`pt,`cint,,`cint`cdate)";
            conn.run(script);
            BasicTable re = (BasicTable)conn.run("select * from t");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppenderTest", "pt", "cint", pool);
            int res = appender.append(re);
            BasicTable ua = (BasicTable)conn.run("select * from pt;");
            Assert.AreEqual(3, ua.rows());
            compareBasicTable(re, ua);
            conn.close();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_dfs_allDateType1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = table(intv,uuidv,ippaddrv,int128v,complexv)\n";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10,engine='TSDB')\n";
            script += "pt = db.createPartitionedTable(t,`pt,`intv,,`intv)\n";
            conn.run(script);
            BasicTable re = (BasicTable)conn.run("select * from t;");
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://tableAppenderTest", "pt", "intv", pool);
            int res = appender.append(re);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt);");
            Assert.AreEqual(100, ua.rows());
            compareBasicTable(re, ua);
            conn.close();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_allDataType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv", "decimal32v", "decimal64v", "decimal128v", "complexv" };
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
            cols.Add(new BasicStringVector(new List<string>(), true));
            cols.Add(new BasicDecimal32Vector(rowNum, 1));
            cols.Add(new BasicDecimal64Vector(rowNum, 10));
            cols.Add(new BasicDecimal128Vector(rowNum, 20));
            cols.Add(new BasicComplexVector(rowNum));
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask task = new BasicDBTask("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v`complexv, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(1), DECIMAL64(10), DECIMAL128(20),COMPLEX]);\n pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
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
        public void Test_PartitionedTableAppender_allDataType_array_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "id", "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "uuidv", "datehourv", "ippaddrv", "int128v", "decimal32v", "decimal64v", "decimal128v", "complexv" };
            int rowNum = 0;
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_BOOL_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_BYTE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_SHORT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_LONG_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DOUBLE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_FLOAT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_MONTH_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_TIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_MINUTE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_SECOND_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATETIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_TIMESTAMP_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIMESTAMP_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_UUID_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATEHOUR_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_IPADDR_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_INT128_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL32_ARRAY, 1));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL64_ARRAY, 10));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL128_ARRAY, 20));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_COMPLEX_ARRAY, 20));
            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n t= table(100:0,`id`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`decimal32v`decimal64v`decimal128v`complexv, [INT, INT[], BOOL[], CHAR[], SHORT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], DECIMAL32(1)[], DECIMAL64(10)[], DECIMAL128(20)[], COMPLEX[]]);\n pt=db.createPartitionedTable(t,`pt,`id,,`id);");
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://empty_table", "pt", "id", pool);
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

        [TestMethod]
        public void Test_PartitionedTableAppender_iotAnyVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value);\n select * from t");
            Console.Out.WriteLine(bt8.getString());
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType", "pt", "deviceId", pool);
            appender.append(bt);
            appender.append(bt1);
            appender.append(bt2);
            appender.append(bt3);
            appender.append(bt4);
            appender.append(bt5);
            appender.append(bt6);
            appender.append(bt7);
            appender.append(bt8);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(11, bt10.rows());
            Console.Out.WriteLine(bt10.getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", bt10.getColumn(3).getString());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_iotAnyVector_compress_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value);\n select * from t");
            Console.Out.WriteLine(bt8.getString());
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 10, true, true, null, null, true, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType", "pt", "deviceId", pool);
            appender.append(bt);
            appender.append(bt1);
            appender.append(bt2);
            appender.append(bt3);
            appender.append(bt4);
            appender.append(bt5);
            appender.append(bt6);
            appender.append(bt7);
            appender.append(bt8);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(11, bt10.rows());
            Console.Out.WriteLine(bt10.getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", bt10.getColumn(3).getString());
            pool.shutdown();
        }
        [TestMethod]
        public void Test_PartitionedTableAppender_iotAnyVector_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
            List<String> colNames = new List<String>() ;
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(0));
            cols.Add(new BasicTimestampVector(0));
            cols.Add(new BasicStringVector(0));
            cols.Add(new BasicIntVector(0));
            BasicTable bt9 = new BasicTable(colNames, cols);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType", "pt", "deviceId", pool);

            appender.append(bt);
            appender.append(bt1);
            appender.append(bt2);
            appender.append(bt3);
            appender.append(bt4);
            appender.append(bt5);
            appender.append(bt6);
            appender.append(bt7);
            appender.append(bt8);
            appender.append(bt9);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(9, bt10.rows());
            Console.Out.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("[, , , , , , , , ]", bt10.getColumn(3).getString());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_iotAnyVector_null_compress_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
            List<String> colNames = new List<String>();
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(0));
            cols.Add(new BasicTimestampVector(0));
            cols.Add(new BasicStringVector(0));
            cols.Add(new BasicIntVector(0));
            BasicTable bt9 = new BasicTable(colNames, cols);
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 10, true, true, null, null, true, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType", "pt", "deviceId", pool);
            appender.append(bt);
            appender.append(bt1);
            appender.append(bt2);
            appender.append(bt3);
            appender.append(bt4);
            appender.append(bt5);
            appender.append(bt6);
            appender.append(bt7);
            appender.append(bt8);
            appender.append(bt9);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(9, bt10.rows());
            Console.Out.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("[, , , , , , , , ]", bt10.getColumn(3).getString());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_PartitionedTableAppender_iotAnyVector_big_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType1\")) dropDatabase(\"dfs://testIOT_allDateType1\")\n" +
                    "     create database \"dfs://testIOT_allDateType1\" partitioned by   RANGE(100000*(0..10)),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType1\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType1\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table(take(1..100000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(char(1..100000),1000000) as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table(take(100001..200000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(short(1..100000),1000000) as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table(take(200001..300000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(int(1..100000),1000000) as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table(take(300001..400000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(long(1..100000),1000000) as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table(take(400001..500000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(true false null,1000000) as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table(take(500001..600000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33f 0 4.44f,1000000) as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table(take(600001..700000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33 0 4.44,1000000) as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table(take(700001..800000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(\"bb\"+string(0..100000), 1000000) as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(take(800001..900000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, symbol(take(NULL`bbb`AAA,1000000)) as value);\n select * from t");
            Console.Out.WriteLine(bt8.getString());
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 3, false, false);
            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://testIOT_allDateType1", "pt", "deviceId", pool);
            DateTime startTime = DateTime.Now;
            appender.append(bt);
            appender.append(bt1);
            appender.append(bt2);
            appender.append(bt3);
            appender.append(bt4);
            appender.append(bt5);
            appender.append(bt6);
            appender.append(bt7);
            appender.append(bt8);
            DateTime endTime = DateTime.Now;
            TimeSpan duration = endTime - startTime;
            Console.WriteLine("操作耗时: " + duration.TotalSeconds + " 秒");
            BasicTable bt10 = (BasicTable)conn.run("select count(*) from loadTable(\"dfs://testIOT_allDateType1\",`pt);");
            Assert.AreEqual("9000000", bt10.getColumn(0).get(0).getString());
            pool.shutdown();
        }
    }
}

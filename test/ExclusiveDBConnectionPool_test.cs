using dolphindb;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using dolphindb_config;
using dolphindb.data;
using System.Threading.Tasks;
using System.Data;
using System.Threading;
using System.IO;

namespace dolphindb_csharp_api_test
{
    /// <summary>
    /// 连接池测试
    /// </summary>
    [TestClass]
    public class ExclusiveDBConnectionPool_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        static private int PORTCON = MyConfigReader.PORTCON;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        private string LOCALHOST = MyConfigReader.LOCALHOST;
        static private string[] HASTREAM_GROUP1 = MyConfigReader.HASTREAM_GROUP1;
        ExclusiveDBConnectionPool pool = null;
        DBConnection conn = null;

        [TestInitialize]
        public void TestInitialize()
        {
            
        }
        [TestCleanup]
        public void TestCleanup()
        {
            try { pool.shutdown(); } catch (Exception ) { }
            try {  } catch (Exception ) { }
        }

        //[TestMethod]
        public void Test_ExclusiveDBConnectionPool_host_null()
        {
            pool = new ExclusiveDBConnectionPool("", 8848, USER, PASSWORD, 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        //[TestMethod]
        public void Test_ExclusiveDBConnectionPool_host_incorrect()
        {
            Exception exception = null;

            try
            {
                pool = new ExclusiveDBConnectionPool("192.111.111.111", PORT, USER, PASSWORD, 10, true, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
             Assert.IsNotNull(exception);
        }

        [TestMethod]
        [Timeout(3000)]
        public void Test_ExclusiveDBConnectionPool_port_incorrect()
        {
            Exception exception = null;

            try
            {
                pool = new ExclusiveDBConnectionPool(SERVER, 8876, USER, PASSWORD, 10, false, false);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_uid_null()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, "", PASSWORD, 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_uid_incorrect()
        {
            Exception exception = null;

            try
            {
                pool = new ExclusiveDBConnectionPool(SERVER, PORT, "q1qaz", PASSWORD, 10, true, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_pwd_null()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, "", 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_pwd_incorrect()
        {
            Exception exception = null;

            try
            {
                pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, "1qaz2wsx", 10, true, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_loadBalance_true()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, false);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_loadBalance_false()
        {

              pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, true);
              Assert.AreEqual(10, pool.getConnectionCount());

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_highAvaliability_true()
        {
               conn = new DBConnection();
               pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, true, HASTREAM_GROUP);
               Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_highAvaliability_false()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_count_zero()
        {
            String exception = null;
            try
            {
                pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 0, false, false);

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The thread count can not be less than 1"));

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_count_less_zero()
        {
            Exception exception = null;
            try
            {
                pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, -1, false, false);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, true);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_smallData()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, true, true);
            List<IDBTask> tasks = new List<IDBTask>(20);
            for (int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
                Assert.AreEqual(flag, true);
            }
        }


        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_bigData()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, false, false);
            conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script0 = null;
            string script1 = null;
            script0 += "m = 3000000;";
            script0 += "n = 100;";
            script0 += "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);";
            script0 += "share exTable0 as ptt;";
            script0 += "symbol_vector=take(`A, n);";
            script0 += "ID_vector=take(100, n);";
            script0 += "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);";
            script0 += "stringv_vector=rand(`name + string(1..100), n);";
            script0 += "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);";
            script0 += "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);";
            script0 += "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);";
            script1 += "undef(`ptt,SHARED)";
            BasicDBTask task = new BasicDBTask(script0);
            BasicDBTask task1 = new BasicDBTask(script1);
            pool.execute(task);
            //pool.waitForThreadCompletion();
            bool flag = task.isSuccessful();
            Assert.AreEqual(true, flag);
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from ptt");
            Assert.AreEqual(100, tmpNum1.getInt());
            pool.execute(task1);

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_connectionBanlance()
        {
            conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            BasicIntVector connCount = (BasicIntVector)conn.run("exec connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port =" + PORT);
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, false, false);
            conn.run("sleep(2000)");
            BasicIntVector connCount1 = (BasicIntVector)conn.run("exec connectionNum  from rpc(getControllerAlias(),getClusterPerf) where mode= 0 and port =" + PORT);
            Assert.AreEqual(true, connCount1.getInt(0) - connCount.getInt(0) >= 20 );
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_haSites_null()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, null);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_error()
        {
            Exception exception = null;
            try
            {
                pool = new ExclusiveDBConnectionPool("wwww", PORT, USER, PASSWORD, 10, true, true, new string[] { "1111www", "ssss", "sss" });
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_haSites()
        {
            conn = new DBConnection();
            pool = new ExclusiveDBConnectionPool("www", PORT, USER, PASSWORD, 10, true, true, HASTREAM_GROUP);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_haSite11s()
        {
            pool = new ExclusiveDBConnectionPool("192.168.1.167", PORT, USER, PASSWORD, 10, true, true, HASTREAM_GROUP);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup_error()
        {    
            Exception exception = null;  
            try
                {
                    pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "ddddd");
                } 
            catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    exception = ex;
                }
                Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup_none()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "");
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup_null()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, null);
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup()
        {
            //string script0 = null;
            //script0 += "t = table([1].int() as id1);";
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "123");
            Assert.AreEqual(10, pool.getConnectionCount());
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_compress_true()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "",true);
            Assert.AreEqual(10, pool.getConnectionCount());
            List<IDBTask> tasks = new List<IDBTask>(20);
            for (int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
                Assert.AreEqual(flag, true);
            }
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_compress_false()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", false);
            Assert.AreEqual(10, pool.getConnectionCount());
            List<IDBTask> tasks = new List<IDBTask>(20);
            for (int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
                Assert.AreEqual(flag, true);
            }
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_useSSL_true()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, true);
            Assert.AreEqual(10, pool.getConnectionCount());
            List<IDBTask> tasks = new List<IDBTask>(20);
            for (int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
                Assert.AreEqual(flag, true);
            }
        }
        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_useSSL_false()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false);
            Assert.AreEqual(10, pool.getConnectionCount());
            List<IDBTask> tasks = new List<IDBTask>(20);
            for (int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
                Assert.AreEqual(flag, true);
            }
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_usePython_true()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false, true);
            string script1 = null;
            script1 += "import pandas as pd";
            BasicDBTask task1 = new BasicDBTask(script1);
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");;
            BasicString version = (BasicString)db.run("version()");
            if (version.getString().Contains("3.00"))
            {
                Assert.AreEqual(10, pool.getConnectionCount());
                pool.execute(task1);
                bool flag = task1.isSuccessful();
                Assert.AreEqual(true, flag);
                pool.run(script1);
                pool.shutdown();
            }
            else
            {
                Console.WriteLine("The current version does not support Python Parser, so this case is skipped");
            }
            db.close();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_usePython_false()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false, false);
            Assert.AreEqual(10, pool.getConnectionCount());
            string script1 = null;
            script1 += "import pandas as pd";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            BasicString version = (BasicString)db.run("version()");
            if (version.getString().Contains("3.00"))
            {
                try
                {
                    BasicDBTask task1 = new BasicDBTask(script1);
                    pool.execute(task1);
                    bool flag = task1.isSuccessful();
                    Assert.AreEqual(false, flag);
                    pool.run(script1);
                    pool.shutdown();
                }
                catch (Exception ex)
                {
                    string s = ex.Message;
                    Assert.AreEqual(s, "Syntax Error: [line #1] Cannot recognize the token import");
                }
            }
            else
            {
                Console.WriteLine("The current version does not support Python Parser, so this case is skipped");
            }
            db.close();


        }


        [TestMethod]
        public void Test_sqllist_create_dfs()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "if(existsDatabase(\"dfs://rangedb_tradedata\")) {dropDatabase(\"dfs://rangedb_tradedata\")}", "n=10000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1 2, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades", "select * from Trades order by value,symbol" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 8)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(1, ((BasicInt)t1.getColumn(0).get(0)).getValue());
                }
                else if (i == 9)
                {
                    BasicTable t2 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(2, ((BasicInt)t2.getColumn(0).get(0)).getValue());
                    Console.Out.WriteLine(entity.getString());
                }
                else if (i == 10)
                {
                    Assert.AreEqual(10000, entity.rows());
                    for (int j = 0; j < 5000; j++)
                    {
                        BasicTable t3 = (BasicTable)entity;
                        Assert.AreEqual(1, ((BasicInt)t3.getColumn(1).get(j)).getValue());
                    }
                    for (int j = 5000; j < 10000; j++)
                    {
                        BasicTable t3 = (BasicTable)entity;
                        Assert.AreEqual(2, ((BasicInt)t3.getColumn(1).get(j)).getValue());
                    }


                }

            }
        }

        [TestMethod]
        public void Test_sqllist_create_dfs_drop()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "if(existsDatabase(\"dfs://rangedb_tradedata\")) {dropDatabase(\"dfs://rangedb_tradedata\")}", "n=1000000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1..10, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades", "dropDatabase(\"dfs://rangedb_tradedata\")", "n=1000000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1..10, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 8)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(1, ((BasicInt)t1.getColumn(0).get(0)).getValue());
                }
                else if (i == 9)
                {
                    BasicTable t2 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(10, ((BasicInt)t2.getColumn(0).get(0)).getValue());
                    Console.Out.WriteLine(entity.getString());
                }
                else if (i == 18)
                {
                    BasicTable t2 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(1, ((BasicInt)t2.getColumn(0).get(0)).getValue());
                    Console.Out.WriteLine(entity.getString());
                }
                else if (i == 19)
                {
                    BasicTable t2 = (BasicTable)entity;
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(10, ((BasicInt)t2.getColumn(0).get(0)).getValue());
                    Console.Out.WriteLine(entity.getString());
                }
            }
        }

        [TestMethod]
        public void Test_sqllist_select()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 5)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(3, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(49.6, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                }

            }
        }


        [TestMethod]
        public void Test_sqllist_insert()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "insert into t1 values(09:36:51,`D,1200,29.44)", "select * from t1" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 5)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(3, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(49.6, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                }
                if (i == 7)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(4, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(3)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual("D", ((BasicString)t1.getColumn(1).get(3)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(1200, ((BasicInt)t1.getColumn(2).get(3)).getValue());
                    Assert.AreEqual(49.6, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                    Assert.AreEqual(29.44, ((BasicDouble)t1.getColumn(3).get(3)).getValue());
                }

            }
        }

        [TestMethod]
        public void Test_sqllist_update()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "update t1 set price=30 where sym=`A ", "select * from t1" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 5)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(3, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(49.6, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                }
                if (i == 7)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(3, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(30, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                }

            }
        }

        [TestMethod]
        public void Test_sqllist_delete()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "delete from t1 where sym=`A ", "select * from t1" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 5)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(3, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 34, 07).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(2)).getValue());
                    Assert.AreEqual("A", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(2)).getValue());
                    Assert.AreEqual(2200, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(2)).getValue());
                    Assert.AreEqual(49.6, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(2)).getValue());
                }
                if (i == 7)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(4, entity.columns());
                    Assert.AreEqual(2, entity.rows());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 42).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(0)).getValue());
                    Assert.AreEqual(new DateTime(1970, 01, 01, 09, 36, 51).TimeOfDay, ((BasicSecond)t1.getColumn(0).get(1)).getValue());
                    Assert.AreEqual("B", ((BasicString)t1.getColumn(1).get(0)).getValue());
                    Assert.AreEqual("C", ((BasicString)t1.getColumn(1).get(1)).getValue());
                    Assert.AreEqual(1900, ((BasicInt)t1.getColumn(2).get(0)).getValue());
                    Assert.AreEqual(2100, ((BasicInt)t1.getColumn(2).get(1)).getValue());
                    Assert.AreEqual(29.46, ((BasicDouble)t1.getColumn(3).get(0)).getValue());
                    Assert.AreEqual(29.52, ((BasicDouble)t1.getColumn(3).get(1)).getValue());
                }

            }
        }

        [TestMethod]
        public void Test_sqllist_def()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "def test_user(){try\n{deleteUser('testuser1')}\n catch(ex)\n{}\n}\n rpc(getControllerAlias(), test_user)", "def test_user1(){try\n{createUser(\"testuser1\", \"123456\")}\n catch(ex)\n{}\n}\n rpc(getControllerAlias(), test_user1)" };
            List<IEntity> entities = pool.run(sqlList);
            foreach (IEntity entity in entities)
            {
                Console.Out.WriteLine(entity.getString());
                entity.getString();
            }
        }

        [TestMethod]
        [Timeout(200000)]
        public void Test_sqllist_longsql()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
            List<string> sqlList = new List<string>() { "m = 30000;", "n = 100;", "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "share exTable0 as ptt;", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "select count(*) from ptt", "select ID from ptt", "select * from ptt", "select intv from ptt" };
            List<IEntity> entities = pool.run(sqlList);
            for (int i = 0; i < entities.ToArray().Length; i++)
            {
                IEntity entity = entities.ToArray()[i];
                if (i == 15)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(1, entity.columns());
                    Assert.AreEqual(1, entity.rows());
                    Assert.AreEqual(100, ((BasicInt)t1.getColumn(0).get(0)).getValue());
                }
                if (i == 16)
                {
                    BasicTable t1 = (BasicTable)entity;
                    Console.Out.WriteLine(entity.getString());
                    Assert.AreEqual(1, entity.columns());
                    Assert.AreEqual(100, entity.rows());
                    for (int j = 0; j < 100; j++)
                    {
                        BasicTable t3 = (BasicTable)entity;
                        Assert.AreEqual(100, ((BasicInt)t3.getColumn(0).get(j)).getValue());
                    }
                }

            }
        }

        //[TestMethod]
        //public void Test_run_return_scalar_decimal64()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
        //    Assert.AreEqual(9223372036854775807, ((BasicDecimal64)db.run("decimal64(999999999999999999999999,0)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(NULL,3)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(),2)")).getValue());
        //    Assert.AreEqual(3000.00, ((BasicDecimal64)db.run("decimal64(short(3000),2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(short(0),2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(short(-1),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(32768),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(-32768),2)")).getValue());
        //    Assert.AreEqual(-32767, ((BasicDecimal64)db.run("decimal64(short(32769),2)")).getValue());
        //    Assert.AreEqual(32767, ((BasicDecimal64)db.run("decimal64(short(-32769),2)")).getValue());
        //    Assert.AreEqual(123, ((BasicDecimal64)db.run("decimal64(123,2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(0,2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(-1,2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(int(2147483648),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(int(-2147483648),2)")).getValue());
        //    Assert.AreEqual(-2147483647, ((BasicDecimal64)db.run("decimal64(int(2147483649),0)")).getValue());
        //    Assert.AreEqual(2147483647, ((BasicDecimal64)db.run("decimal64(int(-2147483649),0)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(long(),2)")).getValue());
        //    Assert.AreEqual(123, ((BasicDecimal64)db.run("decimal64(long(123),2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(long(0),2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(long(-1),2)")).getValue());
        //    Assert.AreEqual(2.12300000, ((BasicDecimal64)db.run("decimal64(string(2.123),8)")).getValue());
        //    db.shutdown();
        //}
        //[TestMethod]
        //public void Test_run_return_scalar_decimal32()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);

        //    Assert.AreEqual(-1, ((BasicDecimal32)db.run("decimal32(long(-1),2)")).getValue());
        //    Assert.AreEqual(-2147483648, ((BasicDecimal32)db.run("decimal32(int(),2)")).getValue());
        //    Assert.AreEqual(123.12, ((BasicDecimal32)db.run("decimal32(123.123f,2)")).getValue());
        //    Assert.AreEqual(-1.1, ((BasicDecimal32)db.run("decimal32(-1.1,9)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal32)db.run("decimal32(short(0),2)")).getValue());
        //    Assert.AreEqual(-1.32, ((BasicDecimal32)db.run("decimal32(string(-00001.32132),2)")).getValue());
        //    db.shutdown();
        //}
        //[TestMethod]
        //public void Test_run_return_vector_decimal64()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
        //    IVector v = (IVector)db.run("decimal64(symbol(`3000`234),2)");
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000.00, ((BasicDecimal64)v.get(0)).getValue());
        //    Assert.AreEqual(234.00, ((BasicDecimal64)v.get(1)).getValue());

        //    IVector v2 = (IVector)db.run("decimal64(symbol(string(0..9999)),2)");
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal64)v2.get(i)).getValue());
        //    }
        //    db.shutdown();
        //}

        //[TestMethod]
        //public void Test_run_return_vector_decimal32()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false);
        //    IVector v = (IVector)db.run("decimal32(symbol(`3000`234),2)");
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal32)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal32)v.get(1)).getValue());

        //    IVector v2 = (IVector)db.run("decimal32(symbol(string(0..9999)),2)");
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal32)v2.get(i)).getValue());
        //    }
        //    db.shutdown();
        //}
        //[TestMethod]
        //public void Test_run_return_scalar_decimal64_compress_true()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false,null,null,true);
        //    Assert.AreEqual(9223372036854775807, ((BasicDecimal64)db.run("decimal64(999999999999999999999999,0)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(NULL,3)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(),2)")).getValue());
        //    Assert.AreEqual(3000.00, ((BasicDecimal64)db.run("decimal64(short(3000),2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(short(0),2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(short(-1),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(32768),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(short(-32768),2)")).getValue());
        //    Assert.AreEqual(-32767, ((BasicDecimal64)db.run("decimal64(short(32769),2)")).getValue());
        //    Assert.AreEqual(32767, ((BasicDecimal64)db.run("decimal64(short(-32769),2)")).getValue());
        //    Assert.AreEqual(123, ((BasicDecimal64)db.run("decimal64(123,2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(0,2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(-1,2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(int(2147483648),2)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(int(-2147483648),2)")).getValue());
        //    Assert.AreEqual(-2147483647, ((BasicDecimal64)db.run("decimal64(int(2147483649),0)")).getValue());
        //    Assert.AreEqual(2147483647, ((BasicDecimal64)db.run("decimal64(int(-2147483649),0)")).getValue());
        //    Assert.AreEqual((double)long.MinValue, ((BasicDecimal64)db.run("decimal64(long(),2)")).getValue());
        //    Assert.AreEqual(123, ((BasicDecimal64)db.run("decimal64(long(123),2)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal64)db.run("decimal64(long(0),2)")).getValue());
        //    Assert.AreEqual(-1, ((BasicDecimal64)db.run("decimal64(long(-1),2)")).getValue());
        //    Assert.AreEqual(2.12300000, ((BasicDecimal64)db.run("decimal64(string(2.123),8)")).getValue());
        //    db.shutdown();
        //}
        //[TestMethod]
        //public void Test_run_return_scalar_decimal32_compress_true()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false, null, null, true);

        //    Assert.AreEqual(-1, ((BasicDecimal32)db.run("decimal32(long(-1),2)")).getValue());
        //    Assert.AreEqual(-2147483648, ((BasicDecimal32)db.run("decimal32(int(),2)")).getValue());
        //    Assert.AreEqual(123.12, ((BasicDecimal32)db.run("decimal32(123.123f,2)")).getValue());
        //    Assert.AreEqual(-1.1, ((BasicDecimal32)db.run("decimal32(-1.1,9)")).getValue());
        //    Assert.AreEqual(0, ((BasicDecimal32)db.run("decimal32(short(0),2)")).getValue());
        //    Assert.AreEqual(-1.32, ((BasicDecimal32)db.run("decimal32(string(-00001.32132),2)")).getValue());
        //    db.shutdown();
        //}
        //[TestMethod]
        //public void Test_run_return_vector_decimal64_compress_true()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false, null, null, true);
        //    IVector v = (IVector)db.run("decimal64(symbol(`3000`234),2)");
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000.00, ((BasicDecimal64)v.get(0)).getValue());
        //    Assert.AreEqual(234.00, ((BasicDecimal64)v.get(1)).getValue());

        //    IVector v2 = (IVector)db.run("decimal64(symbol(string(0..9999)),2)");
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal64)v2.get(i)).getValue());
        //    }
        //    db.shutdown();
        //}

        //[TestMethod]
        //public void Test_run_return_vector_decimal32_compress_true()
        //{
        //    ExclusiveDBConnectionPool db = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 1, false, false, null, null, true);
        //    IVector v = (IVector)db.run("decimal32(symbol(`3000`234),2)");
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal32)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal32)v.get(1)).getValue());

        //    IVector v2 = (IVector)db.run("decimal32(symbol(string(0..9999)),2)");
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal32)v2.get(i)).getValue());
        //    }
        //    db.shutdown();
        //}

     
        [TestMethod]
        public void Test_sqlList_priority()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 1, false, false, null, "", true, false, false);
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var re1 = pool.run(sqlList, 4, 5);
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(5).get(0).getString());
            Assert.AreEqual("4", ((BasicTable)re1[0]).getColumn(5).get(0).getString());
            var re2 = pool.run(sqlList, 0, 5);
            Console.Out.WriteLine(((BasicTable)re2[0]).getColumn(5).get(0).getString());
            Assert.AreEqual("0", ((BasicTable)re2[0]).getColumn(5).get(0).getString());
            var re3 = pool.run(sqlList, 8, 5);
            Console.Out.WriteLine(((BasicTable)re3[0]).getColumn(5).get(0).getString());
            Assert.AreEqual("8", ((BasicTable)re3[0]).getColumn(5).get(0).getString());
            String re4 = null;
            try
            {
                pool.run(sqlList, -1, 5);
            }
            catch (Exception ex)
            {
                re4 = ex.Message;
            }
            Assert.AreEqual(re4, "priority must be greater than -1 and less than 9");
            String re5 = null;
            try
            {
                pool.run(sqlList, 9, 5);
            }
            catch (Exception ex)
            {
                re5 = ex.Message;
            }
            Assert.AreEqual(re5, "priority must be greater than -1 and less than 9");
        }

        public void PrepareUser(String userName, String password)
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("def create_user(){try{deleteUser(`" + userName + ")}catch(ex){};createUser(`" + userName + ", '" + password + "',,true);};" +
                    "rpc(getControllerAlias(),create_user);");
        }

        [TestMethod]//api设置的parallelism小于server的setMaxJobParallelism
        public void test_ExclusiveDBConnectionPool_run_parallelism_1()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);", 4, 5, 0, false);

            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "parallelism_test", "123456", 1, false, false, null, "", true, false, false);
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var re1 = pool.run(sqlList, 4, 5);
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("5", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]//api设置的parallelism大于server的setMaxJobParallelism
        public void test_ExclusiveDBConnectionPool_run_parallelism_2()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);", 4, 30, 0, false);

            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "parallelism_test", "123456", 1, false, false, null, "", true, false, false);
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var re1 = pool.run(sqlList, 4, 30);
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("22", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]//api没有设置parallelism，取默认值64，大于server的setMaxJobParallelism
        public void test_ExclusiveDBConnectionPool_run_parallelism_3()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);");

            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "parallelism_test", "123456", 1, false, false, null, "", true, false, false);
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var re1 = pool.run(sqlList);
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("22", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]
        public void Test_sqlList_parallelism_65()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 1, true, true, HASTREAM_GROUP1, "", true, false, false);

            List<string> sqlList2 = new List<string>() { "t = select * from loadTable(\"dfs://test1\",\"pt\")", "loop(max, t.values()); " };

            try
            {

                List<IEntity> entities1 = pool.run(sqlList2, 4, 65);


            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "parallelism must be greater than 0 and less than 65");
            }
            pool.shutdown();
        }

        [TestMethod]
        public void Test_sqlList_parallelism_0()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 1, true, true, HASTREAM_GROUP1, "", true, false, false);

            List<string> sqlList2 = new List<string>() { "t = select * from loadTable(\"dfs://test1\",\"pt\")", "loop(max, t.values()); " };
            try
            {

                List<IEntity> entities1 = pool.run(sqlList2, 4, 0);


            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "parallelism must be greater than 0 and less than 65");
            }
        }


        [TestMethod]
        public void Test_sqlList_clearMemory_false()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 1, true, true, HASTREAM_GROUP1, "", true, false, false);

            List<string> sqlList = new List<string>() { "exTable=table(100:5, `name`id`value, [STRING,INT,DOUBLE]);", "share exTable as ptt2;" };
            List<IEntity> entities1 = pool.run(sqlList, 4, 1, false);
            pool.shutdown();
        }
        [TestMethod]
        public void Test_sqlList_clearMemory_true()
        {
            pool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 1, true, true, HASTREAM_GROUP1, "", true, false, false);
            List<string> sqlList1 = new List<string>() { "exTable=table(100:5, `name`id`value, [STRING,INT,DOUBLE]);", "share exTable as ptt2;" };
            Exception e = null;
            try
            {
                List<IEntity> entities1 = pool.run(sqlList1, 4, 1, true);
            }
            catch (Exception ex)
            {
                e = ex;

            }
            Assert.IsNotNull(e);
        }


        [TestMethod]
        public void Test_run_return_table_in1t()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name)");
            Assert.IsTrue(tb.isTable());
            Assert.AreEqual(100, tb.rows());
            Assert.AreEqual(2, tb.columns());
            Assert.AreEqual(3, ((BasicInt)tb.getColumn(0).get(2)).getValue());
            Assert.AreEqual("aaa", ((BasicString)tb.getColumn(1).get(2)).getString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicDBTask_Remain()
        {
            conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            BasicDBTask bdt1 = new BasicDBTask("1+1");
            bdt1.setDBConnection(conn);
            bdt1.call();
            Assert.IsTrue(bdt1.isFinished());
            BasicDBTask bdt2 = new BasicDBTask("a=n");
            bdt2.setDBConnection(conn);
            bdt2.call();
            Assert.IsTrue(bdt2.isFinished());
            BasicDBTask bdt3 = new BasicDBTask("sleep(100000)");
            bdt3.setDBConnection(conn);
            Assert.IsFalse(bdt3.isFinished());
            
        }
        [TestMethod]
        [ExpectedException(typeof(Exception))]
        public void Test_Run_not_support_type()
        {
            string scripts = "t = loadText(\"/home/wsun/Downloads/file2.csv\")";
            scripts += "if(existsDatabase(\"dfs://test\")){\n dropDatabase(\"dfs://test\")\n }";
            var connPool = new ExclusiveDBConnectionPool(SERVER, PORT, "admin", "123456", 10, false, true);
            BasicTable tb = (BasicTable)connPool.run("select * from loadTable('dfs://tardis', 'bbg_trade_bar_1m') order by timestamp desc limit 0,10");
            Console.WriteLine(tb.getString());
            connPool.shutdown();
        }
    }
}

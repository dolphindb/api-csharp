using dolphindb;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using dolphindb_config;
using dolphindb.data;

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
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        private string LOCALHOST = MyConfigReader.LOCALHOST;

        
        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_host_null()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("", 8850, USER, PASSWORD, 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_host_incorrect()
        {
            Exception exception = null;

            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("192.111.111.111", PORT, USER, PASSWORD, 10, true, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
             Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_port_incorrect()
        {
            Exception exception = null;

            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, 8876, USER, PASSWORD, 10, true, true);
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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "", PASSWORD, 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_uid_incorrect()
        {
            Exception exception = null;

            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, "q1qaz", PASSWORD, 10, true, true);
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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, "", 10, true, true);
            //There is a default value when it is empty
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_pwd_incorrect()
        {
            Exception exception = null;

            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, "1qaz2wsx", 10, true, true);
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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, false);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_loadBalance_false()
        {

              ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, true);
              Assert.AreEqual(10, pool.getConnectionCount());
              pool.shutdown();

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_highAvaliability_true()
        {
            
               ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, true);
               Assert.AreEqual(10, pool.getConnectionCount());
               pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_highAvaliability_false()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_count_zero()
        {
            Exception exception = null;
            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 0, false, false);

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_count_less_zero()
        {
            Exception exception = null;
            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, -1, false, false);

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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, true);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_smallData()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, true, true);
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
            pool.shutdown();
        }


        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_bigData()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, false, false);
            DBConnection conn = new DBConnection();
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
            pool.shutdown();

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_connectionBanlance()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 20, false, false);
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script0 = null;
            string script1 = null;
            script0 += "t = table([1].int() as id1);";
            script1 += "t.append!(table(1..100000 as id))";
            List<IDBTask> tasksInit = new List<IDBTask>();
            for (int i = 0; i < 100; ++i)
            {
                tasksInit.Add(new BasicDBTask(script0));
            }
            pool.execute(tasksInit);
            List<IDBTask> tasks = new List<IDBTask>();
            for (int i = 0; i < 100; i++)
            {
                BasicDBTask task1 = new BasicDBTask(script1);
                tasks.Add(task1);
            }
            pool.execute(tasks);
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from getSessionMemoryStat() where  memSize=2400196");
            Assert.AreEqual(20, tmpNum1.getInt());
            pool.shutdown();

        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_haSites_null()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, null);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_error()
        {
            Exception exception = null;
            try
            {
                ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("wwww", PORT, USER, PASSWORD, 10, true, true, new string[] { "1111www", "ssss", "sss" });
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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool("wwww", PORT, USER, PASSWORD, 10, true, true, HASTREAM_GROUP);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup_error()
        {    
            Exception exception = null;  
            try
                {
                    ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "ddddd");
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
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "");
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup_null()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, null);
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown(); 
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_startup()
        {
            //string script0 = null;
            //script0 += "t = table([1].int() as id1);";
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "123");
            Assert.AreEqual(10, pool.getConnectionCount());
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_compress_true()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "",true);
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
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_compress_false()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", false);
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
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_useSSL_true()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, true);
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
            pool.shutdown();
        }
        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_useSSL_false()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false);
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
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_usePython_true()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(LOCALHOST, 8850, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false, true);
            Assert.AreEqual(10, pool.getConnectionCount());
            string script1 = null;
            script1 += "import pandas as pd";
            BasicDBTask task1 = new BasicDBTask(script1);
            pool.execute(task1);
            bool flag = task1.isSuccessful();
            Assert.AreEqual(true, flag);
            pool.run(script1);
            pool.shutdown();
        }

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool_usePython_false()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(LOCALHOST, 8850, USER, PASSWORD, 10, false, false, HASTREAM_GROUP, "", true, false, false);
            Assert.AreEqual(10, pool.getConnectionCount());
            Exception exception = null;
            string script1 = null;
            script1 += "import pandas as pd";
            BasicDBTask task1 = new BasicDBTask(script1);
            pool.execute(task1);
            bool flag = task1.isSuccessful();
            Assert.AreEqual(false, flag);
            try
            {
                pool.run(script1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            pool.shutdown();
        }

    }
}

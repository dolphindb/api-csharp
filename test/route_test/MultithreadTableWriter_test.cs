using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using dolphindb.route;
using dolphindb_config;
using System.Text;

namespace dolphindb_csharp_api_test.route_test
{
    public class CallbackHandler : Callback
    {
        private BasicStringVector id1 = new BasicStringVector(0);
        private BasicBooleanVector success1 = new BasicBooleanVector(0);
        public void writeCompletion(ITable callbackTable)
        {
            BasicStringVector idV;
            BasicBooleanVector successV;
            lock (callbackTable)
            {
                 idV = (BasicStringVector)callbackTable.getColumn(0);
                 successV = (BasicBooleanVector)callbackTable.getColumn(1);
            }
            lock (id1)
            {
                id1.append(idV);
            }
            lock (success1)
            {
                success1.append(successV);
            }
            for (int i = 0; i < successV.rows(); i++)
            {
                Console.Out.WriteLine(idV.getString(i) + " " + successV.getBoolean(i));
            }
        }
        public BasicStringVector getId()
        {
            return id1;
        }
        public BasicBooleanVector getIsSuccess()
        {
            return success1;
        }
    }
    [TestClass]
    public class MultithreadedTableWriter_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        static private int PORTCON = MyConfigReader.PORTCON;
        static private int PORTDATE2 = MyConfigReader.PORTDATE2;
        static private int PORTDATE3 = MyConfigReader.PORTDATE3;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string NODE1_HOST = MyConfigReader.NODE1_HOST;
        private readonly int NODE1_PORT = MyConfigReader.NODE1_PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        static private string[] HASTREAM_GROUP1 = MyConfigReader.HASTREAM_GROUP1;

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
                        ////Console.WriteLine(e1.toString());

                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        ////Console.WriteLine(e2.toString());

                        if (e1.getString().Equals(e2.getString()) == false)
                        {
                            //Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }

        }
        static void compareBasicTable16(BasicTable table, BasicTable newTable)
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
                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        if (!e1.getString().Equals(e2.getString()))
                        {
                            //Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }

        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_hostName_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter("not_exist", PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_port_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT + 100, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_userId_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, "user1", PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
               // //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_password_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, "654321", "", "table1", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dbName_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_not_exist", "pt", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_tableName_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt1", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase(\"dfs://test_MultithreadedTableWriter\")");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_user_no_previlege()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            script += "try{rpc(getControllerAlias(), deleteUser, \"user1\")}catch(ex){};";
            script += "rpc(getControllerAlias(), createUser, \"user1\", \"123456\");";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, "user1", "123456", "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10000, 1, 1);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase(\"dfs://test_MultithreadedTableWriter\")");
            conn.run("rpc(getControllerAlias(), deleteUser, \"user1\")");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_batchSize_negative_int()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, -1, 1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_batchSize_zero()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 0, 1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_batchSize_one()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 1, 1, 1);
            mtw.waitForThreadCompletion();
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_throttle_negative()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, -1, 1);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_threadCount_negative()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, -2);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_threadCount_zero()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 0);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_in_memory_table_multi_thread()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 10);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();

        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_partitionedColName_incorrect()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "sym");
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);

        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_insert_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 1);
            int i = 0;
            List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            long sentRows = status.threadStatus[0].sentRows;
            long sendFailedRows = status.threadStatus[0].sendFailedRows;
            long unsentRows = status.threadStatus[0].unsentRows;
            Assert.AreEqual(sentRows, 0);
            Assert.AreEqual(sendFailedRows, 0);
            Assert.AreEqual(unsentRows, 0);
            Assert.IsNotNull(pErrorInfo.ToString());
            mtw.waitForThreadCompletion();
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicBoolean()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicBoolean(true) });
                    mtw.insert(x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicBoolean(false) });
                    mtw.insert(x);
                }
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                MultithreadedTableWriter.Status status = mtw.getStatus();
                if (status.threadStatus[0].sentRows.Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(status.threadStatus[0].sentRows, 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take([true, false], 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicByte()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [CHAR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicByte((byte)(i % 99)) });
                 mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(char(0..999999%99), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicShort()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SHORT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicShort((short)(i % 99)) });
                mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());

            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(short(0..999999%99), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicInt()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i) });
                mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..999999 as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicLong()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [LONG]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicLong(i) });
                mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(long(0..999999) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDate()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DATE]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDate(i) });
                mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(date(0..999999) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicMonth()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [MONTH]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 20; i < 100; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicMonth(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
                Console.WriteLine(pErrorInfo.ToString());

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(month(20..99) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [TIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicTime(new DateTime(1970, 01, 01, i % 24, 00, 00, 123).TimeOfDay) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
                Console.WriteLine(pErrorInfo.ToString());

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00:00.123, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicMinute()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [MINUTE]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicMinute(new DateTime(1970, 01, 01, i % 24, 00, 00, 000).TimeOfDay) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00m, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicSecond()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SECOND]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicSecond(new DateTime(1970, 01, 01, i % 24, 00, 00, 000).TimeOfDay) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00:00, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDateTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DATETIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDateTime(new DateTime(2012, 01, 01, i % 24, 00, 00)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:00:00, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicTimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [TIMESTAMP]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicTimestamp(new DateTime(2012, 01, 01, i % 24, 00, 00, 123)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:00:00.123, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultihreadTableWriter_BasicNanotime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                DateTime dt = new DateTime(2012, 1, 1, i % 24, 41, 45, 123);
                long tickCount = dt.Ticks;
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicNanoTime(new DateTime(tickCount + 4567L).TimeOfDay) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:41:45.123456700, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable16(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicNanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [NANOTIMESTAMP]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                DateTime dt = new DateTime(2012, 1, 1, i % 24, 41, 45, 123);
                long tickCount = dt.Ticks;
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicNanoTimestamp(new DateTime(tickCount + 4567L)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:41:45.123456700, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable16(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDouble()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DOUBLE]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDouble(i + 0.1) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(double(0..999999+0.1) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicFloat()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [FLOAT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicFloat((float)(i + 0.1)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(float(0..999999+0.1) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicString()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString("AA" + (i % 99).ToString()) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table('AA'+string(0..999999%99) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicSymbol()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SYMBOL]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString("AA" + (i % 99).ToString()) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(symbol('AA'+string(0..999999%99)) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicUuid()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [UUID]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicUuid(14, 15) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicUuid(14, 20) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(uuid(['00000000-0000-000e-0000-00000000000f','00000000-0000-000e-0000-000000000014']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicIpaddr()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [IPADDR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicIPAddr(14, 15) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicIPAddr(14, 20) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(ipaddr(['0::e:0:0:0:f','0::e:0:0:0:14']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicInt128()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT128]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt128(14, 15) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt128(14, 20) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(int128(['000000000000000e000000000000000f','000000000000000e0000000000000014']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal64()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL64(0),DECIMAL64(2),DECIMAL64(6),DECIMAL64(7)]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 7), new BasicDecimal64((i % 99).ToString(), 8) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal64(0..999999%99,0) as col0, decimal64(0..999999%99,2) as col1, decimal64(0..999999%99,6) as col2, decimal64(0..999999%99,7) as col3)");
            //compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            Assert.AreEqual(re.getColumn(0).get(0).getObject(), expected.getColumn(0).get(0).getObject());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal32()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 6), new BasicDecimal64((i % 99).ToString(), 7) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal32(0..999999%99,0) as col0, decimal32(0..999999%99,2) as col1, decimal32(0..999999%99,3) as col2, decimal32(0..999999%99,4) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal128()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL128(0),DECIMAL128(4),DECIMAL128(20),DECIMAL128(33)]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal128((i * 99).ToString(), 0), new BasicDecimal128((i % 99).ToString(), 4), new BasicDecimal128((i % 99).ToString(), 20), new BasicDecimal128((i % 99).ToString(), 33) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal128(0..999999*99,0) as col0, decimal128(0..999999%99,4) as col1, decimal128(0..999999%99,20) as col2, decimal128(0..999999 % 99,33) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal_dateType_change()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3`col4`col5`col6`col7, [DECIMAL128(0),DECIMAL128(4),DECIMAL128(37),DECIMAL64(0),DECIMAL32(8),DECIMAL32(0),DECIMAL32(4),INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            ErrorCodeInfo pErrorInfo = mtw.insert("1.011", "1.011", "1.011", "1.011", "1.011", "1.011", "1.011",  1);
            Thread.Sleep(100);
            Console.Out.WriteLine(pErrorInfo.ToString());

            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            Console.Out.WriteLine(status.ToString());
            BasicTable re = (BasicTable)conn.run("t1=select * from table1;\n t1");
            Assert.AreEqual("1", re.getColumn(0).get(0).getString());
            Assert.AreEqual("1.0110", re.getColumn(1).get(0).getString());
            Assert.AreEqual("1.0110000000000000000000000000000000000", re.getColumn(2).get(0).getString());
            Assert.AreEqual("1", re.getColumn(3).get(0).getString());
            Assert.AreEqual("1.01100000", re.getColumn(4).get(0).getString());
            Assert.AreEqual("1", re.getColumn(5).get(0).getString());
            Assert.AreEqual("1.0110", re.getColumn(6).get(0).getString());
            Assert.AreEqual(1, re.getColumn(7).get(0).getObject());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal64_compression_LZ4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL64(0),DECIMAL64(2),DECIMAL64(6),DECIMAL64(7)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_LZ4;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 6), new BasicDecimal64((i % 99).ToString(), 7) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal64(0..999999%99,0) as col0, decimal64(0..999999%99,2) as col1, decimal64(0..999999%99,6) as col2, decimal64(0..999999%99,7) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject().ToString(), expected.getColumn(i).get(j).getObject().ToString());

                }
            }
            Assert.AreEqual(re.getColumn(0).get(0).getObject(), expected.getColumn(0).get(0).getObject());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal64_compression_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL64(0),DECIMAL64(2),DECIMAL64(6),DECIMAL64(7)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 6), new BasicDecimal64((i % 99).ToString(), 7) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal64(0..999999%99,0) as col0, decimal64(0..999999%99,2) as col1, decimal64(0..999999%99,6) as col2, decimal64(0..999999%99,7) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            Assert.AreEqual(re.getColumn(0).get(0).getObject(), expected.getColumn(0).get(0).getObject());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal32_compression_LZ4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_LZ4;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 6), new BasicDecimal64((i % 99).ToString(), 7) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal32(0..999999%99,0) as col0, decimal32(0..999999%99,2) as col1, decimal32(0..999999%99,6) as col2, decimal32(0..999999%99,7) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal32_compression_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal64((i % 99).ToString(), 0), new BasicDecimal64((i % 99).ToString(), 2), new BasicDecimal64((i % 99).ToString(), 6), new BasicDecimal64((i % 99).ToString(), 7) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal32(0..999999%99,0) as col0, decimal32(0..999999%99,2) as col1, decimal32(0..999999%99,6) as col2, decimal32(0..999999%99,7) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal128_compression_LZ4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL128(0),DECIMAL128(4),DECIMAL128(20),DECIMAL128(33)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_LZ4;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal128((i * 99).ToString(), 0), new BasicDecimal128((i % 99).ToString(), 4), new BasicDecimal128((i % 99).ToString(), 20), new BasicDecimal128((i % 99).ToString(), 33) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal128(0..999999*99,0) as col0, decimal128(0..999999%99,4) as col1, decimal128(0..999999%99,20) as col2, decimal128(0..999999 % 99,33) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicDecimal128_compression_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL128(0),DECIMAL128(4),DECIMAL128(20),DECIMAL128(33)]) as table1";
            conn.run(script);
            int[] compressMethods = new int[4];
            for (int i = 0; i < 4; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDecimal128((i * 99).ToString(), 0), new BasicDecimal128((i % 99).ToString(), 4), new BasicDecimal128((i % 99).ToString(), 20), new BasicDecimal128((i % 99).ToString(), 33) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(decimal128(0..999999*99,0) as col0, decimal128(0..999999%99,4) as col1, decimal128(0..999999%99,20) as col2, decimal128(0..999999 % 99,33) as col3)");
            // compareBasicTable(re, expected);
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), expected.getColumn(i).get(j).getObject());

                }
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicArrayVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5, `col6, `col7, `col8, `col9, `col10, `col11, `col12, `col13, `col14, `col15, `col16, `col17, `col18], [INT[], BOOL[], SHORT[], CHAR[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DOUBLE[], FLOAT[], UUID[], INT128[], IPADDR[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 1000, 1, 1);
            conn.run("v1 = array(ANY, 0, 1000000);v2 = array(ANY, 0, 1000000);v3 = array(ANY, 0, 1000000);v4 = array(ANY, 0, 1000000);v5 = array(ANY, 0, 1000000);v6 = array(ANY, 0, 1000000);v7 = array(ANY, 0, 1000000);v8 = array(ANY, 0, 1000000);v9 = array(ANY, 0, 1000000);v10 = array(ANY, 0, 1000000);v11 = array(ANY, 0, 1000000);v12 = array(ANY, 0, 1000000);v13 = array(ANY, 0, 1000000);v14 = array(ANY, 0, 1000000);v15 = array(ANY, 0, 1000000);v16 = array(ANY, 0, 1000000);v17 = array(ANY, 0, 1000000);v18 = array(ANY, 0, 1000000);v19 = array(ANY, 0, 1000000);");
            for (int i = 0; i < 10000; i++)
            {

                BasicIntVector col1v = (BasicIntVector)conn.run("tmp = rand(-100..100, 10); v1.append!(tmp);tmp;");
                BasicBooleanVector col2v = (BasicBooleanVector)conn.run("tmp = rand([true, false], 10); v2.append!(tmp);tmp;");
                BasicShortVector col3v = (BasicShortVector)conn.run("tmp = rand(short(-100..100), 10); v3.append!(tmp);tmp;");
                BasicByteVector col4v = (BasicByteVector)conn.run("tmp = rand(char(1..100), 10); v4.append!(tmp);tmp;");
                BasicLongVector col5v = (BasicLongVector)conn.run("tmp = rand(long(-100..100), 10); v5.append!(tmp);tmp;");
                BasicDateVector col6v = (BasicDateVector)conn.run("tmp = rand(2012.01.01..2012.01.10, 10); v6.append!(tmp);tmp;");
                BasicMonthVector col7v = (BasicMonthVector)conn.run("tmp = rand(2012.01M..2012.12M, 10); v7.append!(tmp);tmp;");
                BasicTimeVector col8v = (BasicTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000, 0..100, 's'), 10); v8.append!(tmp);tmp;");
                BasicMinuteVector col9v = (BasicMinuteVector)conn.run("tmp = rand(09:00m..12:00m, 10); v9.append!(tmp);tmp;");
                BasicSecondVector col10v = (BasicSecondVector)conn.run("tmp = rand(09:00:00..12:00:00, 10); v10.append!(tmp);tmp;");
                BasicDateTimeVector col11v = (BasicDateTimeVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00, 0..100, 's'), 10); v11.append!(tmp);tmp;");
                BasicTimestampVector col12v = (BasicTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000, 0..100, 's'), 10); v12.append!(tmp);tmp;");
                BasicNanoTimeVector col13v = (BasicNanoTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000000000, 0..100, 's'), 10); v13.append!(tmp);tmp;");
                BasicNanoTimestampVector col14v = (BasicNanoTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000000000, 0..100, 's'), 10); v14.append!(tmp);tmp;");
                BasicDoubleVector col15v = (BasicDoubleVector)conn.run("tmp = rand(double(-100..100), 10); v15.append!(tmp);tmp;");
                BasicFloatVector col16v = (BasicFloatVector)conn.run("tmp = rand(float(-100..100), 10); v16.append!(tmp);tmp;");
                BasicUuidVector col17v = (BasicUuidVector)conn.run("tmp = rand(uuid(), 10); v17.append!(tmp);tmp;");
                BasicInt128Vector col18v = (BasicInt128Vector)conn.run("tmp = rand(int128(), 10); v18.append!(tmp);tmp;");
                BasicIPAddrVector col19v = (BasicIPAddrVector)conn.run("tmp = rand(ipaddr(), 10); v19.append!(tmp);tmp;");

                ErrorCodeInfo pErrorInfo = mtw.insert(col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v, col14v, col15v, col16v, col17v, col18v, col19v);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("t1=select * from table1;\n t1");
            Assert.AreEqual(10000, re.rows());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_BasicArrayVector_decimal()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5, `col6, `col7, `col8, `col9, `col10, `col11, `col12, `col13, `col14, `col15, `col16, `col17, `col18, `col19, `col20], [INT[], BOOL[], SHORT[], CHAR[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DOUBLE[], FLOAT[], UUID[], INT128[], IPADDR[], DECIMAL32(3)[], DECIMAL64(4)[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 1000, 1, 1);
            conn.run("v1 = array(ANY, 0, 1000000);v2 = array(ANY, 0, 1000000);v3 = array(ANY, 0, 1000000);v4 = array(ANY, 0, 1000000);v5 = array(ANY, 0, 1000000);v6 = array(ANY, 0, 1000000);v7 = array(ANY, 0, 1000000);v8 = array(ANY, 0, 1000000);v9 = array(ANY, 0, 1000000);v10 = array(ANY, 0, 1000000);v11 = array(ANY, 0, 1000000);v12 = array(ANY, 0, 1000000);v13 = array(ANY, 0, 1000000);v14 = array(ANY, 0, 1000000);v15 = array(ANY, 0, 1000000);v16 = array(ANY, 0, 1000000);v17 = array(ANY, 0, 1000000);v18 = array(ANY, 0, 1000000);v19 = array(ANY, 0, 1000000);v20 = array(ANY, 0, 1000000);v21 = array(ANY, 0, 1000000);");
            for (int i = 0; i < 10000; i++)
            {

                BasicIntVector col1v = (BasicIntVector)conn.run("tmp = rand(-100..100, 10); v1.append!(tmp);tmp;");
                BasicBooleanVector col2v = (BasicBooleanVector)conn.run("tmp = rand([true, false], 10); v2.append!(tmp);tmp;");
                BasicShortVector col3v = (BasicShortVector)conn.run("tmp = rand(short(-100..100), 10); v3.append!(tmp);tmp;");
                BasicByteVector col4v = (BasicByteVector)conn.run("tmp = rand(char(1..100), 10); v4.append!(tmp);tmp;");
                BasicLongVector col5v = (BasicLongVector)conn.run("tmp = rand(long(-100..100), 10); v5.append!(tmp);tmp;");
                BasicDateVector col6v = (BasicDateVector)conn.run("tmp = rand(2012.01.01..2012.01.10, 10); v6.append!(tmp);tmp;");
                BasicMonthVector col7v = (BasicMonthVector)conn.run("tmp = rand(2012.01M..2012.12M, 10); v7.append!(tmp);tmp;");
                BasicTimeVector col8v = (BasicTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000, 0..100, 's'), 10); v8.append!(tmp);tmp;");
                BasicMinuteVector col9v = (BasicMinuteVector)conn.run("tmp = rand(09:00m..12:00m, 10); v9.append!(tmp);tmp;");
                BasicSecondVector col10v = (BasicSecondVector)conn.run("tmp = rand(09:00:00..12:00:00, 10); v10.append!(tmp);tmp;");
                BasicDateTimeVector col11v = (BasicDateTimeVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00, 0..100, 's'), 10); v11.append!(tmp);tmp;");
                BasicTimestampVector col12v = (BasicTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000, 0..100, 's'), 10); v12.append!(tmp);tmp;");
                BasicNanoTimeVector col13v = (BasicNanoTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000000000, 0..100, 's'), 10); v13.append!(tmp);tmp;");
                BasicNanoTimestampVector col14v = (BasicNanoTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000000000, 0..100, 's'), 10); v14.append!(tmp);tmp;");
                BasicDoubleVector col15v = (BasicDoubleVector)conn.run("tmp = rand(double(-100..100), 10); v15.append!(tmp);tmp;");
                BasicFloatVector col16v = (BasicFloatVector)conn.run("tmp = rand(float(-100..100), 10); v16.append!(tmp);tmp;");
                BasicUuidVector col17v = (BasicUuidVector)conn.run("tmp = rand(uuid(), 10); v17.append!(tmp);tmp;");
                BasicInt128Vector col18v = (BasicInt128Vector)conn.run("tmp = rand(int128(), 10); v18.append!(tmp);tmp;");
                BasicIPAddrVector col19v = (BasicIPAddrVector)conn.run("tmp = rand(ipaddr(), 10); v19.append!(tmp);tmp;");
                BasicDecimal32Vector col20v = (BasicDecimal32Vector)conn.run("tmp = take(decimal32(3,3), 10); v20.append!(tmp);tmp;");
                BasicDecimal64Vector col21v = (BasicDecimal64Vector)conn.run("tmp = take(decimal64(3,4), 10); v21.append!(tmp);tmp;");
                ErrorCodeInfo pErrorInfo = mtw.insert(col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v, col14v, col15v, col16v, col17v, col18v, col19v, col20v, col21v);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("t1=select * from table1;\n t1");
            Assert.AreEqual(10000, re.rows());

            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal_to_partition_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int");

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal32[] { new BasicDecimal32(1, 0), new BasicDecimal32("1.00", 0), new BasicDecimal32("3.0001", 0), new BasicDecimal32("99999.99999999999", 0) }, new BasicDecimal32[] { new BasicDecimal32(1, 3), new BasicDecimal32("1.00", 3), new BasicDecimal32("3.0001", 3), new BasicDecimal32("99999.99999999999", 3) }, new BasicDecimal64[] { new BasicDecimal64(1, 0), new BasicDecimal64("1.00", 0), new BasicDecimal64("3.0001", 0), new BasicDecimal64("99999.99999999999", 0) }, new BasicDecimal64[] { new BasicDecimal64(1, 4), new BasicDecimal64("1.00", 4), new BasicDecimal64("3.0001", 4), new BasicDecimal64("99999.99999999999", 4) }, new BasicDecimal64[] { new BasicDecimal64(1, 8), new BasicDecimal64("1.00", 8), new BasicDecimal64("3.0001", 8), new BasicDecimal64("99999.99999999999", 8) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal32Vector decv1 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal32Vector decv2 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
            BasicDecimal64Vector decv3 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal64Vector decv4 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
            BasicDecimal64Vector decv5 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal_to_partition_table_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int");

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new decimal[] { 1m, 1m, 3m, 99999m }, new string[] { "1.000", "1.000", "3.000", "100000.000" }, new decimal[] { 1m, 1m, 3m, 99999m }, new decimal[] { 1.0000m, 1.0000m, 3.0001m, 100000.0000m }, new string[] { "1.00000000", "1.00000000", "3.00010000", "100000.00000000" });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal32Vector decv1 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.09999999999],0)");
            BasicDecimal32Vector decv2 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
            BasicDecimal64Vector decv3 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.09999999999],0)");
            BasicDecimal64Vector decv4 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
            BasicDecimal64Vector decv5 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal_to_partition_table_compress_lz4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int", new int[] { Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4 });

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal32[] { new BasicDecimal32(1, 0), new BasicDecimal32("1.00", 0), new BasicDecimal32("3.0001", 0), new BasicDecimal32("99999.99999999999", 0) }, new BasicDecimal32[] { new BasicDecimal32(1, 3), new BasicDecimal32("1.00", 3), new BasicDecimal32("3.0001", 3), new BasicDecimal32("99999.99999999999", 3) }, new BasicDecimal64[] { new BasicDecimal64(1, 0), new BasicDecimal64("1.00", 0), new BasicDecimal64("3.0001", 0), new BasicDecimal64("99999.99999999999", 0) }, new BasicDecimal64[] { new BasicDecimal64(1, 4), new BasicDecimal64("1.00", 4), new BasicDecimal64("3.0001", 4), new BasicDecimal64("99999.99999999999", 4) }, new BasicDecimal64[] { new BasicDecimal64(1, 8), new BasicDecimal64("1.00", 8), new BasicDecimal64("3.0001", 8), new BasicDecimal64("99999.99999999999", 8) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal32Vector decv1 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal32Vector decv2 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
            BasicDecimal64Vector decv3 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal64Vector decv4 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
            BasicDecimal64Vector decv5 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal_to_partition_table_compress_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int", new int[] { Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA });

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal32[] { new BasicDecimal32(1, 0), new BasicDecimal32("1.00", 0), new BasicDecimal32("3.0001", 0), new BasicDecimal32("99999.99999999999", 0) }, new BasicDecimal32[] { new BasicDecimal32(1, 3), new BasicDecimal32("1.00", 3), new BasicDecimal32("3.0001", 3), new BasicDecimal32("99999.99999999999", 3) }, new BasicDecimal64[] { new BasicDecimal64(1, 0), new BasicDecimal64("1.00", 0), new BasicDecimal64("3.0001", 0), new BasicDecimal64("99999.99999999999", 0) }, new BasicDecimal64[] { new BasicDecimal64(1, 4), new BasicDecimal64("1.00", 4), new BasicDecimal64("3.0001", 4), new BasicDecimal64("99999.99999999999", 4) }, new BasicDecimal64[] { new BasicDecimal64(1, 8), new BasicDecimal64("1.00", 8), new BasicDecimal64("3.0001", 8), new BasicDecimal64("99999.99999999999", 8) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal32Vector decv1 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal32Vector decv2 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
            BasicDecimal64Vector decv3 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal64Vector decv4 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
            BasicDecimal64Vector decv5 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal_to_memory_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append(
                    "share table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL32(0)[],DECIMAL32(3)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[]]) as tt1 ;");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "", "tt1", false, false, null, 1, 1,
                    1, "int");

            for (short i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal32[] { new BasicDecimal32(1, 0), new BasicDecimal32("1.00", 0), new BasicDecimal32("3.0001", 0), new BasicDecimal32("99999.99999999999", 0) }, new BasicDecimal32[] { new BasicDecimal32(1, 3), new BasicDecimal32("1.00", 3), new BasicDecimal32("3.0001", 3), new BasicDecimal32("99999.99999999999", 3) }, new BasicDecimal64[] { new BasicDecimal64(1, 0), new BasicDecimal64("1.00", 0), new BasicDecimal64("3.0001", 0), new BasicDecimal64("99999.99999999999", 0) }, new BasicDecimal64[] { new BasicDecimal64(1, 4), new BasicDecimal64("1.00", 4), new BasicDecimal64("3.0001", 4), new BasicDecimal64("99999.99999999999", 4) }, new BasicDecimal64[] { new BasicDecimal64(1, 8), new BasicDecimal64("1.00", 8), new BasicDecimal64("3.0001", 8), new BasicDecimal64("99999.99999999999", 8) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from tt1;");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal32Vector decv1 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal32Vector decv2 = (BasicDecimal32Vector)conn.run("decimal32([1,1.00,3.0001,99999.99999999999],3)");
            BasicDecimal64Vector decv3 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal64Vector decv4 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],4)");
            BasicDecimal64Vector decv5 = (BasicDecimal64Vector)conn.run("decimal64([1,1.00,3.0001,99999.99999999999],8)");

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
            conn.run("undef(`tt1,SHARED)");
            conn.close();
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal128_to_partition_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,-1 1000 2000,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int");

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal128[] { new BasicDecimal128("1", 0), new BasicDecimal128("1.00", 0), new BasicDecimal128("3.0001", 0), new BasicDecimal128("99999.99999999999", 0) }, new BasicDecimal128[] { new BasicDecimal128("1", 1), new BasicDecimal128("1.00", 1), new BasicDecimal128("3.0001", 1), new BasicDecimal128("99999.99999999999", 1) }, new BasicDecimal128[] { new BasicDecimal128("1", 10), new BasicDecimal128("1.00", 10), new BasicDecimal128("3.0001", 10), new BasicDecimal128("99999.99999999999", 10) }, new BasicDecimal128[] { new BasicDecimal128("1", 18), new BasicDecimal128("1.00", 18), new BasicDecimal128("3.0001", 18), new BasicDecimal128("99999.99999999999", 18) }, new BasicDecimal128[] { new BasicDecimal128("1", 30), new BasicDecimal128("1.00", 30), new BasicDecimal128("3.0001", 30), new BasicDecimal128("99999.99999999999", 30) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            Console.WriteLine(bt1.getColumn("col3").getString());
            BasicDecimal128Vector decv1 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal128Vector decv2 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
            BasicDecimal128Vector decv3 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
            String decv4 = "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
            String decv5 = "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal128_to_partition_table_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,-1 1000 2000,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int");

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new decimal[] { 1m, 1m, 3m, 100000m }, new string[] { "1.0", "1.0", "3.0", "100000.0" }, new string[] { "1", "1.00", "3.0001", "99999.99999999999" }, new decimal[] { 1m, 1.00m, 3.0001m, 99999.99999999999m }, new string[] { "1", "1.00",  "3.0001","99999.99999999999" });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            Console.WriteLine(bt1.getColumn("col3").getString());
            BasicDecimal128Vector decv1 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal128Vector decv2 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
            BasicDecimal128Vector decv3 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
            String decv4 = "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
            String decv5 = "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal128_to_partition_table_compress_lz4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int", new int[] { Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4, Vector_Fields.COMPRESS_LZ4 });

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal128[] { new BasicDecimal128("1", 0), new BasicDecimal128("1.00", 0), new BasicDecimal128("3.0001", 0), new BasicDecimal128("99999.99999999999", 0) }, new BasicDecimal128[] { new BasicDecimal128(1, 1), new BasicDecimal128("1.00", 1), new BasicDecimal128("3.0001", 1), new BasicDecimal128("99999.99999999999", 1) }, new BasicDecimal128[] { new BasicDecimal128(1, 10), new BasicDecimal128("1.00", 10), new BasicDecimal128("3.0001", 10), new BasicDecimal128("99999.99999999999", 10) }, new BasicDecimal128[] { new BasicDecimal128(1, 18), new BasicDecimal128("1.00", 18), new BasicDecimal128("3.0001", 18), new BasicDecimal128("99999.99999999999", 18) }, new BasicDecimal128[] { new BasicDecimal128(1, 30), new BasicDecimal128("1.00", 30), new BasicDecimal128("3.0001", 30), new BasicDecimal128("99999.99999999999", 30) });

                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal128Vector decv1 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal128Vector decv2 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
            BasicDecimal128Vector decv3 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
            String decv4 = "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
            String decv5 = "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());
            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal128_to_partition_table_compress_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append("dbName = 'dfs://test_arrayVector_in_partition_table';\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "\tdropDB(dbName);\n" +
                    "}\n" +
                    "db = database(dbName,RANGE,0 5 10 15 20,,'TSDB')\n" +
                    "t = table(1000:0, `int`col0`col1`col2`col3`col4," +
                    "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]);" +
                    "pt = db.createPartitionedTable(t,`pt,`int,,`int);");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "dfs://test_arrayVector_in_partition_table", "pt", false, false, null, 1, 1,
                    1, "int", new int[] { Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA, Vector_Fields.COMPRESS_DELTA });

            for (int i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal128[] { new BasicDecimal128("1", 0), new BasicDecimal128("1.00", 0), new BasicDecimal128("3.0001", 0), new BasicDecimal128("99999.99999999999", 0) }, new BasicDecimal128[] { new BasicDecimal128(1, 1), new BasicDecimal128("1.00", 1), new BasicDecimal128("3.0001", 1), new BasicDecimal128("99999.99999999999", 1) }, new BasicDecimal128[] { new BasicDecimal128(1, 10), new BasicDecimal128("1.00", 10), new BasicDecimal128("3.0001", 10), new BasicDecimal128("99999.99999999999", 10) }, new BasicDecimal128[] { new BasicDecimal128(1, 18), new BasicDecimal128("1.00", 18), new BasicDecimal128("3.0001", 18), new BasicDecimal128("99999.99999999999", 18) }, new BasicDecimal128[] { new BasicDecimal128(1, 30), new BasicDecimal128("1.00", 30), new BasicDecimal128("3.0001", 30), new BasicDecimal128("99999.99999999999", 30) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_arrayVector_in_partition_table\",`pt);");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal128Vector decv1 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal128Vector decv2 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
            BasicDecimal128Vector decv3 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
            BasicDecimal128Vector decv4 = new BasicDecimal128Vector(new string[] { "1", "1.00", "3.0001", "99999.99999999999" },18);
            BasicDecimal128Vector decv5 = new BasicDecimal128Vector(new string[] { "1", "1.00", "3.0001", "99999.99999999999" }, 30);

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4.getString(), ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5.getString(), ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());

            }
        }

        [TestMethod]
        public void test_insert_arrayVector_decimal128_to_memory_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            StringBuilder sb = new StringBuilder();
            sb.Append(
                    "share table(1000:0, `int`col0`col1`col2`col3`col4," +
                            "[INT,DECIMAL128(0)[],DECIMAL128(1)[],DECIMAL128(10)[],DECIMAL128(18)[],DECIMAL128(30)[]]) as tt1 ;");
            int time = 1048;
            conn.run(sb.ToString());
            MultithreadedTableWriter mutithreadTableWriter_ = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD,
                    "", "tt1", false, false, null, 1, 1,
                    1, "int");

            for (short i = 0; i < time; i++)
            {
                ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(1, new BasicDecimal128[] { new BasicDecimal128("1", 0), new BasicDecimal128("1.00", 0), new BasicDecimal128("3.0001", 0), new BasicDecimal128("99999.99999999999", 0) }, new BasicDecimal128[] { new BasicDecimal128(1, 1), new BasicDecimal128("1.00", 1), new BasicDecimal128("3.0001", 1), new BasicDecimal128("99999.99999999999", 1) }, new BasicDecimal128[] { new BasicDecimal128(1, 10), new BasicDecimal128("1.00", 10), new BasicDecimal128("3.0001", 10), new BasicDecimal128("99999.99999999999", 10) }, new BasicDecimal128[] { new BasicDecimal128(1, 18), new BasicDecimal128("1.00", 18), new BasicDecimal128("3.0001", 18), new BasicDecimal128("99999.99999999999", 18) }, new BasicDecimal128[] { new BasicDecimal128(1, 30), new BasicDecimal128("1.00", 30), new BasicDecimal128("3.0001", 30), new BasicDecimal128("99999.99999999999", 30) });
                //第一列写成i不会有问题
                //ErrorCodeInfo pErrorInfo = mutithreadTableWriter_.insert(i, new BasicDecimal128[] { new BasicDecimal128("1", 0), new BasicDecimal128("1.00", 0), new BasicDecimal128("3.0001", 0), new BasicDecimal128("99999.99999999999", 0) }, new BasicDecimal128[] { new BasicDecimal128(1, 1), new BasicDecimal128("1.00", 1), new BasicDecimal128("3.0001", 1), new BasicDecimal128("99999.99999999999", 1) }, new BasicDecimal128[] { new BasicDecimal128(1, 10), new BasicDecimal128("1.00", 10), new BasicDecimal128("3.0001", 10), new BasicDecimal128("99999.99999999999", 10) }, new BasicDecimal128[] { new BasicDecimal128(1, 18), new BasicDecimal128("1.00", 18), new BasicDecimal128("3.0001", 18), new BasicDecimal128("99999.99999999999", 18) }, new BasicDecimal128[] { new BasicDecimal128(1, 30), new BasicDecimal128("1.00", 30), new BasicDecimal128("3.0001", 30), new BasicDecimal128("99999.99999999999", 30) });
                Assert.AreEqual("code =  info = ", pErrorInfo.ToString());
            }
            mutithreadTableWriter_.waitForThreadCompletion();
            BasicTable bt1 = (BasicTable)conn.run("select * from tt1;");
            Assert.AreEqual(time, bt1.rows());
            BasicDecimal128Vector decv1 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],0)");
            BasicDecimal128Vector decv2 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],1)");
            BasicDecimal128Vector decv3 = (BasicDecimal128Vector)conn.run("decimal128([1,1.00,3.0001,99999.99999999999],10)");
            String decv4 = "[1.000000000000000000,1.000000000000000000,3.000100000000000000,99999.999999999990000000]";
            String decv5 = "[1.000000000000000000000000000000,1.000000000000000000000000000000,3.000100000000000000000000000000,99999.999999999990000000000000000000]";

            for (int i = 0; i < time; i++)
            {
                Assert.AreEqual(decv1.getString(), ((BasicArrayVector)(bt1.getColumn("col0"))).getSubVector(i).getString());
                Assert.AreEqual(decv2.getString(), ((BasicArrayVector)(bt1.getColumn("col1"))).getSubVector(i).getString());
                Assert.AreEqual(decv3.getString(), ((BasicArrayVector)(bt1.getColumn("col2"))).getSubVector(i).getString());
                Assert.AreEqual(decv4, ((BasicArrayVector)(bt1.getColumn("col3"))).getSubVector(i).getString());
                Assert.AreEqual(decv5, ((BasicArrayVector)(bt1.getColumn("col4"))).getSubVector(i).getString());
            }
            conn.run("undef(`tt1,SHARED)");
            conn.close();
        }

        //raw datatype
        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_bool()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(true);
                }
                else if (i % 2 == 1)
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(false);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take([true, false], 1000000) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_byte()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [CHAR, INT, SHORT, LONG]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert((byte)(i % 99), (byte)(i % 99), (byte)(i % 99), (byte)(i % 99));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(take(char(0..98), 1000000) as col0, take(0..98, 1000000) as col1, take(short(0..98), 1000000) as col2, take(long(0..98), 1000000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_short()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [CHAR, INT, SHORT, LONG]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert((short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(take(char(0..98), 1000000) as col0, take(0..98, 1000000) as col1, take(short(0..98), 1000000) as col2, take(long(0..98), 1000000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_int()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [CHAR, INT, SHORT, LONG]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(i % 99, i - 500000, i % 99, i - 500000);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(take(char(0..98), 1000000) as col0, (0..999999) - 500000 as col1, take(short(0..98), 1000000) as col2, long((0..999999) - 500000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_long()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [CHAR, INT, SHORT, LONG]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert((long)(i % 99), (long)(i - 500000), (long)(i % 99), (long)(i - 500000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(take(char(0..98), 1000000) as col0, (0..999999) - 500000 as col1, take(short(0..98), 1000000) as col2, long((0..999999) - 500000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_double()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1], [FLOAT, DOUBLE]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert((double)(i - 500000), (double)(i - 500000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(float((0..999999) - 500000) as col0, double((0..999999) - 500000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_float()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1], [FLOAT, DOUBLE]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert((float)(i - 500000), (float)(i - 500000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(float((0..999999) - 500000) as col0, double((0..999999) - 500000) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_string()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4], [STRING, SYMBOL, UUID, IPADDR, INT128]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            string[] v1 = new string[] { "5d212a78-cc48-e3b1-4235-b4d91473ee85", "5d212a78-cc48-e3b1-4235-b4d91473ee86", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "00000000-0000-0000-0000-000000000000" };
            string[] v2 = new string[] { "192.168.1.13", "192.168.1.14", "d0db:d6d:d253:8222:d525:31b6:62d4:c774", "d0db:d6d:d253:8222:d525:31b6:62d4:c774", "0.0.0.0" };
            string[] v3 = new string[] { "e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34", "e1671797c52e15f763380b45e841ec35", "00000000000000000000000000000000" };
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert("A" + i.ToString(), "B" + i.ToString(), v1[i % 5], v2[i % 5], v3[i % 5]);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(\"A\"+string(0..999999) as col0, symbol(\"B\"+string(0..999999)) as col1, take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee85\", \"5d212a78-cc48-e3b1-4235-b4d91473ee86\", \"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", NULL]), 1000000) as col2, take(ipaddr([\"192.168.1.13\", \"192.168.1.14\", \"d0db:d6d:d253:8222:d525:31b6:62d4:c774\", \"d0db:d6d:d253:8222:d525:31b6:62d4:c774\", NULL]), 1000000) as col3, take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec34\", \"e1671797c52e15f763380b45e841ec35\", NULL]), 1000000) as col4)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_DateTime_only_date()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5], [DATE, MONTH, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new DateTime(1969, 1, 1).AddDays(i % 3000), new DateTime(1969, 1, 1).AddDays(i % 3000), new DateTime(1969, 1, 1).AddDays(i % 3000), new DateTime(1969, 1, 1).AddDays(i % 3000), new DateTime(1969, 1, 1).AddDays(i % 3000), new DateTime(1969, 1, 1).AddDays(i % 3000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(1969.01.01+0..999999 % 3000 as col0, month(1969.01.01+0..999999 % 3000) as col1, datetime(1969.01.01+0..999999 % 3000) as col2, timestamp(1969.01.01+0..999999 % 3000) as col3, nanotimestamp(1969.01.01+0..999999 % 3000) as col4, datehour(1969.01.01+0..999999 % 3000) as col5)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_DateTime_second()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5], [DATE, MONTH, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36).AddDays(i % 3000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(date(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col0, month(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col1, datetime(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col2, timestamp(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col3, nanotimestamp(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col4, datehour(temporalAdd(1969.01.01T12:58:36, 0..999999 % 3000, 'd')) as col5)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_DateTime_millisecond()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5], [DATE, MONTH, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000), new DateTime(1969, 1, 1, 12, 58, 36, 008).AddDays(i % 3000));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(date(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col0, month(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col1, datetime(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col2, timestamp(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col3, nanotimestamp(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col4, datehour(temporalAdd(1969.01.01T12:58:36.008, 0..999999 % 3000, 'd')) as col5)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_one_argument()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(i), new TimeSpan(i), new TimeSpan(i), new TimeSpan(i));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(take(00:00m, 1000000) as col0, take(00:00:00, 1000000) as col1, time(00:00:00.000000000+0..999999 * 100) as col2, 00:00:00.000000000+0..999999 * 100 as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_three_argument()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(12, 15, i % 60), new TimeSpan(12, 15, i % 60), new TimeSpan(12, 15, i % 60), new TimeSpan(12, 15, i % 60));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(minute(take(12:15:00 + 0..59, 1000000)) as col0, second(take(12:15:00 + 0..59, 1000000)) as col1, time(take(12:15:00 + 0..59, 1000000)) as col2, nanotime(take(12:15:00 + 0..59, 1000000)) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_four_argument()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(0, 12, 15, i % 60), new TimeSpan(0, 12, 15, i % 60), new TimeSpan(0, 12, 15, i % 60), new TimeSpan(0, 12, 15, i % 60));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(minute(take(12:15:00 + 0..59, 1000000)) as col0, second(take(12:15:00 + 0..59, 1000000)) as col1, time(take(12:15:00 + 0..59, 1000000)) as col2, nanotime(take(12:15:00 + 0..59, 1000000)) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_four_argument_day_not_zero()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(1, 12, 15, 59), new TimeSpan(1, 12, 15, 59), new TimeSpan(1, 12, 15, 59), new TimeSpan(1, 12, 15, 59));
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            string re = pErrorInfo.ToString();
            Assert.IsNotNull(re);
            BasicInt num = (BasicInt)conn.run("exec count(*) from table1");
            Assert.AreEqual(num.getInt(), 0);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_five_argument()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {

                ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(0, 12, 15, i % 60, 23), new TimeSpan(0, 12, 15, i % 60, 23), new TimeSpan(0, 12, 15, i % 60, 23), new TimeSpan(0, 12, 15, i % 60, 23));
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("ex = table(minute(take(temporalAdd(12:15:00.023, 0..59, 's'), 1000000)) as col0, second(take(temporalAdd(12:15:00.023, 0..59, 's'), 1000000)) as col1, time(take(temporalAdd(12:15:00.023, 0..59, 's'), 1000000)) as col2, nanotime(take(temporalAdd(12:15:00.023, 0..59, 's'), 1000000)) as col3)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_TimeSpan_five_argument_day_not_zero()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [MINUTE, SECOND, TIME, NANOTIME]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            ErrorCodeInfo pErrorInfo = mtw.insert(new TimeSpan(1, 12, 15, 25, 23), new TimeSpan(1, 12, 15, 25, 23), new TimeSpan(1, 12, 15, 25, 23), new TimeSpan(1, 12, 15, 25, 23));
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            string re = pErrorInfo.ToString();
            Assert.IsNotNull(re);
            BasicInt num = (BasicInt)conn.run("exec count(*) from table1");
            Assert.AreEqual(num.getInt(), 0);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_bool_array()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    bool[] col1v = new bool[] { true, false, true, false, true, false, true, false, true, false };
                    ErrorCodeInfo pErrorInfo = mtw.insert(col1v);
                }
                else
                {
                    bool[] col1v = new bool[] { true, false, true, false, true };
                    ErrorCodeInfo pErrorInfo = mtw.insert(col1v);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v1 = [true, false, true, false, true, false, true, false, true, false];v2 = [true, false, true, false, true];v3 = take([v1, v2], 1000000);v4 = array(BOOL[], 0, 1000000).append!(v3)");
            BasicBoolean re = (BasicBoolean)conn.run("eqObj(table1[`col0], v4)");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_short_array()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3], [CHAR[], INT[], SHORT[], LONG[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    short[] col0v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col1v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col2v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col3v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    ErrorCodeInfo pErrorInfo = mtw.insert(col0v, col1v, col2v, col3v);
                }
                else
                {
                    short[] col0v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col1v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col2v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    short[] col3v = new short[] { (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99), (short)(i % 99) };
                    ErrorCodeInfo pErrorInfo = mtw.insert(col0v, col1v, col2v, col3v);
                }

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            String script2 = "ex = table(1000000:0, `col0`col1`col2`col3, [CHAR[], INT[], SHORT[], LONG[]]);";
            script2 += "for(i in 0..999999){ \n";
            script2 += "    if(i%2==0){ \n";
            script2 += "        tmp  = table(arrayVector([10], char(take(i%99, 10))) as col0, arrayVector([10], take(i%99, 10)) as col1, arrayVector([10], short(take(i%99, 10))) as col2, arrayVector([10], long(take(i%99, 10))) as col3) \n";
            script2 += "    }else{ \n";
            script2 += "        tmp  = table(arrayVector([5], char(take(i%99, 5))) as col0, arrayVector([5], take(i%99, 5)) as col1, arrayVector([5], short(take(i%99, 5))) as col2, arrayVector([5], long(take(i%99, 5))) as col3) \n";
            script2 += "    } \n";
            script2 += "    ex.append!(tmp) \n";
            script2 += "}";
            //Console.WriteLine(script2);
            conn.run(script2);
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, table1.values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_satisfied_batchSize_not_satisfied_throttle()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 30, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(30000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 30000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..29999 as id, 'AA'+string(0..29999%99) as sym, 'BB'+string(0..29999%99) as str, 0..29999%999+0.1 as price, 0..29999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_satisfied_throttle_not_satisfied_batchSize()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 5, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(30000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 30000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..29999 as id, 'AA'+string(0..29999%99) as sym, 'BB'+string(0..29999%99) as str, 0..29999%999+0.1 as price, 0..29999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_object_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, \"col\"+string(1..22), [INT, BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, UUID, INT128, IPADDR]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null);

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("n=1000000;table(0..999999 as col0, take(bool(), n) as col1, take(char(), n) as col2, take(short(), n) as col3, take(int(), n) as col4, take(long(), n) as col5, take(date(), n) as col6, take(month(), n) as col7, take(time(), n) as col8, take(minute(), n) as col9, take(second(), n) as col10, take(datetime(), n) as col11, take(timestamp(), n) as col12, take(nanotime(), n) as col13, take(nanotimestamp(), n) as col14, take(float(), n) as col15, take(double(), n) as col16, symbol(take(string(), n)) as col17, take(string(), n) as col18, take(uuid(), n) as col19, take(int128(), n) as col20, take(ipaddr(), n) as col21)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_raw_datatype_empty_array()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, \"col\"+string(1..20), [INT, BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], UUID[], INT128[], IPADDR[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            bool[] col1v = new bool[] { };
            byte[] col2v = new byte[] { };
            short[] col3v = new short[] { };
            int[] col4v = new int[] { };
            long[] col5v = new long[] { };
            DateTime[] col6v = new DateTime[] { };
            DateTime[] col7v = new DateTime[] { };
            TimeSpan[] col8v = new TimeSpan[] { };
            TimeSpan[] col9v = new TimeSpan[] { };
            TimeSpan[] col10v = new TimeSpan[] { };
            DateTime[] col11v = new DateTime[] { };
            DateTime[] col12v = new DateTime[] { };
            TimeSpan[] col13v = new TimeSpan[] { };
            DateTime[] col14v = new DateTime[] { };
            float[] col15v = new float[] { };
            double[] col16v = new double[] { };
            string[] col17v = new string[] { };
            string[] col18v = new string[] { };
            string[] col19v = new string[] { };
            for (int i = 0; i < 100000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v, col14v, col15v, col16v, col17v, col18v, col19v);

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("n=100000; vec = 1..n; ex = table(0..(n - 1) as col0, arrayVector(vec, take(bool(), n)) as col1, arrayVector(vec, take(char(), n)) as col2, arrayVector(vec, take(short(), n)) as col3, arrayVector(vec, take(int(), n)) as col4, arrayVector(vec, take(long(), n)) as col5,"
              + "arrayVector(vec, take(date(), n)) as col6, arrayVector(vec, take(month(), n)) as col7, arrayVector(vec, take(time(), n)) as col8, arrayVector(vec, take(minute(), n)) as col9, arrayVector(vec, take(second(), n)) as col10, arrayVector(vec, take(datetime(), n)) as col11, arrayVector(vec, take(timestamp(), n)) as col12,"
               + "arrayVector(vec, take(nanotime(), n)) as col13, arrayVector(vec, take(nanotimestamp(), n)) as col14, arrayVector(vec, take(float(), n)) as col15, arrayVector(vec, take(double(), n)) as col16,"
               + "arrayVector(vec, take(uuid(), n)) as col17,arrayVector(vec, take(int128(), n)) as col18, arrayVector(vec, take(ipaddr(), n)) as col19)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from table1 order by col1).values(), (select * from ex order by col0).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        //in-memory table huge data

        [TestMethod]
        public void Test_MultithreadedTableWriter_in_memory_table_huge_data_one_thread()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 1);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_in_memory_table_multi_threadCount()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_stream_table_single_threadCount()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share streamTable(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 1);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_stream_table_multi_threadCount()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share streamTable(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_keyedTable_single_threadCount()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share keyedTable(`id, 3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 1, 1);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_getStatus()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 10, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            Thread.Sleep(1000);
            MultithreadedTableWriter.Status status1 = mtw.getStatus();
            long sentRows1 = status1.threadStatus[0].sentRows;
            long unsentRows1 = status1.threadStatus[0].unsentRows;
            long sendFailedRows1 = status1.threadStatus[0].sendFailedRows;
            bool isExiting1 = status1.isExiting;
            Assert.AreEqual(sentRows1, 0);
            Assert.AreEqual(unsentRows1, 30000);
            Assert.AreEqual(sendFailedRows1, 0);
            Assert.AreEqual(isExiting1, false);
            Thread.Sleep(11000);
            mtw.waitForThreadCompletion();
            MultithreadedTableWriter.Status status2 = mtw.getStatus();
            long sentRows2 = status2.threadStatus[0].sentRows;
            long unsentRows2 = status2.threadStatus[0].unsentRows;
            long sendFailedRows2 = status2.threadStatus[0].sendFailedRows;
            bool isExiting2 = status2.isExiting;
            Assert.AreEqual(sentRows2, 30000);
            Assert.AreEqual(unsentRows2, 0);
            Assert.AreEqual(sendFailedRows2, 0);
            Assert.AreEqual(isExiting2, true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_waitForThreadCompletion()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 10, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            Thread.Sleep(1000);
            mtw.waitForThreadCompletion();
            MultithreadedTableWriter.Status status = mtw.getStatus();
            long sentRows2 = status.threadStatus[0].sentRows;
            long unsentRows2 = status.threadStatus[0].unsentRows;
            long sendFailedRows2 = status.threadStatus[0].sendFailedRows;
            bool isExiting2 = status.isExiting;
            Assert.AreEqual(sentRows2, 30000);
            Assert.AreEqual(unsentRows2, 0);
            Assert.AreEqual(sendFailedRows2, 0);
            Assert.AreEqual(isExiting2, true);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_getUnwrittenData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 100000, 5, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            List<List<IEntity>> unwritten1 = mtw.getUnwrittenData();
            int len1 = 0;
            len1 = unwritten1.Count;
            Assert.AreEqual(len1, 30000);
            Thread.Sleep(7000);
            List<List<IEntity>> unwritten2 = mtw.getUnwrittenData();
            int len2 = 0;
            len2 = unwritten2.Count;
            Assert.AreEqual(len2, 0);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_insertUnwrittenData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");

            MultithreadedTableWriter mtw2 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");

            MultithreadedTableWriter mtw3 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(1), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo1 = mtw1.insert(x);
            }
            mtw1.waitForThreadCompletion();
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(1), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo2 = mtw2.insert(x);
            }
            mtw2.waitForThreadCompletion();
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(1), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo3 = mtw3.insert(x);
            }
            mtw3.waitForThreadCompletion();
            Thread.Sleep(7000);
            List<List<IEntity>> unwritten1 = mtw1.getUnwrittenData();
            List<List<IEntity>> unwritten2 = mtw2.getUnwrittenData();
            List<List<IEntity>> unwritten3 = mtw3.getUnwrittenData();
            ////Console.WriteLine(unwritten1.Count);
            ////Console.WriteLine(unwritten2.Count);
            ////Console.WriteLine(unwritten3.Count);
            MultithreadedTableWriter mtw4 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");

            MultithreadedTableWriter mtw5 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");

            MultithreadedTableWriter mtw6 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 5, 5, "id");
            if (unwritten1.Count > 0)
            {
                ErrorCodeInfo pErrorInfo1 = mtw4.insertUnwrittenData(unwritten1);
                //Console.WriteLine(pErrorInfo1.errorInfo);
            }
            mtw4.waitForThreadCompletion();
            if (unwritten2.Count > 0)
            {
                ErrorCodeInfo pErrorInfo2 = mtw5.insertUnwrittenData(unwritten2);
                //Console.WriteLine(pErrorInfo2.errorInfo);
            }
            mtw5.waitForThreadCompletion();
            if (unwritten3.Count > 0)
            {
                ErrorCodeInfo pErrorInfo3 = mtw6.insertUnwrittenData(unwritten3);
                //Console.WriteLine(pErrorInfo3.errorInfo);
            }
            mtw6.waitForThreadCompletion();
            BasicInt total = (BasicInt)conn.run("exec count(*) from loadTable(\"dfs://test_MultithreadedTableWriter\", \"pt\")");
            Assert.AreEqual(total.getValue(), 90000);
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_useSSL_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(3000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", true, false, null, 100000, 1, 1);
            for (int i = 0; i < 30000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                if (total.Equals(30000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 30000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from table1 order by id, sym, price");
            conn.run("expected = table(0..29999 as id, 'AA'+string(0..29999%99) as sym, 'BB'+string(0..29999%99) as str, 0..29999%999+0.1 as price, 0..29999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            mtw.waitForThreadCompletion();
            conn.close();

        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_huge_data_single_thread()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 1, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(2000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_huge_data_single_thread_tsdb()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10], engine=\"TSDB\");";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `sym`id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 1, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(3000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_huge_data_multiple_thread_tsdb()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10], engine=\"TSDB\");";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `sym`id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(3000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }


        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_partitionType_HASH()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_partitionType_VALUE()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 7, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i % 10), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows + status.threadStatus[5].sentRows + status.threadStatus[6].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999%10 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val);");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_partitionType_RANGE()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, RANGE, cutPoints(0..99, 6));";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 7, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i % 100), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows + status.threadStatus[5].sentRows + status.threadStatus[6].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999%100 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val);");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_partitionType_LIST()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, LIST, [[1, 4], [2, 5], [3, 7], [6, 9], [8, 0]]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 7, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i % 10), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows + status.threadStatus[5].sentRows + status.threadStatus[6].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999%10 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999 + 0.1 as price, 0..2999999%999 as val);");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_compo_partition_first_partitionCol()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db1 = database(\"\", HASH, [INT, 10]);db2 = database(\"\", HASH, [SYMBOL, 10]);db = database(dbName,COMPO,[db1,db2]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id`sym);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(3000);
                long totalSentRows = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                totalSentRows = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                if (totalSentRows.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(totalSentRows, 3000000);
                    }
                }
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        // different partitionScheme and partitionCol type
        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_datehour_partitionCol_datetime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, datehour(2012.01.01T00:00:00)..datehour(2012.01.01T23:00:00));";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATETIME, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(2012, 1, 1, i % 24, 12, 10), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take(temporalAdd(2012.01.01T00:12:10, 0..23, \"H\"), size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_datehour_partitionCol_timestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, datehour(2012.01.01T00:00:00)..datehour(2012.01.01T23:00:00));";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, TIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(2012, 1, 1, i % 24, 12, 10), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take(temporalAdd(2012.01.01T00:12:10.000, 0..23, \"H\"), size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_datehour_partitionCol_nanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, datehour(2012.01.01T00:00:00)..datehour(2012.01.01T23:00:00));";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, NANOTIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(2012, 1, 1, i % 24, 12, 10), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take(temporalAdd(2012.01.01T00:12:10.000000000, 0..23, \"H\"), size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_date_partitionCol_date()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATE, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_date_partitionCol_datetime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATETIME, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36, 2013.01.01T12:25:36, 2014.01.01T12:25:36, 2015.01.01T12:25:36], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_date_partitionCol_timestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, TIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36.000, 2013.01.01T12:25:36.000, 2014.01.01T12:25:36.000, 2015.01.01T12:25:36.000], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_date_partitionCol_nanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, NANOTIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36.000000000, 2013.01.01T12:25:36.000000000, 2014.01.01T12:25:36.000000000, 2015.01.01T12:25:36.000000000], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_month_partitionCol_datetime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01M, 2013.01M, 2014.01M, 2015.01M, 2016.01M]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATETIME, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36, 2013.01.01T12:25:36, 2014.01.01T12:25:36, 2015.01.01T12:25:36], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_month_partitionCol_timestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01M, 2013.01M, 2014.01M, 2015.01M, 2016.01M]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, TIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = new MultithreadedTableWriter.Status();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36.000, 2013.01.01T12:25:36.000, 2014.01.01T12:25:36.000, 2015.01.01T12:25:36.000], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_value_month_partitionCol_nanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, [2012.01M, 2013.01M, 2014.01M, 2015.01M, 2016.01M]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, NANOTIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 25, 36), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:25:36.000000000, 2013.01.01T12:25:36.000000000, 2014.01.01T12:25:36.000000000, 2015.01.01T12:25:36.000000000], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_range_date_partitionCol_date()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, RANGE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATE, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_range_date_partitionCol_datetime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, RANGE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, DATETIME, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 15, 26), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:15:26, 2013.01.01T12:15:26, 2014.01.01T12:15:26, 2015.01.01T12:15:26], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_range_date_partitionCol_timestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, RANGE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, TIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 15, 26, 145), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:15:26.145, 2013.01.01T12:15:26.145, 2014.01.01T12:15:26.145, 2015.01.01T12:15:26.145], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionScheme_range_date_partitionCol_nanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, RANGE, [2012.01.01, 2013.01.01, 2014.01.01, 2015.01.01, 2016.01.01]);";
            script += "dummy = table(100:0, [`id, `date, `sym, `str, `price, `val], [INT, NANOTIMESTAMP, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `date);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "date");
            int[] yearVec = new int[] { 2012, 2013, 2014, 2015 };
            for (int i = 0; i < 1000000; i++)
            {
                ErrorCodeInfo pErrorInfo = mtw.insert(i, new DateTime(yearVec[i % 4], 1, 1, 12, 15, 26, 145), "A" + i.ToString(), "AAA" + i.ToString(), (double)i, i);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("v = 0..999999;ex = table(v as id, take([2012.01.01T12:15:26.145000000, 2013.01.01T12:15:26.145000000, 2014.01.01T12:15:26.145000000, 2015.01.01T12:15:26.145000000], size(v)) as date, symbol(\"A\"+string(v)) as sym, \"AAA\"+string(v) as str, double(v) as price, 0..999999 as val)");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id).values(), ex.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }


        [TestMethod]
        public void Test_MultithreadedTableWriter_dimensional_table_huge_data_single_thread()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createTable(dummy, `pt);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 1);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(2000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                ////Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dimensional_table_multi_thread()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createTable(dummy, `pt);";
            conn.run(script);
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            }
            catch (Exception ex)
            {
                ////Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dimensional_table_huge_data_single_thread_tsdb()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10], engine=\"TSDB\");";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createTable(dummy, `pt, , `sym`id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 1);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                //Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            //conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        //compression
        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_lz4()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9);";
            script += "dummy = table(100:0, [`id, `cbool, `cchar, `cshort, `cint, `clong, `cdate, `cmonth, `cminute, `csecond, `ctime, `cnanotime, `cdatetime, `ctimestamp, `cnanotimestamp, `cdatehour, `cdouble, `cfloat, `cstring, `csymbol, `cuuid, `cint128, `cipaddr], [INT, BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, MINUTE, SECOND, TIME, NANOTIME, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR, DOUBLE, FLOAT, STRING, SYMBOL, UUID, INT128, IPADDR]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            int[] compressMethods = new int[23];
            for (int i = 0; i < 23; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_LZ4;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, true, 97, 97, 97, 97, new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), (double)i, (float)i, "A" + i.ToString(), "B" + (i % 10).ToString(), "5d212a78-cc48-e3b1-4235-b4d91473ee87", "e1671797c52e15f763380b45e841ec32", "192.168.1.3");
                }
                else
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("n = 1000000;x = 0..(n - 1);ex = table(x % 10 as id, take([true, NULL], n) as cbool, take(char([97, NULL]), n) as cchar, take(short([97, NULL]), n) as cshort, take(int([97, NULL]), n) as cint, take(long([97, NULL]), n) as clong, take([2012.01.01, NULL], n) as cdate, take([2012.01M, NULL], n) as cmonth, take([12:30m, NULL], n) as cminute, take([12:30:25, NULL], n) as csecond, take([12:30:25.000, NULL], n) as ctime, take([12:30:25.000000000, NULL], n) as cnanotime, take([2012.01.01T00:00:00, NULL], n) as cdatetime, take([2012.01.01T00:00:00.000, NULL], n) as ctimestamp, take([2012.01.01T00:00:00.000000000, NULL], n) as cnanotimestamp, take(datehour([2012.01.01T00:00:00, NULL]), n) as cdatehour, double(iif(x %2 == 0, x, NULL)) as cdouble, float(iif(x %2 == 0, x, NULL)) as cfloat, iif(x %2 == 0, \"A\"+string(x), string()) as cstring, symbol(iif(x %2 == 0, \"B\"+string(x%10), string())) as csymbol, take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", NULL]), n) as cuuid, take(int128([\"e1671797c52e15f763380b45e841ec32\", NULL]), n) as cint128, take(ipaddr([\"192.168.1.3\", NULL]), n) as cipaddr); ");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, cint).values(), (select * from ex order by id, cint).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_lz4_array_vector_blob()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cblob, `cint_array], [INT, BLOB, INT[]]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[3];
            for (int i = 0; i < 3; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_LZ4;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    int[] col1v = new int[] { i, i, i, i, i };
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, "C" + i.ToString(), col1v);
                }
                else
                {
                    int[] col1v = new int[] { };
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, (object)null, col1v);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicLong total = (BasicLong)conn.run("exec count(*) from loadTable('dfs://test_MultithreadedTableWriter', 'pt')");
            Assert.AreEqual(total.getValue(), 1000000);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9);";
            script += "dummy = table(100:0, [`id, `cshort, `cint, `clong, `cdate, `cmonth, `cminute, `csecond, `ctime, `cnanotime, `cdatetime, `ctimestamp, `cnanotimestamp, `cdatehour], [INT,  SHORT, INT, LONG, DATE, MONTH, MINUTE, SECOND, TIME, NANOTIME, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            int[] compressMethods = new int[14];
            for (int i = 0; i < 14; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, 97, 97, 97, new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new TimeSpan(12, 30, 25), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1), new DateTime(2012, 1, 1));
                }
                else
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(i % 10, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null, (object)null);
                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            conn.run("n = 1000000;x = 0..(n - 1);ex = table(x % 10 as id, take(short([97, NULL]), n) as cshort, take(int([97, NULL]), n) as cint, take(long([97, NULL]), n) as clong, take([2012.01.01, NULL], n) as cdate, take([2012.01M, NULL], n) as cmonth, take([12:30m, NULL], n) as cminute, take([12:30:25, NULL], n) as csecond, take([12:30:25.000, NULL], n) as ctime, take([12:30:25.000000000, NULL], n) as cnanotime, take([2012.01.01T00:00:00, NULL], n) as cdatetime, take([2012.01.01T00:00:00.000, NULL], n) as ctimestamp, take([2012.01.01T00:00:00.000000000, NULL], n) as cnanotimestamp, take(datehour([2012.01.01T00:00:00, NULL]), n) as cdatehour); ");
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, cint).values(), (select * from ex order by id, cint).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_bool_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cbool], [INT, BOOL]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_char_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cchar], [INT, CHAR]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_double_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cdouble], [INT, DOUBLE]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_float_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cfloat], [INT, FLOAT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_symbol_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `csymbol], [INT, SYMBOL]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_string_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cstring], [INT, STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_uuid_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cuuid], [INT, UUID]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_int128_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cint128], [INT, INT128]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_compression_ipaddr_not_support_delta()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, VALUE, 0..9, , \"TSDB\");";
            script += "dummy = table(100:0, [`id, `cipaddr], [INT, IPADDR]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);
            int[] compressMethods = new int[2];
            for (int i = 0; i < 2; i++)
            {
                compressMethods[i] = Vector_Fields.COMPRESS_DELTA;
            }
            Exception exception = null;
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 4, "id", compressMethods);
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_dfs_table_huge_multi_Patiton_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db1 = database(\"\", HASH, [INT, 10]);db2 = database(\"\", HASH, [SYMBOL, 10]);db = database(dbName,COMPO,[db1,db2]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id`sym);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(2000);
                bool flag = true;
                if (!status.threadStatus[0].sentRows.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(status.threadStatus[0].sentRows, 3000000);
                    }
                }
                if (flag)
                    break;
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_threadCount_larger_than_partition()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 5]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 7, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows + status.threadStatus[5].sentRows + status.threadStatus[6].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_threadCount_less_than_partition()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 5]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 3, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_throttle_double()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 5]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, (float)0.5, 3, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_enableHighAvailabity_insert_dfs_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(NODE1_HOST, NODE1_PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 5]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(NODE1_HOST, NODE1_PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, true, HASTREAM_GROUP, 1000000, 1, 3, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_enableHighAvailabity_insert_haStreamTable_from_leader()
        {
            DBConnection conn = new DBConnection();
            conn.connect(NODE1_HOST, NODE1_PORT, USER, PASSWORD);
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderHost = StreamLeaderHostTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            //Console.WriteLine(StreamLeaderHost);
            //Console.WriteLine(StreamLeaderPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            try
            {
                conn1.run("dropStreamTable(\"haSt1\")");
            }
            catch (Exception )
            {
                //Console.WriteLine(ex.ToString());
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]), \"haSt1\", 5000000)", HASTREAM_GROUPID));
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(StreamLeaderHost, StreamLeaderPort, USER, PASSWORD, "", "haSt1", false, true, HASTREAM_GROUP, 1000000, 1, 3, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn1.run("select * from haSt1 order by id, sym, price");
            conn1.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn1.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn1.run("dropStreamTable(\"haSt1\")");
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_enableHighAvailabity_insert_haStreamTable_from_follower()
        {
            DBConnection conn = new DBConnection();
            conn.connect(NODE1_HOST, NODE1_PORT, USER, PASSWORD);
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            conn.run(String.Format("leader = getStreamingLeader({0}); sites = (exec sites from getStreamingRaftGroups() where id == {1})[0]; sitesVec = split(sites, \",\"); followerSite = sitesVec[like(sitesVec, \"%\" + leader) == false][0]", HASTREAM_GROUPID.ToString(), HASTREAM_GROUPID.ToString()));
            BasicString followerSite = (BasicString)conn.run("followerSite");
            //Console.WriteLine(followerSite.getString());
            BasicString StreamFollowerHostTmp = (BasicString)conn.run("split(followerSite, \":\")[0]");
            BasicString StreamFollowerPortTmp = (BasicString)conn.run("split(followerSite, \":\")[1]");
            String StreamFollowerHost = StreamFollowerHostTmp.getString();
            int StreamFollowerPort = int.Parse(StreamFollowerPortTmp.getString());
            //Console.WriteLine(StreamFollowerHost);
            //Console.WriteLine(StreamFollowerPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            try
            {
                conn1.run("dropStreamTable(\"haSt1\")");
            }
            catch (Exception )
            {
                //Console.WriteLine(ex.ToString());
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]), \"haSt1\", 5000000)", HASTREAM_GROUPID));
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(StreamFollowerHost, StreamFollowerPort, USER, PASSWORD, "", "haSt1", false, true, HASTREAM_GROUP, 1000000, 1, 3, "id");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                bool flag = true;
                long total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows;
                if (!total.Equals(3000000))
                {
                    flag = false;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }
                if (flag)
                    break;
            }
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn1.run("select * from haSt1 order by id, sym, price");
            conn1.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn1.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn1.run("dropStreamTable(\"haSt1\")");
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccessr_dfs_table_huge_data_single_thread_tsdb()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10], engine=\"TSDB\");";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `sym`id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 1, "id");
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();

                ////Console.WriteLine(darray);
                //Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }

                ////Console.WriteLine(list[100]);
                return;
            };
            mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999), new BasicInt(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(2000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows;
                //Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }
            list.Sort();
            for (int i = 0; i < 3000000; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccess_dfs_table_huge_data_multiple_thread_tsdb()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10], engine=\"TSDB\");";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `sym`id);";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1000000, 1, 5, "id");
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();

                ////Console.WriteLine(darray);
                ////Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }
                ////Console.WriteLine(list[100]);
                return;
            };
            mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);

            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999), new BasicInt(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }

            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                long total = 0;
                MultithreadedTableWriter.Status status = mtw.getStatus();
                total = status.threadStatus[0].sentRows + status.threadStatus[1].sentRows + status.threadStatus[2].sentRows + status.threadStatus[3].sentRows + status.threadStatus[4].sentRows;
                //Console.WriteLine(total);
                if (total.Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(total, 3000000);
                    }
                }

            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            conn.run("dropDatabase('dfs://test_MultithreadedTableWriter')");
            mtw.waitForThreadCompletion();
            list.Sort();
            for (int i = 0; i < 3000000; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccess_BasicArrayVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0, `col1, `col2, `col3, `col4, `col5, `col6, `col7, `col8, `col9, `col10, `col11, `col12, `col13, `col14, `col15, `col16, `col17, `col18], [INT[], BOOL[], SHORT[], CHAR[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DOUBLE[], FLOAT[], UUID[], INT128[], IPADDR[]]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 1000, 1, 1);
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();

                ////Console.WriteLine(darray);
                ////Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }
                ////Console.WriteLine(list[0]);
                return;
            };
            mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
            conn.run("v1 = array(ANY, 0, 1000000);v2 = array(ANY, 0, 1000000);v3 = array(ANY, 0, 1000000);v4 = array(ANY, 0, 1000000);v5 = array(ANY, 0, 1000000);v6 = array(ANY, 0, 1000000);v7 = array(ANY, 0, 1000000);v8 = array(ANY, 0, 1000000);v9 = array(ANY, 0, 1000000);v10 = array(ANY, 0, 1000000);v11 = array(ANY, 0, 1000000);v12 = array(ANY, 0, 1000000);v13 = array(ANY, 0, 1000000);v14 = array(ANY, 0, 1000000);v15 = array(ANY, 0, 1000000);v16 = array(ANY, 0, 1000000);v17 = array(ANY, 0, 1000000);v18 = array(ANY, 0, 1000000);v19 = array(ANY, 0, 1000000);");
            for (int i = 0; i < 1000; i++)
            {

                BasicIntVector col1v = (BasicIntVector)conn.run("tmp = rand(-100..100, 10); v1.append!(tmp);tmp;");
                BasicBooleanVector col2v = (BasicBooleanVector)conn.run("tmp = rand([true, false], 10); v2.append!(tmp);tmp;");
                BasicShortVector col3v = (BasicShortVector)conn.run("tmp = rand(short(-100..100), 10); v3.append!(tmp);tmp;");
                BasicByteVector col4v = (BasicByteVector)conn.run("tmp = rand(char(1..100), 10); v4.append!(tmp);tmp;");
                BasicLongVector col5v = (BasicLongVector)conn.run("tmp = rand(long(-100..100), 10); v5.append!(tmp);tmp;");
                BasicDateVector col6v = (BasicDateVector)conn.run("tmp = rand(2012.01.01..2012.01.10, 10); v6.append!(tmp);tmp;");
                BasicMonthVector col7v = (BasicMonthVector)conn.run("tmp = rand(2012.01M..2012.12M, 10); v7.append!(tmp);tmp;");
                BasicTimeVector col8v = (BasicTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000, 0..100, 's'), 10); v8.append!(tmp);tmp;");
                BasicMinuteVector col9v = (BasicMinuteVector)conn.run("tmp = rand(09:00m..12:00m, 10); v9.append!(tmp);tmp;");
                BasicSecondVector col10v = (BasicSecondVector)conn.run("tmp = rand(09:00:00..12:00:00, 10); v10.append!(tmp);tmp;");
                BasicDateTimeVector col11v = (BasicDateTimeVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00, 0..100, 's'), 10); v11.append!(tmp);tmp;");
                BasicTimestampVector col12v = (BasicTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000, 0..100, 's'), 10); v12.append!(tmp);tmp;");
                BasicNanoTimeVector col13v = (BasicNanoTimeVector)conn.run("tmp = rand(temporalAdd(09:00:00.000000000, 0..100, 's'), 10); v13.append!(tmp);tmp;");
                BasicNanoTimestampVector col14v = (BasicNanoTimestampVector)conn.run("tmp = rand(temporalAdd(2012.01.01T09:00:00.000000000, 0..100, 's'), 10); v14.append!(tmp);tmp;");
                BasicDoubleVector col15v = (BasicDoubleVector)conn.run("tmp = rand(double(-100..100), 10); v15.append!(tmp);tmp;");
                BasicFloatVector col16v = (BasicFloatVector)conn.run("tmp = rand(float(-100..100), 10); v16.append!(tmp);tmp;");
                BasicUuidVector col17v = (BasicUuidVector)conn.run("tmp = rand(uuid(), 10); v17.append!(tmp);tmp;");
                BasicInt128Vector col18v = (BasicInt128Vector)conn.run("tmp = rand(int128(), 10); v18.append!(tmp);tmp;");
                BasicIPAddrVector col19v = (BasicIPAddrVector)conn.run("tmp = rand(ipaddr(), 10); v19.append!(tmp);tmp;");
                BasicInt col20 = new BasicInt(i);

                ErrorCodeInfo pErrorInfo = mtw.insert(col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v, col14v, col15v, col16v, col17v, col18v, col19v, col20);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            list.Sort();
            for (int i = 0; i < 1000; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccess_BasicInt()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();

                ////Console.WriteLine(darray);
                //Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }
                ////Console.WriteLine(list[0]);
                return;
            };
            mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicInt(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            list.Sort();
            for (int i = 0; i < 1000000; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..999999 as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccess_small_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();

                ////Console.WriteLine(darray);
                //Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }
                //Console.WriteLine(list[0]);
                return;
            };
            mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
            for (int i = 0; i < 10; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicInt(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            list.Sort();
            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..9 as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_setNotifyOnSuccess_Multiple_calls()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            List<int> list = new List<int>();
            Action<IVector> notifyOnSuccess = (IVector items) =>
            {
                BasicIntVector lastCol = (BasicIntVector)items;
                var darray = lastCol.getdataArray();
                //Console.WriteLine($"darrycount:{darray.Count}");
                for (int i = 0; i < darray.Count; i++)
                {
                    var tag = (ulong)darray[i];
                    ////Console.WriteLine($"rtag:{tag}");
                    lock (list)
                    {
                        list.Add(darray[i]);
                    }
                }
                return;
            };
            Exception exception = null;
            try
            {
                mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
                mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_INT);
            }
            catch (Exception ex)

            {
                exception = ex;

            }
            Assert.IsNotNull(exception);
            Console.Out.WriteLine(exception);
            for (int i = 0; i < 10; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicInt(i) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            list.Sort();
            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(i, list[i]);
            }
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_memoryTable_single_thread_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            for (int i = 0; i < 10; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from table1 order by col0");
            Console.Out.WriteLine(re.rows());
            BasicTable expected = (BasicTable)conn.run("table(string(0..9) as col0)");
            compareBasicTable(re, expected);

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn.upload(obj);
            BasicTable act = (BasicTable)conn.run("select * from callback where issuccess = true order by id");

            Assert.AreEqual(act.rows(), re.rows());
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_memoryTable_single_thread_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 5)
                {
                    try
                    {
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            try
            {
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(1000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback where issuccess = false order by id");
            Assert.AreEqual(act.rows(), 6);
            conn.close();
            conn1.close();
            conn2.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_single_thread_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter1';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter1", "pt", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            for (int i = 0; i < 10; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter1', 'pt') order by id");
            Console.Out.WriteLine(re.rows());
            BasicTable expected = (BasicTable)conn.run("table(string(0..9) as col0)");
            compareBasicTable(re, expected);

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn.upload(obj);
            BasicTable act = (BasicTable)conn.run("select * from callback where issuccess = true order by id");

            Assert.AreEqual(act.rows(), re.rows());
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_single_thread_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter1';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter1", "pt", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 5)
                {
                    try
                    {
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();

            try
            {
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(20000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);
            BasicTable re = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter1', 'pt') order by id");
            Console.Out.WriteLine(re.rows());

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback where issuccess = false order by id");
            BasicTable act2 = (BasicTable)conn2.run("select * from callback where issuccess = true order by id");

            Assert.AreEqual(act.rows(), 6);
            Assert.AreEqual(act2.rows(), re.rows());
            conn.close();
            conn1.close();
            conn2.close();
        }

        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1, 10, "id", null, 0, null, callbackHandler);
            for (int i = 0; i < 100; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            BasicTable expected = (BasicTable)conn.run("select * from table(string(0..99) as col0) order by col0");
            compareBasicTable(re, expected);

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();
            Console.Out.WriteLine(issuccess.rows());

            for (int i = 0; i < issuccess.rows(); i++)
            {
                Console.Out.WriteLine(id.getString(i) + " " + issuccess.getBoolean(i));
            }

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
                new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            //object locker = new object();
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);

            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn.upload(obj);
            BasicTable act = (BasicTable)conn.run("select * from callback where issuccess = true order by id ");
            Assert.AreEqual(act.rows(), re.rows());
            Assert.AreEqual(100, act.rows());
            conn.close();
        }

        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1, 10, "id", null, 0, null, callbackHandler);
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 80)
                {
                    try
                    {
                        conn1.run("sleep(1000)");
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();

            try
            {
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(20000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);
            BasicTable re = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Console.Out.WriteLine(re.rows());

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            object locker = new object();
            lock (locker)
            {
                bt.getColumn(0).append(id);
                bt.getColumn(1).append(issuccess);
            }
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback ");
            BasicTable act2 = (BasicTable)conn2.run("select * from callback where issuccess = true order by id");
            Console.Out.WriteLine(act.rows());
            Console.Out.WriteLine(act2.rows());
            Assert.AreEqual(act2.rows(), re.rows());
            for (int j = 0; j < re.rows(); j++)
            {
                Assert.AreEqual((AbstractScalar)re.getColumn(0).get(j), (AbstractScalar)act2.getColumn(0).get(j));
            }
            conn.close();
            conn1.close();
            conn2.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_bigdata_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 100000, 1, 20, "id", null, 0, null, callbackHandler);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                try
                {
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {
                    string s = e.Message;
                }

            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Console.Out.WriteLine(re.rows());
            BasicTable expected = (BasicTable)conn.run("select * from table(string(0..999999) as col0) order by col0");
            compareBasicTable(re, expected);

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            object locker = new object();
            lock (locker)
            {
                bt.getColumn(0).append(id);
                bt.getColumn(1).append(issuccess);
            }
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn.upload(obj);
            BasicTable act = (BasicTable)conn.run("select * from callback where issuccess = true order by id ");
            Assert.AreEqual(act.rows(), re.rows());
            for (int j = 0; j < re.rows(); j++)
            {
                Assert.AreEqual((AbstractScalar)re.getColumn(0).get(j), (AbstractScalar)act.getColumn(0).get(j));
            }
            conn.close();
        }

        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_bigdata_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10000, 1, 20, "id", null, 0, null, callbackHandler);
            for (int i = 0; i < 1000000; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 500000)
                {
                    try
                    {
                        conn1.run("sleep(5000)");
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();

            try
            {
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(20000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);
            BasicTable re = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Console.Out.WriteLine(re.rows());

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();


            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            object locker = new object();
            lock (locker)
            {
                bt.getColumn(0).append(id);
                bt.getColumn(1).append(issuccess);
            }
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback ");
            BasicTable act2 = (BasicTable)conn2.run("select * from callback where issuccess = true order by id");
            Console.Out.WriteLine(act.rows());
            Console.Out.WriteLine(act2.rows());
            Assert.AreEqual(act2.rows(), re.rows());
            conn.close();
            conn1.close();
            conn2.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_single_thread_false_insertUnwrittenData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 1, 1, 1, "", null, 0, null, callbackHandler);
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 5)
                {
                    try
                    {
                        conn1.run("sleep(1000)");
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();

            try
            {               
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(20000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);
            BasicTable re = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Console.Out.WriteLine(re.rows());

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback ");
            BasicTable act2 = (BasicTable)conn2.run("select * from callback where issuccess = true order by id");
            //Assert.AreEqual(act.rows(), 6);
            Console.Out.WriteLine("act.rows()"+act.rows());
            Console.Out.WriteLine("act2.rows()" + act2.rows());
            Assert.AreEqual(act2.rows(), re.rows());
            List<List<IEntity>> unwrite1 = mtw.getUnwrittenData();
            MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            mtw1.insertUnwrittenData(unwrite1);
            mtw1.waitForThreadCompletion();
            BasicTable ex1 = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Assert.AreEqual(act.rows(), ex1.rows());
            Console.Out.WriteLine("ex1.rows()" + ex1.rows());
            conn.close();
            conn1.close();
            conn2.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_Callback_dfs_multiple_thread_false_insertUnwrittenData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORTCON, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_MultithreadedTableWriter';";
            script += "if(existsDatabase(dbName)){";
            script += "dropDB(dbName);";
            script += "}";
            script += "db = database(dbName, HASH, [STRING, 10], engine=\"TSDB\"); ";
            script += "dummy = table(100:0, [`id], [STRING]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, , `id);";
            conn.run(script);

            CallbackHandler callbackHandler = new CallbackHandler();
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10, 1, 5, "id", null, 0, null, callbackHandler);
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString(Convert.ToString(i)), new BasicString(Convert.ToString(i)) });
                    ErrorCodeInfo pErrorInfo = mtw.insert(x);
                }
                catch (Exception e)
                {

                    Console.Out.WriteLine(e.Message);

                }

                if (i == 5)
                {
                    try
                    {
                        conn1.run("sleep(1000)");
                        conn1.run("stopDataNode([\"" + SERVER + ":" + PORT + "\"])");
                    }
                    catch (Exception e)
                    {

                        Console.Out.WriteLine(e.Message);

                    }

                }
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();

            try
            {
                conn1.run("startDataNode([\"" + SERVER + ":" + PORT + "\"])");
            }
            catch (Exception e)
            {

                Console.Out.WriteLine(e.Message);

            }
            conn1.run("sleep(20000)");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, USER, PASSWORD);
            BasicTable re = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Console.Out.WriteLine(re.rows());

            BasicStringVector id = callbackHandler.getId();
            BasicBooleanVector issuccess = callbackHandler.getIsSuccess();

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("id",Type.GetType("System.String")),
               new DataColumn("issuccess",Type.GetType("System.Boolean"))
            };
            dt.Columns.AddRange(cols.ToArray());
            BasicTable bt = new BasicTable(dt);
            bt.getColumn(0).append(id);
            bt.getColumn(1).append(issuccess);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("callback", (IEntity)bt);
            conn2.upload(obj);
            BasicTable act = (BasicTable)conn2.run("select * from callback ");
            BasicTable act2 = (BasicTable)conn2.run("select * from callback where issuccess = true order by id");
            //Assert.AreEqual(act.rows(), 6);
            Console.Out.WriteLine("act.rows()" + act.rows());
            Console.Out.WriteLine("act2.rows()" + act2.rows());
            Assert.AreEqual(act2.rows(), re.rows());
            List<List<IEntity>> unwrite1 = mtw.getUnwrittenData();
            MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://test_MultithreadedTableWriter", "pt", false, false, null, 10000, 1, 1, "", null, 0, null, callbackHandler);
            mtw1.insertUnwrittenData(unwrite1);
            mtw1.waitForThreadCompletion();
            BasicTable ex1 = (BasicTable)conn2.run("select * from loadTable('dfs://test_MultithreadedTableWriter', 'pt') order by id");
            Assert.AreEqual(act.rows(), ex1.rows());
            Console.Out.WriteLine("ex1.rows()" + ex1.rows());
            conn.close();
            conn1.close();
            conn2.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_keyedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
           // script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
           // script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "st", false, false, null, 10, 1, 1, "", null, 0, new String[] { "ignoreNull=false", "keyColNames=null" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 9;");
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(20).get(0)).getValue(), ((BasicDecimal64)act.getColumn(20).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        //[TestMethod]
        //public void test_MultithreadedTableWriter_upsert_keyedTable_allDateType_update()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    string script = null;
        //    script += "cbool = true false false;\n";
        //    script += "cchar = 'a' 'b' 'c';\n";
        //    script += "cshort = 122h 32h 45h;\n";
        //    script += "cint = 1 4 9;\n";
        //    script += "clong = 17l 39l 72l;\n";
        //    script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
        //    script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
        //    script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
        //    script += "cminute = 03:25m 08:12m 10:15m;\n";
        //    script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
        //    script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
        //    script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
        //    script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
        //    script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
        //    script += "cfloat = 7.5f 0.79f 8.27f;\n";
        //    script += "cdouble = 5.7 7.2 3.9;\n";
        //    script += "cstring = \"hello\" \"hi\" \"here\";\n";
        //    script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
        //    script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
        //    script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
        //    script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
        //    script += "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
        //    script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
        //    script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64);";
        //    script += "share t as st;";
        //    conn.run(script);
        //    BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64)");
        //    MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "st", false, false, null, 10, 1, 1, "", null, 0, new String[] { "ignoreNull=false", "keyColNames=null" });
        //    List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0), bt.getColumn(19).get(0), bt.getColumn(20).get(0) });
        //    ErrorCodeInfo pErrorInfo = mtw.insert(x);
        //    MultithreadedTableWriter.Status status = mtw.getStatus();
        //    mtw.waitForThreadCompletion();
        //    BasicTable ua = (BasicTable)conn.run("select * from st;");
        //    Assert.AreEqual(3, ua.rows());
        //    BasicTable act = (BasicTable)conn.run("select * from st where cint = 9;");
        //    Assert.AreEqual(((BasicDecimal64)bt.getColumn(20).get(0)).getValue(), ((BasicDecimal64)act.getColumn(20).get(0)).getValue());
        //    compareBasicTable(bt, act);
        //    conn.run("undef(`st, SHARED)");
        //    conn.close();
        //}
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_keyedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "st", false, false, null, 10, 1, 1, "", null, 0, new String[] { "ignoreNull=false", "keyColNames=null" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0), bt.getColumn(19).get(0), bt.getColumn(20).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(20).get(0)).getValue(), ((BasicDecimal64)act.getColumn(20).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_indexedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            //script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64);";
            script += "cstring,cdatehour,cblob);";

            script += "share t as st;";
            conn.run(script);
            //BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64)");

            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "st", false, false, null, 10, 1, 1, "", null, MultithreadedTableWriter.Mode.M_Upsert, new String[] { "ignoreNull=false" });
            //List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0), bt.getColumn(19).get(0), bt.getColumn(20).get(0) });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0) });

            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 9;");
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(20).get(0)).getValue(), ((BasicDecimal64)act.getColumn(20).get(0)).getValue());
            compareBasicTable(bt, act);//APICS-192
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_indexedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "st", false, false, null, 10, 1, 1, "", null, 0, new String[] { "ignoreNull=false", "keyColNames=null" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0), bt.getColumn(18).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(20).get(0)).getValue(), ((BasicDecimal64)act.getColumn(20).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_partitionedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://partitionedTable", "pt", false, false, null, 10, 1, 5, "cint", null, MultithreadedTableWriter.Mode.M_Upsert, new String[] { "ignoreNull=false", "keyColNames=`cint" });
            List <IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 9;");
            Assert.AreEqual(1, act.rows());

            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(19).get(0)).getValue(), ((BasicDecimal64)act.getColumn(19).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_partitionedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://partitionedTable", "pt", false, false, null, 10, 1, 5, "cint", null, MultithreadedTableWriter.Mode.M_Upsert, new String[] { "ignoreNull=false", "keyColNames=`cint" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 9;");
            Assert.AreEqual(1, act.rows());

            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(19).get(0)).getValue(), ((BasicDecimal64)act.getColumn(19).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.close();
        }

        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_DimensionTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://partitionedTable", "pt", false, false, null, 10, 1, 1, "cint", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames=`cint" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 9;");
            Assert.AreEqual(1, act.rows());
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(19).get(0)).getValue(), ((BasicDecimal64)act.getColumn(19).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_DimensionTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            //script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            //script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://partitionedTable", "pt", false, false, null, 10, 1, 1, "cint", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames=`cint" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0), bt.getColumn(3).get(0), bt.getColumn(4).get(0), bt.getColumn(5).get(0), bt.getColumn(6).get(0), bt.getColumn(7).get(0), bt.getColumn(8).get(0), bt.getColumn(9).get(0), bt.getColumn(10).get(0), bt.getColumn(11).get(0), bt.getColumn(12).get(0), bt.getColumn(13).get(0), bt.getColumn(14).get(0), bt.getColumn(15).get(0), bt.getColumn(16).get(0), bt.getColumn(17).get(0) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 10;");
            //Assert.AreEqual(((BasicDecimal64)bt.getColumn(19).get(0)).getValue(), ((BasicDecimal64)act.getColumn(19).get(0)).getValue());
            compareBasicTable(bt, act);
            conn.close();
        }
        [TestMethod]
        public void test_MultithreadedTableWriter_upsert_pModeOption_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value) as kt;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "kt", false, false, null, 10, 1, 1, "", null, (MultithreadedTableWriter.Mode)1, null);
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            MultithreadedTableWriter.Status status = mtw.getStatus();
            mtw.waitForThreadCompletion();
            BasicTable ua = (BasicTable)conn.run("select * from kt;");
            Assert.AreEqual(100, ua.rows());
            //compareBasicTable(bt, act);
            conn.run("undef(`kt, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_keyedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value) as kt;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "kt", false, false, null, 10, 1, 1, "", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=true" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.run("undef(`kt, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_keyedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value) as kt;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "kt", false, false, null, 10, 1, 1, "", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.run("undef(`kt, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_indexedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share indexedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value) as kt;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "kt", false, false, null, 10, 1, 1, "", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=true" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.run("undef(`kt, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_indexedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share indexedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value) as kt;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "kt", false, false, null, 10, 1, 1, "", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.run("undef(`kt, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_partitionedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=true" , "keyColNames=`id2" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_partitionedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames=`id2" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_DimensionTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=true", "keyColNames=`id2" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_DimensionTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames=`id2" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_DFS_keyColNames_null()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 101 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames= \"\"" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            mtw.waitForThreadCompletion();
            Console.Out.WriteLine(pErrorInfo.ToString());

            //Console.Out.WriteLine(pErrorInfo1.ToString());
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_DFS_sortColumns()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames= `id2", "sortColumns= `value" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            BasicTable res1 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(99).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_notMatchColumns()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 101 as id2,1 2 as id3, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames= \"\"" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            Console.Out.WriteLine(pErrorInfo.ToString());
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_upsert_notMatchDataType()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(string(1..10), 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id2).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 101 102 as id2, 1001 NULL as value);t2;");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames= \"\"" });
            List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(0), bt.getColumn(1).get(0), bt.getColumn(2).get(0) });
            List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(1), bt.getColumn(1).get(1), bt.getColumn(2).get(1) });
            ErrorCodeInfo pErrorInfo = mtw.insert(x);
            ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            mtw.waitForThreadCompletion();
            //Console.Out.WriteLine(pErrorInfo.ToString());
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            conn.close();
        }
        [TestMethod]
       public void Test_MultithreadedTableWriter_upsert_big_data()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..100, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( take(1..1000,3000000) as id, 1..3000000 as id2, 1..3000000 as value);t2;");
            Console.Out.WriteLine(bt.rows());
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 1, "id2", null, (MultithreadedTableWriter.Mode)1, new String[] { "ignoreNull=false", "keyColNames= \"id2\"" });
            for (int i = 0; i < 3000000; i++)
            {
               List<IScalar> x = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(i), bt.getColumn(1).get(i), bt.getColumn(2).get(i) });
               ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            mtw.waitForThreadCompletion();

            BasicTable res1 = (BasicTable)conn.run("select * from loadTable(\"dfs://tableUpsert_test\",`pt) ");
            Assert.AreEqual(3000000, res1.rows());
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x1 = new List<IScalar>(new IScalar[] { bt.getColumn(0).get(i), bt.getColumn(1).get(i), bt.getColumn(2).get(i) });
                ErrorCodeInfo pErrorInfo1 = mtw.insert(x1);
            }
            mtw.waitForThreadCompletion();
            BasicTable res2 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(3000000, res2.rows());
            conn.close();
        }
        [TestMethod]
        public void Test_MultithreadedTableWriter_partitionCol_notTrue()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(string(1..10), 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id2).append!(t)\n";
            conn.run(script);
            Exception exception = null;
            try 
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://tableUpsert_test", "pt", false, false, null, 10, 1, 5, null, null, 0, new String[] { "ignoreNull=false", "keyColNames=null" });

            }
            catch (Exception ex) 
            { 
                exception = ex; 
            }
            Assert.AreEqual("The parameter partionCol must be the partitioning column \"\" in the table. ",exception.Message);            
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_Status_ToString()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString("AA" + (i % 99).ToString()) });
                ErrorCodeInfo pErrorInfo = mtw.insert(x);
            }
            MultithreadedTableWriter.Status status = mtw.getStatus();
            //Console.WriteLine(status.ToString());
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table('AA'+string(0..999999%99) as col0)");
            compareBasicTable(re, expected);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_Create_Remain()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false,
                false, null, 10000, 1, 1, "", new int[] { 1, 2, 1, 2 });
            }
            catch(Exception e)
            {
                Assert.AreEqual("The number of elements in parameter compressMethods does not match the column size 1. ", e.Message);
                //Console.WriteLine(e.Message);
            }

            string sc3 = "n=1000\n" +
                "ID=rand(100, n)\n" +
                "dates=2017.08.07..2017.08.11\n" +
                "date=rand(dates, n)\n" +
                "x=rand(10.0, n)\n" +
                "t=table(ID, date, x)\n" +
                "\n" +
                "if(existsDatabase(\"dfs://compoDB\")){dropDatabase(\"dfs://compoDB\")}\n" +
                "dbDate = database(, partitionType=VALUE, partitionScheme=2017.08.07..2017.08.11)\n" +
                "dbID = database(, partitionType=RANGE, partitionScheme=0 50 100)\n" +
                "db = database(directory=\"dfs://compoDB\", partitionType=COMPO, partitionScheme=[dbDate, dbID])\n" +
                "pt = db.createPartitionedTable(t, `pt, `date`ID)\n" +
                "pt.append!(t)";
            conn.run(sc3);
            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://compoDB", "pt", false,
                false, null, 10000, 1, 5, "");
            }
            catch(Exception e)
            {
                Assert.AreEqual("The parameter partitionCol must be specified when threadCount is greater than 1.", e.Message);
                //Console.WriteLine(e.Message);
            }

            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://compoDB", "pt", false,
                false, null, 10000, 1, 5, "hkjbsbgjka");
            }
            catch (Exception e)
            {
                Assert.AreEqual("The parameter partionCol must be the partitioning column \"hkjbsbgjka\" in the table. ", e.Message);
                //Console.WriteLine(e.Message);
            }

            try
            {
                MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false,
                false, null, 10000, 1, 5, "gakhfgah");
            }
            catch(Exception e)
            {
                Assert.AreEqual("The column gakhfgah does not belong to table table1", e.Message);
                //Console.WriteLine(e.Message);
            }

            MultithreadedTableWriter mtw1 = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            ErrorCodeInfo errorCodeInfo = mtw1.insert(new List<IEntity>());
            Assert.AreEqual(errorCodeInfo.errorInfo, "");
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_InsertUnwrittenData_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 1);
            List<List<IEntity>> table = new List<List<IEntity>>();
            for (int i = 0; i < 100000; i++)
            {
                List<IEntity> data = new List<IEntity>();
                BasicString bs = new BasicString("A" + i);
                data.Add(bs);
                table.Add(data);
            }
            mtw.insertUnwrittenData(table);
            mtw.waitForThreadCompletion();
            MultithreadedTableWriter.Status status = mtw.getStatus();
            //Console.WriteLine(status.ToString());
            BasicTable re = (BasicTable)conn.run("table1");
            Assert.AreEqual(re.rows(), 100000);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_InsertUnwrittenData_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "", "table1", false, false, null, 10000, 1, 5, "col0");
            List<List<IEntity>> table = new List<List<IEntity>>();
            for (int i = 0; i < 100000; i++)
            {
                List<IEntity> data = new List<IEntity>();
                BasicString bs = new BasicString("A" + i);
                data.Add(bs);
                table.Add(data);
            }
            mtw.insertUnwrittenData(table);
            mtw.waitForThreadCompletion();
            MultithreadedTableWriter.Status status = mtw.getStatus();
            //Console.WriteLine(status.ToString());
            BasicTable re = (BasicTable)conn.run("table1");
            Assert.AreEqual(re.rows(), 100000);
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_InsertUnwrittenData_3()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string sc = "if(existsDatabase(\"dfs://dataXdemo\")){dropDatabase(\"dfs://dataXdemo\")}\n" +
                "db = database(\"dfs://dataXdemo\", VALUE, 1..10)\n" +
                "t = table(take(1..10, 100) as id, take([`A, `B, `C], 100) as sym, 1..100 as qty, 100..1 as price)\n" +
                "pt = db.createPartitionedTable(t, `pt, `id).append!(t)";
            conn.run(sc);
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://dataXdemo", "pt", false, false, null, 10000, 1, 5, "id");
            List<List<IEntity>> table = new List<List<IEntity>>();
            for (int i = 0; i < 100000; i++)
            {
                List<IEntity> data = new List<IEntity>();
                BasicInt bi = new BasicInt(i);
                BasicString bs = new BasicString("A" + i);
                data.Add(bi);
                data.Add(bs);
                data.Add(bi);
                data.Add(bi);
                table.Add(data);
            }
            mtw.insertUnwrittenData(table);
            mtw.waitForThreadCompletion();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_allDataType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv", "decimal32v", "decimal64v", "decimal128v" };
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
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(1), DECIMAL64(10), DECIMAL128(20)]);\n pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://empty_table", "pt", false, false, null, 10000, 1, 5, "stringv");
            ErrorCodeInfo errorCodeInfo = mtw.insert(cols);
            string err = errorCodeInfo.ToString();
            //Console.WriteLine(err);
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            conn.close();
        }

        [TestMethod]
        public void Test_MultithreadedTableWriter_allDataType_array_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "id", "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "uuidv", "datehourv", "ippaddrv", "int128v", "decimal32v", "decimal64v", "decimal128v" };
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
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n t= table(100:0,`id`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`decimal32v`decimal64v`decimal128v, [INT, INT[], BOOL[], CHAR[], SHORT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], DECIMAL32(1)[], DECIMAL64(10)[], DECIMAL128(20)[]]);\n pt=db.createPartitionedTable(t,`pt,`id,,`id);");
            MultithreadedTableWriter mtw = new MultithreadedTableWriter(SERVER, PORT, USER, PASSWORD, "dfs://empty_table", "pt", false, false, null, 10000, 1, 5, "id");
            ErrorCodeInfo errorCodeInfo = mtw.insert(cols);
            string err = errorCodeInfo.ToString();
            //Console.WriteLine(err);
            mtw.waitForThreadCompletion();
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            conn.close();
        }
    }
}


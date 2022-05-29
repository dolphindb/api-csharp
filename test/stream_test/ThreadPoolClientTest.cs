using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using System.Threading;

namespace dolphindb_csharp_api_test.stream_test
{
    [TestClass]
    public class ThreadedPoolClientSubscribeTest
    {
        public static DBConnection streamConn;
        public static String HOST = "192.168.1.37";
        public static int PORT = 18104;
        public static String LOCALHOST = "192.168.1.36";
        public static int LOCALPORT = 8676;
        public static int HASTREAM_GROUPID = 11;
        public static string[] HASTREAM_GROUP = new string[] { "192.168.1.37:18103", "192.168.1.37:18104", "192.168.1.37:18105" };
        public static int SUB_FLAG = 0;

        public void PrepareStreamTable1(DBConnection conn, String tableName)
        {
            try
            {
                String script = String.Format("share(streamTable(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"{0}\")", tableName);
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareStreamTable2(DBConnection conn, String tableName)
        {
            try
            {
                String script = String.Format("share(streamTable(1000000:0, `permno`timestamp`ticker`price1`vol1, [INT, TIMESTAMP, SYMBOL, DOUBLE, INT]), \"{0}\")", tableName);
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void WriteStreamTable1(DBConnection conn, String tableName, int batch)
        {
            try
            {
                String str = "";
                str += "tmp = table({1}:{1},  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
                str += "tmp[`permno] = rand(1000, {1});";
                str += "tmp[`timestamp] = take(now(), {1});";
                str += "tmp[`ticker] = rand(\"A\"+string(1..1000), {1});";
                str += "tmp[`price1] = rand(100, {1});";
                str += "tmp[`price2] = rand(100, {1});";
                str += "tmp[`price3] = rand(100, {1});";
                str += "tmp[`price4] = rand(100, {1});";
                str += "tmp[`price5] = rand(100, {1});";
                str += "tmp[`vol1] = rand(100, {1});";
                str += "tmp[`vol2] = rand(100, {1});";
                str += "tmp[`vol3] = rand(100, {1});";
                str += "tmp[`vol4] = rand(100, {1});";
                str += "tmp[`vol5] = rand(100, {1});";
                str += "{0}.append!(tmp);";
                String script = String.Format(str, tableName, batch.ToString());
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        class Handler1 : MessageHandler
        {

            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    streamConn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        class Handler2 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub2 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    streamConn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler3 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub3 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    streamConn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler4 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {

                    SUB_FLAG += 1;
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler5 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                try
                {
                    int rows = msgs.Count;
                    BasicIntVector col1v = new BasicIntVector(rows);
                    BasicTimestampVector col2v = new BasicTimestampVector(rows);
                    BasicSymbolVector col3v = new BasicSymbolVector(rows);
                    BasicDoubleVector col4v = new BasicDoubleVector(rows);
                    BasicDoubleVector col5v = new BasicDoubleVector(rows);
                    BasicDoubleVector col6v = new BasicDoubleVector(rows);
                    BasicDoubleVector col7v = new BasicDoubleVector(rows);
                    BasicDoubleVector col8v = new BasicDoubleVector(rows);
                    BasicIntVector col9v = new BasicIntVector(rows);
                    BasicIntVector col10v = new BasicIntVector(rows);
                    BasicIntVector col11v = new BasicIntVector(rows);
                    BasicIntVector col12v = new BasicIntVector(rows);
                    BasicIntVector col13v = new BasicIntVector(rows);

                    for (int i = 0; i < rows; ++i)
                    {
                        col1v.set(i, (IScalar)msgs[i].getEntity(0));
                        col2v.set(i, (IScalar)msgs[i].getEntity(1));
                        col3v.set(i, (IScalar)msgs[i].getEntity(2));
                        col4v.set(i, (IScalar)msgs[i].getEntity(3));
                        col5v.set(i, (IScalar)msgs[i].getEntity(4));
                        col6v.set(i, (IScalar)msgs[i].getEntity(5));
                        col7v.set(i, (IScalar)msgs[i].getEntity(6));
                        col8v.set(i, (IScalar)msgs[i].getEntity(7));
                        col9v.set(i, (IScalar)msgs[i].getEntity(8));
                        col10v.set(i, (IScalar)msgs[i].getEntity(9));
                        col11v.set(i, (IScalar)msgs[i].getEntity(10));
                        col12v.set(i, (IScalar)msgs[i].getEntity(11));
                        col13v.set(i, (IScalar)msgs[i].getEntity(12));
                    }
                    BasicTable tmp = new BasicTable(new List<string> { "permno", "timestamp", "ticker", "price1", "price2", "price3", "price4", "price5", "vol1", "vol2", "vol3", "vol4", "vol5" }, new List<IVector> { col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v });
                    streamConn.run("append!{sub1}", new List<IEntity> { tmp });
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }

            public void doEvent(IMessage msg)
            {
                throw new NotImplementedException();
            }
        };

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_host_localhost()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            Exception exception = null;
            try
            {
                //Not allow to use localhost 
                ThreadPooledClient client = new ThreadPooledClient("localhost", LOCALPORT);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_streamTable_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            Exception exception = null;
            try
            {
                //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
                client.subscribe(HOST, PORT, "trades", "trades", handler, -1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.close();
            conn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_duplicated_actionName()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            client.subscribe(HOST, PORT, "pub", "action1", handler1, 0);
            Exception exception = null;
            try
            {
                //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
                client.subscribe(HOST, PORT, "pub", "action1", handler2, 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.unsubscribe(HOST, PORT, "pub", "action1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreadPooledClient_not_specify_host()
        {
            //DBConnection conn = new DBConnection();
            //conn.connect(HOST, PORT, "admin", "123456");
            //streamConn = new DBConnection();
            //streamConn.connect(HOST, PORT, "admin", "123456");
            //PrepareStreamTable1(conn, "pub");
            //PrepareStreamTable1(conn, "sub1");
            ////write 1000 rows first
            //WriteStreamTable1(conn, "pub", 1000);
            //BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            //Assert.AreEqual(1000, num1.getInt());
            //Handler1 handler1 = new Handler1();
            //ThreadPooledClient client = new ThreadPooledClient(LOCALPORT);
            ////usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            //client.subscribe(HOST, PORT, "pub", handler1, -1);
            ////write 1000 rows after subscribe
            //WriteStreamTable1(conn, "pub", 1000);
            //for (int i = 0; i < 10; i++)
            //{
            //    BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            //    if (tmpNum.getInt().Equals(1000))
            //    {
            //        break;
            //    }
            //    Thread.Sleep(1000);
            //}
            //BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            //Assert.AreEqual(1000, num2.getInt());
            //client.unsubscribe(HOST, PORT, "pub");
            //client.close();
            //conn.run("undef(`pub, SHARED)");
            //conn.run("undef(`sub1, SHARED)");
            //conn.close();
            //streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_default()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", handler1, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_neg_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_less_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", handler1, 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1500))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1500, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_larger_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            Exception exception = null;
            try
            {
                client.subscribe(HOST, PORT, "pub", handler1, 1500);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_actionName()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", "sub1", handler1, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_int_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            client.subscribe(HOST, PORT, "pub", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10 order by permno).values(), (select * from sub1 order by permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(HOST, PORT, "pub", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10) order by ticker, permno).values(), (select * from sub1 order by ticker, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_string_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `ticker)";
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicStringVector filter = new BasicStringVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(HOST, PORT, "pub", handler1, 0, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10) order by ticker, permno).values(), (select * from sub1 order by ticker, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_date_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `date)";
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
            BasicDateVector filter = (BasicDateVector)conn.run("2012.01.01..2012.01.10");
            client.subscribe(HOST, PORT, "pub", "sub1", handler1, 0, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where date in 2012.01.01..2012.01.10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where date in 2012.01.01..2012.01.10 order by date, permno).values(), (select * from sub1 order by date, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(HOST, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_reconnect_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(HOST, PORT, "pub", handler1, 0, true);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(HOST, PORT, "pub", handler1, 0, false);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_one_client_multiple_subscription()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", "action1", handler1, 0);
            client.subscribe(HOST, PORT, "pub", "action2", handler2, -1);
            client.subscribe(HOST, PORT, "pub", "action3", handler3, 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(1000) && tmpNum3.getInt().Equals(1500))
                {
                    break;
                }
                Thread.Sleep(2000);
            }
            BasicInt sub1_num = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num.getInt());
            BasicInt sub2_num = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(1000, sub2_num.getInt());
            BasicInt sub3_num = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(1500, sub3_num.getInt());
            client.unsubscribe(HOST, PORT, "pub", "action1");
            client.unsubscribe(HOST, PORT, "pub", "action2");
            client.unsubscribe(HOST, PORT, "pub", "action3");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_multiple_subscribe_unsubscribe_part()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
            client.subscribe(HOST, PORT, "pub", "action1", handler1, 0);
            client.subscribe(HOST, PORT, "pub", "action2", handler2, 0);
            client.subscribe(HOST, PORT, "pub", "action3", handler3, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(2000) && tmpNum3.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_1 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_1.getInt());
            BasicInt sub2_num_1 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(2000, sub2_num_1.getInt());
            BasicInt sub3_num_1 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(2000, sub3_num_1.getInt());
            client.unsubscribe(HOST, PORT, "pub", "action1");
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(3000) && tmpNum3.getInt().Equals(3000))
                {
                    break;
                }
                Thread.Sleep(3000);
            }
            BasicInt sub1_num_2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_2.getInt());
            BasicInt sub2_num_2 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_2.getInt());
            BasicInt sub3_num_2 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(3000, sub3_num_2.getInt());
            client.unsubscribe(HOST, PORT, "pub", "action2");
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(3000) && tmpNum3.getInt().Equals(4000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_3 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_3.getInt());
            BasicInt sub2_num_3 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_3.getInt());
            BasicInt sub3_num_3 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(4000, sub3_num_3.getInt());
            client.unsubscribe(HOST, PORT, "pub", "action3");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_actionName_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(HOST, PORT, "pub", "sub1", handler1, 0, false);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(HOST, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_default_specify_actionName_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(HOST, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(HOST, PORT, "pub", "action1", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10) order by ticker, permno).values(), (select * from sub1 order by ticker, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(HOST, PORT, "pub", "action1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        

        //subscribe haStreamTable
        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_haStreamTable_on_leader()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderHost = StreamLeaderHostTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            Console.WriteLine(StreamLeaderHost);
            Console.WriteLine(StreamLeaderPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            try
            {
                conn1.run("dropStreamTable(\"haSt1\")");
            }catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn1, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            client.subscribe(StreamLeaderHost, StreamLeaderPort, "haSt1", handler1, 0);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn1.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicBoolean re = (BasicBoolean)conn1.run("each(eqObj, (select * from haSt1 order by ticker, permno).values(), (select * from sub1 order by ticker, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "haSt1");
            conn1.run("dropStreamTable(\"haSt1\")");
            client.close();
            conn.close();
            conn1.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_haStreamTable_on_follower()
        {
            DBConnection conn = new DBConnection();
            conn.connect(HOST, PORT, "admin", "123456");
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamFollowerHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name!=\"{0}\")[0]", StreamLeader));
            String StreamFollowerHost = StreamFollowerHostTmp.getString();
            BasicInt StreamFollowerPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where name!=\"{0}\")[0]", StreamLeader));
            int StreamFollowerPort = StreamFollowerPortTmp.getInt();
            Console.WriteLine(StreamFollowerHost);
            Console.WriteLine(StreamFollowerPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456", "", true, HASTREAM_GROUP);
            try
            {
                conn1.run("dropStreamTable(\"haSt1\")");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            DBConnection conn2 = new DBConnection();
            conn2.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            PrepareStreamTable1(conn2, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, LOCALPORT);
            client.subscribe(StreamFollowerHost, StreamFollowerPort, "haSt1", handler1, 0);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn2.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicBoolean re = (BasicBoolean)conn2.run("each(eqObj, (select * from haSt1 order by ticker, permno).values(), (select * from sub1 order by ticker, permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(StreamFollowerHost, StreamFollowerPort, "haSt1");
            conn1.run("dropStreamTable(\"haSt1\")");
            conn2.run("undef(`sub1, SHARED)");
            client.close();
            conn.close();
            conn1.close();
            conn2.close();
            streamConn.close();
        }

    }
}

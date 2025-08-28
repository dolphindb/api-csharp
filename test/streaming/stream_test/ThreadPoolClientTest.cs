using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using System.Threading;
using dolphindb_config;

namespace dolphindb_csharp_api_test.stream_test
{
    [TestClass]
    public class ThreadedPoolClientSubscribeTest
    {

        public static DBConnection streamConn;
        private string SERVER = MyConfigReader.SERVER;
        private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string LOCALHOST = MyConfigReader.LOCALHOST;
        private readonly int LOCALPORT = MyConfigReader.LOCALPORT;
        static private int SUB_FLAG = MyConfigReader.SUB_FLAG;
        private string NODE1_HOST = MyConfigReader.NODE1_HOST;
        private readonly int NODE1_PORT = MyConfigReader.NODE1_PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;

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


        public void PrepareUser1(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser1\", \"123456\");grant(\"testuser1\", TABLE_READ, \"*\")}\n rpc(getControllerAlias(), test_user)";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareUser2(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser2\", \"123456\")}\n rpc(getControllerAlias(), test_user)";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareUser3(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser3\", \"123456\")}\n rpc(getControllerAlias(), test_user)";
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

        public class Handler6 : MessageHandler
        {
            private StreamDeserializer deserializer_;
            private List<BasicMessage> msg1 = new List<BasicMessage>();
            private List<BasicMessage> msg2 = new List<BasicMessage>();
            public static object locker = new object();

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


                    lock (locker)
                    {
                        if (message.getSym() == "msg1")
                        {
                            msg1.Add(message);
                        }
                        else if (message.getSym() == "msg2")
                        {
                            msg2.Add(message);
                        }
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


        public class Handler7 : MessageHandler
        {
            private List<BasicMessage> msg1 = new List<BasicMessage>();
            private List<BasicMessage> msg2 = new List<BasicMessage>();
            public static object locker = new object();


            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    lock (locker)
                    {
                        if (((BasicMessage)msg).getSym() == "msg1")
                        {
                            msg1.Add((BasicMessage)msg);
                        }
                        else if (((BasicMessage)msg).getSym() == "msg2")
                        {
                            msg2.Add((BasicMessage)msg);
                        }
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


        [TestMethod]
        [DeploymentItem(@"settings.txt")]
        public void Test_ThreadPooledClient_subscribe_host_localhost()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            Exception exception = null;
            ThreadPooledClient client = null;
            try
            {
                //Not allow to use localhost 
                client = new ThreadPooledClient("localhost", 10200);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNull(exception);
            //client.close();
            conn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_streamTable_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10201);
            Exception exception = null;
            try
            {
                //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
                client.subscribe(SERVER, PORT, "trades", "trades", handler, -1);
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
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub1");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10202);
            client.subscribe(SERVER, PORT, "pub1", "action1", handler1, 0);
            Exception exception = null;
            try
            {
                //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
                client.subscribe(SERVER, PORT, "pub1", "action1", handler2, 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.unsubscribe(SERVER, PORT, "pub1", "action1");
            client.close();
            conn.run("undef(`pub1, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreadPooledClient_not_specify_host()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub2");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub2", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub2");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(10203);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub2", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub2", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub2");

            try
            {
                client.close();
                Thread.Sleep(1000);
                conn.run("undef(`pub2, SHARED)");
                conn.run("undef(`sub1, SHARED)");
                conn.close();
                streamConn.close();
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
                Console.Out.WriteLine(e.StackTrace);
            }
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_default()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub3");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub3", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub3");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10204);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub3", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub3", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub3");
            client.close();
            conn.run("undef(`pub3, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub4");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub4", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub4");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10205);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub4", handler1, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub4", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub4");
            client.close();
            conn.run("undef(`pub4, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_neg_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub5");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub5", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub5");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10206);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub5", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub5", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub5");
            client.close();
            conn.run("undef(`pub5, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_less_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub6");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub6", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub6");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10207);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub6", handler1, 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub6", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub6");
            client.close();
            conn.run("undef(`pub6, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_larger_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub7");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub7", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub7");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10208);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            Exception exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub7", handler1, 1500);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.close();
            conn.run("undef(`pub7, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_actionName()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub8");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub8", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub8");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10209);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub8", "sub1", handler1, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub8", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub8", "sub1");
            client.close();
            conn.run("undef(`pub8, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_int_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub9");
            conn.run("setStreamTableFilterColumn(pub9, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10210);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            client.subscribe(SERVER, PORT, "pub9", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub9.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub9 where permno in 1..10");
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub9 where permno in 1..10 order by permno,ticker).values(), (select * from sub1 order by permno,ticker).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            //client.unsubscribe(SERVER, PORT, "pub9");
            //client.close();
            //conn.run("undef(`pub9, SHARED)");
            //conn.run("undef(`sub1, SHARED)");
            //conn.close();
            //streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub10");
            conn.run("setStreamTableFilterColumn(pub10, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10211);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub10", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub10.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub10 where ticker in \"A\"+string(1..10)");
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub10 where ticker in \"A\"+string(1..10) order by ticker, permno,price1).values(), (select * from sub1 order by ticker, permno,price1).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub10");
            client.close();
            conn.run("undef(`pub10, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_string_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub11;setStreamTableFilterColumn(pub11, `ticker)";
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10212);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
            BasicStringVector filter = new BasicStringVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub11", handler1, 0, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub11.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub11 where ticker in \"A\"+string(1..10)");
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub11 where ticker in \"A\"+string(1..10) order by ticker, permno,price1).values(), (select * from sub1 order by ticker, permno,price1).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub11");
            client.close();
            conn.run("undef(`pub11, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_0_specify_date_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub12;setStreamTableFilterColumn(pub12, `date)";
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10213);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
            BasicDateVector filter = (BasicDateVector)conn.run("2012.01.01..2012.01.10");
            client.subscribe(SERVER, PORT, "pub12", "sub1", handler1, 0, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub12.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub12 where date in 2012.01.01..2012.01.10");
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub12 where date in 2012.01.01..2012.01.10 order by date, permno,ticker).values(), (select * from sub1 order by date, permno,ticker).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub12", "sub1");
            client.close();
            conn.run("undef(`pub12, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_reconnect_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub13");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub13", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub13");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10214);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(SERVER, PORT, "pub13", handler1, 0, true);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub13", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub13");
            client.close();
            conn.run("undef(`pub13, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_specify_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub14");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub14", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub14");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10215);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(SERVER, PORT, "pub14", handler1, 0, false);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub14", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub14");
            client.close();
            conn.run("undef(`pub14, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_one_client_multiple_subscription()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub15");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub15", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub15");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10216);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub15", "action1", handler1, 0);
            client.subscribe(SERVER, PORT, "pub15", "action2", handler2, -1);
            client.subscribe(SERVER, PORT, "pub15", "action3", handler3, 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub15", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub15", "action1");
            client.unsubscribe(SERVER, PORT, "pub15", "action2");
            client.unsubscribe(SERVER, PORT, "pub15", "action3");
            client.close();
            conn.run("undef(`pub15, SHARED)");
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
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub16");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub16", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub16");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10217);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub16", "action1", handler1, 0);
            client.subscribe(SERVER, PORT, "pub16", "action2", handler2, 0);
            client.subscribe(SERVER, PORT, "pub16", "action3", handler3, 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub16", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub16", "action1");
            WriteStreamTable1(conn, "pub16", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub16", "action2");
            WriteStreamTable1(conn, "pub16", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub16", "action3");
            client.close();
            conn.run("undef(`pub16, SHARED)");
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
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub17");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub17", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub17");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10218);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect)
            client.subscribe(SERVER, PORT, "pub17", "sub1", handler1, 0, false);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub17", 1000);
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
            client.unsubscribe(SERVER, PORT, "pub17", "sub1");
            client.close();
            conn.run("undef(`pub17, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_offset_default_specify_actionName_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub18");
            conn.run("setStreamTableFilterColumn(pub18, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10219);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub18", "action1", handler1, 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub18.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub18 where ticker in \"A\"+string(1..10)");
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub18 where ticker in \"A\"+string(1..10) order by ticker, permno,price1).values(), (select * from sub1 order by ticker, permno,price1).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub18", "action1");
            client.close();
            conn.run("undef(`pub18, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }



        //subscribe haStreamTable
        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_haStreamTable_on_leader()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
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
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn1, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10220);
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
            conn.connect(SERVER, PORT, "admin", "123456");
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
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10221);
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

        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_user_notExist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10222);
            Exception exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub19", "sub1", handler1, 0, false, filter, null, "a1234", "a1234");
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
        public void Test_ThreadPooledClient_subscribe_user_password_notTrue()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10223);
            Exception exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub20", "sub1", handler1, 0, false, filter, null, "admin", "123455");
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
        public void Test_ThreadPooledClient_subscribe_user_password()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            PrepareUser1(conn);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "testuser1", "123456");
            PrepareUser3(conn);
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "testuser3", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub21");
            conn.run("setStreamTableFilterColumn(pub21, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10224);
            Exception exception = null;
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            // client.subscribe(SERVER, PORT, "pub21", handler1, 0, filter, -1);
            client.subscribe(SERVER, PORT, "pub21", "sub1", handler1, 0, false, filter, null, "admin", "123456");
            //client.subscribe(SERVER, PORT, "pub21", "sub1", handler1, 0, false, filter, null, "testuser1", "123456");
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)

            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub21.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub21 where permno in 1..10");
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
            try
            {
                BasicInt num3 = (BasicInt)conn1.run("exec count(*) from sub1");
                Assert.AreEqual(expectedNum.getInt(), num3.getInt());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            try
            {
                BasicInt num4 = (BasicInt)conn2.run("exec count(*) from sub1");
                Assert.AreEqual(expectedNum.getInt(), num4.getInt());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            //Assert.IsNotNull(exception);
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub21 where permno in 1..10  order by  permno,ticker ).values(), (select * from sub1 order by  permno,ticker).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub21", "sub1");
            client.close();
            String script1 = "def test_user(){deleteUser(\"testuser1\")}\n rpc(getControllerAlias(), test_user)";
            String script2 = "def test_user(){deleteUser(\"testuser3\")}\n rpc(getControllerAlias(), test_user)";
            conn.run(script1);
            conn.run(script2);
            conn.run("undef(`pub21, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            conn1.close();
            conn2.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_schema()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables1pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "st1pool = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1pool, tableName=`outTables1pool, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables1pool, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            //schema StreamDeserializer
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
            Dictionary<string, BasicDictionary> schemas = new Dictionary<string, BasicDictionary>();
            schemas["msg1"] = outSharedTables1Schema;
            schemas["msg2"] = outSharedTables2Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(schemas);
            Handler6 handler = new Handler6(streamFilter);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10225);
            client.subscribe(SERVER, PORT, "outTables1pool", "mutiSchema", handler, 0, true);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(4000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            //for (int i = 0; i < table1.columns(); ++i)
            //{
            //    IVector tableCol = table1.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
            //    }
            //}
            //for (int i = 0; i < table2.columns(); ++i)
            //{
            //    IVector tableCol = table2.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
            //    }
            //}
            client.unsubscribe(SERVER, PORT, "outTables1pool", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables1pool, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_colType()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables2pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            String script = "st1pool = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1pool, tableName=`outTables2pool, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables2pool, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            //2¡¢colType
            Dictionary<string, List<DATA_TYPE>> colTypes = new Dictionary<string, List<DATA_TYPE>>();
            List<DATA_TYPE> table1ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_DOUBLE };
            colTypes["msg1"] = table1ColTypes;
            List<DATA_TYPE> table2ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE };
            colTypes["msg2"] = table2ColTypes;
            StreamDeserializer streamFilter = new StreamDeserializer(colTypes);

            Handler6 handler = new Handler6(streamFilter);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10226);
            client.subscribe(SERVER, PORT, "outTables2pool", "mutiSchema", handler, 0, true);

            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(40000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            //for (int i = 0; i < table1.columns(); ++i)
            //{
            //    IVector tableCol = table1.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
            //    }
            //}
            //for (int i = 0; i < table2.columns(); ++i)
            //{
            //    IVector tableCol = table2.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
            //    }
            //}
            client.unsubscribe(SERVER, PORT, "outTables2pool", "mutiSchema");
            client.close();
            //     conn.run("undef(`outTables2pool, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_tablename()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables3pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "st1pool = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1pool, tableName=`outTables3pool, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables3pool, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            //tablename
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "table1");
            tables["msg2"] = new Tuple<string, string>("", "table2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            Handler6 handler = new Handler6(streamFilter);
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10227);
            client.subscribe(SERVER, PORT, "outTables3pool", "mutiSchema", handler, 0, true);

            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(4000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            //for (int i = 0; i < table1.columns(); ++i)
            //{
            //    IVector tableCol = table1.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
            //    }
            //}
            //for (int i = 0; i < table2.columns(); ++i)
            //{
            //    IVector tableCol = table2.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
            //    }
            //}
            client.unsubscribe(SERVER, PORT, "outTables3pool", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables3pool, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables4pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            try
            {
                conn.run("dropStreamTable(`st1pool)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "st1pool = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1pool, tableName=`outTables4pool, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 100000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables4pool, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            filter["msg2"] = outSharedTables2Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10228);
            client.subscribe(SERVER, PORT, "outTables4pool", "mutiSchema", handler, 0, true, null, streamFilter);

            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(50000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            //for (int i = 0; i < table1.columns(); ++i)
            //{
            //    IVector tableCol = table1.getColumn(i);
            //    for (int j = 0; j < 100000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
            //    }
            //}
            //for (int i = 0; i < table2.columns(); ++i)
            //{
            //    IVector tableCol = table2.getColumn(i);
            //    for (int j = 0; j < 10000; ++j)
            //    {
            //        Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
            //    }
            //}
            client.unsubscribe(SERVER, PORT, "outTables4pool", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables4pool, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_memorytable()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Exception exception = null;
            try
            {
                conn.run("dropStreamTable(`outTables5)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            //String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
            //       "enableTableShareAndPersistence(table=st1, tableName=`outTables5, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            //conn.run(script);

            string replayScript = "n = 100000;t1 = table(100:0, `timestampv`sym`blob`price2, [TIMESTAMP, SYMBOL, BLOB, DOUBLE]);" +
                "share t1 as table1";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");

            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10229);
            try
            {
                client.subscribe(SERVER, PORT, "table1", "mutiSchema", handler, 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.close();
            //conn.run("undef(`table1, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_streamTable_colTypeFalse2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Exception exception = null;
            try
            {
                conn.run("dropStreamTable(`outTables6)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,INT,BLOB,DOUBLE]) as st1\n" +
                    "enableTablePersistence(table=st1, cacheSize=1200000)";
            conn.run(script);

            string replayScript = "insert into st1 values(NULL,NULL,NULL,NULL)\t\n";
            conn.run(replayScript);
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10230);
            try
            {

                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Thread.Sleep(10000);
            Assert.IsNotNull(exception);
            //client.unsubscribe(SERVER, PORT, "st1", "mutiSchema");
            client.close();
            //conn.run("undef(`st1, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreaedPoolClient_subscribe_streamDeserilizer_streamTable_colTypeFalse3()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Exception exception = null;
            try
            {
                conn.run("dropStreamTable(`outTables7)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,INT,DOUBLE]) as st1\n" +
                    "enableTablePersistence(table=st1, cacheSize=1200000)";
            conn.run(script);

            //string replayScript = "insert into st1 values(NULL,NULL,NULL,NULL)\t\n";
            //conn.run(replayScript);


            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10231);
            try
            {

                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Thread.Sleep(1000);
            Assert.IsNotNull(exception);
            //client.unsubscribe(SERVER, PORT, "st1", "mutiSchema");
            client.close();
            //conn.run("undef(`st1, SHARED)");
            conn.close();
            streamConn.close();
        }
        [TestMethod]
        public void Test_ThreadPooledClient_subscribe_one_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub3");
            PrepareStreamTable1(conn, "sub1");
            //write 1 rows first
            WriteStreamTable1(conn, "pub3", 1);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub3");
            Assert.AreEqual(1, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadPooledClient client = new ThreadPooledClient(LOCALHOST, 10232);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
            client.subscribe(SERVER, PORT, "pub3", handler1, -1);
            //write 1 rows after subscribe
            WriteStreamTable1(conn, "pub3", 1);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub3");
            client.close();
            conn.run("undef(`pub3, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

    }
}

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using System.Threading;
using dolphindb_config;

namespace dolphindb_csharp_api_test.streamReverse_test
{
    [TestClass]
    public class ThreadedClientReverseTest
    {

        public static DBConnection streamConn;
        public static string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string LOCALHOST = MyConfigReader.LOCALHOST;
        private readonly int LOCALPORT = MyConfigReader.LOCALPORT;
        static private int SUB_FLAG = MyConfigReader.SUB_FLAG;
        private string NODE1_HOST = MyConfigReader.NODE1_HOST;
        private readonly int NODE1_PORT = MyConfigReader.NODE1_PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        private readonly int TIMEOUT = 10000;

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
        
        public class Handler6 : MessageHandler
        {
            private StreamDeserializer deserializer_;
            private List<BasicMessage> msg1 = new List<BasicMessage>();
            private List<BasicMessage> msg2 = new List<BasicMessage>();

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
                    if (message.getSym() == "msg1")
                    {
                        msg1.Add(message);
                    }
                    else if (message.getSym() == "msg2")
                    {
                        msg2.Add(message);
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


            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
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
        class Handler8 : MessageHandler
        {
            
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                
                try
                {

                    DBConnection streamConn1 = new DBConnection();
                    streamConn1.connect(SERVER, PORT, "admin", "123456");
                    msg.getEntity(0);
                    String script = String.Format("t= table(10000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);\n insert into t values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} );\n pt=loadTable(\"dfs://test_stream\",`pt);\n pt.append!(t);\n", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    streamConn1.run(script);
                    
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadedClient_subscribe_host_localhost()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            Exception exception = null;
            ThreadedClient client = null;
            try
            {
                //allow to use localhost 
                 client = new ThreadedClient("localhost", 10100);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNull(exception);
            client.close();
            conn.close();

        }
        [TestMethod]

        public void Test_ThreadedClient_subscribe_port_null()
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
            ThreadedClient client = new ThreadedClient();
            client.subscribe(SERVER, PORT, "pub1", "action1", handler1, 0);
            client.close();
            conn.close();
        }

        [TestMethod]
        public void Test_ThreadedClient_subscribe_streamTable_not_exist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            Exception exception = null;
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "trades", handler, -1, -1);
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
        public void Test_ThreadedClient_subscribe_duplicated_actionName()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0);
            Exception exception = null;
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "action1", handler2, 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreaedClient_not_specify_host()
        {
            //DBConnection conn = new DBConnection();
            //conn.connect(SERVER, PORT, "admin", "123456");
            //streamConn = new DBConnection();
            //streamConn.connect(SERVER, PORT, "admin", "123456");
            //PrepareStreamTable1(conn, "pub");
            //PrepareStreamTable1(conn, "sub1");
            ////write 1000 rows first
            //WriteStreamTable1(conn, "pub", 1000);
            //BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            //Assert.AreEqual(1000, num1.getInt());
            //Handler1 handler1 = new Handler1();
            //ThreadedClient client = new ThreadedClient(10103);
            ////usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler,int batchSize=-1)
            //client.subscribe(SERVER, PORT, "pub", handler1, -1);
            ////write 1000 rows after subscribe
            //WriteStreamTable1(conn, "pub", 1000);
            //for (int i = 0; i < 10; i++)
            //{
            //    BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
            //    if (tmpNum.getInt().Equals(1000))
            //    {
            //        break;
            //   }
            //    Thread.Sleep(1000);
            //}
            //BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            //Assert.AreEqual(1000, num2.getInt());
            //client.unsubscribe(SERVER, PORT, "pub");
            //client.close();
            //conn.run("undef(`pub, SHARED)");
            //conn.run("undef(`sub1, SHARED)");
            //conn.close();
            //streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_0()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_neg_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_less_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 500, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_larger_than_existing_rows()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            Exception exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub", handler1, 1500, -1);
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
        public void Test_ThreaedClient_subscribe_specify_actionName()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, -1);
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
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_int_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_string_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `ticker)";
            try
            {
                conn.run("dropStreamTable(`pub)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicStringVector filter = new BasicStringVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_date_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `date)";
            try
            {
                conn.run("dropStreamTable(`pub)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicDateVector filter = (BasicDateVector)conn.run("2012.01.01..2012.01.10");
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, filter, -1);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where date in 2012.01.01..2012.01.10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_reconnect_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, true, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, false, -1);
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
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_one_client_multiple_subscription()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action2", handler2, -1, -1);
            client.subscribe(SERVER, PORT, "pub", "action3", handler3, 500, -1);
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
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            client.unsubscribe(SERVER, PORT, "pub", "action2");
            client.unsubscribe(SERVER, PORT, "pub", "action3");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_multiple_subscribe_unsubscribe_part()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action2", handler2, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action3", handler3, 0, -1);
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
            client.unsubscribe(SERVER, PORT, "pub", "action1");
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
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_2.getInt());
            BasicInt sub2_num_2 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_2.getInt());
            BasicInt sub3_num_2 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(3000, sub3_num_2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action2");
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
            client.unsubscribe(SERVER, PORT, "pub", "action3");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_actionName_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, -1);
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
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_actionName_symbol_filter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, filter, -1);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        //batchSize not -1
        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_actionName_batchSize_100()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler5 handler5 = new Handler5();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler5, 0, 100);
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
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_not_0_batchSize_100()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler5 handler5 = new Handler5();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler5, 500, 100);
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
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_symbol_filter_batchSize_100()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler5 handler5 = new Handler5();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler5, 0, filter, 100);
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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            streamConn.close();
        }
        
        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_schema()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables1)");
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

            String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1, tableName=`outTables1, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables1, dateColumn=`timestampv, timeColumn=`timestampv)";
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(SERVER, PORT, "outTables1", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables1", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables1, SHARED)");
            conn.close();
            streamConn.close();

        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_colType()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables2)");
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
            String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1, tableName=`outTables2, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables2, dateColumn=`timestampv, timeColumn=`timestampv)";
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(SERVER, PORT, "outTables2", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables2", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables2, SHARED)");
            conn.close();
            streamConn.close();
        }

        
        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_tablename()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables3)");
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

            String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1, tableName=`outTables3, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables3, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");


            //tablename
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "table1");
            tables["msg2"] = new Tuple<string, string>("", "table2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);


            Handler6 handler = new Handler6(streamFilter);
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(SERVER, PORT, "outTables3", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables3", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables3, SHARED)");
            conn.close();
            streamConn.close();
        }
        
        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            try
            {
                conn.run("dropStreamTable(`outTables4)");
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

            String script = "st1 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE])\n" +
                    "enableTableShareAndPersistence(table=st1, tableName=`outTables4, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\t\n";
            conn.run(script);

            string replayScript = "n = 100000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables4, dateColumn=`timestampv, timeColumn=`timestampv)";
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(SERVER, PORT, "outTables4", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
            
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(10000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 100000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables4", "mutiSchema");
            client.close();
            //conn.run("undef(`outTables4, SHARED)");
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_memorytable()
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            try
            {
                client.subscribe(SERVER, PORT, "table1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
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
        public void Test_ThreaedClient_subscribe_streamDeserilizer_streamTable_colTypeFalse2()
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            try
            {
                
                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
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
        public void Test_ThreaedClient_subscribe_streamDeserilizer_streamTable_colTypeFalse3()
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
            ThreadedClient client = new ThreadedClient(LOCALHOST, 12129);
            try
            {

                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
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

        //subscribe haStreamTable
        [TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_on_leader()
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
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn1, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(StreamLeaderHost, StreamLeaderPort, "haSt1", handler1, 0, -1);
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
            BasicBoolean re = (BasicBoolean)conn1.run("each(eqObj, haSt1.values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "haSt1");
            conn1.run("dropStreamTable(\"haSt1\")");
            client.close();
            conn.close();
            conn1.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_DBConnection_connect_follower_append_haStreamTable()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            string script0 = "leader = getStreamingLeader(3);";
            script0 += "groupSitesStr = (exec sites from getStreamingRaftGroups() where id ==3)[0];";
            script0 += "groupSites = split(groupSitesStr, \",\");";
            script0 += "followerInfo = exec top 1 *  from rpc(getControllerAlias(), getClusterPerf) where site in groupSites and name!=leader;";
            conn.run(script0);
            BasicString StreamFollowerHostTmp = (BasicString)conn.run("(exec host from followerInfo)[0]");
            String StreamFollowerHost = StreamFollowerHostTmp.getString();
            BasicInt StreamFollowerPortTmp = (BasicInt)conn.run("(exec port from followerInfo)[0]");
            int StreamFollowerPort = StreamFollowerPortTmp.getInt();

            Console.WriteLine(StreamFollowerHost);
            Console.WriteLine(StreamFollowerPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456", "", true, HASTREAM_GROUP);
            try
            {
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn1, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            BasicLong total = (BasicLong)conn1.run("getPersistenceMeta(haSt1).totalSize");
            Assert.AreEqual(total.getValue(), 1000);
            conn1.run("dropStreamTable('haSt1')");
            conn1.close();
            conn.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_on_follower()
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
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            DBConnection conn2 = new DBConnection();
            conn2.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            PrepareStreamTable1(conn2, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(StreamFollowerHost, StreamFollowerPort, "haSt1", handler1, 0, -1);
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
            BasicBoolean re = (BasicBoolean)conn2.run("each(eqObj, haSt1.values(), sub1.values()).all()");
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
        public void Test_ThreaedClient_subscribe_reconnect_false()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "";
            script += "dbName = 'dfs://test_stream';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
            script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
            conn.run(script);
            //conn.run("undef(`pub, SHARED)");
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
            String nodeAlias = nodeAliasTmp.getString();
            BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
            String ControllerHost = ControllerHostTmp.getString();
            BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
            int ControllerPort = ControllerPortTmp.getInt();
            DBConnection conn1 = new DBConnection();
            conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("enableTablePersistence(pub, true, true, 1000000)");            
            Handler8 handler8 = new Handler8();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler8, -1, false, -1);
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt tmpNum_1 = (BasicInt)conn.run("exec count(*) from pub");
            Console.Out.WriteLine(tmpNum_1.ToString());
            Thread.Sleep(10000);
            BasicTable tmpNum_2 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine("---------------11111111111111---------");
            Console.Out.WriteLine(tmpNum_2.rows());
            Console.Out.WriteLine("---------------11111111111111---------");
            Assert.AreEqual(true, 1000>tmpNum_2.rows());
            try
            {
                conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
            Thread.Sleep(5000);
            conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
            Thread.Sleep(50000);
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "admin", "123456");
            Console.Out.WriteLine("---------------12222222222222222---------");
            BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine(tmpNum_3.rows());
            Console.Out.WriteLine("---------------12222222222222222---------");

            Assert.AreEqual(true, tmpNum_3.rows() > tmpNum_2.rows());
            Thread.Sleep(5000);

            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            conn.close();
        }
        [TestMethod]
        public void Test_ThreaedClient_subscribe_reconnect_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "";
            script += "dbName = 'dfs://test_stream';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
            script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
            conn.run(script);
            //conn.run("undef(`pub, SHARED)");
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
            String nodeAlias = nodeAliasTmp.getString();
            BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
            String ControllerHost = ControllerHostTmp.getString();
            BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
            int ControllerPort = ControllerPortTmp.getInt();
            DBConnection conn1 = new DBConnection();
            conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("enableTablePersistence(pub, true, true, 1000000)");
            Handler8 handler8 = new Handler8();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler8, -1, true, -1);
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt tmpNum_1 = (BasicInt)conn.run("exec count(*) from pub");
            Console.Out.WriteLine(tmpNum_1.ToString());
            Thread.Sleep(10000);
            BasicTable tmpNum_2 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine("---------------11111111111111---------");
            Console.Out.WriteLine(tmpNum_2.rows());
            Console.Out.WriteLine("---------------11111111111111---------");
            Assert.AreEqual(true, 1000 > tmpNum_2.rows());
            try
            {
                conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
            Thread.Sleep(5000);
            conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
            Thread.Sleep(50000);
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "admin", "123456");
            Console.Out.WriteLine("---------------12222222222222222---------");
            BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine(tmpNum_3.rows());
            Console.Out.WriteLine("---------------12222222222222222---------");

            Assert.AreEqual(true, tmpNum_3.rows()>tmpNum_2.rows());
            Thread.Sleep(5000);

            client.unsubscribe(SERVER, PORT, "pub");
            client.close();
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            conn.close();
        }
        //[TestMethod]//High availability when stream subscription is established is temporarily not supported
        //public void Test_ThreaedClient_subscribe_reconnect_true()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, "admin", "123456");
        //    String script = "";
        //    script += "dbName = 'dfs://test_stream';";
        //    script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
        //    script += "db = database(dbName, HASH, [INT, 10]);";
        //    script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
        //    script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
        //    conn.run(script);
        //    conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        //    BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
        //    String nodeAlias = nodeAliasTmp.getString();
        //    BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
        //    String ControllerHost = ControllerHostTmp.getString();
        //    BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
        //    int ControllerPort = ControllerPortTmp.getInt();
        //    DBConnection conn1 = new DBConnection();
        //    conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
        //    PrepareStreamTable1(conn, "pub");
        //    conn.run("enableTablePersistence(pub, true, true, 1000000)");
        //    Handler8 handler8 = new Handler8();
        //    ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
        //    //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
        //    WriteStreamTable1(conn, "pub", 1000);

        //    try
        //    {
        //        conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.StackTrace);
        //    }
        //    Thread.Sleep(5000);
        //    client.subscribe(SERVER, PORT, "pub", handler8, -1, true, -1);
        //    conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
        //    Thread.Sleep(50000);
        //    DBConnection conn2 = new DBConnection();
        //    conn2.connect(SERVER, PORT, "admin", "123456");

        //    Console.Out.WriteLine("---------------22222222222---------");
        //    BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream\",`pt)");
        //    Console.Out.WriteLine(tmpNum_3.rows());
        //    Console.Out.WriteLine("---------------12222222222222222---------");

        //    Thread.Sleep(5000);


        //    client.unsubscribe(SERVER, PORT, "pub");
        //    client.close();
        //    conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        //    conn.run("undef(`pub, SHARED)");
        //    conn.close();
        //}

        //[TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_reconnect_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderHost = StreamLeaderHostTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            Handler4 handler4 = new Handler4();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            client.subscribe(StreamLeaderHost, StreamLeaderPort, "haPub1", handler4, -1, true, -1);
            for (int i = 0; i < 30; i++)
            {
                Console.WriteLine(SUB_FLAG);
                Thread.Sleep(2000);
            }
        }
        

        [TestMethod]
        public void Test_ThreaedClient_subscribe_user_notExist()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            Exception exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "a1234", "a1234");
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
        public void Test_ThreaedClient_subscribe_user_password_notTrue()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            Exception exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "admin", "123455");
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
        public void Test_ThreaedClient_subscribe_user_password()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            PrepareUser1(conn);
            PrepareUser3(conn);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "testuser1", "123456");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "testuser3", "123456");
            streamConn = new DBConnection();
            streamConn.connect(SERVER, PORT, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(LOCALHOST, 0);
            Exception exception = null;
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            // client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "admin", "123456");
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)

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
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            client.close();
            String script1 = "def test_user(){deleteUser(\"testuser1\")}\n rpc(getControllerAlias(), test_user)";
            String script2 = "def test_user(){deleteUser(\"testuser3\")}\n rpc(getControllerAlias(), test_user)";
            conn.run(script1);
            conn.run(script2);
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.close();
            conn1.close();
            conn2.close();
            streamConn.close();
        }
        
        //[TestMethod]
        //public void Test_ThreaedClient_subscribe_user_not_grant()
        //{
        //    DBConnection conn1 = new DBConnection();
        //    conn1.connect(SERVER, PORT, "admin", "123456");
        //    PrepareUser2(conn1);
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, "uesr123", "123456");
        //    streamConn = new DBConnection();
        //    streamConn.connect(SERVER, PORT, "admin", "123456");
        //    PrepareStreamTable1(conn, "pub");
        //    conn.run("setStreamTableFilterColumn(pub, `permno)");
        //    PrepareStreamTable1(conn, "sub1");
        //    Handler1 handler1 = new Handler1();
        //    ThreadedClient client = new ThreadedClient(LOCALHOST, LOCALPORT);
        //    Exception exception = null;
        //    //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        //    BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });     
        //    try
        //    {
        //        //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
        //        client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "testuser2", "123456");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.ToString());
        //        exception = ex;
        //    }
        //    Assert.IsNotNull(exception);
        //    client.unsubscribe(SERVER, PORT, "pub", "sub1");
        //    conn.run("undef(`pub, SHARED)");
        //    conn.run("undef(`sub1, SHARED)");
        //    client.close();
        //    conn.close();
        //    streamConn.close();
        //}

    }
}

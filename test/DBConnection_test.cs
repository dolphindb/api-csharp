using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using System.IO;
using System.Net.Sockets;
using dolphindb.io;
using System.Text;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Collections;
using System.Threading.Tasks;
using System.Configuration;
using dolphindb_config;

namespace dolphindb_csharpapi_test
{

    [TestClass]
    public class DBConnection_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_connect_asynchronousTask_true()
        {
            DBConnection conn = new DBConnection(true);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DateTime beforeDT = System.DateTime.Now;
            conn.run("sleep(10000)");
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            Assert.AreEqual(true, ts.TotalMilliseconds < 100);
            conn.close();

        }

        [TestMethod]
        public void Test_connect_asynchronousTask_false()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DateTime beforeDT = System.DateTime.Now;
            conn.run("sleep(10000)");
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            Assert.AreEqual(true, ts.TotalMilliseconds > 10000);
            conn.close();
        }

        [TestMethod]
        public void Test_connect_useSSL_true()
        {
            DBConnection conn = new DBConnection(false, true);
           // public DBConnection(bool asynchronousTask, bool useSSL, bool compress, bool usePython = false)
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Assert.AreEqual("'c'", ((BasicByte)conn.run("'c'")).getString());
            conn.close();

        }

        [TestMethod]
        public void Test_connect_useSSL_false()
        {
            DBConnection conn = new DBConnection(false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Assert.AreEqual("'c'", ((BasicByte)conn.run("'c'")).getString());
            conn.close();
        }

        [TestMethod]
        public void Test_connect_compress_true()
        {
            DBConnection conn = new DBConnection(false, true, true);
            // public DBConnection(bool asynchronousTask, bool useSSL, bool compress, bool usePython = false)
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Assert.AreEqual("'c'", ((BasicByte)conn.run("'c'")).getString());
            conn.close();

        }

        [TestMethod]
        public void Test_connect_compress_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Assert.AreEqual("'c'", ((BasicByte)conn.run("'c'")).getString());
            conn.close();
        }

        [TestMethod]
        public void Test_connect_host_not_exist()
        {
            DBConnection conn = new DBConnection();
            Exception exception = null;
            try
            {
                conn.connect("not_exist", PORT);
            }catch(Exception ex)
            {
                exception = ex;
            }
            //Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_connect_password_incorrect()
        {
            DBConnection conn = new DBConnection();
            Exception exception = null;
            try
            {
                conn.connect(SERVER, PORT, USER, USER);
            }catch(Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        

        [TestMethod]
        public void Test_connect_user_not_exist()
        {
            DBConnection conn = new DBConnection();
            Exception exception = null;
            try
            {
                conn.connect(SERVER, PORT, "not_exist", USER);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_connect_user_access()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            //only admin allows to excute getSessionMemoryStat
            try
            {
                conn.run("getSessionMemoryStat()");
            }catch(Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_Connect()
        {
            DBConnection db = new DBConnection();
            Assert.AreEqual(true, db.connect(SERVER, PORT));
            db.close();
        }

        [TestMethod]
        public void Test_Connect_withLogin()
        {
            DBConnection db = new DBConnection();
            bool re = db.connect(SERVER, PORT);
            Assert.AreEqual(true, re);
            db.close();
        }


        [TestMethod]
        public void Test_Connect_withLogout()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            db.run("logout()");
            db.close();
        }

        [TestMethod]
        public void Test_isBusy()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(this.getConn));
            t.IsBackground = true;
            t.Start(db);
            bool b = db.isBusy();
            Assert.IsFalse(b);
            db.close();
        }

        [TestMethod]
        public void getConn(object db)
        {
            DBConnection conn = (DBConnection)db;
            bool b = conn.isBusy();
            Assert.IsFalse(b);
            conn.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.IsTrue(((BasicBoolean)db.run("true")).getValue());
            Assert.IsFalse(((BasicBoolean)db.run("false")).getValue());
            Assert.IsFalse(((BasicBoolean)db.run("1==2")).getValue());
            Assert.IsTrue(((BasicBoolean)db.run("2==2")).getValue());
            Assert.IsTrue(((BasicBoolean)db.run("bool(NULL)")).getString() == "");
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_byte()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(97, ((BasicByte)db.run("'a'")).getValue());
            Assert.AreEqual("'c'", ((BasicByte)db.run("'c'")).getString());
            Assert.AreEqual(128, ((BasicByte)db.run("char()")).getValue());
            Assert.AreEqual(0, ((BasicByte)db.run("char(0)")).getValue());
            //Assert.AreEqual(-10, ((BasicByte)db.run("char(-10)")).getValue());
            //Assert.AreEqual(-127, ((BasicByte)db.run("char(-127)")).getValue());
            //Assert.AreEqual(127, ((BasicByte)db.run("char(127)")).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(1, ((BasicShort)db.run("1h")).getValue());
            Assert.AreEqual(256, ((BasicShort)db.run("256h")).getValue());
            Assert.AreEqual(1024, ((BasicShort)db.run("1024h")).getValue());
            Assert.AreEqual(0, ((BasicShort)db.run("0h")).getValue());
            Assert.AreEqual(-10, ((BasicShort)db.run("-10h")).getValue());
            Assert.AreEqual(32767, ((BasicShort)db.run("32767h")).getValue());
            Assert.AreEqual(-32767, ((BasicShort)db.run("-32767h")).getValue());
            //Assert.ThrowsException<InvalidCastException>(() => { ((BasicShort)db.run("100h+5000h")).getValue(); });
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(63, ((BasicInt)db.run("63")).getValue());
            Assert.AreEqual(129, ((BasicInt)db.run("129")).getValue());
            Assert.AreEqual(255, ((BasicInt)db.run("255")).getValue());
            Assert.AreEqual(1023, ((BasicInt)db.run("1023")).getValue());
            Assert.AreEqual(2047, ((BasicInt)db.run("2047")).getValue());
            Assert.AreEqual(-2047, ((BasicInt)db.run("-2047")).getValue());
            Assert.AreEqual(-129, ((BasicInt)db.run("-129")).getValue());
            //Assert.i<InvalidCastException>(() => { ((BasicInt)db.run("129123456456")).getValue(); });
            Assert.AreEqual(0, ((BasicInt)db.run("0")).getValue());
            Assert.AreEqual(-2147483648, ((BasicInt)db.run("int()")).getValue());
            Assert.AreEqual(-2147483647, ((BasicInt)db.run("-2147483647")).getValue());
            Assert.AreEqual(2147483647, ((BasicInt)db.run("2147483647")).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(1, ((BasicLong)db.run("1l")).getValue());
            Assert.AreEqual(0, ((BasicLong)db.run("long(0)")).getValue());
            Assert.AreEqual(-100, ((BasicLong)db.run("long(-100)")).getValue());
            Assert.AreEqual(100, ((BasicLong)db.run("long(100)")).getValue());
            Assert.AreEqual("", ((BasicLong)db.run("long()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14), ((BasicDate)db.run("2018.03.14")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01), ((BasicDate)db.run("1970.01.01")).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01), ((BasicDate)db.run("1969.01.01")).getValue());
            Assert.AreEqual("", ((BasicDate)db.run("date()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 01), ((BasicMonth)db.run("2018.03M")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01), ((BasicMonth)db.run("1970.01M")).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01), ((BasicMonth)db.run("1969.01M")).getValue());
            Assert.AreEqual("", ((BasicMonth)db.run("month()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45, 123).TimeOfDay, ((BasicTime)db.run("15:41:45.123")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000).TimeOfDay, ((BasicTime)db.run("00:00:00.000")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59, 999).TimeOfDay, ((BasicTime)db.run("23:59:59.999")).getValue());
            Assert.AreEqual("", ((BasicTime)db.run("time()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 14, 48, 00).TimeOfDay, ((BasicMinute)db.run("14:48m")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicMinute)db.run("00:00m")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 00).TimeOfDay, ((BasicMinute)db.run("23:59m")).getValue());
            Assert.AreEqual("", ((BasicMinute)db.run("minute()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45).TimeOfDay, ((BasicSecond)db.run("15:41:45")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicSecond)db.run("00:00:00")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay, ((BasicSecond)db.run("23:59:59")).getValue());
            Assert.AreEqual("", ((BasicSecond)db.run("second()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), ((BasicDateTime)db.run("2018.03.14T11:28:04")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicDateTime)db.run("1970.01.01T00:00:00")).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00), ((BasicDateTime)db.run("1969.01.01T00:00:00")).getValue());
            Assert.AreEqual("", ((BasicDateTime)db.run("datetime()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 3, 14, 15, 41, 45, 123), ((BasicTimestamp)db.run("2018.03.14T15:41:45.123")).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000), ((BasicTimestamp)db.run("1970.01.01T00:00:00.000")).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00, 000), ((BasicTimestamp)db.run("1969.01.01T00:00:00.000")).getValue());
            Assert.AreEqual("", ((BasicTimestamp)db.run("timestamp()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay, (((BasicNanoTime)db.run("15:41:45.123456789")).getValue()));
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 2223L), ((BasicNanoTimestamp)db.run("2018.03.14T15:41:45.123222321")).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3, ((BasicFloat)db.run("1.0f+2.0f")).getValue());
            Assert.AreEqual(Math.Round(129.1, 1), Math.Round(((BasicFloat)db.run("127.1f+2.0f")).getValue(), 1));
            Assert.AreEqual(Math.Round(1.2536, 4), Math.Round(((BasicFloat)db.run("1.2536f")).getValue(), 4));
            Assert.AreEqual(Math.Round(-1.2536, 4), Math.Round(((BasicFloat)db.run("-1.2536f")).getValue(), 4));
            Assert.AreEqual("", ((BasicFloat)db.run("float()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3, ((BasicDouble)db.run("1.0+2.0")).getValue());
            Assert.AreEqual(129.1, ((BasicDouble)db.run("127.1+2.0")).getValue());
            Assert.IsTrue(Math.Abs(1114.4 - ((BasicDouble)db.run("1127.1-12.7")).getValue()) < 0.000001);
            Assert.AreEqual(Math.Round(-1.2536, 4), Math.Round(((BasicDouble)db.run("-1.2536")).getValue(), 4));
            Assert.AreEqual("", ((BasicDouble)db.run("double()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("abc", ((BasicString)db.run("`abc")).getValue());
            Assert.AreEqual("abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl", ((BasicString)db.run("`abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl")).getValue());
            Assert.AreEqual("", ((BasicString)db.run("string()")).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_uuid()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee87", ((BasicUuid)db.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')")).getString());
            Assert.AreEqual("00000000-0000-0000-0000-000000000000", ((BasicUuid)db.run("uuid()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int128()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            //Assert.AreEqual("e1671797c52e15f763380b45e841ec32", ((BasicInt128)db.run("int128('e1671797c52e15f763380b45e841ec32')")).getString());
            //Assert.AreEqual("00000000000000000000000000000000", ((BasicInt128)db.run("int128()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_ipaddr()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("192.168.1.13", ((BasicIPAddr)db.run("ipaddr('192.168.1.13')")).getString());
            Assert.AreEqual("0.0.0.0", ((BasicIPAddr)db.run("ipaddr()")).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicBooleanVector)db.run("true false");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(true, ((BasicBoolean)v.get(0)).getValue());
            IVector v2 = (BasicBooleanVector)db.run("take(true, 10000)");
            for(int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(true, ((BasicBoolean)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_bool_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicBooleanVector)db.run("true false");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(true, ((BasicBoolean)v.get(0)).getValue());
            IVector v2 = (BasicBooleanVector)db.run("take(true, 10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(true, ((BasicBoolean)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicIntVector)db.run("1 2 3");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2, ((BasicInt)v.get(1)).getValue());
            IVector v2 = (BasicIntVector)db.run("1..10000");
            for(int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i+1, ((BasicInt)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_int_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicIntVector)db.run("1 2 3");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2, ((BasicInt)v.get(1)).getValue());
            IVector v2 = (BasicIntVector)db.run("1..10000");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicInt)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicLongVector)db.run("11111111111111111l 222222222222222l 3333333333333333333l");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(222222222222222L, ((BasicLong)v.get(1)).getValue());
            IVector v2 = (BasicLongVector)db.run("long(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicLong)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_long_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicLongVector)db.run("11111111111111111l 222222222222222l 3333333333333333333l");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(222222222222222L, ((BasicLong)v.get(1)).getValue());
            IVector v2 = (BasicLongVector)db.run("long(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicLong)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicShortVector)db.run("123h 234h 345h");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(234, ((BasicShort)v.get(1)).getValue());
            IVector v2 = (BasicShortVector)db.run("short(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicShort)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_short_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicShortVector)db.run("123h 234h 345h");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(234, ((BasicShort)v.get(1)).getValue());
            IVector v2 = (BasicShortVector)db.run("short(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicShort)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicFloatVector)db.run("1.123f 2.2234f 3.4567f");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicFloat)v.get(1)).getValue(), 4));
            IVector v2 = (BasicFloatVector)db.run("float(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicFloat)v2.get(i)).getValue(),1));
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_float_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicFloatVector)db.run("1.123f 2.2234f 3.4567f");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicFloat)v.get(1)).getValue(), 4));
            IVector v2 = (BasicFloatVector)db.run("float(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicFloat)v2.getEntity(i)).getValue(), 1));
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDoubleVector)db.run("[1.123,2.2234,3.4567]");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
            IVector v2 = (BasicDoubleVector)db.run("double(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicDouble)v2.get(i)).getValue(), 1));
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_double_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDoubleVector)db.run("[1.123,2.2234,3.4567]");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
            IVector v2 = (BasicDoubleVector)db.run("double(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicDouble)v2.getEntity(i)).getValue(), 1));
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateVector)db.run("2018.03.01 2017.04.02 2016.05.03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 02, 0, 0, 0), ((BasicDate)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_date_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateVector)db.run("2018.03.01 2017.04.02 2016.05.03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 02, 0, 0, 0), ((BasicDate)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicMonthVector)db.run("2018.03M 2017.04M 2016.05M");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 01, 0, 0, 0), ((BasicMonth)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_month_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicMonthVector)db.run("2018.03M 2017.04M 2016.05M");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 01, 0, 0, 0), ((BasicMonth)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicTimeVector)db.run("10:57:01.001 10:58:02.002 10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, ((BasicTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_time_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicTimeVector)db.run("10:57:01.001 10:58:02.002 10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, ((BasicTime)v.getEntity(1)).getValue());
            db.close();
        }


        [TestMethod]
        public void Test_run_return_vector_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicNanoTimeVector)db.run("15:41:45.123456789 15:41:45.123456889 15:41:45.123456989");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4568L).TimeOfDay, ((BasicNanoTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_nanotime_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicNanoTimeVector)db.run("15:41:45.123456789 15:41:45.123456889 15:41:45.123456989");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4568L).TimeOfDay, ((BasicNanoTime)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_dateHour()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateHourVector)db.run("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2012, 06, 15, 17, 00, 00), ((BasicDateHour)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_dateHour_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateHourVector)db.run("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2012, 06, 15, 17, 00, 00), ((BasicDateHour)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicMinuteVector)db.run("10:47m 10:48m 10:49m");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 0).TimeOfDay, ((BasicMinute)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_minute_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicMinuteVector)db.run("10:47m 10:48m 10:49m");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 0).TimeOfDay, ((BasicMinute)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicSecondVector)db.run("10:47:02 10:48:03 10:49:04");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, ((BasicSecond)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_second_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicSecondVector)db.run("10:47:02 10:48:03 10:49:04");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, ((BasicSecond)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateTimeVector)db.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02), ((BasicDateTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_datetime_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicDateTimeVector)db.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02), ((BasicDateTime)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicTimestampVector)db.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02, 002), ((BasicTimestamp)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_timestamp_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicTimestampVector)db.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02, 002), ((BasicTimestamp)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);

            IVector v = (BasicNanoTimestampVector)db.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            Assert.AreEqual(new DateTime(dt.Ticks + 2224), ((BasicNanoTimestamp)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_nanotimestamp_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);

            IVector v = (BasicNanoTimestampVector)db.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            Assert.AreEqual(new DateTime(dt.Ticks + 2224), ((BasicNanoTimestamp)v.getEntity(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicStringVector)db.run("`aaa `bbb `ccc");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            IVector v2 = (BasicStringVector)db.run("string(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual((i+1).ToString(), ((BasicString)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_string_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (BasicStringVector)db.run("`aaa `bbb `ccc");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            IVector v2 = (BasicStringVector)db.run("string(1..10000)");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual((i + 1).ToString(), ((BasicString)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_symbol()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (IVector)db.run("symbol(`aaa `bbb `ccc)");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            IVector v2 = (IVector)db.run("symbol('AA'+string(1..10000))");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual("AA"+(i + 1).ToString(), ((BasicString)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_symbol_getEntity()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IVector v = (IVector)db.run("symbol(`aaa `bbb `ccc)");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            IVector v2 = (IVector)db.run("symbol('AA'+string(1..10000))");
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual("AA" + (i + 1).ToString(), ((BasicString)v2.getEntity(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicBooleanMatrix)db.run("matrix(true false true,false true true)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(false, ((BasicBoolean)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_byte()
        {
            //Assert.Fail("can't defined byte datatype on server");
            //DBConnection db = new DBConnection();
            //db.connect(SERVER, PORT);
            //IMatrix m = (BasicBooleanMatrix)db.run("matrix(true false true,false true true)");
            //Assert.IsTrue(m.isMatrix());
            //Assert.AreEqual(3, m.rows());
            //Assert.AreEqual(2, m.columns());
            //Assert.AreEqual(false, ((BasicBoolean)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicShortMatrix)db.run("matrix(45h 47h 48h,56h 65h 67h)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56, ((BasicShort)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicIntMatrix)db.run("matrix(45 47 48,56 65 67)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56, ((BasicInt)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicLongMatrix)db.run("matrix(450000000000000 47000000000000 4811111111111111,5622222222222222 6533333333333333 6744444444444444)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(5622222222222222L, ((BasicLong)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicDoubleMatrix)db.run("matrix(45.02 47.01 48.03,56.123 65.04 67.21)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56.123, Math.Round(((BasicDouble)m.get(0, 1)).getValue(), 3));
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicFloatMatrix)db.run("matrix(45.02f 47.01f 48.03f,56.123f 65.04f 67.21f)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56.123, Math.Round(((BasicFloat)m.get(0, 1)).getValue(), 3));
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_string()
        {
            //Assert.Fail("matrix type of string does not supported");
        }

        [TestMethod]
        public void Test_run_return_matrix_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicDateMatrix)db.run("matrix(2018.03.01 2017.04.02 2016.05.03,2018.03.03 2017.04.03 2016.05.04)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018, 03, 03), ((BasicDate)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicDateTimeMatrix)db.run("matrix(2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03,2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018, 03, 14, 10, 57, 01), ((BasicDateTime)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicTimeMatrix)db.run("matrix(10:57:01.001 10:58:02.002 10:59:03.003,10:58:01.001 10:58:02.002 10:59:03.003)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            BasicTime bt = (BasicTime)m.get(0, 1);
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 01, 001).TimeOfDay, bt.getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicNanoTimeMatrix)db.run("matrix(15:41:45.123456789 15:41:45.123456789 15:41:45.123456789,15:41:45.123956789 15:41:45.123486789 15:41:45.123476789)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 9567L).TimeOfDay, ((BasicNanoTime)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicTimestampMatrix)db.run("matrix(2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003,2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018, 3, 14, 10, 57, 01, 001), ((BasicTimestamp)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicNanoTimestampMatrix)db.run("matrix(2018.03.14T10:57:01.001123456 2018.03.15T10:58:02.002123456 2018.03.16T10:59:03.003123456,2018.03.14T10:57:01.001456789 2018.03.15T10:58:02.002456789 2018.03.16T10:59:03.003456789)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            DateTime dt = new DateTime(2018, 03, 14, 10, 57, 01, 001);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4567L), ((BasicNanoTimestamp)m.get(0, 1)).getValue());
            db.close();
        }
        [TestMethod]
        public void Test_run_return_matrix_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicMonthMatrix)db.run("matrix(2018.03M 2017.04M 2016.05M,2018.02M 2017.03M 2016.01M)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018, 2, 1, 0, 0, 0), ((BasicMonth)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicMinuteMatrix)db.run("matrix(10:47m 10:48m 10:49m,16:47m 15:48m 14:49m)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new TimeSpan(16, 47, 0), ((BasicMinute)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_matrix_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IMatrix m = (BasicSecondMatrix)db.run("matrix(10:47:02 10:48:03 10:49:04,16:47:02 15:48:03 14:49:04)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new TimeSpan(16, 47, 02), ((BasicSecond)m.get(0, 1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_table_int()
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
        public void Test_run_return_dict()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicDictionary dict = (BasicDictionary)db.run("dict(1 2 3, 2.3 3.4 5.5)");
            BasicDouble v = (BasicDouble)dict.get(new BasicInt(2));
            Assert.AreEqual(3.4, v.getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_set()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet dict = (BasicSet)db.run("set(1 3 5)");
            Assert.AreEqual(3, dict.rows());
            Assert.AreEqual(1, dict.columns());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicAnyVector v = (BasicAnyVector)db.run("[1 2 3,3.4 3.5 3.6]");
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(1, v.columns());
            Assert.AreEqual(3.4, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector_getException()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Exception exception = null;
            BasicAnyVector v = (BasicAnyVector)db.run("[1 2 3]");
            try
            {
                BasicAnyVector a = (BasicAnyVector)v.get(0);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex;
            }
            Assert.IsNotNull(exception);

            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector_getString()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicAnyVector v = (BasicAnyVector)db.run("[`q `a `s,`www `2wss `rfgg]");
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(1, v.columns());
            Assert.AreEqual("www", ((BasicString)((BasicStringVector)v.getEntity(1)).get(0)).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector_IScalar()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicAnyVector v = (BasicAnyVector)db.run("[`q `a `s,`www `2wss `rfgg]");
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(1, v.columns());
            Assert.AreEqual("www", ((BasicString)((BasicStringVector)v.getEntity(1)).get(0)).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_table_toDataTable()
        {
            string script = @"table(take(0b 1b, 10) as tBOOL, char(1..10) as tCHAR, short(1..10) as tSHORT, int(1..10) as tINT, long(1..10) as tLONG, 2000.01.01 + 1..10 as tDATE, 2000.01M + 1..10 as tMONTH, 13:30:10.008 + 1..10 as tTIME, 13:30m + 1..10 as tMINUTE, 13:30:10 + 1..10 as tSECOND, 2012.06.13T13:30:10 + 1..10 as tDATETIME, 2012.06.13T13:30:10.008 + 1..10 as tTIMESTAMP,09:00:01.000100001 + 1..10 as tNANOTIME,2016.12.30T09:00:01.000100001 + 1..10 as tNANOTIMESTAMP, 2.1f + 1..10 as tFLOAT, 2.1 + 1..10 as tDOUBLE, take(`A`B`C`D, 10) as tSYMBOL)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("3", dt.Rows[2]["tSHORT"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_table_toDataTable_char()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run("table(1 as id,'a' as name)");
            DataTable dt = tb.toDataTable();
            db.close();
        }




        [TestMethod]
        public void Test_chinese_Table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            prepareChineseTable(conn);
            BasicTable bt = (BasicTable)conn.run("sharedTable");
            Assert.AreEqual(8000, bt.rows());
            Assert.AreEqual("备注1", 
                ((BasicString)bt.getColumn("备注").get(0)).getString());
            DataTable dt =  bt.toDataTable();
            Assert.AreEqual("备注10", dt.Rows[9]["备注"].ToString());
            conn.close();

        }

        [TestMethod]
        public void Test_chinese_DataTable()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("股票代码",Type.GetType("System.String")),
                new DataColumn("股票日期",Type.GetType("System.DateTime")),
                new DataColumn("买方报价", Type.GetType("System.Double")),
                new DataColumn("卖方报价", Type.GetType("System.Double")),
                new DataColumn("时间戳", Type.GetType("System.DateTime")),
                new DataColumn("备注", Type.GetType("System.String"))
            };
            dt.Columns.AddRange(cols.ToArray());
            for(int i = 0; i < 10; i++)
            {
                DataRow dr = dt.NewRow();
                dr["股票代码"] = new string[] { "GGG", "MSSS", "FBBBB" }[i%3];
                dr["股票日期"] = DateTime.Now.Date;
                dr["买方报价"] = 22222.5544;
                dr["卖方报价"] = 3333.33322145;
                dr["时间戳"] = DateTime.Now;
                dr["备注"] = "备注" + i.ToString();
                dt.Rows.Add(dr);
            }
            BasicTable bt = new BasicTable(dt);
            Dictionary<String, IEntity> var = new Dictionary<string, IEntity>();
            var.Add("temptb", bt);
            conn.upload(var);
            conn.run("share temptb as ttttb");
            
        }

        
        public void prepareChineseTable(DBConnection conn)
        {
            conn.run("t =table(10000:0,['股票代码','股票日期','买方报价','卖方报价','时间戳','备注'],[SYMBOL,DATE,DOUBLE,DOUBLE,TIMESTAMP,STRING])");
            conn.run("share t as sharedTable");
            conn.run("sharedTable.append!(table(symbol(take(`GGG`MMS`FABB`APPL, 8000)) as 股票代码, take(today(), 8000) as 股票日期, norm(40, 5, 8000) as 买方报价, norm(45, 5, 8000) as 卖方报价, take(now(), 8000) as 时间戳,'备注' + string(1..8000) as 备注)) ");
            
        }

        [TestMethod]
        public void Test_Upload_DataTable()
        {
            DataTable dt = new DataTable();
            DataColumn dc = new DataColumn("dt_string", Type.GetType("System.String"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_short", Type.GetType("System.Int16"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_int", Type.GetType("System.Int32"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_long", Type.GetType("System.Int64"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_double", Type.GetType("System.Int32"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_datetime", Type.GetType("System.DateTime"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_bool", Type.GetType("System.Boolean"));
            dt.Columns.Add(dc);
            dc = new DataColumn("dt_byte", Type.GetType("System.Byte"));
            dt.Columns.Add(dc);
            for(int i = 0; i < 20000; i++)
            {
                DataRow dr = dt.NewRow();
                dr["dt_short"] = 1;
                dr["dt_int"] = 2147483646;
                dr["dt_long"] = 2147483649;
                dr["dt_double"] = 3.14159893984;
                dr["dt_datetime"] = new DateTime(2018, 03, 30, 14, 59, 02, 111);
                dr["dt_bool"] = false;
                dr["dt_byte"] = (byte)97;
                dr["dt_string"] = "test_string";
                dt.Rows.Add(dr);
            }
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable bt = new BasicTable(dt);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("up_datatable", (IEntity)bt);
            db.upload(obj);
            
            BasicIntVector v = (BasicIntVector)db.run("up_datatable.dt_int");
            Assert.AreEqual(2147483646, ((BasicInt)v.get(0)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_upload_table()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name,rand(1.01,100) as dbl)");
            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("table_uploaded", (IEntity)tb);
            db.upload(upObj);
            BasicIntVector v = (BasicIntVector)db.run("table_uploaded.id");
            Assert.AreEqual(100, v.rows());
            db.close();
        }


        [TestMethod]
        public void Test_dict_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicDictionary tb = (BasicDictionary)db.run("dict(1 2 3 4,5 6 7 8)");
            foreach (var key in tb.keys())
            {
                BasicInt val = (BasicInt)tb.get(key);
            }
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(4, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_set_toDataTable_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(8 9 9 5 6)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(4, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_set_toDataTable_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09M 2018.08M)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_set_toDataTable_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01 2018.08.01)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_set_toDataTable_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01T01:01:01 2018.08.01T01:01:01)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_set_toDataTable_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01T01:01:01.001 2018.08.01T01:01:01.001)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_vector_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicIntVector tb = (BasicIntVector)db.run("1 2 3 4 5");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(5, dt.Rows.Count);
            db.close();
        }

        [TestMethod]
        public void Test_martix_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicIntMatrix tb = (BasicIntMatrix)db.run("1..9$3:3");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(3, dt.Columns.Count);
            db.close();
        }

        [TestMethod]
        public void Test_pair_getString_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IEntity tb = (IEntity)db.run("1:2");
            if (tb.isPair())
            {
                Assert.AreEqual("[1,2]", tb.getString());
            }
            db.close();
        }

        [TestMethod]
        public void Test_Void()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IEntity obj = db.run("NULL");
            Assert.AreEqual(obj.getObject(), null);
            db.close();
        }

        [TestMethod]
        public void Test_Function_Double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            List<IEntity> args = new List<IEntity>(1);
            BasicDoubleVector x = new BasicDoubleVector(3);
            x.setDouble(0, 1.5);
            x.setDouble(1, 2.5);
            x.setDouble(2, 7);

            BasicDoubleVector y = new BasicDoubleVector(3);
            y.setDouble(0, 2.5);
            y.setDouble(1, 3.5);
            y.setDouble(2, 5);

            args.Add(x);
            args.Add(y);
            BasicDoubleVector result = (BasicDoubleVector)db.run("add", args);
            Assert.AreEqual(3, result.rows());
            Assert.AreEqual(4.0, ((BasicDouble)result.get(0)).getValue());
            Assert.AreEqual(6.0, ((BasicDouble)result.get(1)).getValue());
            Assert.AreEqual(12.0, ((BasicDouble)result.get(2)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_Function_Float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            List<IEntity> args = new List<IEntity>(1);
            BasicFloatVector vec = new BasicFloatVector(3);
            vec.setFloat(0, 1.5f);
            vec.setFloat(1, 2.5f);
            vec.setFloat(2, 7f);

            args.Add(vec);
            IScalar result = (IScalar)db.run("sum", args);
            Assert.AreEqual("11", result.getString());
            db.close();
        }

        [TestMethod]
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
            db.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
            db.run("db = database('dfs://testDatabase',VALUE,'MS' 'GOOG' 'FB')");
            db.run("tb= table('MS' as sym,datetime(now()) as dt,1.01 as prc,1 as cnt)");
            db.run("db.createPartitionedTable(tb,'tb1','sym')");

            db.run("def saveQuotes(t){ loadTable('dfs://testDatabase','tb1').append!(t)}");
            List<IEntity> args = new List<IEntity>(1);
            args.Add(table1);
            db.run("saveQuotes", args);
            db.close();
        }

        [TestMethod]
        public void Test_NewBasicTimestampWithDatetime()
        {
            BasicTimestampVector btv = new BasicTimestampVector(10);
            DateTime time = new DateTime(1970, 01, 01, 8, 2, 33);
            BasicTimestamp bt = new BasicTimestamp(time);
            Assert.AreEqual("1970.01.01T08:02:33.000", bt.getString());
        }

        [TestMethod]
        public void Test_ConstructBasicTableWithDataTable()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("col_string", Type.GetType("System.String"));
            dt.Columns.Add("col_date", Type.GetType("System.DateTime"));
            dt.Columns.Add("col_time", Type.GetType("System.TimeSpan"));
            dt.Columns.Add("col_int", Type.GetType("System.Int16"));
            dt.Columns.Add("col_double", Type.GetType("System.Double"));
            dt.Columns.Add("col_long", Type.GetType("System.Int64"));
            dt.Columns.Add("col_char", Type.GetType("System.Char"));
            dt.Columns.Add("col_bool", Type.GetType("System.Boolean"));

            DataRow dr = dt.NewRow();
            dr["col_string"] = "test";
            dr["col_date"] = new DateTime(2018, 07, 25, 15, 14, 23);
            dr["col_time"] = new TimeSpan(0, 15, 15, 14, 123);
            dr["col_int"] = 123;
            dr["col_double"] = 3.1415926;
            dr["col_long"] = 2147483647;
            dr["col_char"] = 'X';
            dr["col_bool"] = true;
            dt.Rows.Add(dr);

            BasicTable bt = new BasicTable(dt);

            List<double> a = (List<double>)bt.getColumn(4).getList();
            List<long> b = (List<long>)bt.getColumn(5).getList();
            
            Assert.AreEqual(DATA_TYPE.DT_STRING, bt.getColumn(0).getDataType());
            Assert.AreEqual(DATA_TYPE.DT_DATETIME, bt.getColumn(1).getDataType());
            Assert.AreEqual(DATA_TYPE.DT_TIME, bt.getColumn(2).getDataType());
            Assert.AreEqual(DATA_TYPE.DT_SHORT, bt.getColumn(3).getDataType());
        }

        [TestMethod]
        public void Test_MCorr()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("x", (IEntity)new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }));
            obj.Add("y", (IEntity)new BasicIntVector(new int[] { 9, 5, 3, 4, 5, 4, 7, 1, 3, 4 }));
            db.upload(obj);
            BasicDoubleVector t = (BasicDoubleVector)db.run("ttt = nullFill!(mcorr(x,y,5),0);ttt");
            db.close();
        }

        [TestMethod]
        public void Test_GetList()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicIntVector iv = (BasicIntVector)db.run("1 2 3 4");
            List<int> r = (List<int>)iv.getList();
            Assert.AreEqual(4, r.Count);

            BasicDoubleVector dv = (BasicDoubleVector)db.run("1.1 2.01 3.25 4.251441 3.32222");
            List<double> dr = (List<double>)dv.getList();
            Assert.AreEqual(5, dr.Count);
            Assert.AreEqual(1.1, dr[0]);

            BasicLongVector lv = (BasicLongVector)db.run("102012522 12345678900 12221114455");
            List<long> lr = (List<long>)lv.getList();
            Assert.AreEqual(3, lr.Count);
            Assert.AreEqual(12345678900, lr[1]);
            db.close();
        }


        [TestMethod]
        public void Test_getInternalValue()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);

            BasicDate date = (BasicDate)db.run("2018.07.10");
            int r = date.getInternalValue();
            Assert.AreEqual(17722, r);

            BasicTimestamp timestamp = (BasicTimestamp)db.run("2018.07.10T10:12:13.005");
            long l = timestamp.getInternalValue();
            Assert.AreEqual(1531217533005, l);

            BasicTime time = (BasicTime)db.run("10:12:13.005");
            int t = time.getInternalValue();
            Assert.AreEqual(36733005, t);
            //min
            BasicMinute min = (BasicMinute)db.run("10:12m");
            int m = min.getInternalValue();
            Assert.AreEqual(612, m);
            //sec
            BasicSecond sec = (BasicSecond)db.run("10:12:11");
            int s = sec.getInternalValue();
            Assert.AreEqual(36731, s);

            //month
            BasicMonth mon = (BasicMonth)db.run("2018.07M");
            int mo = mon.getInternalValue();
            Assert.AreEqual(24222, mo);

            //nanotime
            BasicNanoTime nt = (BasicNanoTime)db.run("10:12:13.005002003");
            long nti = nt.getInternalValue();
            Assert.AreEqual(36733005002003, nti);
            //nanotimestamp
            BasicNanoTimestamp nts = (BasicNanoTimestamp)db.run("2018.07.10T10:12:13.005002003");
            long ntsi = nts.getInternalValue();
            Assert.AreEqual(1531217533005002003, ntsi);
            db.close();
        }

        [TestMethod]
        public void Test_ContructBasicTableByDataTable()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("dt", Type.GetType("System.DateTime"));
            dt.Columns.Add("bl", Type.GetType("System.Boolean"));
            dt.Columns.Add("sb", Type.GetType("System.Byte"));
            dt.Columns.Add("db", Type.GetType("System.Double"));
            dt.Columns.Add("ts", Type.GetType("System.TimeSpan"));
            dt.Columns.Add("i1", Type.GetType("System.Int16"));
            dt.Columns.Add("i3", Type.GetType("System.Int32"));
            dt.Columns.Add("i6", Type.GetType("System.Int64"));
            dt.Columns.Add("s", Type.GetType("System.String"));
            DataRow dr = dt.NewRow();
            dr["dt"] = DBNull.Value;
            dr["bl"] = DBNull.Value;
            dr["sb"] = DBNull.Value;
            dr["db"] = DBNull.Value;
            dr["ts"] = DBNull.Value;
            dr["i1"] = DBNull.Value;
            dr["i3"] = DBNull.Value;
            dr["i6"] = DBNull.Value;
            dr["s"] = DBNull.Value;
            dt.Rows.Add(dr);
            BasicTable bt = new BasicTable(dt);

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("up_datatable", (IEntity)bt);
            db.upload(obj);
            db.run("share up_datatable as tb1");

            BasicStringVector bsv = (BasicStringVector)bt.getColumn(8);
            Assert.AreEqual("", bsv.get(0).getString());
            db.close();
        }

        [TestMethod]
        public void Test_GetNullTable()
        {
            string sql = "t=table(10:0,`id`str`long`double,[INT,STRING,LONG,DOUBLE]);insert into t values(1,NULL,NULL,NULL);t";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable bt = (BasicTable)db.run(sql);
            DataTable dt = bt.toDataTable();
            Assert.AreEqual(DBNull.Value, dt.Rows[0][1]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0][2]);
            Assert.AreEqual(DBNull.Value, dt.Rows[0][3]);
            db.close();
        }

        
        private BasicTable createBasicTable()
        {
            List<String> colNames = new List<String>();
            colNames.Add("cbool");
            colNames.Add("cchar");
            colNames.Add("cshort");
            colNames.Add("cint");
            colNames.Add("clong");
            colNames.Add("cdate");
            colNames.Add("cmonth");
            colNames.Add("ctime");
            colNames.Add("cminute");
            colNames.Add("csecond");
            colNames.Add("cdatetime");
            colNames.Add("ctimestamp");
            colNames.Add("cnanotime");
            colNames.Add("cnanotimestamp");
            colNames.Add("cfloat");
            colNames.Add("cdouble");
            colNames.Add("csymbol");
            colNames.Add("cstring");
            List<IVector> cols = new List<IVector>();
            //boolean
            byte[] vbool = new byte[] { 1, 0 };
            BasicBooleanVector bbv = new BasicBooleanVector(vbool);
            cols.Add(bbv);
            //char
            byte[] vchar = new byte[] { (byte)'c', (byte)'a' };
            BasicByteVector bcv = new BasicByteVector(vchar);
            cols.Add(bcv);
            //cshort
            short[] vshort = new short[] { 32767, 29 };
            BasicShortVector bshv = new BasicShortVector(vshort);
            cols.Add(bshv);
            //cint
            int[] vint = new int[] { 2147483647, 483647 };
            BasicIntVector bintv = new BasicIntVector(vint);
            cols.Add(bintv);
            //clong
            long[] vlong = new long[] { 2147483647, 483647 };
            BasicLongVector blongv = new BasicLongVector(vlong);
            cols.Add(blongv);
            //cdate
            int[] vdate = new int[] { Utils.countDays(new DateTime(2018, 2, 14)), Utils.countDays(new DateTime(2018, 8, 15)) };
            BasicDateVector bdatev = new BasicDateVector(vdate);
            cols.Add(bdatev);
            //cmonth
            int[] vmonth = new int[] { Utils.countMonths(new DateTime(2018, 2, 1)), Utils.countMonths(new DateTime(2018, 8, 1)) };
            BasicMonthVector bmonthv = new BasicMonthVector(vmonth);
            cols.Add(bmonthv);
            //ctime
            int[] vtime = new int[] { Utils.countMilliseconds(16, 46, 05, 123), Utils.countMilliseconds(18, 32, 05, 321) };
            BasicTimeVector btimev = new BasicTimeVector(vtime);
            cols.Add(btimev);
            //cminute
            int[] vminute = new int[] { Utils.countMinutes(new TimeSpan(16, 30, 00)), Utils.countMinutes(new TimeSpan(9, 30, 00)) };
            BasicMinuteVector bminutev = new BasicMinuteVector(vminute);
            cols.Add(bminutev);
            //csecond
            int[] vsecond = new int[] { Utils.countSeconds(new TimeSpan(9, 30, 30)), Utils.countSeconds(new TimeSpan(16, 30, 50)) };
            BasicSecondVector bsecondv = new BasicSecondVector(vsecond);
            cols.Add(bsecondv);
            //cdatetime
            int[] vdatetime = new int[] { Utils.countSeconds(new DateTime(2018, 9, 8, 9, 30, 01)), Utils.countSeconds(new DateTime(2018, 11, 8, 16, 30, 01)) };
            BasicDateTimeVector bdatetimev = new BasicDateTimeVector(vdatetime);
            cols.Add(bdatetimev);
            //ctimestamp
            long[] vtimestamp = new long[] { Utils.countMilliseconds(2018, 11, 12, 9, 30, 01, 123), Utils.countMilliseconds(2018, 11, 12, 16, 30, 01, 123) };
            BasicTimestampVector btimestampv = new BasicTimestampVector(vtimestamp);
            cols.Add(btimestampv);
            //cnanotime
            long[] vnanotime = new long[] { Utils.countNanoseconds(new TimeSpan(0, 9, 30, 05, 123)), Utils.countNanoseconds(new TimeSpan(0, 16, 30, 05, 987)) };
            BasicNanoTimeVector bnanotimev = new BasicNanoTimeVector(vnanotime);
            cols.Add(bnanotimev);
            //cnanotimestamp
            long[] vnanotimestamp = new long[] { Utils.countNanoseconds(new DateTime(2018, 11, 12, 9, 30, 05, 123)), Utils.countNanoseconds(new DateTime(2018, 11, 13, 16, 30, 05, 987)) };
            BasicNanoTimestampVector bnanotimestampv = new BasicNanoTimestampVector(vnanotimestamp);
            cols.Add(bnanotimestampv);
            //cfloat
            float[] vfloat = new float[] { 2147.483647f, 483.647f };
            BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
            cols.Add(bfloatv);
            //cdouble
            double[] vdouble = new double[] { 214.7483647, 48.3647 };
            BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
            cols.Add(bdoublev);
            //csymbol
            String[] vsymbol = new String[] { "GOOG", "MS" };
            BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
            cols.Add(bsymbolv);
            //cstring
            String[] vstring = new String[] { "", "test string" };
            BasicStringVector bstringv = new BasicStringVector(vstring);
            cols.Add(bstringv);
            BasicTable t1 = new BasicTable(colNames, cols);
            return t1;
        }

        [TestMethod]
        public void Test_SaveTable_MemoryDB()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable table1 = createBasicTable();
            db.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
            db.run("share t as memoryTable");
            db.run("def saveData(data){ memoryTable.append!(data)}");
            List<IEntity> args = new List<IEntity>(1);
            args.Add(table1);
            db.run("saveData", args);
            db.close();
        }

        [TestMethod]
        public void Test_SaveTable_LocalDB()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable table1 = createBasicTable();
            db.login("admin", "123456", false);
            String dbpath = "dfs://testDatabase";
            db.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
            db.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
            db.run("db = database('dfs://testDatabase',RANGE,2018.01.01..2018.12.31)");
            db.run("db.createPartitionedTable(t,'tb1','cdate')");
            db.run("def saveData(data){ loadTable('dfs://testDatabase','tb1').append!(data)}");
            List<IEntity> args = new List<IEntity>(1);
            args.Add(table1);
            db.run("saveData", args);
            db.close();
        }

        [TestMethod]
        public void Test_SaveTable_DfsDB()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT,"admin","123456");
            BasicTable table1 = createBasicTable();
            String dbpath = "dfs://testDatabase1";
            db.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
            string script = "if(existsDatabase('dfs://testDatabase1')){dropDatabase('dfs://testDatabase1')}";
            db.run(script);
            script = "db = database('dfs://testDatabase1',VALUE,'MS' 'GOOG' 'FB')";
            db.run(script);
            db.run("db.createPartitionedTable(t,'tb1','csymbol')");
            db.run("def saveData(data){ loadTable('dfs://testDatabase1','tb1').append!(data)}");
            List<IEntity> args = new List<IEntity>(1);
            args.Add(table1);
            db.run("saveData", args);
            db.close();
        }


        [TestMethod]
        public void Test_Loop_BasicTable()
        {

            BasicTable table1 = createBasicTable();
            for (int ri = 0; ri < table1.rows(); ri++)
            {
                BasicBoolean boolv = (BasicBoolean)table1.getColumn("cbool").get(ri);
                Console.WriteLine(boolv.getValue());

                BasicByte charv = (BasicByte)table1.getColumn("cchar").get(ri);
                Console.WriteLine((char)charv.getValue());

                BasicShort shortv = (BasicShort)table1.getColumn("cshort").get(ri);
                Console.WriteLine(shortv.getValue());

                BasicInt intv = (BasicInt)table1.getColumn("cint").get(ri);
                Console.WriteLine(intv.getValue());

                BasicLong longv = (BasicLong)table1.getColumn("clong").get(ri);
                Console.WriteLine(longv.getValue());

                BasicDate datev = (BasicDate)table1.getColumn("cdate").get(ri);
                DateTime date = datev.getValue();
                Console.WriteLine(date.ToShortDateString());

                BasicMonth monthv = (BasicMonth)table1.getColumn("cmonth").get(ri);
                DateTime ym = monthv.getValue();
                Console.WriteLine(ym.ToShortDateString());

                BasicTime timev = (BasicTime)table1.getColumn("ctime").get(ri);
                TimeSpan time = timev.getValue();
                Console.WriteLine(time.ToString());

                BasicMinute minutev = (BasicMinute)table1.getColumn("cminute").get(ri);
                TimeSpan minute = minutev.getValue();
                Console.WriteLine(minute);

                BasicSecond secondv = (BasicSecond)table1.getColumn("csecond").get(ri);
                TimeSpan second = secondv.getValue();
                Console.WriteLine(second);

                BasicDateTime datetimev = (BasicDateTime)table1.getColumn("cdatetime").get(ri);
                DateTime datetime = datetimev.getValue();
                Console.WriteLine(datetime);

                BasicTimestamp timestampv = (BasicTimestamp)table1.getColumn("ctimestamp").get(ri);
                DateTime timestamp = timestampv.getValue();
                Console.WriteLine(timestamp);

                BasicNanoTime nanotimev = (BasicNanoTime)table1.getColumn("cnanotime").get(ri);
                TimeSpan nanotime = nanotimev.getValue();
                Console.WriteLine(nanotime);

                BasicNanoTimestamp nanotimestampv = (BasicNanoTimestamp)table1.getColumn("cnanotimestamp").get(ri);
                DateTime nanotimestamp = nanotimestampv.getValue();
                Console.WriteLine(nanotimestamp);

                BasicFloat floatv = (BasicFloat)table1.getColumn("cfloat").get(ri);
                Console.WriteLine(floatv.getValue());

                BasicDouble doublev = (BasicDouble)table1.getColumn("cdouble").get(ri);
                Console.WriteLine(doublev.getValue());

                BasicString symbolv = (BasicString)table1.getColumn("csymbol").get(ri);
                Console.WriteLine(symbolv.getValue());

                BasicString stringv = (BasicString)table1.getColumn("cstring").get(ri);
                Console.WriteLine(stringv.getValue());
            }
        }

        [TestMethod]
        public void Test_Read_Table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            //conn.connect();
            conn.run("x = [1,3,5]");
            //BasicTable table1 = (BasicTable)db.run("select field1,field2 from loadTable('dfs://dbpath','table1') where dtime >2018.12.03T11.16.00");
            List<IEntity> args = new List<IEntity>(1);
            BasicDoubleVector y = new BasicDoubleVector(3);
            y.setDouble(0, 2.5);
            y.setDouble(1, 3.5);
            y.setDouble(2, 5);
            args.Add(y);
            IVector result = (IVector)conn.run("add{x}", args);
            Console.WriteLine(result.getString());

            BasicAnyVector v = (BasicAnyVector)conn.run("[1 2 3,3.4 3.5 3.6]");
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(1, v.columns());
            //Assert.AreEqual(1, ((BasicInt)((BasicAnyVector)v.getEntity(0)).get(0)).getValue());
            Assert.AreEqual(1, ((BasicInt)((BasicIntVector)v.getEntity(0)).get(0)).getValue());
            Assert.AreEqual(2, ((BasicInt)((BasicIntVector)v.getEntity(0)).get(1)).getValue());
            Assert.AreEqual(3, ((BasicInt)((BasicIntVector)v.getEntity(0)).get(2)).getValue());
            Assert.AreEqual(3.4, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
            Assert.AreEqual(3.5, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(1)).getValue());
            Assert.AreEqual(3.6, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(2)).getValue());

            conn.close();
        }

        [TestMethod]
        public void Test_Module()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT,"admin","123456");
          
            var args = new List<IEntity>
            {
                new BasicString("linl")
            };
            conn.run("module option::Utils;");
            //conn.run("use option::Utils;");

            //conn.run("option::Utils::SaveWingModelConfig", args);
            conn.close();
        }

        [TestMethod]
        public void Test_udf()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("def Foo5(a,b){f=file('testsharp.csv','w');f.writeLine(string(a));}");

            var args = new List<IEntity>();
            args.Add(new BasicDouble(1.3));
            args.Add(new BasicDouble(1.4));
            conn.run("Foo5", args);

            args.Clear();
            args.Add(new BasicFloat(1.3f));
            args.Add(new BasicFloat(1.4f));
            conn.run("Foo5", args);

            args.Clear();
            args.Add(new BasicInt(3));
            args.Add(new BasicInt(4));
            conn.run("Foo5", args);
            conn.close();
        }

        [TestMethod]
        public void Test_null_Table_upload()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var dt = new DataTable();
            try
            {
                var bt = new BasicTable(dt);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("DataTable must contain at least one column", ex.Message);
            }
            conn.close();

        }


        [TestMethod]
        public void Test_BasicTable_upload()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicTable table1 = createBasicTable();
            //conn.run("undef(`table1,SHARED)");
            var cols = new List<IVector>() {};
            var colNames = new List<String>() { "Symbol", "TradingDate", "TradingTime", "RecID", "TradeChannel", "TradePrice", "TradeVolume", "TradeAmount", "UNIX", "Market", "BuyRecID", "SellRecID", "BuySellFlag", "SecurityID" };
            int rowNum = 1;
            cols.Add(new BasicStringVector(rowNum));
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicDateTimeVector(rowNum));
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicStringVector(rowNum));
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicStringVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            int i = 0;
            //for(int i = 0;i< rowNum; i++) {
                cols[0].set(i, new BasicString("995000"));
                cols[1].set(i, new BasicInt(9));
                cols[2].set(i, new BasicDateTime(DateTime.Now));
                cols[3].set(i, new BasicInt(1));
                cols[4].set(i, new BasicInt(1));
                cols[5].set(i, new BasicDouble(995000.21));
                cols[6].set(i, new BasicDouble(995000.21));
                cols[7].set(i, new BasicDouble(995000.21));
                cols[8].set(i, new BasicDouble(995000.21));
                cols[9].set(i, new BasicString("SZ"));
                cols[10].set(i, new BasicInt(995000));
                cols[11].set(i, new BasicInt(77755));
                cols[12].set(i, new BasicString("995000"));
                cols[13].set(i, new BasicDouble(995000.22));
            //}
            
            var bt = new BasicTable(colNames ,cols);
            var variable = new Dictionary<string, IEntity>();
            variable.Add("table1", bt);
            try
            {
                conn.upload(variable);
            }
            catch (Exception ex)
            {
                Assert.AreEqual("ex", ex.Message);
            }
            conn.close();
        }

        [TestMethod]
        public void Test_up_down()
        {
            DBConnection conn = new DBConnection();
            //conn.connect("115.239.209.223", 8951, "admin", "123456");
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("def Foo5(a){ return a; }");

            var args = new List<IEntity>();
            args.Add(new BasicDouble(1.31111111111222344555));
            BasicDouble rd = (BasicDouble)conn.run("Foo5", args);
            Assert.AreEqual(1.31111111111222344555, rd.getValue());

            args.Clear();
            args.Add(new BasicFloat(1.32233445667889f));
            BasicFloat rf = (BasicFloat)conn.run("Foo5", args);
            Assert.AreEqual(1.32233445667889f, rf.getValue());

            args.Clear();
            args.Add(new BasicInt(323452234));
            BasicInt ri = (BasicInt)conn.run("Foo5", args);
            Assert.AreEqual(323452234, ri.getValue());

            args.Clear();
         
            args.Add(new BasicLong(2444422222222222));
            BasicLong rts = (BasicLong)conn.run("Foo5", args);
            Assert.AreEqual(2444422222222222, rts.getValue());

            args.Clear();

            args.Add(new BasicDate(DateTime.Now));
            BasicDate rtd = (BasicDate)conn.run("Foo5", args);
            Assert.AreEqual(DateTime.Now.Date, rtd.getValue().Date);

            args.Clear();
            DateTime dt = DateTime.Now;
            args.Add(new BasicDateTime(dt));
            BasicDateTime rtdm = (BasicDateTime)conn.run("Foo5", args);
            Assert.AreEqual(dt.ToString(), rtdm.getValue().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_MultiTask_SaveData()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string script = "if(existsDatabase('dfs://testMultiTaskSaveData')){dropDatabase('dfs://testMultiTaskSaveData')}";
            db.run(script);
            script = "login('admin','123456'); t = table(100:0, `symbol`price , [SYMBOL,INT]);db = database('dfs://testMultiTaskSaveData', VALUE, `A`B`C);db.createPartitionedTable(t,'table1',`symbol)";
            db.run(script);
            
            var colNames = new List<String>() { "symbol", "price" };
            var cols1 = new List<IVector>();
            int size = 100;
            var bs = new BasicStringVector(size);
            var ints = new BasicIntVector(size);
            for (int i = 0; i < size; i++)
            {
                bs.setString(i, "A");
                ints.setInt(i, i);
            }
            cols1.Add(bs);
            cols1.Add(ints);
            BasicTable bt1 = new BasicTable(colNames, cols1);


            var cols2 = new List<IVector>();
            bs = new BasicStringVector(size);
            ints = new BasicIntVector(size);
            for (int i = size; i < size * 2; i++)
            {
                int pos = i - size;
                bs.setString(pos, "B");
                ints.setInt(pos, i);
            }
            cols2.Add(bs);
            cols2.Add(ints);
            BasicTable bt2 = new BasicTable(colNames, cols2);

            var cols3 = new List<IVector>();
            bs = new BasicStringVector(size);
            ints = new BasicIntVector(size);
            for (int i = size * 2; i < size * 3; i++)
            {
                int pos = i - size * 2;
                bs.setString(pos, "C");
                ints.setInt(pos, i);
            }
            cols3.Add(bs);
            cols3.Add(ints);
            BasicTable bt3 = new BasicTable(colNames, cols3);

            Task t1 = Task.Factory.StartNew(delegate { saveData(bt1, "bt1.csv",false); });
            Task t2 = Task.Factory.StartNew(delegate { saveData(bt2, "bt2.csv",false); });
            Task t3 = Task.Factory.StartNew(delegate { saveData(bt3, "bt3.csv",false); });
            Task.WaitAll(t1, t2, t3);
            System.Threading.Thread.Sleep(2000);
            BasicTable btResult = (BasicTable)db.run("select * from loadTable('dfs://testMultiTaskSaveData','table1')");
            db.run("dropDatabase('dfs://testMultiTaskSaveData')");
            Assert.AreEqual(size * 3, btResult.rows());
            db.close();
        }

        private void saveData(BasicTable bt, string filename, bool asyntask)
        {
            DBConnection db = new DBConnection(asyntask);
            string InitScript = "def saveData(t){loadTable('dfs://testMultiTaskSaveData','table1').append!(t)}";
            db.connect(SERVER, PORT, USER, PASSWORD, InitScript);

            List<IEntity> args = new List<IEntity>(1);
            args.Add(bt);
            db.run("submitJob{'saveData','test csharp', saveData}", args);
            db.close();
        }

        [TestMethod]
        public void Test_FirstLineIsEmpty()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string sql = @"a = 1+1
a";

            BasicInt re = (BasicInt)db.run(sql);
            Assert.AreEqual(2, re.getValue());
            db.close();
        }

        [TestMethod]
        public void Test_ReLogin()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);

            db.run(@"if(existsDatabase('dfs://db1')) dropDatabase('dfs://db1')
                    db = database('dfs://db1', VALUE, 1..10)
                    t = table(1..100 as id)
                    db.createPartitionedTable(t,'t1', 'id')");
            db.run("logout()");
            db.run("login(`admin,`123456)");
            db.run("exec count(*) from loadTable('dfs://db1','t1')");
            BasicInt re = (BasicInt)db.run("exec count(*) from loadTable('dfs://db1','t1')");
            Assert.AreEqual(0, re.getValue());
            db.close();
        }

        [TestMethod]
        public void testUUID()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string uuidStr = "92274dfe-d589-4598-84a3-c381592fdf3f";
            Guid a =  Guid.Parse(uuidStr);
            BasicUuid b = BasicUuid.fromString(uuidStr);
		    List<IEntity> args = new List<IEntity>(1);
            args.Add(b);
            BasicUuid reuuid = (BasicUuid)db.run("uuid", args);
		    BasicString re = (BasicString)db.run("string", args);
            Assert.AreEqual(uuidStr, re.getString());
            db.close();
        }

        [TestMethod]
        public void testIPADDR6()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string ipv6Str = "aba8:f04:e12c:e0aa:b967:f4bf:481c:d400";
            BasicIPAddr b = BasicIPAddr.fromString(ipv6Str);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(b);
            BasicIPAddr reuuid = (BasicIPAddr)db.run("ipaddr", args);
            BasicString re = (BasicString)db.run("string", args);
            Assert.AreEqual(ipv6Str, re.getString());
            db.close();
        }

        [TestMethod]
        public void testIPADDR4()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string ipv4Str = "192.168.1.142";
            BasicIPAddr b = BasicIPAddr.fromString(ipv4Str);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(b);
            BasicIPAddr reuuid = (BasicIPAddr)db.run("ipaddr", args);
            BasicString re = (BasicString)db.run("string", args);
            Assert.AreEqual(ipv4Str, re.getString());
            db.close();
        }

        [TestMethod]
        public void testAsyntask()
        {


            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            string script = "if(existsDatabase('dfs://testMultiTaskSaveData')){dropDatabase('dfs://testMultiTaskSaveData')}";
            db.run(script);
            script = "login('admin','123456'); t = table(100:0, `symbol`price , [SYMBOL,INT]);db = database('dfs://testMultiTaskSaveData', VALUE, `A`B`C);db.createPartitionedTable(t,'table1',`symbol)";
            db.run(script);

            var colNames = new List<String>() { "symbol", "price" };
            var cols1 = new List<IVector>();
            int size = 10;
            var bs = new BasicStringVector(size);
            var ints = new BasicIntVector(size);
            for (int i = 0; i < size; i++)
            {
                bs.setString(i, "A");
                ints.setInt(i, i);
            }
            cols1.Add(bs);
            cols1.Add(ints);
            BasicTable bt1 = new BasicTable(colNames, cols1);


            var cols2 = new List<IVector>();
            bs = new BasicStringVector(size);
            ints = new BasicIntVector(size);
            for (int i = size; i < size * 2; i++)
            {
                int pos = i - size;
                bs.setString(pos, "B");
                ints.setInt(pos, i);
            }
            cols2.Add(bs);
            cols2.Add(ints);
            BasicTable bt2 = new BasicTable(colNames, cols2);

            var cols3 = new List<IVector>();
            bs = new BasicStringVector(size);
            ints = new BasicIntVector(size);
            for (int i = size * 2; i < size * 3; i++)
            {
                int pos = i - size * 2;
                bs.setString(pos, "C");
                ints.setInt(pos, i);
            }
            cols3.Add(bs);
            cols3.Add(ints);
            BasicTable bt3 = new BasicTable(colNames, cols3);

            Task t1 = Task.Factory.StartNew(delegate { saveData(bt1, "bt1.csv",false); });
            Task t2 = Task.Factory.StartNew(delegate { saveData(bt2, "bt2.csv",false); });
            //Task t3 = Task.Factory.StartNew(delegate { saveData(bt3, "bt3.csv",true); });
            Task.WaitAll(t1, t2);
            System.Threading.Thread.Sleep(3000);
            BasicTable btResult = (BasicTable)db.run("select * from loadTable('dfs://testMultiTaskSaveData','table1')");
            db.run("dropDatabase('dfs://testMultiTaskSaveData')");
            Assert.AreEqual(size * 2, btResult.rows());
            db.close();






        }

        [TestMethod]
        public void TestRunClearSessionMemory()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            db.run("testVar=1", true);
            try
            {
                db.run("print testVar");
            }
            catch (IOException ex)
            {
                string s = ex.Message;
                Assert.AreEqual(s, "Syntax Error: [line #1] Cannot recognize the token testVar");
            }
            db.close();

        }


        [TestMethod]
        public void TestSSL()
        {
            DBConnection db = new DBConnection(false, true);
            db.connect(SERVER, PORT, "admin", "123456");

            IScalar scalar = (IScalar)db.run("'a'");
            Assert.AreEqual(97, ((BasicByte)scalar).getValue());
            db.close();
        }

        [TestMethod]
        public void test_python_true()
        {
            DBConnection conn = new DBConnection(false, false, false, true);
            conn.connect("192.168.0.44", 8850);
            conn.run("import pandas as pd");
            conn.close();
        }

        [TestMethod]
        public void test_python_false()
        {
            DBConnection conn = new DBConnection(false, false, false, false);
            conn.connect("192.168.0.44", 8850);
            Exception ex1 = null;
            try
            {
                conn.run("import pandas as pd");
            }
            catch (IOException ex)
            {
                string s = ex.Message;
                Assert.AreEqual(s, "Syntax Error: [line #1] Cannot recognize the token import");
            }            
            conn.close();
        }


        //[TestMethod]
        //public void test_close()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, "admin", "123456");
        //    Exception exception = null;
        //    conn.close();
        //    try
        //    {
        //        conn.close();
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.ToString());
        //        exception = ex;
        //    }
        //    Assert.IsNotNull(exception);

        //}

        [TestMethod]
        public void test_sessionID()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            string re = null;
            re = conn.getSessionID();
            IVector v1 = (BasicStringVector)conn.run("exec string(sessionId) from getSessionMemoryStat()");
            //Array.IndexOf(v1, re);
            //bool exists = ((IList)v1).Contains(re);
            //Assert.AreEqual(exists, true);
            conn.close();

        }
    }
}

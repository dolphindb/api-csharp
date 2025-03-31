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
using dolphindb_csharpapi_net_core.src;

namespace dolphindb_csharp_api_test
{

    [TestClass]
    public class DBConnection_test2
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
            Task.Run(() => conn.run("sleep(10000)"));
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
            Task.Run(() => conn.run("sleep(10000)"));
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            Assert.AreEqual(true, ts.TotalMilliseconds < 100);
            conn.close();
        }

        [TestMethod]

        public void Test_connect_useSSL_true()
        {
            DBConnection conn = new DBConnection(false, true);
           // public DBConnection(bool asynchronousTask, bool useSSL, bool compress, bool usePython = false)
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Task<BasicByte> result = Task.Run(() => (((BasicByte)conn.run("'c'"))));
            Console.Out.WriteLine(result.Result);
            Assert.AreEqual("'c'", (result.Result).getString());
            conn.close();

        }

        [TestMethod]
        public void Test_connect_useSSL_false()
        {
            DBConnection conn = new DBConnection(false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Task<BasicByte> result = Task.Run(() => (((BasicByte)conn.run("'c'"))));
            Console.Out.WriteLine(result.Result);
            Assert.AreEqual("'c'", (result.Result).getString());
            conn.close();
        }

        [TestMethod]
        public void Test_connect_compress_true()
        {
            DBConnection conn = new DBConnection(false, false, true);
            // public DBConnection(bool asynchronousTask, bool useSSL, bool compress, bool usePython = false)
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Task<BasicByte> result = Task.Run(() => (((BasicByte)conn.run("'c'"))));
            Console.Out.WriteLine(result.Result);
            Assert.AreEqual("'c'", (result.Result).getString());
            conn.close();

        }

        [TestMethod]
        public void Test_connect_compress_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            Task<BasicByte> result = Task.Run(() => (((BasicByte)conn.run("'c'"))));
            Console.Out.WriteLine(result.Result);
            Assert.AreEqual("'c'", (result.Result).getString());
            conn.close();
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

                Task<IEntity> result = Task.Run(() => conn.run("getSessionMemoryStat()"));

                result.Result.getString();


            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
        }

        [TestMethod]
        public void Test_Connect_withLogout()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task.Run(() => db.run("logout()"));
            
            db.close();
        }


        [TestMethod]
        public void Test_run_return_scalar_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.IsTrue((Task.Run(() => (((BasicBoolean)db.run("true")))).Result).getValue());
            Assert.IsFalse((Task.Run(() => (((BasicBoolean)db.run("false")))).Result).getValue());
            Assert.IsFalse((Task.Run(() => (((BasicBoolean)db.run("1==2")))).Result).getValue());
            Assert.IsTrue((Task.Run(() => (((BasicBoolean)db.run("2==2")))).Result).getValue());
            Assert.IsTrue((Task.Run(() => (((BasicBoolean)db.run("bool(NULL)")))).Result).getString() == "");
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_byte()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(97, (Task.Run(() => (((BasicByte)db.run("'a'")))).Result).getValue());
            Assert.AreEqual("'c'", (Task.Run(() => (((BasicByte)db.run("'c'")))).Result).getString());
            Assert.AreEqual(128, (Task.Run(() => (((BasicByte)db.run("char()")))).Result).getValue());
            Assert.AreEqual(0, (Task.Run(() => (((BasicByte)db.run("char(0)")))).Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(1, (Task.Run(() => ((BasicShort)db.run("1h"))).Result).getValue());
            Assert.AreEqual(256, (Task.Run(() => ((BasicShort)db.run("256h"))).Result).getValue());
            Assert.AreEqual(1024, (Task.Run(() => ((BasicShort)db.run("1024h"))).Result).getValue());
            Assert.AreEqual(0, (Task.Run(() => ((BasicShort)db.run("0h"))).Result).getValue());
            Assert.AreEqual(-10, (Task.Run(() => ((BasicShort)db.run("-10h"))).Result).getValue());
            Assert.AreEqual(32767, (Task.Run(() => ((BasicShort)db.run("32767h"))).Result).getValue());
            Assert.AreEqual(-32767, (Task.Run(() => ((BasicShort)db.run("-32767h"))).Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(63, (Task.Run(() => ((BasicInt)db.run("63"))).Result).getValue());
            Assert.AreEqual(129, (Task.Run(() => ((BasicInt)db.run("129"))).Result).getValue());
            Assert.AreEqual(255, (Task.Run(() => ((BasicInt)db.run("255"))).Result).getValue());
            Assert.AreEqual(1023, (Task.Run(() => ((BasicInt)db.run("1023"))).Result).getValue());
            Assert.AreEqual(2047, (Task.Run(() => ((BasicInt)db.run("2047"))).Result).getValue());
            Assert.AreEqual(-2047, (Task.Run(() => ((BasicInt)db.run("-2047"))).Result).getValue());
            Assert.AreEqual(-129, (Task.Run(() => ((BasicInt)db.run("-129"))).Result).getValue());
            Assert.AreEqual(0, (Task.Run(() => ((BasicInt)db.run("0"))).Result).getValue());
            Assert.AreEqual(-2147483648, (Task.Run(() => ((BasicInt)db.run("int()"))).Result).getValue());
            Assert.AreEqual(-2147483647, (Task.Run(() => ((BasicInt)db.run("-2147483647"))).Result).getValue());
            Assert.AreEqual(2147483647, (Task.Run(() => ((BasicInt)db.run("2147483647"))).Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(1, (Task.Run(() => ((BasicLong)db.run("1l"))).Result).getValue());
            Assert.AreEqual(0, (Task.Run(() => ((BasicLong)db.run("long(0)"))).Result).getValue());
            Assert.AreEqual(-100, (Task.Run(() => ((BasicLong)db.run("long(-100)"))).Result).getValue());
            Assert.AreEqual(100, (Task.Run(() => ((BasicLong)db.run("long(100)"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicLong)db.run("long()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(2018, 03, 14), (Task.Run(() => ((BasicDate)db.run("2018.03.14"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01), (Task.Run(() => ((BasicDate)db.run("1970.01.01"))).Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01), (Task.Run(() => ((BasicDate)db.run("1969.01.01"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicDate)db.run("date()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(2018, 03, 01), (Task.Run(() => ((BasicMonth)db.run("2018.03M"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01), (Task.Run(() => ((BasicMonth)db.run("1970.01M"))).Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01), (Task.Run(() => ((BasicMonth)db.run("1969.01M"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicMonth)db.run("month()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45, 123).TimeOfDay, (Task.Run(() => ((BasicTime)db.run("15:41:45.123"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000).TimeOfDay, (Task.Run(() => ((BasicTime)db.run("00:00:00.000"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59, 999).TimeOfDay, (Task.Run(() => ((BasicTime)db.run("23:59:59.999"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicTime)db.run("time()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(1970, 01, 01, 14, 48, 00).TimeOfDay, (Task.Run(() => ((BasicMinute)db.run("14:48m"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, (Task.Run(() => ((BasicMinute)db.run("00:00m"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 00).TimeOfDay, (Task.Run(() => ((BasicMinute)db.run("23:59m"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicMinute)db.run("minute()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45).TimeOfDay, (Task.Run(() => ((BasicSecond)db.run("15:41:45"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, (Task.Run(() => ((BasicSecond)db.run("00:00:00"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay, (Task.Run(() => ((BasicSecond)db.run("23:59:59"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicSecond)db.run("second()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), (Task.Run(() => ((BasicDateTime)db.run("2018.03.14T11:28:04"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), (Task.Run(() => ((BasicDateTime)db.run("1970.01.01T00:00:00"))).Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00), (Task.Run(() => ((BasicDateTime)db.run("1969.01.01T00:00:00"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicDateTime)db.run("datetime()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(new DateTime(2018, 3, 14, 15, 41, 45, 123), (Task.Run(() => ((BasicTimestamp)db.run("2018.03.14T15:41:45.123"))).Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000), (Task.Run(() => ((BasicTimestamp)db.run("1970.01.01T00:00:00.000"))).Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00, 000), (Task.Run(() => ((BasicTimestamp)db.run("1969.01.01T00:00:00.000"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicTimestamp)db.run("timestamp()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay, (Task.Run(() => ((BasicNanoTime)db.run("15:41:45.123456789"))).Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 2223L), (Task.Run(() => ((BasicNanoTimestamp)db.run("2018.03.14T15:41:45.123222321"))).Result).getValue());
            db.close();
        }
        [TestMethod]
        public void Test_run_return_scalar_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(3, (Task.Run(() => ((BasicFloat)db.run("1.0f+2.0f"))).Result).getValue());
            Assert.AreEqual(Math.Round(129.1, 1), Math.Round((Task.Run(() => ((BasicFloat)db.run("127.1f+2.0f"))).Result).getValue(), 1));
            Assert.AreEqual(Math.Round(1.2536, 4), Math.Round((Task.Run(() => ((BasicFloat)db.run("1.2536f"))).Result).getValue(), 4));
            Assert.AreEqual(Math.Round(-1.2536, 4), Math.Round((Task.Run(() => ((BasicFloat)db.run("-1.2536f"))).Result).getValue(), 4));
            Assert.AreEqual("", (Task.Run(() => ((BasicFloat)db.run("float()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual(3, (Task.Run(() => ((BasicDouble)db.run("1.0+2.0"))).Result).getValue());
            Assert.AreEqual(129.1, (Task.Run(() => ((BasicDouble)db.run("127.1+2.0"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicDouble)db.run("double()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual("abc", (Task.Run(() => ((BasicString)db.run("`abc"))).Result).getValue());
            Assert.AreEqual("abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl", (Task.Run(() => ((BasicString)db.run("`abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl"))).Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicString)db.run("string()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_uuid()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee87", (Task.Run(() => ((BasicUuid)db.run("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')"))).Result).getString());
            Assert.AreEqual("", (Task.Run(() => ((BasicUuid)db.run("uuid()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int128()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual("e1671797c52e15f763380b45e841ec32", (Task.Run(() => ((BasicInt128)db.run("int128('e1671797c52e15f763380b45e841ec32')"))).Result).getString());
            Assert.AreEqual("", (Task.Run(() => ((BasicInt128)db.run("int128()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_ipaddr()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Assert.AreEqual("192.168.1.13", (Task.Run(() => ((BasicIPAddr)db.run("ipaddr('192.168.1.13')"))).Result).getString());
            Assert.AreEqual("", (Task.Run(() => ((BasicIPAddr)db.run("ipaddr()"))).Result).getString());
            db.close();
        }
        //[TestMethod]
        //public void Test_run_return_scalar_decimal64()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Assert.AreEqual(9223372036854775807, (Task.Run(() => ((BasicDecimal64)db.run("decimal64(999999999999999999999999,0)"))).Result).getValue());
        //    Assert.AreEqual((double)long.MinValue, (Task.Run(() => ((BasicDecimal64)db.run("decimal64(NULL,3)"))).Result).getValue());
        //    db.close();
        //}
        //[TestMethod]
        //public void Test_run_return_scalar_decimal32()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Assert.AreEqual(-1, (Task.Run(() => ((BasicDecimal32)db.run("decimal32(long(-1),2)"))).Result).getValue());
        //    Assert.AreEqual(-2147483648, (Task.Run(() => ((BasicDecimal32)db.run("decimal32(int(),2)"))).Result).getValue());
        //    db.close();
        //}
        [TestMethod]
        public void Test_run_return_vector_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicBooleanVector> result = Task.Run(() => ((BasicBooleanVector)db.run("true false")));
            IVector v = ((BasicBooleanVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(true, ((BasicBoolean)result.Result.get(0)).getValue());
            Assert.AreEqual(false, ((BasicBoolean)result.Result.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicIntVector> result = Task.Run(() => ((BasicIntVector)db.run("1 2 3")));
            IVector v = ((BasicIntVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2, ((BasicInt)v.get(1)).getValue());
            Task<BasicIntVector> result1 = Task.Run(() => ((BasicIntVector)db.run("1..10000")));
            IVector v2 = ((BasicIntVector)result1.Result);
            for(int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i+1, ((BasicInt)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicLongVector> result = Task.Run(() => ((BasicLongVector)db.run("11111111111111111l 222222222222222l 3333333333333333333l")));
            IVector v = ((BasicLongVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(222222222222222L, ((BasicLong)v.get(1)).getValue());
            Task<BasicLongVector> result1 = Task.Run(() => ((BasicLongVector)db.run("long(1..10000)")));
            IVector v2 = ((BasicLongVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicLong)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicShortVector> result = Task.Run(() => ((BasicShortVector)db.run("123h 234h 345h")));
            IVector v = ((BasicShortVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(234, ((BasicShort)v.get(1)).getValue());
            Task<BasicShortVector> result1 = Task.Run(() => ((BasicShortVector)db.run("short(1..10000)")));
            IVector v2 = ((BasicShortVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, ((BasicShort)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicFloatVector> result = Task.Run(() => ((BasicFloatVector)db.run("1.123f 2.2234f 3.4567f")));
            IVector v = ((BasicFloatVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicFloat)v.get(1)).getValue(), 4));
            Task<BasicFloatVector> result1 = Task.Run(() => ((BasicFloatVector)db.run("float(1..10000)")));
            IVector v2 = ((BasicFloatVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicFloat)v2.get(i)).getValue(),1));
            }
            db.close();
        }

  
        [TestMethod]
        public void Test_run_return_vector_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDoubleVector> result = Task.Run(() => ((BasicDoubleVector)db.run("[1.123,2.2234,3.4567]")));
            IVector v = ((BasicDoubleVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
            Task<BasicDoubleVector> result1 = Task.Run(() => ((BasicDoubleVector)db.run("double(1..10000)")));
            IVector v2 = ((BasicDoubleVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual(i + 1, Math.Round(((BasicDouble)v2.get(i)).getValue(), 1));
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDateVector> result = Task.Run(() => ((BasicDateVector)db.run("2018.03.01 2017.04.02 2016.05.03")));
            IVector v = ((BasicDateVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 02, 0, 0, 0), ((BasicDate)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicMonthVector> result = Task.Run(() => ((BasicMonthVector)db.run("2018.03M 2017.04M 2016.05M")));
            IVector v = ((BasicMonthVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 01, 0, 0, 0), ((BasicMonth)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTimeVector> result = Task.Run(() => ((BasicTimeVector)db.run("10:57:01.001 10:58:02.002 10:59:03.003")));
            IVector v = ((BasicTimeVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, ((BasicTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicNanoTimeVector> result = Task.Run(() => ((BasicNanoTimeVector)db.run("15:41:45.123456789 15:41:45.123456889 15:41:45.123456989")));
            IVector v = ((BasicNanoTimeVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4568L).TimeOfDay, ((BasicNanoTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_dateHour()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDateHourVector> result = Task.Run(() => ((BasicDateHourVector)db.run("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])")));
            IVector v = ((BasicDateHourVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2012, 06, 15, 17, 00, 00), ((BasicDateHour)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicMinuteVector> result = Task.Run(() => ((BasicMinuteVector)db.run("10:47m 10:48m 10:49m")));
            IVector v = ((BasicMinuteVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 0).TimeOfDay, ((BasicMinute)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicSecondVector> result = Task.Run(() => ((BasicSecondVector)db.run("10:47:02 10:48:03 10:49:04")));
            IVector v = ((BasicSecondVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, ((BasicSecond)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDateTimeVector> result = Task.Run(() => ((BasicDateTimeVector)db.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03")));
            IVector v = ((BasicDateTimeVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02), ((BasicDateTime)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTimestampVector> result = Task.Run(() => ((BasicTimestampVector)db.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003")));
            IVector v = ((BasicTimestampVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02, 002), ((BasicTimestamp)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicNanoTimestampVector> result = Task.Run(() => ((BasicNanoTimestampVector)db.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521")));
            IVector v = ((BasicNanoTimestampVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            Assert.AreEqual(new DateTime(dt.Ticks + 2224), ((BasicNanoTimestamp)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicStringVector> result = Task.Run(() => ((BasicStringVector)db.run("`aaa `bbb `ccc")));
            IVector v = ((BasicStringVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            Task<BasicStringVector> result1 = Task.Run(() => ((BasicStringVector)db.run("string(1..10000)")));
            IVector v2 = ((BasicStringVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual((i+1).ToString(), ((BasicString)v2.get(i)).getValue());
            }
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_symbol()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<IVector> result = Task.Run(() => ((IVector)db.run("symbol(`aaa `bbb `ccc)")));
            IVector v = ((IVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            Task<IVector> result1 = Task.Run(() => ((IVector)db.run("symbol('AA'+string(1..10000))")));
            IVector v2 = ((IVector)result1.Result);
            for (int i = 0; i < 10000; i++)
            {
                Assert.AreEqual("AA"+(i + 1).ToString(), ((BasicString)v2.get(i)).getValue());
            }
            db.close();
        }
        //[TestMethod]
        //public void Test_run_return_vector_decimal64()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Task<IVector> result = Task.Run(() => ((IVector)db.run("decimal64(symbol(`3000`234),2)")));
        //    IVector v = ((IVector)result.Result);
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal64)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal64)v.get(1)).getValue());

        //    Task<IVector> result2 = Task.Run(() => ((IVector)db.run("decimal64(symbol(string(0..9999)),2)")));
        //    IVector v2 = ((IVector)result2.Result);
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal64)v2.get(i)).getValue());
        //    }
        //    db.close();
        //}

        //[TestMethod]
        //public void Test_run_return_vector_decimal32()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Task<IVector> result = Task.Run(() => ((IVector)db.run("decimal32(symbol(`3000`234),2)")));
        //    IVector v = ((IVector)result.Result);
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal32)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal32)v.get(1)).getValue());

        //    Task<IVector> result2 = Task.Run(() => ((IVector)db.run("decimal32(symbol(string(0..9999)),2)")));
        //    IVector v2 = ((IVector)result2.Result);
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal32)v2.get(i)).getValue());
        //    }
        //    db.close();
        //}
        [TestMethod]
        public void Test_run_return_matrix_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicBooleanMatrix> result = Task.Run(() => ((BasicBooleanMatrix)db.run("matrix(true false true,false true true)")));
            IMatrix m = ((BasicBooleanMatrix)result.Result);
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(false, ((BasicBoolean)m.get(0, 1)).getValue());
            db.close();
        }


        [TestMethod]
        public void Test_run_return_matrix_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicShortMatrix> result = Task.Run(() => ((BasicShortMatrix)db.run("matrix(45h 47h 48h,56h 65h 67h)")));
            IMatrix m = ((BasicShortMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicIntMatrix> result = Task.Run(() => ((BasicIntMatrix)db.run("matrix(45 47 48,56 65 67)")));
            IMatrix m = ((BasicIntMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicLongMatrix> result = Task.Run(() => ((BasicLongMatrix)db.run("matrix(450000000000000 47000000000000 4811111111111111,5622222222222222 6533333333333333 6744444444444444)")));
            IMatrix m = ((BasicLongMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDoubleMatrix> result = Task.Run(() => ((BasicDoubleMatrix)db.run("matrix(45.02 47.01 48.03,56.123 65.04 67.21)")));
            IMatrix m = ((BasicDoubleMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicFloatMatrix> result = Task.Run(() => ((BasicFloatMatrix)db.run("matrix(45.02f 47.01f 48.03f,56.123f 65.04f 67.21f)")));
            IMatrix m = ((BasicFloatMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDateMatrix> result = Task.Run(() => ((BasicDateMatrix)db.run("matrix(2018.03.01 2017.04.02 2016.05.03,2018.03.03 2017.04.03 2016.05.04)")));
            IMatrix m = ((BasicDateMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDateTimeMatrix> result = Task.Run(() => ((BasicDateTimeMatrix)db.run("matrix(2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03,2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03)")));
            IMatrix m = ((BasicDateTimeMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTimeMatrix> result = Task.Run(() => ((BasicTimeMatrix)db.run("matrix(10:57:01.001 10:58:02.002 10:59:03.003,10:58:01.001 10:58:02.002 10:59:03.003)")));
            IMatrix m = ((BasicTimeMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicNanoTimeMatrix> result = Task.Run(() => ((BasicNanoTimeMatrix)db.run("matrix(15:41:45.123456789 15:41:45.123456789 15:41:45.123456789,15:41:45.123956789 15:41:45.123486789 15:41:45.123476789)")));
            IMatrix m = ((BasicNanoTimeMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTimestampMatrix> result = Task.Run(() => ((BasicTimestampMatrix)db.run("matrix(2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003,2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003)")));
            IMatrix m = ((BasicTimestampMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicNanoTimestampMatrix> result = Task.Run(() => ((BasicNanoTimestampMatrix)db.run("matrix(2018.03.14T10:57:01.001123456 2018.03.15T10:58:02.002123456 2018.03.16T10:59:03.003123456,2018.03.14T10:57:01.001456789 2018.03.15T10:58:02.002456789 2018.03.16T10:59:03.003456789)")));
            IMatrix m = ((BasicNanoTimestampMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicMonthMatrix> result = Task.Run(() => ((BasicMonthMatrix)db.run("matrix(2018.03M 2017.04M 2016.05M,2018.02M 2017.03M 2016.01M)")));
            IMatrix m = ((BasicMonthMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicMinuteMatrix> result = Task.Run(() => ((BasicMinuteMatrix)db.run("matrix(10:47m 10:48m 10:49m,16:47m 15:48m 14:49m)")));
            IMatrix m = ((BasicMinuteMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicSecondMatrix> result = Task.Run(() => ((BasicSecondMatrix)db.run("matrix(10:47:02 10:48:03 10:49:04,16:47:02 15:48:03 14:49:04)")));
            IMatrix m = ((BasicSecondMatrix)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTable> result = Task.Run(() => ((BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name)")));
            BasicTable tb = ((BasicTable)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicDictionary> result = Task.Run(() => ((BasicDictionary)db.run("dict(1 2 3, 2.3 3.4 5.5)")));
            BasicDictionary dict = ((BasicDictionary)result.Result);
            BasicDouble v = (BasicDouble)dict.get(new BasicInt(2));
            Assert.AreEqual(3.4, v.getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_set()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicSet> result = Task.Run(() => ((BasicSet)db.run("set(1 3 5)")));
            BasicSet dict = ((BasicSet)result.Result);
            Assert.AreEqual(3, dict.rows());
            Assert.AreEqual(1, dict.columns());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT,"admin","123456");
            Task<BasicAnyVector> result = Task.Run(() => ((BasicAnyVector)db.run("[1 2 3,3.4 3.5 3.6]")));
            BasicAnyVector v = ((BasicAnyVector)result.Result);
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(1, v.columns());
            Assert.AreEqual(3.4, ((BasicDouble)((BasicDoubleVector)v.getEntity(1)).get(0)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector_getException()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Exception exception = null;
            Task<BasicAnyVector> result = Task.Run(() => ((BasicAnyVector)db.run("[1 2 3]")));
            BasicAnyVector v = ((BasicAnyVector)result.Result);
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
        public void Test_run_return_anyvector_IScalar()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicAnyVector> result = Task.Run(() => ((BasicAnyVector)db.run("[`q `a `s,`www `2wss `rfgg]")));
            BasicAnyVector v = ((BasicAnyVector)result.Result);
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
            db.connect(SERVER, PORT, "admin", "123456");
            Task<BasicTable> result = Task.Run(() => ((BasicTable)db.run(script)));
            BasicTable tb = ((BasicTable)result.Result);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("3", dt.Rows[2]["tSHORT"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_chinese_Table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT,USER,PASSWORD);
            prepareChineseTable(conn);
            Task<BasicTable> result = Task.Run(() => ((BasicTable)conn.run("sharedTable")));
            BasicTable bt = ((BasicTable)result.Result);
            Assert.AreEqual(8000, bt.rows());
            Assert.AreEqual("备注1", 
                ((BasicString)bt.getColumn("备注").get(0)).getString());
            DataTable dt =  bt.toDataTable();
            Assert.AreEqual("备注10", dt.Rows[9]["备注"].ToString());
            conn.close();

        }
        public void prepareChineseTable(DBConnection conn)
        {
            conn.run("t =table(10000:0,['股票代码','股票日期','买方报价','卖方报价','时间戳','备注'],[SYMBOL,DATE,DOUBLE,DOUBLE,TIMESTAMP,STRING])");
            conn.run("share t as sharedTable");
            conn.run("sharedTable.append!(table(symbol(take(`GGG`MMS`FABB`APPL, 8000)) as 股票代码, take(today(), 8000) as 股票日期, norm(40, 5, 8000) as 买方报价, norm(45, 5, 8000) as 卖方报价, take(now(), 8000) as 时间戳,'备注' + string(1..8000) as 备注)) ");
            
        }

        [TestMethod]
        public void Test_run_bigData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script0 = null;
            string script1 = null;
            script0 += "m = 30000;";
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
            script1 += "undef(`exTable0,SHARED)";
            Task.Run(() => (conn.run(script0)));
            BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from ptt");
            Assert.AreEqual(100, tmpNum1.getInt());
            Task.Run(() => (conn.run(script1)));
            conn.close();
        }
    }
}

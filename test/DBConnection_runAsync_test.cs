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
    public class DBConnection_runAsync_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        //DBConnection conn = null;

        //[TestInitialize]
        //public void TestInitialize()
        //{

        //}
        //[TestCleanup]
        //public void TestCleanup()
        //{
        //    try { conn.close(); } catch (Exception ex) { }
        //    try { } catch (Exception ex) { }
        //}

        [TestMethod]
        public void Test_connect_asynchronousTask_true()
        {
            DBConnection conn = new DBConnection(true);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DateTime beforeDT = System.DateTime.Now;
            conn.runAsync("sleep(10000)");
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
            conn.runAsync("sleep(10000)");
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            Assert.AreEqual(true, ts.TotalMilliseconds < 100);
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

                Task<IEntity> result = conn.runAsync("getSessionMemoryStat()");

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
            db.runAsync("logout()");
            
            db.close();
        }


        [TestMethod]
        public void Test_run_return_scalar_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.IsTrue(((BasicBoolean)db.runAsync("true").Result).getValue());
            Assert.IsFalse(((BasicBoolean)db.runAsync("false").Result).getValue());
            Assert.IsFalse(((BasicBoolean)db.runAsync("1==2").Result).getValue());
            Assert.IsTrue(((BasicBoolean)db.runAsync("2==2").Result).getValue());
            Assert.IsTrue(((BasicBoolean)db.runAsync("bool(NULL)").Result).getString() == "");
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_byte()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(97, ((BasicByte)db.runAsync("'a'").Result).getValue());
            Assert.AreEqual("'c'", ((BasicByte)db.runAsync("'c'").Result).getString());
            Assert.AreEqual(128,  ((BasicByte)db.runAsync("char()").Result).getValue());
            Assert.AreEqual(0, ((BasicByte)db.runAsync("char(0)").Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(1, ((BasicShort)db.runAsync("1h").Result).getValue());
            Assert.AreEqual(256, ((BasicShort)db.runAsync("256h").Result).getValue());
            Assert.AreEqual(1024, ((BasicShort)db.runAsync("1024h").Result).getValue());
            Assert.AreEqual(0,  ((BasicShort)db.runAsync("0h").Result).getValue());
            Assert.AreEqual(-10,  ((BasicShort)db.runAsync("-10h").Result).getValue());
            Assert.AreEqual(32767, ((BasicShort)db.runAsync("32767h").Result).getValue());
            Assert.AreEqual(-32767, ((BasicShort)db.runAsync("-32767h").Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(63,  ((BasicInt)db.runAsync("63").Result).getValue());
            Assert.AreEqual(129, ((BasicInt)db.runAsync("129").Result).getValue());
            Assert.AreEqual(255, ((BasicInt)db.runAsync("255").Result).getValue());
            Assert.AreEqual(1023, ((BasicInt)db.runAsync("1023").Result).getValue());
            Assert.AreEqual(2047, ((BasicInt)db.runAsync("2047").Result).getValue());
            Assert.AreEqual(-2047, ((BasicInt)db.runAsync("-2047").Result).getValue());
            Assert.AreEqual(-129, ((BasicInt)db.runAsync("-129").Result).getValue());
            Assert.AreEqual(0,  ((BasicInt)db.runAsync("0").Result).getValue());
            Assert.AreEqual(-2147483648, ((BasicInt)db.runAsync("int()").Result).getValue());
            Assert.AreEqual(-2147483647, ((BasicInt)db.runAsync("-2147483647").Result).getValue());
            Assert.AreEqual(2147483647, ((BasicInt)db.runAsync("2147483647").Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(1,  ((BasicLong)db.runAsync("1l").Result).getValue());
            Assert.AreEqual(0,  ((BasicLong)db.runAsync("long(0)").Result).getValue());
            Assert.AreEqual(-100, ((BasicLong)db.runAsync("long(-100)").Result).getValue());
            Assert.AreEqual(100, ((BasicLong)db.runAsync("long(100)").Result).getValue());
            Assert.AreEqual("",  ((BasicLong)db.runAsync("long()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14),  ((BasicDate)db.runAsync("2018.03.14").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01),  ((BasicDate)db.runAsync("1970.01.01").Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01),  ((BasicDate)db.runAsync("1969.01.01").Result).getValue());
            Assert.AreEqual("", ((BasicDate)db.runAsync("date()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 01),  ((BasicMonth)db.runAsync("2018.03M").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01),  ((BasicMonth)db.runAsync("1970.01M").Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01),  ((BasicMonth)db.runAsync("1969.01M").Result).getValue());
            Assert.AreEqual("",  ((BasicMonth)db.runAsync("month()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45, 123).TimeOfDay,  ((BasicTime)db.runAsync("15:41:45.123").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000).TimeOfDay,  ((BasicTime)db.runAsync("00:00:00.000").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59, 999).TimeOfDay,  ((BasicTime)db.runAsync("23:59:59.999").Result).getValue());
            Assert.AreEqual("", ((BasicTime)db.runAsync("time()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 14, 48, 00).TimeOfDay, ((BasicMinute)db.runAsync("14:48m").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicMinute)db.runAsync("00:00m").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 00).TimeOfDay, ((BasicMinute)db.runAsync("23:59m").Result).getValue());
            Assert.AreEqual("", ((BasicMinute)db.runAsync("minute()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45).TimeOfDay,  ((BasicSecond)db.runAsync("15:41:45").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicSecond)db.runAsync("00:00:00").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay,  ((BasicSecond)db.runAsync("23:59:59").Result).getValue());
            Assert.AreEqual("",  ((BasicSecond)db.runAsync("second()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), ((BasicDateTime)db.runAsync("2018.03.14T11:28:04").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicDateTime)db.runAsync("1970.01.01T00:00:00").Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00), ((BasicDateTime)db.runAsync("1969.01.01T00:00:00").Result).getValue());
            Assert.AreEqual("",  ((BasicDateTime)db.runAsync("datetime()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 3, 14, 15, 41, 45, 123), ((BasicTimestamp)db.runAsync("2018.03.14T15:41:45.123").Result).getValue());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00, 000), ((BasicTimestamp)db.runAsync("1970.01.01T00:00:00.000").Result).getValue());
            Assert.AreEqual(new DateTime(1969, 01, 01, 00, 00, 00, 000), ((BasicTimestamp)db.runAsync("1969.01.01T00:00:00.000").Result).getValue());
            Assert.AreEqual("", (Task.Run(() => ((BasicTimestamp)db.run("timestamp()"))).Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay,  ((BasicNanoTime)db.runAsync("15:41:45.123456789").Result).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 2223L), ((BasicNanoTimestamp)db.runAsync("2018.03.14T15:41:45.123222321").Result).getValue());
            db.close();
        }
        [TestMethod]
        public void Test_run_return_scalar_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3,  ((BasicFloat)db.runAsync("1.0f+2.0f").Result).getValue());
            Assert.AreEqual(Math.Round(129.1, 1), Math.Round(((BasicFloat)db.runAsync("127.1f+2.0f").Result).getValue(), 1));
            Assert.AreEqual(Math.Round(1.2536, 4), Math.Round(((BasicFloat)db.runAsync("1.2536f").Result).getValue(), 4));
            Assert.AreEqual(Math.Round(-1.2536, 4), Math.Round(((BasicFloat)db.runAsync("-1.2536f").Result).getValue(), 4));
            Assert.AreEqual("", ((BasicFloat)db.runAsync("float()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3, ((BasicDouble)db.runAsync("1.0+2.0").Result).getValue());
            Assert.AreEqual(129.1,  ((BasicDouble)db.runAsync("127.1+2.0").Result).getValue());
            Assert.AreEqual("", ((BasicDouble)db.runAsync("double()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("abc", ((BasicString)db.runAsync("`abc").Result).getValue());
            Assert.AreEqual("abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl", ((BasicString)db.runAsync("`abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl").Result).getValue());
            Assert.AreEqual("", ((BasicString)db.runAsync("string()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_uuid()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee87", ((BasicUuid)db.runAsync("uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')").Result).getString());
            Assert.AreEqual("", ((BasicUuid)db.runAsync("uuid()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_int128()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("e1671797c52e15f763380b45e841ec32", ((BasicInt128)db.runAsync("int128('e1671797c52e15f763380b45e841ec32')").Result).getString());
            Assert.AreEqual("", ((BasicInt128)db.runAsync("int128()").Result).getString());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_scalar_ipaddr()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("ipaddr('192.168.1.13')");
            BasicIPAddr v = (BasicIPAddr)result.Result;
            Assert.AreEqual("192.168.1.13", v.getString());
            Task<IEntity> result2 = db.runAsync("ipaddr()");
            BasicIPAddr v2 = (BasicIPAddr)result2.Result;
            Assert.AreEqual("", v2.getString());
            db.close();
        }
        //[TestMethod]
        //public void Test_run_return_scalar_decimal64()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Task<IEntity> result = db.runAsync("decimal64(999999999999999999999999,0)");
        //    BasicDecimal64 v = (BasicDecimal64)result.Result;
        //    Assert.AreEqual(9223372036854775807, v.getValue());
        //    Task<IEntity> result2 = db.runAsync("decimal64(NULL,3)");
        //    BasicDecimal64 v2 = (BasicDecimal64)result2.Result;
        //    Assert.AreEqual((double)long.MinValue, v2.getValue());
        //    db.close();
        //}
        ////[TestMethod]
        //public void Test_run_return_scalar_decimal32()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Task<IEntity> result = db.runAsync("decimal32(long(-1),2)");
        //    BasicDecimal32 v = (BasicDecimal32)result.Result;
        //    Assert.AreEqual(-1, v.getValue());
        //    Task<IEntity> result2 = db.runAsync("decimal32(123.123f,2)");
        //    BasicDecimal32 v2 = (BasicDecimal32)result2.Result;
        //    Assert.AreEqual(123.12, v2.getValue());
        //    db.close();
        //}

        [TestMethod]
        public void Test_run_return_vector_bool()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("true false");
            IVector v = ((BasicBooleanVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(2, v.rows());
            Assert.AreEqual(true, ((BasicBoolean)v.get(0)).getValue());
            Assert.AreEqual(false, ((BasicBoolean)v.get(1)).getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_vector_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("1 2 3");
            IVector v = ((BasicIntVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2, ((BasicInt)v.get(1)).getValue());
            Task<IEntity> result1 = db.runAsync("1..10000");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("11111111111111111l 222222222222222l 3333333333333333333l");
            IVector v = ((BasicLongVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(222222222222222L, ((BasicLong)v.get(1)).getValue());
            Task<IEntity> result1 = db.runAsync("long(1..10000)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("123h 234h 345h");
            IVector v = ((BasicShortVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(234, ((BasicShort)v.get(1)).getValue());
            Task<IEntity> result1 = db.runAsync("short(1..10000)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("1.123f 2.2234f 3.4567f");
            IVector v = ((BasicFloatVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicFloat)v.get(1)).getValue(), 4));
            Task<IEntity> result1 = db.runAsync("float(1..10000)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("[1.123,2.2234,3.4567]");
            IVector v = ((BasicDoubleVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
            Task<IEntity> result1 = db.runAsync("double(1..10000)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("2018.03.01 2017.04.02 2016.05.03");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("2018.03M 2017.04M 2016.05M");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("10:57:01.001 10:58:02.002 10:59:03.003");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("15:41:45.123456789 15:41:45.123456889 15:41:45.123456989");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("10:47m 10:48m 10:49m");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("10:47:02 10:48:03 10:49:04");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("`aaa `bbb `ccc");
            IVector v = ((BasicStringVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            Task<IEntity> result1 = db.runAsync("string(1..10000)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("symbol(`aaa `bbb `ccc)");
            IVector v = ((IVector)result.Result);
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
            Task<IEntity> result1 = db.runAsync("symbol('AA'+string(1..10000))");
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
        //    Task<IEntity> result = db.runAsync("decimal64(symbol(`3000`234),2)");
        //    IVector v = ((IVector)result.Result);
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal64)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal64)v.get(1)).getValue());

        //    Task<IEntity> result2 = db.runAsync("decimal64(symbol(string(0..9999)),2)");
        //    IVector v2 = ((IVector)result2.Result);
        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Assert.AreEqual(i, ((BasicDecimal64)v2.get(i)).getValue());
        //    }
        //    db.close();
        //}

        ////[TestMethod]
        //public void Test_run_return_vector_decimal32()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect(SERVER, PORT);
        //    Task<IEntity> result = db.runAsync("decimal32(symbol(`3000`234),2)");
        //    IVector v = ((IVector)result.Result);
        //    Assert.IsTrue(v.isVector());
        //    Assert.AreEqual(2, v.rows());
        //    Assert.AreEqual(3000, ((BasicDecimal32)v.get(0)).getValue());
        //    Assert.AreEqual(234, ((BasicDecimal32)v.get(1)).getValue());

        //    Task<IEntity> result2 = db.runAsync("decimal32(symbol(string(0..9999)),2)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(true false true,false true true)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(45h 47h 48h,56h 65h 67h)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(45 47 48,56 65 67)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(450000000000000 47000000000000 4811111111111111,5622222222222222 6533333333333333 6744444444444444)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(45.02 47.01 48.03,56.123 65.04 67.21)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(45.02f 47.01f 48.03f,56.123f 65.04f 67.21f)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(2018.03.01 2017.04.02 2016.05.03,2018.03.03 2017.04.03 2016.05.04)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03,2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(10:57:01.001 10:58:02.002 10:59:03.003,10:58:01.001 10:58:02.002 10:59:03.003)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(15:41:45.123456789 15:41:45.123456789 15:41:45.123456789,15:41:45.123956789 15:41:45.123486789 15:41:45.123476789)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003,2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(2018.03.14T10:57:01.001123456 2018.03.15T10:58:02.002123456 2018.03.16T10:59:03.003123456,2018.03.14T10:57:01.001456789 2018.03.15T10:58:02.002456789 2018.03.16T10:59:03.003456789)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(2018.03M 2017.04M 2016.05M,2018.02M 2017.03M 2016.01M)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(10:47m 10:48m 10:49m,16:47m 15:48m 14:49m)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("matrix(10:47:02 10:48:03 10:49:04,16:47:02 15:48:03 14:49:04)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("table(1..100 as id,take(`aaa,100) as name)");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("dict(1 2 3, 2.3 3.4 5.5)");
            BasicDictionary dict = ((BasicDictionary)result.Result);
            BasicDouble v = (BasicDouble)dict.get(new BasicInt(2));
            Assert.AreEqual(3.4, v.getValue());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_set()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("set(1 3 5)");
            BasicSet dict = ((BasicSet)result.Result);
            Assert.AreEqual(3, dict.rows());
            Assert.AreEqual(1, dict.columns());
            db.close();
        }

        [TestMethod]
        public void Test_run_return_anyvector()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("[1 2 3,3.4 3.5 3.6]");
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
            db.connect(SERVER, PORT);
            Exception exception = null;
            Task<IEntity> result = db.runAsync("[1 2 3]");
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
            db.connect(SERVER, PORT);
            Task<IEntity> result = db.runAsync("[`q `a `s,`www `2wss `rfgg]");
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
            db.connect(SERVER, PORT, USER, PASSWORD);
            Task<IEntity> result = db.runAsync(script);
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
            Task<IEntity> result = conn.runAsync("sharedTable");
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
        public void Test_runAsync_one_parameter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script0 = null;
            string script1 = null;
            script0 += "m = 3000000;";
            script0 += "n = 100;";
            script0 += "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);";
            script0 += "share exTable0 as ptt1;";
            script0 += "symbol_vector=take(`A, n);";
            script0 += "ID_vector=take(100, n);";
            script0 += "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);";
            script0 += "stringv_vector=take(`name + string(0..99), n);";
            script0 += "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);";
            script0 += "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);";
            script0 += "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);";
            script1 += "undef(`ptt1,SHARED)";
            conn.runAsync(script0);
            Task<IEntity> t1 = conn.runAsync("exec count(*) from ptt1");
            BasicInt tmpNum1 = ((BasicInt)t1.Result);
            Assert.AreEqual(100, tmpNum1.getValue());
            Task<IEntity> t2 = conn.runAsync("select symbolv,ID,stringv from ptt1");
            Console.Out.WriteLine(t2.Result.getString());
            BasicTable t3 = (BasicTable)t2.Result;
            for (int j = 0; j < 100; j++)
            {
                Assert.AreEqual("A", ((BasicString)t3.getColumn(0).get(j)).getValue());
                Assert.AreEqual(100, ((BasicInt)t3.getColumn(1).get(j)).getValue());
                Assert.AreEqual("name"+j, ((BasicString)t3.getColumn(2).get(j)).getValue());

            }
            Task<IEntity> t4 = conn.runAsync("select intv from ptt1");
            BasicTable t5 = (BasicTable)t4.Result;
            Console.Out.WriteLine(t5.getString());

            conn.runAsync(script1);
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_two_parameter()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script0 = null;
            string script1 = null;
            script0 += "m = 3000000;";
            script0 += "n = 10;";
            script0 += "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);";
            script0 += "exTable1 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);";
            script0 += "share exTable0 as ptt0;";
            script0 += "share exTable1 as ptt1;";
            script0 += "symbol_vector=take(`A, n);";
            script0 += "ID_vector=take(100, n);";
            script0 += "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);";
            script0 += "stringv_vector=take(`name + string(0..99), n);";
            script0 += "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);";
            script0 += "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);";
            script0 += "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);";
            script0 += "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);";
            script1 += "undef(`ptt0,SHARED)";
            script1 += "undef(`ptt1,SHARED)";
            DateTime beforeDT = System.DateTime.Now;
            conn.runAsync(script0);
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            Assert.AreEqual(true, ts.TotalMilliseconds < 10);

            Task<IEntity> t1 = conn.runAsync("exec count(*) from ptt0");
            BasicInt tmpNum1 = ((BasicInt)t1.Result);
            //Assert.AreEqual(10, tmpNum1.getValue());

            DateTime beforeDT1 = System.DateTime.Now;
            Task<IEntity> tt1 = conn.runAsync("select * from ptt0");
            DateTime afterDT1 = System.DateTime.Now;
            TimeSpan ts1 = afterDT1.Subtract(beforeDT1);
            Console.WriteLine("DateTime costed: {0}ms", ts1.TotalMilliseconds);
            Assert.AreEqual(true, ts1.TotalMilliseconds < 10);

            DateTime beforeDT2 = System.DateTime.Now;
            BasicTable t2 = (BasicTable)tt1.Result;
            DateTime afterDT2 = System.DateTime.Now;
            TimeSpan ts2 = afterDT2.Subtract(beforeDT2);
            Console.WriteLine("DateTime costed: {0}ms", ts2.TotalMilliseconds);
            Assert.AreEqual(true, ts2.TotalMilliseconds > 1000);

            
            DateTime beforeDT3 = System.DateTime.Now;
            List<IEntity> args = new List<IEntity>() { t2 };
            Task<IEntity> t3 = conn.runAsync("tableInsert{ptt1}", args);
            DateTime afterDT3 = System.DateTime.Now;
            TimeSpan ts3 = afterDT3.Subtract(beforeDT3);
            Console.WriteLine("DateTime costed: {0}ms", ts3.TotalMilliseconds);
            Assert.AreEqual(true, ts3.TotalMilliseconds < 10);

            DateTime beforeDT4 = System.DateTime.Now;
            BasicInt tmpNum2 = ((BasicInt)t3.Result);
            Assert.AreEqual(10, tmpNum2.getValue());
            DateTime afterDT4 = System.DateTime.Now;
            TimeSpan ts4 = afterDT4.Subtract(beforeDT4);
            Console.WriteLine("DateTime costed: {0}ms", ts4.TotalMilliseconds);
            Assert.AreEqual(true, ts4.TotalMilliseconds > 1000);

            conn.runAsync(script1);
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_create_dfs()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "dropDatabase(\"dfs://rangedb_tradedata\")", "n=1000000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1 2, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades", "select * from Trades order by value,symbol" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
                    Assert.AreEqual(1000000, entity.rows());
                    for (int j = 0; j < 500000; j++)
                    {
                        BasicTable t3 = (BasicTable)entity;
                        Assert.AreEqual(1, ((BasicInt)t3.getColumn(1).get(j)).getValue());
                    }
                    for (int j = 500000; j < 1000000; j++)
                    {
                        BasicTable t3 = (BasicTable)entity;
                        Assert.AreEqual(2, ((BasicInt)t3.getColumn(1).get(j)).getValue());
                    }


                }

            }
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_create_dfs_drop()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "dropDatabase(\"dfs://rangedb_tradedata\")", "n=1000000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1..10, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades", "dropDatabase(\"dfs://rangedb_tradedata\")", "n=1000000", "t=table(rand(`IBM`MS`APPL`AMZN,n) as symbol, take(1..10, n) as value)", "db = database(\"dfs://rangedb_tradedata\", RANGE, `A`F`M`S`ZZZZ)", "Trades = db.createPartitionedTable(table=t, tableName=\"Trades\", partitionColumns=\"symbol\")", "Trades.append!(t)", "pt = db.createPartitionedTable(table=t, tableName=`pt, partitionColumns=`symbol)", "Trades=loadTable(db,`Trades)", "select min(value) from Trades", "select max(value) from Trades" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_select()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();

        }


        [TestMethod]
        public void Test_runAsync_MutiSQL_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "insert into t1 values(09:36:51,`D,1200,29.44)", "select * from t1" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "update t1 set price=30 where sym=`A ", "select * from t1" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_delete()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "sym = `A`B`C$SYMBOL", "price= 49.6 29.46 29.52", "qty = 2200 1900 2100 ", "timestamp = [09:34:07,09:36:42,09:36:51]", "t1 = table(timestamp, sym, qty, price)", "select * from t1", "delete from t1 where sym=`A ", "select * from t1" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_def()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "def test_user1(){try{deleteUser('testuser1')}\n catch(ex){}\n createUser(\"testuser1\", \"123456\")}\n rpc(getControllerAlias(), test_user1)", "def test_user(){deleteUser('testuser1')}\n rpc(getControllerAlias(), test_user)" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            foreach (IEntity entity in ret1)
            {
                Console.Out.WriteLine(entity.getString());
                entity.getString();
            }
            conn.close();
        }

        [TestMethod]
        public void Test_runAsync_MutiSQL_longsql()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "m = 3000;", "n = 100;", "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "share exTable0 as ptt;", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "select count(*) from ptt", "select ID from ptt", "select * from ptt", "select intv from ptt" };
            var ret = conn.runAsync(sqlList);
            var ret1 = ret.Result;
            for (int i = 0; i < ret1.ToArray().Length; i++)
            {
                IEntity entity = ret1.ToArray()[i];
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
            conn.close();
        }

        [TestMethod]
        public void Test_sqlList_priority()
        {
            DBConnection connection = new DBConnection();
            connection.connect(SERVER, PORT, "admin", "123456");

            //connection.connect("192.168.1.167", 8851, "admin", "123456");
            List<string> sqlList = new List<string>() { "m = 30000;", "n = 100;", "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "share exTable0 as ptt;", "select count(*) from ptt", "select ID from ptt", "select * from ptt", "select intv from ptt" };

            DBConnection connection1 = new DBConnection();
            connection1.connect(SERVER, PORT, "admin", "123456");
            //connection1.connect("192.168.1.167", 8851, "admin", "123456");
            List<string> sqlList1 = new List<string>() { "m = 30000;", "n = 100;", "exTable1 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable1.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "share exTable1 as ptt1;", "select count(*) from ptt1", "select ID from ptt1", "select * from ptt1", "select intv from ptt1" };

            var ret = connection.runAsync(sqlList, 0);
            var ret2 = connection1.runAsync(sqlList1, 0);

            DateTime beforeDT = System.DateTime.Now;
            var ret1 = ret.Result;
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            //Assert.AreEqual(true, ts.TotalMilliseconds > 20000);

            DateTime beforeDT1 = System.DateTime.Now;
            var ret3 = ret2.Result;
            DateTime afterDT1 = System.DateTime.Now;
            TimeSpan ts1 = afterDT1.Subtract(beforeDT1);
            Console.WriteLine("DateTime costed: {0}ms", ts1.TotalMilliseconds);
            //Assert.AreEqual(true, ts1.TotalMilliseconds < 100);
            connection.close();
            connection1.close();
        }
        [TestMethod]
        public void Test_sqlList_priority1()
        {
            
            DBConnection connection = new DBConnection();
            connection.connect(SERVER, PORT, "admin", "123456");
            //connection.connect("192.168.1.167", 8851, "admin", "123456");
            List<string> sqlList = new List<string>() { "m = 30000;", "n = 100;", "exTable0 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable0.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "share exTable0 as ptt;", "select count(*) from ptt", "select ID from ptt", "select * from ptt", "select intv from ptt" };

            DBConnection connection1 = new DBConnection();
            connection1.connect(SERVER, PORT, "admin", "123456");
            //connection1.connect("192.168.1.167", 8851, "admin", "123456");
            List<string> sqlList1 = new List<string>() { "m = 30000;", "n = 100;", "exTable1 = table(n:0, `symbolv`ID`timestampx`stringv`boolv`intv`datev`datetimev`timestampv`floatv, [SYMBOL, INT, TIMESTAMP, STRING, BOOL[], INT[], DATE[], DATETIME[], TIMESTAMP[], FLOAT[]]);", "symbol_vector=take(`A, n);", "ID_vector=take(100, n);", "timestampx_vector=take(temporalAdd(2020.01.01T12:23:24.345, (1..n), `m), n);", "stringv_vector=rand(`name + string(1..100), n);", "bool_vector=take([take(take(true, 5) join take(false, 5), 10)], n);", "int_vector=take([int(take([40,48,4,3,52,18,21,73,82,67], m)), int(take([36,98,95,69,41,60,78,92,78,21], m)), int(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "date_vector=take([date(take([40,48,4,3,52,18,21,73,82,67], m)), date(take([36,98,95,69,41,60,78,92,78,21], m)), date(take([92,40,13,93,9,34,86,60,43,64],m))], n);", "datetime_vector=take([datetime(take([40,48,4,3,52,18,21,73,82,67], m)), datetime(take([36,98,95,69,41,60,78,92,78,21], m)), datetime(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "timestamp_vector=take([timestamp(take([40,48,4,3,52,18,21,73,82,67], m)), timestamp(take([36,98,95,69,41,60,78,92,78,21], m)), timestamp(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "float_vector=take([float(take([40,48,4,3,52,18,21,73,82,67], m)), float(take([36,98,95,69,41,60,78,92,78,21], m)), float(take([92,40,13,93,9,34,86,60,43,64], m))], n);", "exTable1.tableInsert(symbol_vector, ID_vector, timestampx_vector, stringv_vector, bool_vector, int_vector, date_vector, datetime_vector, timestamp_vector, float_vector);", "share exTable1 as ptt1;", "select count(*) from ptt1", "select ID from ptt1", "select * from ptt1", "select intv from ptt1" };

            var ret = connection.runAsync(sqlList, 0);
            var ret2 = connection1.runAsync(sqlList1, 8);

            DateTime beforeDT1 = System.DateTime.Now;
            var ret3 = ret2.Result;
            DateTime afterDT1 = System.DateTime.Now;
            TimeSpan ts1 = afterDT1.Subtract(beforeDT1);
            Console.WriteLine("DateTime costed: {0}ms", ts1.TotalMilliseconds);
            //Assert.AreEqual(true, ts1.TotalMilliseconds > 10000);

            DateTime beforeDT = System.DateTime.Now;
            var ret1 = ret.Result;
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforeDT);
            Console.WriteLine("DateTime costed: {0}ms", ts.TotalMilliseconds);
            //Assert.AreEqual(true, ts.TotalMilliseconds > 10000);
            connection.close();
            connection1.close();
        }

        public void PrepareUser(String userName, String password)
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("def create_user(){try{deleteUser(`" + userName + ")}catch(ex){};createUser(`" + userName + ", '" + password + "',,true);};" +
                    "rpc(getControllerAlias(),create_user);");
        }

        [TestMethod]//api设置的parallelism小于server的setMaxJobParallelism
        public void test_DBConnection_runAsync_parallelism_1()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);", 4, 5, 0, false);

            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "parallelism_test", "123456");
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var ret1 = conn1.runAsync(sqlList, 4, 5);
            var re1 = ret1.Result;
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("5", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]//api设置的parallelism大于server的setMaxJobParallelism
        public void test_DBConnection_runAsync_parallelism_2()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);", 4, 30, 0, false);

            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "parallelism_test", "123456");
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var ret1 = conn1.runAsync(sqlList, 4, 30);
            var re1 = ret1.Result;
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("22", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]//api没有设置parallelism，取默认值64，大于server的setMaxJobParallelism
        public void test_DBConnection_runAsync_parallelism_3()
        {
            PrepareUser("parallelism_test", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "parallelism_test", "123456");
            conn.run("setMaxJobParallelism(\"parallelism_test\",22);");

            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "parallelism_test", "123456");
            List<string> sqlList = new List<string>() { "getConsoleJobs();" };
            var ret1 = conn1.runAsync(sqlList);
            var re1 = ret1.Result;
            Console.Out.WriteLine(((BasicTable)re1[0]).getColumn(6).get(0).getString());
            Assert.AreEqual("22", ((BasicTable)re1[0]).getColumn(6).get(0).getString());
        }

        [TestMethod]
        public void Test_sqlList_parallelism_65()
        {
            DBConnection connection = new DBConnection();
            connection.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList2 = new List<string>() { "t = select * from loadTable(\"dfs://test1\",\"pt\")", "loop(max, t.values()); " };

            try
            {

                var ret1 = connection.runAsync(sqlList2, 4, 65);
                var ret2 = ret1.Result;


            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "One or more errors occurred. (parallelism must be greater than 0 and less than 65)");
            }
            connection.close();
        }

        [TestMethod]
        public void Test_sqlList_parallelism_0()
        {
            DBConnection connection = new DBConnection();
            connection.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList2 = new List<string>() { "t = select * from loadTable(\"dfs://test1\",\"pt\")", "loop(max, t.values()); " };
            try
            {

                var ret1 = connection.runAsync(sqlList2, 4, 0);
                var ret2 = ret1.Result;


            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "One or more errors occurred. (parallelism must be greater than 0 and less than 65)");
            }
            connection.close();
        }


        [TestMethod]
        public void Test_sqlList_clearMemory_false()
        {
            DBConnection connection = new DBConnection();
            connection.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList = new List<string>() { "exTable=table(100:5, `name`id`value, [STRING,INT,DOUBLE]);", "share exTable as ptt2;" };
            var ret = connection.runAsync(sqlList, 4, 1, false);
            var ret1 = ret.Result;
            connection.close();
         }
        [TestMethod]
        public void Test_sqlList_clearMemory_true()
        {
            DBConnection connection = new DBConnection();

            DBConnection connection1 = new DBConnection();
            connection1.connect(SERVER, PORT, "admin", "123456");
            List<string> sqlList1 = new List<string>() { "exTable=table(100:5, `name`id`value, [STRING,INT,DOUBLE]);", "share exTable as ptt2;" };
            Exception e = null;
            try
            {
                var ret2 = connection1.runAsync(sqlList1, 4, 1, true);
                var ret3 = ret2.Result;
            }
            catch (Exception ex)
            {
                e=ex;

            }
            Assert.IsNotNull(e);
            connection1.close();
        }
    }
}

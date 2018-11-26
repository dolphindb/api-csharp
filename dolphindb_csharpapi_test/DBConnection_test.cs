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

namespace dolphindb_csharpapi_test
{
    [TestClass]
    public class DBConnection_test
    {
        private readonly string SERVER = "115.239.209.223";
        private readonly int PORT = 8951;

        [TestMethod]
        public void Test_MyDemo()
        {
            DBConnection db = new DBConnection();
        }

        [TestMethod]
        public void Test_Connect()
        {
            DBConnection db = new DBConnection();
            Assert.AreEqual(true, db.connect(SERVER, PORT));

        }

        [TestMethod]
        public void Test_Connect_withLogin()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            // Assert.AreEqual(true, db.connect(SERVER, PORT,"admin","1234567"));
            db.login("admin", "123456", false);
            //db.login("admin", "123456", true);
            //db.run("login('admin', '123456')");
        }


        [TestMethod]
        public void Test_Connect_withLogout()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            db.run("logout()");
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
        }

        public void getConn(object db)
        {
            DBConnection conn = (DBConnection)db;
            bool b = conn.isBusy();
            Assert.IsFalse(b);
        }

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
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable bt = new BasicTable(dt);
            Dictionary<string, IEntity> obj = new Dictionary<string, IEntity>();
            obj.Add("up_datatable", (IEntity)bt);
            db.upload(obj);
            BasicIntVector v = (BasicIntVector)db.run("up_datatable.dt_int");
            Assert.AreEqual(2147483646, v.get(0));

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
        }

        [TestMethod]
        public void Test_run_return_scalar_long()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicLong re = (BasicLong)db.run("1l");
            Assert.AreEqual(1, re.getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3, ((BasicDouble)db.run("1.0+2.0")).getValue());
            Assert.AreEqual(129.1, ((BasicDouble)db.run("127.1+2.0")).getValue());
            Assert.IsTrue(Math.Abs(1114.4 - ((BasicDouble)db.run("1127.1-12.7")).getValue()) < 0.000001);
        }

        [TestMethod]
        public void Test_run_return_scalar_float()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(3, ((BasicFloat)db.run("1.0f+2.0f")).getValue());
            Assert.AreEqual(Math.Round(129.1, 1), Math.Round(((BasicFloat)db.run("127.1f+2.0f")).getValue(), 1));
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
            Assert.IsTrue(((BasicBoolean)db.run("bool(NULL)")).getString()=="");
        }

        [TestMethod]
        public void Test_run_return_scalar_byte()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(97, ((BasicByte)db.run("'a'")).getValue());
            Assert.AreEqual("'c'", ((BasicByte)db.run("'c'")).getString());
        }

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(1, ((BasicShort)db.run("1h")).getValue());
            Assert.AreEqual(256, ((BasicShort)db.run("256h")).getValue());
            Assert.AreEqual(1024, ((BasicShort)db.run("1024h")).getValue());
            //Assert.ThrowsException<InvalidCastException>(() => { ((BasicShort)db.run("100h+5000h")).getValue(); });
        }

        [TestMethod]
        public void Test_run_return_scalar_string()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual("abc", ((BasicString)db.run("`abc")).getValue());
            Assert.AreEqual("abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl", ((BasicString)db.run("`abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14), ((BasicDate)db.run("2018.03.14")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), ((BasicDateTime)db.run("2018.03.14T11:28:04")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 03, 01), ((BasicMonth)db.run("2018.03M")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_minute()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 14, 48, 00).TimeOfDay, ((BasicMinute)db.run("14:48m")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_second()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45).TimeOfDay, ((BasicSecond)db.run("15:41:45")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_time()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45, 123).TimeOfDay, ((BasicTime)db.run("15:41:45.123")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(new DateTime(2018, 3, 14, 15, 41, 45, 123), ((BasicTimestamp)db.run("2018.03.14T15:41:45.123")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay, (((BasicNanoTime)db.run("15:41:45.123456789")).getValue()));
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            DateTime dt = new DateTime(2018, 03, 14, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 2223L), ((BasicNanoTimestamp)db.run("2018.03.14T15:41:45.123222321")).getValue());
            Assert.AreEqual("2018.03.14 15:41:45.1232223", ((BasicNanoTimestamp)db.run("2018.03.14T15:41:45.123222321")).getString());

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
        }
        [TestMethod]
        public void Test_run_return_dict()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicDictionary dict = (BasicDictionary)db.run("dict(1 2 3, 2.3 3.4 5.5)");
            BasicDouble v = (BasicDouble)dict.get(new BasicInt(2));
            Assert.AreEqual(3.4, v.getValue());
        }

        [TestMethod]
        public void Test_run_return_set()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet dict = (BasicSet)db.run("set(1 3 5)");
            Assert.AreEqual(3, dict.rows());
            Assert.AreEqual(1, dict.columns());
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
        }

        [TestMethod]
        public void Test_run_return_table_toDataTable_char()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run("table(1 as id,'a' as name)");
            DataTable dt = tb.toDataTable();
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
        }


        [TestMethod]
        public void Test_dict_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicDictionary tb = (BasicDictionary)db.run("dict(1 2 3 4,5 6 7 8)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(4, dt.Rows.Count);
        }

        [TestMethod]
        public void Test_set_toDataTable_int()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(8 9 9 5 6)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(4, dt.Rows.Count);
        }
        [TestMethod]
        public void Test_set_toDataTable_month()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09M 2018.08M)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
        }

        [TestMethod]
        public void Test_set_toDataTable_date()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01 2018.08.01)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
        }

        [TestMethod]
        public void Test_set_toDataTable_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01T01:01:01 2018.08.01T01:01:01)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
        }

        [TestMethod]
        public void Test_set_toDataTable_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicSet tb = (BasicSet)db.run("set(2018.09.01T01:01:01.001 2018.08.01T01:01:01.001)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(2, dt.Rows.Count);
        }

        [TestMethod]
        public void Test_vector_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicIntVector tb = (BasicIntVector)db.run("1 2 3 4 5");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(5, dt.Rows.Count);
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
        }

        [TestMethod]
        public void Test_Void()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            IEntity obj = db.run("NULL");
            Assert.AreEqual(obj.getObject(), null);
        }
        [TestMethod]
        public void Test_Function_Double()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            List<IEntity> args = new List<IEntity>(1);
            BasicDoubleVector vec = new BasicDoubleVector(3);
            vec.setDouble(0, 1.5);
            vec.setDouble(1, 2.5);
            vec.setDouble(2, 7);

            args.Add(vec);
            BasicDouble result = (BasicDouble)db.run("sum", args);
            Assert.AreEqual(11, result.getValue());
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
        }
        [TestMethod]
        public void Test_NewBasicTimestampWithDatetime()
        {
            BasicTimestampVector btv = new BasicTimestampVector(10);
            DateTime time = new DateTime(1970, 01, 01, 8, 2, 33);
            BasicTimestamp bt = new BasicTimestamp(time);
            //Assert.AreEqual("1970/1/1T8:02:33",bt.getString());
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
            dr["col_time"] = new TimeSpan(25, 15, 15, 14, 123);
            dr["col_int"] = 123;
            dr["col_double"] = 3.1415926;
            dr["col_long"] = 2147483647;
            dr["col_char"] = 'X';
            dr["col_bool"] = true;
            dt.Rows.Add(dr);

            BasicTable bt = new BasicTable(dt);

            double[] a =  (double[])bt.getColumn(4).getList();
            long[] b = (long[])bt.getColumn(5).getList();


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
            obj.Add("x", (IEntity)new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
            obj.Add("y", (IEntity)new BasicIntVector(new int[] { 9, 5, 3, 4, 5, 4, 7, 1, 3, 4 }));
            db.upload(obj);
            BasicDoubleVector t = (BasicDoubleVector)db.run("ttt = nullFill!(mcorr(x,y,5),0);ttt");
        }

        [TestMethod]
        public void Test_GetList()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicIntVector iv = (BasicIntVector)db.run("1 2 3 4");
            int[] r = (int[])iv.getList();
            Assert.AreEqual(4,r.Length);

            BasicDoubleVector dv = (BasicDoubleVector)db.run("1.1 2.01 3.25 4.251441 3.32222");
            double[] dr = (double[])dv.getList();
            Assert.AreEqual(5, dr.Length);
            Assert.AreEqual(1.1, dr[0]);

            BasicLongVector lv = (BasicLongVector)db.run("102012522 12345678900 12221114455");
            long[] lr = (long[])lv.getList();
            Assert.AreEqual(3, lr.Length);
            Assert.AreEqual(12345678900, lr[1]);
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
        }

        [TestMethod]
        public void Test_ContructBasicTableByDataTable()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("dt",Type.GetType("System.DateTime"));
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
            Assert.AreEqual("" , bsv.get(0).getString());
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
        }
    }
}

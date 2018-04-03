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

namespace dolphindb_csharpapi_test
{
    [TestClass]
    public class DBConnection_test
    {
        private readonly string SERVER = "localhost";
        private readonly int PORT = 8081;
        [TestMethod]
        public void Test_MyDemo()
        {
            
        }

        [TestMethod]
        public void Test_Connect()
        {
            DBConnection db = new DBConnection();
            Assert.AreEqual(true, db.connect("localhost", 8900));
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
            dc = new DataColumn("dt_string", Type.GetType("System.String"));
            dt.Columns.Add(dc);
            DataRow dr = dt.NewRow();
            dr["dt_short"] = 1;
            dr["dt_int"] = 2147483646;
            dr["dt_long"] = 2147483649;
            dr["dt_double"] = 3.14159893984;
            dr["dt_datetime"] = new DateTime(2018,03,30,14,59,02,111);
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
        }

        [TestMethod]
        public void Test_run_return_scalar_byte()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            Assert.AreEqual(97,((BasicByte)db.run("'a'")).getValue());
            Assert.AreEqual("'c'", ((BasicByte)db.run("'c'")).getString());
        }

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
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
            IVector v = (BasicDoubleVector)db.run("1.123 2.2234 3.4567");
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
            string script = @"n = 1000
cols = `tBOOL`tCHAR`tSHORT`tINT`tLONG`tDATE`tMONTH`tTIME`tMINUTE`tSECOND`tDATETIME`tTIMESTAMP`tNANOTIME`tNANOTIMESTAMP`tFLOAT`tDOUBLE`tSYMBOL
types = [BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL]
t = table(n:0, cols, types)
t.append!(table(take(0b 1b, n) as tBOOL, char(1..n) as tCHAR, short(1..n) as tSHORT, int(1..n) as tINT, long(1..n) as tLONG, 2000.01.01 + 1..n as tDATE, 2000.01M + 1..n as tMONTH, 13:30:10.008 + 1..n as tTIME, 13:30m + 1..n as tMINUTE, 13:30:10 + 1..n as tSECOND, 2012.06.13T13:30:10 + 1..n as tDATETIME, 2012.06.13T13:30:10.008 + 1..n as tTIMESTAMP, 	
09:00:01.000100001 + 1..n as tNANOTIME, 	
2016.12.30T09:00:01.000100001 + 1..n as tNANOTIMESTAMP, 2.1f + 1..n as tFLOAT, 2.1 + 1..n as tDOUBLE, take(`A`B`C`D, n) as tSYMBOL))";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(100, dt.Rows.Count);
            Assert.AreEqual(100, dt.DefaultView.Count);
            dt.Rows[0].Delete();
            Assert.AreEqual(99, dt.Rows.Count);
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
            BasicTable tb = (BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name)");
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
            BasicIntMatrix tb = (BasicIntMatrix)db.run("matrix(1 2 3, 4 5 6)");
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(2, dt.Columns.Count);
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
    }
}

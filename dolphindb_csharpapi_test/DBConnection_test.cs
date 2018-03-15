using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using System.IO;
using System.Net.Sockets;
using dolphindb.io;
using System.Text;
using System.Data;

namespace dolphindb_csharpapi_test
{
    [TestClass]
    public class DBConnection_test
    {
        [TestMethod]
        public void Test_MyDemo()
        {

            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            socket.Connect("192.168.1.61", 8702);
            StreamWriter @out = new StreamWriter(new NetworkStream(socket));

            StreamReader @in = new StreamReader(new NetworkStream(socket),Encoding.Default);
            string body = "connect\n";
            @out.Write("API 0 ");
            @out.Write(body.Length.ToString());
            @out.Write('\n');
            @out.Write(body);
            @out.Flush();

            string line = @in.ReadLine();
            int endPos = line.IndexOf(' ');
            string sessionID = line.Substring(0, endPos);

            BinaryReader binreader = new BinaryReader(new NetworkStream(socket), Encoding.Default);
            string script = "129";
            body = "script\n" + script;
            //string header = null;
            try
            {
                @out.Write("API2 " + sessionID + " ");
                @out.Write(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                @out.Write('\n');
                @out.Write(body);
                @out.Flush();

                //header = @in.ReadLine();
                //string session = @in.ReadLine();
                //string msg = @in.ReadLine();
                //string type = @in.ReadLine();
                //string res = @in.ReadLine();
                //long len = @in.BaseStream.Length;

                byte[] result = binreader.ReadBytes(20);

                //int i = 0;
                //do
                //{
                //    if (i>23) break;
                //    int j = @in.BaseStream.Read(result, i, 1);
                //    i++;
                //    //Console.Out.WriteLine(i.ToString() + " : " + Convert.ToString((int)result[i]));
                //} while (true);

                Assert.AreEqual(129, (int)result[23]);
            }
            catch
            {
                throw;
            }
        }

        [TestMethod]
        public void Test_Connect()
        {
            DBConnection db = new DBConnection();
            Assert.AreEqual(true,db.connect("localhost", 8900));
        }

        [TestMethod]
        public void Test_Connect_demo()
        {
            DBConnection db = new DBConnection();
           // Console.Out.WriteLine(db.connect());
        }

        [TestMethod]
        public void Test_run_return_scalar_int()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            Assert.AreEqual(63, ((BasicInt)db.run("63")).getValue());
            Assert.AreEqual(129, ((BasicInt)db.run("129")).getValue());
            Assert.AreEqual(255, ((BasicInt)db.run("255")).getValue());
            Assert.AreEqual(1023, ((BasicInt)db.run("1023")).getValue());
            Assert.AreEqual(2047, ((BasicInt)db.run("2047")).getValue());
            Assert.AreEqual(-2047, ((BasicInt)db.run("-2047")).getValue());
            Assert.AreEqual(-129, ((BasicInt)db.run("-129")).getValue());
            Assert.ThrowsException<InvalidCastException>(()=>{ ((BasicInt)db.run("129123456456")).getValue(); });
        }

        [TestMethod]
        public void Test_run_return_scalar_long()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            BasicLong re = (BasicLong)db.run("1l");
            Assert.AreEqual(1, re.getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_double()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            Assert.AreEqual(3, ((BasicDouble)db.run("1.0+2.0")).getValue());
            Assert.AreEqual(129.1, ((BasicDouble)db.run("127.1+2.0")).getValue());
            Assert.IsTrue(Math.Abs(1114.4-((BasicDouble)db.run("1127.1-12.7")).getValue())<0.000001);
        }

        [TestMethod]
        public void Test_run_return_scalar_float()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            Assert.AreEqual(3, ((BasicFloat)db.run("1.0f+2.0f")).getValue());
            Assert.AreEqual(Math.Round(129.1,1), Math.Round(((BasicFloat)db.run("127.1f+2.0f")).getValue(),1));
        }

        [TestMethod]
        public void Test_run_return_scalar_bool()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            Assert.IsTrue(((BasicBoolean)db.run("true")).getValue());
            Assert.IsFalse(((BasicBoolean)db.run("false")).getValue());
            Assert.IsFalse(((BasicBoolean)db.run("1==2")).getValue());
            Assert.IsTrue(((BasicBoolean)db.run("2==2")).getValue());
        }

        //[TestMethod]
        //public void Test_run_return_scalar_byte()
        //{
        //    DBConnection db = new DBConnection();
        //    db.connect("localhost", 8900);
        //    //Assert.AreEqual(1,((BasicByte)db.run("true")).getValue());
        //    //Assert.AreEqual(0, ((BasicByte)db.run("false")).getValue());
        //}

        [TestMethod]
        public void Test_run_return_scalar_short()
        {
            DBConnection db = new DBConnection();
            db.connect("localhost", 8900);
            Assert.AreEqual(1,((BasicShort)db.run("1h")).getValue());
            Assert.AreEqual(256,((BasicShort)db.run("256h")).getValue());
            Assert.AreEqual(1024,((BasicShort)db.run("1024h")).getValue());
            Assert.ThrowsException<InvalidCastException>(() => { ((BasicShort)db.run("100h+5000h")).getValue(); });
        }

        [TestMethod]
        public void Test_run_return_scalar_string()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual("abc", ((BasicString)db.run("`abc")).getValue());
            Assert.AreEqual("abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl", ((BasicString)db.run("`abcdklslkdjflkdjlskjlfkjlkhlksldkfjlkjlskdfjlskjdfl")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_date()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(2018,03,14), ((BasicDate)db.run("2018.03.14")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(2018, 03, 14,11,28,4), ((BasicDateTime)db.run("2018.03.14T11:28:04")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_month()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(2018, 03,01),((BasicMonth)db.run("2018.03M")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_minute()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(1970, 01, 01,14,48,00), ((BasicMinute)db.run("14:48m")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_second()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45), ((BasicSecond)db.run("15:41:45")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_time()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(1970, 01, 01, 15, 41, 45,123), ((BasicTime)db.run("15:41:45.123")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(2018, 3, 14, 15, 41, 45, 123), ((BasicTimestamp)db.run("2018.03.14T15:41:45.123")).getValue());
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotime()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            //Assert.AreEqual(new DateTime(565051230000L‭‬), (((BasicNanoTime)db.run("15:41:45.123000001")).getValue()));
        }

        [TestMethod]
        public void Test_run_return_scalar_nanotimestamp()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            Assert.AreEqual(new DateTime(1521042105123222300L), ((BasicNanoTimestamp)db.run("2018.03.14T15:41:45.123222300")).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_bool()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicBooleanVector)db.run("true false");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(2,v.rows());
            Assert.AreEqual(true, ((BasicBoolean)v.get(0)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_int()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicIntVector)db.run("1 2 3");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2, ((BasicInt)v.get(1)).getValue());

        }

        [TestMethod]
        public void Test_run_return_vector_long()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicLongVector)db.run("11111111111111111l 222222222222222l 3333333333333333333l");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(222222222222222L, ((BasicLong)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_short()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicShortVector)db.run("123h 234h 345h");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(234, ((BasicShort)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_float()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicFloatVector)db.run("1.123f 2.2234f 3.4567f");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicFloat)v.get(1)).getValue(), 4));
        }

        [TestMethod]
        public void Test_run_return_vector_double()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicDoubleVector)db.run("1.123 2.2234 3.4567");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(2.2234, Math.Round(((BasicDouble)v.get(1)).getValue(), 4));
        }

        [TestMethod]
        public void Test_run_return_vector_date()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicDateVector)db.run("2018.03.01 2017.04.02 2016.05.03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017, 04, 02, 0, 0, 0), ((BasicDate)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_month()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicMonthVector)db.run("2018.03M 2017.04M 2016.05M");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2017,04,01,0,0,0), ((BasicMonth)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_time()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicTimeVector)db.run("10:57:01.001 10:58:02.002 10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02,002), ((BasicTime)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_minute()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicMinuteVector)db.run("10:47m 10:48m 10:49m");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 0), ((BasicMinute)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_second()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicSecondVector)db.run("10:47:02 10:48:03 10:49:04");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03), ((BasicSecond)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicDateTimeVector)db.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02), ((BasicDateTime)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicTimestampVector)db.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 58, 02,002), ((BasicTimestamp)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_vector_string()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IVector v = (BasicStringVector)db.run("`aaa `bbb `ccc");
            Assert.IsTrue(v.isVector());
            Assert.AreEqual(3, v.rows());
            Assert.AreEqual("bbb", ((BasicString)v.get(1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_bool()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicBooleanMatrix)db.run("matrix(true false true,false true true)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(false, ((BasicBoolean)m.get(0,1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_byte()
        {
            Assert.Fail("can note defined byte datatype on server");
            //DBConnection db = new DBConnection();
            //db.connect("192.168.1.61", 8702);
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
            db.connect("192.168.1.61", 8702);
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
            db.connect("192.168.1.61", 8702);
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
            db.connect("192.168.1.61", 8702);
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
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicDoubleMatrix)db.run("matrix(45.02 47.01 48.03,56.123 65.04 67.21)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56.123, Math.Round(((BasicDouble)m.get(0, 1)).getValue(),3));
        }

        [TestMethod]
        public void Test_run_return_matrix_float()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicFloatMatrix)db.run("matrix(45.02f 47.01f 48.03f,56.123f 65.04f 67.21f)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(56.123, Math.Round(((BasicFloat)m.get(0, 1)).getValue(), 3));
        }

        [TestMethod]
        public void Test_run_return_matrix_string()
        {
            Assert.Fail("string data matrix not supported");
        }

        [TestMethod]
        public void Test_run_return_matrix_date()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicDateMatrix)db.run("matrix(2018.03.01 2017.04.02 2016.05.03,2018.03.03 2017.04.03 2016.05.04)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018,03,03), ((BasicDate)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_datetime()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicDateTimeMatrix)db.run("matrix(2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03,2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018,03,14,10,57,01), ((BasicDateTime)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_time()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicTimeMatrix)db.run("matrix(10:57:01.001 10:58:02.002 10:59:03.003,10:58:01.001 10:58:02.002 10:59:03.003)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(1970,1,1,10,58,01,001), ((BasicTime)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_timestamp()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicTimestampMatrix)db.run("matrix(2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003,2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(2018, 3, 14, 10, 57, 01, 001), ((BasicTimestamp)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_month()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
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
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicMinuteMatrix)db.run("matrix(10:47m 10:48m 10:49m,16:47m 15:48m 14:49m)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(1970, 1, 1, 16, 47, 0), ((BasicMinute)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_matrix_second()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            IMatrix m = (BasicSecondMatrix)db.run("matrix(10:47:02 10:48:03 10:49:04,16:47:02 15:48:03 14:49:04)");
            Assert.IsTrue(m.isMatrix());
            Assert.AreEqual(3, m.rows());
            Assert.AreEqual(2, m.columns());
            Assert.AreEqual(new DateTime(1970, 1, 1, 16, 47, 02), ((BasicSecond)m.get(0, 1)).getValue());
        }

        [TestMethod]
        public void Test_run_return_table_int()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            BasicTable tb = (BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name)");
            Assert.IsTrue(tb.isTable());
            Assert.AreEqual(100, tb.rows());
            Assert.AreEqual(2, tb.columns());
            Assert.AreEqual(3,((BasicInt)tb.getColumn(0).get(2)).getValue());
        }

        [TestMethod]
        public void Test_run_return_table_toDataTable()
        {
            DBConnection db = new DBConnection();
            db.connect("192.168.1.61", 8702);
            BasicTable tb = (BasicTable)db.run("table(1..100 as id,take(`aaa,100) as name)");
            DataTable dt = tb.ToDataTable();
            Assert.AreEqual(100, dt.Rows.Count);
            Assert.AreEqual(100, dt.DefaultView.Count);
            dt.Rows[0].Delete();
            Assert.AreEqual(99, dt.Rows.Count);
            DataRow[] drs = dt.Select("id > 50");
            Assert.AreEqual(50, drs.Length);
            dt.DefaultView.RowFilter = "id > 50 and name = 'abc'";
            Assert.AreEqual(0, dt.DefaultView.Count);

        }
    }
}

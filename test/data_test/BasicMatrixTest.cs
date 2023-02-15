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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicMatrixTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_BasicByteMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicByteMatrix bcv = new BasicByteMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual("0", ((BasicByte)bcv.get(0, 0)).getValue().ToString());
            IList<byte[]> args = new List<byte[]>();
            byte[] byteD = new byte[4] { 4, 5, 6, 10 };
            args.Add(byteD);
            BasicByteMatrix bcv1 = new BasicByteMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual("4", (bcv1.get(0, 0)).getObject().ToString());
            Assert.AreEqual(4, ((BasicByte)bcv1.get(0, 0)).getValue());
            bcv1.setInt(0, 0, 1);
            Assert.AreEqual(1, ((BasicByte)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1, bcv1.getByte(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Exception ex = null;
            try
            {
                BasicByteMatrix bcv2 = new BasicByteMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicByteMatrix bcv3 = new BasicByteMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            Assert.AreEqual("INTEGRAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_BYTE", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicByte", bcv.getElementClass().ToString());
            conn.close();

        }

        [TestMethod]
        public void Test_BasicBooleanMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicBooleanMatrix bcv = new BasicBooleanMatrix(1,1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(false, ((BasicBoolean)bcv.get(0, 0)).getValue());
            IList<byte[]> args = new List<byte[]>();
            byte[] byteD = new byte[4] { 4, 5, 6, 10 };
            args.Add(byteD);
            BasicBooleanMatrix bcv1 = new BasicBooleanMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(true, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(true, ((BasicBoolean)bcv1.get(0, 0)).getValue());
            bcv1.setBoolean(0, 0, false);
            Assert.AreEqual(false, ((BasicBoolean)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(false, bcv1.getBoolean(0,0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Exception ex = null;
            try
            {
                BasicBooleanMatrix bcv2 = new BasicBooleanMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicBooleanMatrix bcv3 = new BasicBooleanMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            Assert.AreEqual("LOGICAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_BOOL", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicBoolean", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateHourMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDateHourMatrix bcv = new BasicDateHourMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicDateHour)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 5, 6, 10 };
            args.Add(intD);
            BasicDateHourMatrix bcv1 = new BasicDateHourMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 01, 01, 04, 00, 00), (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 04, 00, 00), ((BasicDateHour)bcv1.get(0, 0)).getValue());
            bcv1.setDateTime(0, 0, new DateTime(2000, 12, 31, 23, 59, 59));
            Assert.AreEqual(new DateTime(2000, 12, 31, 23, 00, 00), ((BasicDateHour)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(2000, 12, 31, 23, 00, 00), bcv1.getDateTime(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_DATEHOUR", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicDateHour", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDateMatrix bcv = new BasicDateMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicDate)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 5, 6, 10 };
            args.Add(intD);
            BasicDateMatrix bcv1 = new BasicDateMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 01, 05, 00, 00, 00), (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 05, 00, 00, 00), ((BasicDate)bcv1.get(0, 0)).getValue());
            bcv1.setDate(0, 0, new DateTime(2000, 12, 31, 23, 59, 59));
            Assert.AreEqual(new DateTime(2000, 12, 31, 00, 00, 00), ((BasicDate)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(2000, 12, 31, 00, 00, 00), bcv1.getDate(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_DATE", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicDate", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateTimeMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDateTimeMatrix bcv = new BasicDateTimeMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicDateTime)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 5, 6, 10 };
            args.Add(intD);
            BasicDateTimeMatrix bcv1 = new BasicDateTimeMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 04), (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 04), ((BasicDateTime)bcv1.get(0, 0)).getValue());
            bcv1.setDateTime(0, 0, new DateTime(2000, 12, 31, 23, 59, 59));
            Assert.AreEqual(new DateTime(2000, 12, 31, 23, 59, 59), ((BasicDateTime)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(2000, 12, 31, 23, 59, 59), bcv1.getDateTime(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_DATETIME", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicDateTime", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDoubleMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDoubleMatrix bcv = new BasicDoubleMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(0, ((BasicDouble)bcv.get(0, 0)).getValue());
            IList<double[]> args = new List<double[]>();
            double[] doubleD = new double[4] { -4.1, 5.7, 6.99, 10 };
            args.Add(doubleD);
            BasicDoubleMatrix bcv1 = new BasicDoubleMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(-4.1, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(-4.1, ((BasicDouble)bcv1.get(0, 0)).getValue());
            bcv1.setDouble(0, 0, 1.2);
            Assert.AreEqual(1.2, ((BasicDouble)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1.2, bcv1.getDouble(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicDoubleMatrix bcv2 = new BasicDoubleMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicDoubleMatrix bcv3 = new BasicDoubleMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("FLOATING", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_DOUBLE", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicDouble", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicFloatMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicFloatMatrix bcv = new BasicFloatMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(0, ((BasicFloat)bcv.get(0, 0)).getValue());
            IList<float[]> args = new List<float[]>();
            float[] floatD = new float[4] { -4.1f, 5.7f, 6.99f, 10f };
            args.Add(floatD);
            BasicFloatMatrix bcv1 = new BasicFloatMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(-4.1f, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(-4.1f, ((BasicFloat)bcv1.get(0, 0)).getValue());
            bcv1.setFloat(0, 0, 1.2f);
            Assert.AreEqual(1.2f, ((BasicFloat)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1.2f, bcv1.getFloat(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicFloatMatrix bcv2 = new BasicFloatMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicFloatMatrix bcv3 = new BasicFloatMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("FLOATING", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_FLOAT", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicFloat", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicIntMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicIntMatrix bcv = new BasicIntMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(0, ((BasicInt)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { -4, 0, 6, 10 };
            args.Add(intD);
            BasicIntMatrix bcv1 = new BasicIntMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(-4, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(-4, ((BasicInt)bcv1.get(0, 0)).getValue());
            bcv1.setInt(0, 0, 1);
            Assert.AreEqual(1, ((BasicInt)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1, bcv1.getInt(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicIntMatrix bcv2 = new BasicIntMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicIntMatrix bcv3 = new BasicIntMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("INTEGRAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_INT", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicInt", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicLongMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicLongMatrix bcv = new BasicLongMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(0, ((BasicLong)bcv.get(0, 0)).getValue());
            IList<long[]> args = new List<long[]>();
            long[] longD = new long[4] { -4, 0, 6, 12222222 };
            args.Add(longD);
            BasicLongMatrix bcv1 = new BasicLongMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            long a = 12222222;
            Assert.AreEqual(a, (bcv1.get(3, 0)).getObject());
            Assert.AreEqual(a, ((BasicLong)bcv1.get(3, 0)).getValue());
            bcv1.setLong(0, 0, 1);
            Assert.AreEqual(1, ((BasicLong)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1, bcv1.getLong(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicLongMatrix bcv2 = new BasicLongMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicLongMatrix bcv3 = new BasicLongMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("INTEGRAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_LONG", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicLong", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicMinuteMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicMinuteMatrix bcv = new BasicMinuteMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicMinute)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 0, 6, 12222222 };
            args.Add(intD);
            BasicMinuteMatrix bcv1 = new BasicMinuteMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 04, 00).TimeOfDay, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 04, 00).TimeOfDay, ((BasicMinute)bcv1.get(0, 0)).getValue());
            bcv1.setMinute(0, 0, new DateTime(2000, 01, 01, 23, 59, 59).TimeOfDay);
            Assert.AreEqual(new DateTime(2000, 01, 01, 23, 59, 00).TimeOfDay, ((BasicMinute)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(2000, 01, 01, 23, 59, 00).TimeOfDay, bcv1.getMinute(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_MINUTE", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicMinute", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]//APICS-156
        public void Test_BasicMonthMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicMonthMatrix bcv = new BasicMonthMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Console.Out.WriteLine(bcv.getMonth(0,0));
            Assert.AreEqual(new DateTime(0001, 01, 01), ((BasicMonth)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[3] { 4, 0, 6 };
            args.Add(intD);
            BasicMonthMatrix bcv1 = new BasicMonthMatrix(3, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(3, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            //Assert.AreEqual(new DateTime(1970, 05, 01), (bcv1.get(0, 0)).getObject());
            //Assert.AreEqual(new DateTime(1970, 05, 01), ((BasicMonth)bcv1.get(0, 0)).getValue());
            bcv1.setMonth(0, 0, new DateTime(9999, 12, 01));
            //Assert.AreEqual(new DateTime(9999, 12, 01), ((BasicMonth)bcv1.get(0, 0)).getValue());
            //Assert.AreEqual(new DateTime(9999, 12, 01), bcv1.getMonth(0, 0));
            bcv1.setNull(0, 0);
            //Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_MONTH", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicMonth", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]//APICS-179
        public void Test_BasicNanoTimeMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicNanoTimeMatrix bcv = new BasicNanoTimeMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Console.Out.WriteLine(bcv.getNanoTime(0,0));
            //DateTime dt = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            //long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00).TimeOfDay, bcv.getNanoTime(0,0));
            IList<long[]> args = new List<long[]>();
            long[] longD = new long[4] { 4, 0, 6, 12222222 };
            args.Add(longD);
            BasicNanoTimeMatrix bcv1 = new BasicNanoTimeMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            DateTime dt1 = new DateTime(1970, 1, 1, 00, 00, 00, 012);
            long tickCount1 = dt1.Ticks;
            Assert.AreEqual(new DateTime(tickCount1 + 2222L).TimeOfDay, bcv1.getNanoTime(3,0));
            Assert.AreEqual(new DateTime(tickCount1 + 2222L).TimeOfDay, ((BasicNanoTime)bcv1.get(3, 0)).getValue());

            bcv1.setNanoTime(0, 0, new DateTime(1970, 1, 1, 00, 00, 00, 012));
            Assert.AreEqual(new DateTime(tickCount1 + 0000L).TimeOfDay, ((BasicNanoTime)bcv1.get(0, 0)).getValue());
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_NANOTIME", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicNanoTime", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicNanoTimestampMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicNanoTimestampMatrix bcv = new BasicNanoTimestampMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Console.Out.WriteLine(bcv.getNanoTimestamp(0, 0));
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00), bcv.getNanoTimestamp(0, 0));
            IList<long[]> args = new List<long[]>();
            long[] longD = new long[4] { 4, 0, 6, 12222222 };
            args.Add(longD);
            BasicNanoTimestampMatrix bcv1 = new BasicNanoTimestampMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            DateTime dt1 = new DateTime(1970, 1, 1, 00, 00, 00, 012);
            long tickCount1 = dt1.Ticks;
            Assert.AreEqual(new DateTime(tickCount1 + 2222L), bcv1.getNanoTimestamp(3, 0));
            Assert.AreEqual(new DateTime(tickCount1 + 2222L), ((BasicNanoTimestamp)bcv1.get(3, 0)).getValue());

            bcv1.setNanoTimestamp(0, 0, new DateTime(1970, 1, 1, 00, 00, 00, 012));
            Assert.AreEqual(new DateTime(tickCount1 + 0000L), ((BasicNanoTimestamp)bcv1.get(0, 0)).getValue());
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_NANOTIMESTAMP", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicNanoTimestamp", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicSecondMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicSecondMatrix bcv = new BasicSecondMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Console.Out.WriteLine(bcv.getSecond(0, 0));
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00).TimeOfDay, bcv.getSecond(0, 0));
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 0, 6, 12222222 };
            args.Add(intD);
            BasicSecondMatrix bcv1 = new BasicSecondMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 04).TimeOfDay, bcv1.getSecond(0, 0));
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 04).TimeOfDay, ((BasicSecond)bcv1.get(0, 0)).getValue());

            bcv1.setSecond(0, 0, new DateTime(1970, 01, 01, 00, 00, 01).TimeOfDay);
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 01).TimeOfDay, ((BasicSecond)bcv1.get(0, 0)).getValue());
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_SECOND", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicSecond", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicShortMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicShortMatrix bcv = new BasicShortMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(0, ((BasicShort)bcv.get(0, 0)).getValue());
            IList<short[]> args = new List<short[]>();
            short[] shortD = new short[4] { -4, 0, 6, 10 };
            args.Add(shortD);
            BasicShortMatrix bcv1 = new BasicShortMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            short a = -4;
            Assert.AreEqual(a, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(a, ((BasicShort)bcv1.get(0, 0)).getValue());
            bcv1.setShort(0, 0, 1);
            Assert.AreEqual(1, ((BasicShort)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(1, bcv1.getShort(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicShortMatrix bcv2 = new BasicShortMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicShortMatrix bcv3 = new BasicShortMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("INTEGRAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_SHORT", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicShort", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicStringMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringMatrix bcv = new BasicStringMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(null, ((BasicString)bcv.get(0, 0)).getValue());
            IList<string[]> args = new List<string[]>();
            string[] stringD = new string[4] { "112", "1234567890QAZWSXEDCRFVTGBYHNUJMIKOLP!@#$%^&*()_+{}|:\" <>?/.,;'[]\' ", "qazwsxedcrtyuioplkjhgfcvbnm", "-1" };
            args.Add(stringD);
            BasicStringMatrix bcv1 = new BasicStringMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual("1234567890QAZWSXEDCRFVTGBYHNUJMIKOLP!@#$%^&*()_+{}|:\" <>?/.,;'[]\' ", (bcv1.get(1, 0)).getObject());
            Assert.AreEqual("1234567890QAZWSXEDCRFVTGBYHNUJMIKOLP!@#$%^&*()_+{}|:\" <>?/.,;'[]\' ", ((BasicString)bcv1.get(1, 0)).getValue());
            bcv1.setString(0, 0, "1");
            Assert.AreEqual("1", ((BasicString)bcv1.get(0, 0)).getValue());
            Assert.AreEqual("1", bcv1.getString(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));
            Exception ex = null;
            try
            {
                BasicStringMatrix bcv2 = new BasicStringMatrix(2, 2, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                BasicStringMatrix bcv3 = new BasicStringMatrix(1, 1, args);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);

            Assert.AreEqual("LITERAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_SYMBOL", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicString", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicTimeMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicTimeMatrix bcv = new BasicTimeMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, ((BasicTime)bcv.get(0, 0)).getValue());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[4] { 4, 0, 6, 12222222 };
            args.Add(intD);
            BasicTimeMatrix bcv1 = new BasicTimeMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            DateTime dt1 = new DateTime(1970, 1, 1, 00, 00, 00, 004);
            long tickCount1 = dt1.Ticks;
            Assert.AreEqual(new DateTime(tickCount1 + 0000L).TimeOfDay, (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(tickCount1 + 0000L).TimeOfDay, ((BasicTime)bcv1.get(0, 0)).getValue());
            bcv1.setTime(0, 0, new DateTime(1970, 1, 1, 00, 00, 00, 012));
            DateTime dt2 = new DateTime(1970, 1, 1, 00, 00, 00, 012);
            long tickCount2 = dt2.Ticks;
            Assert.AreEqual(new DateTime(tickCount2 + 0000L).TimeOfDay, ((BasicTime)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(tickCount2 + 0000L).TimeOfDay, bcv1.getTime(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_TIME", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicTime", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicTimestampMatrix()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicTimestampMatrix bcv = new BasicTimestampMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), ((BasicTimestamp)bcv.get(0, 0)).getValue());
            IList<long[]> args = new List<long[]>();
            long[] longD = new long[4] { 4, 0, 6, 12222222 };
            args.Add(longD);
            BasicTimestampMatrix bcv1 = new BasicTimestampMatrix(4, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(4, bcv1.rows());
            Console.Out.WriteLine(bcv1.getString());
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 004), (bcv1.get(0, 0)).getObject());
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 004), ((BasicTimestamp)bcv1.get(0, 0)).getValue());
            bcv1.setTimestamp(0, 0, new DateTime(1970, 1, 1, 00, 00, 00, 012));
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 012), ((BasicTimestamp)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 012), bcv1.getTimestamp(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_TIMESTAMP", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicTimestamp", bcv.getElementClass().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicShortMartrix_Remain()
        {
            try
            {
                BasicShortMatrix bsm1 = new BasicShortMatrix(2, 2, new List<short[]> { new short[] { 1, 2 }, null });
            }
            catch(Exception e)
            {
                Assert.AreEqual("The length of array " + 2 + " doesn't have " + 2 + " elements", e.Message);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicShortMatrix bsm1 = new BasicShortMatrix(2, 2, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have " + 2 + " columns", e.Message);
                Console.WriteLine(e.Message);
            }
        }



        [TestMethod]
        public void Test_BasicLongMartrix_Remain()
        {
            try
            {
                BasicLongMatrix bsm1 = new BasicLongMatrix(2, 2, new List<long[]> { new long[] { 1, 2 }, null });
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array " + 2 + " doesn't have " + 2 + " elements", e.Message);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicLongMatrix bsm1 = new BasicLongMatrix(2, 2, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have " + 2 + " columns", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicIntMartrix_Remain()
        {
            try
            {
                BasicIntMatrix bsm1 = new BasicIntMatrix(2, 2, new List<int[]> { new int[] { 1, 2 }, null });
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array " + 2 + " doesn't have " + 2 + " elements", e.Message);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicIntMatrix bsm1 = new BasicIntMatrix(2, 2, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have " + 2 + " columns", e.Message);
                Console.WriteLine(e.Message);
            }
        }


        [TestMethod]
        public void Test_BasicFloatMartrix_Remain()
        {
            try
            {
                BasicFloatMatrix bsm1 = new BasicFloatMatrix(2, 2, new List<float[]> { new float[] { 1.3f, 2.1f }, null });
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array " + 2 + " doesn't have " + 2 + " elements", e.Message);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicFloatMatrix bsm1 = new BasicFloatMatrix(2, 2, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have " + 2 + " columns", e.Message);
                Console.WriteLine(e.Message);
            }
        }


        [TestMethod]
        public void Test_BasicDoubleMartrix_Remain()
        {
            try
            {
                BasicDoubleMatrix bsm1 = new BasicDoubleMatrix(2, 2, new List<double[]> { new double[] { 1.3, 2.1 }, null });
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array " + 2 + " doesn't have " + 2 + " elements", e.Message);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicDoubleMatrix bsm1 = new BasicDoubleMatrix(2, 2, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have " + 2 + " columns", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicBooleanMatrix_Remain()
        {
            List<byte[]> values = new List<byte[]>();
            byte[] b1 = new byte[] { 0, 1, 4, 3 };
            values.Add(b1);
            values.Add(b1);
            try
            {
                BasicBooleanMatrix bbm1 = new BasicBooleanMatrix(1, 1, values);
            }catch(Exception e)
            {
                Assert.AreEqual("input list of arrays does not have 1 columns", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                BasicBooleanMatrix bbm1 = new BasicBooleanMatrix(1, 1, null);
            }
            catch(Exception e)
            {
                Assert.AreEqual("input list of arrays does not have 1 columns", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                BasicBooleanMatrix bbm1 = new BasicBooleanMatrix(2, 2, values);
            }
            catch(Exception e)
            {
                Assert.AreEqual("The length of array 1 doesn't have 2 elements", e.Message);
                Console.WriteLine(e.Message);
            }

            List<byte[]> datas = new List<byte[]>();
            datas.Add(null);
            try
            {
                BasicBooleanMatrix bbm1 = new BasicBooleanMatrix(2, 1, datas);
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array 1 doesn't have 2 elements", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicStringMatrix_Remain()
        {
            List<string[]> values = new List<string[]>();
            string[] s1 = new string[] { "jhgsdhg", "dgjhg", "hguhgka", "hgsj" };
            values.Add(s1);
            values.Add(s1);
            try
            {
                BasicStringMatrix bsm1 = new BasicStringMatrix(1, 1, values);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have 1 columns", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                BasicStringMatrix bsm1 = new BasicStringMatrix(1, 1, null);
            }
            catch (Exception e)
            {
                Assert.AreEqual("input list of arrays does not have 1 columns", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                BasicStringMatrix bsm1 = new BasicStringMatrix(2, 2, values);
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array 1 doesn't have 2 elements", e.Message);
                Console.WriteLine(e.Message);
            }

            List<string[]> datas = new List<string[]>();
            datas.Add(null);
            try
            {
                BasicStringMatrix bsm1 = new BasicStringMatrix(2, 1, datas);
            }
            catch (Exception e)
            {
                Assert.AreEqual("The length of array 1 doesn't have 2 elements", e.Message);
                Console.WriteLine(e.Message);
            }
        }
    }
}

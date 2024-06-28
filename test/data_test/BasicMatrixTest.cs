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
        }

        [TestMethod]
        public void Test_BasicBooleanMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicDateHourMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicDateMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicDateTimeMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicDoubleMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicFloatMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicIntMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicLongMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicMinuteMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicMonthMatrix()
        {
            BasicMonthMatrix bcv = new BasicMonthMatrix(1, 1);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(1, bcv.rows());
            IList<int[]> args = new List<int[]>();
            int[] intD = new int[3] { 4, 0, 6 };
            args.Add(intD);
            BasicMonthMatrix bcv1 = new BasicMonthMatrix(3, 1, args);
            Assert.AreEqual(1, bcv1.columns());
            Assert.AreEqual(3, bcv1.rows());
            //Console.Out.WriteLine(bcv1.getString());DateTime not support 0000.01.01
            //Assert.AreEqual(new DateTime(1970, 05, 01), (bcv1.get(0, 0)).getObject());
            //Assert.AreEqual(new DateTime(1970, 05, 01), ((BasicMonth)bcv1.get(0, 0)).getValue());
            bcv1.setMonth(0, 0, new DateTime(9999, 12, 01));
            Assert.AreEqual(new DateTime(9999, 12, 01), ((BasicMonth)bcv1.get(0, 0)).getValue());
            Assert.AreEqual(new DateTime(9999, 12, 01), bcv1.getMonth(0, 0));
            bcv1.setNull(0, 0);
            Assert.AreEqual(true, bcv1.isNull(0, 0));

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_MONTH", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicMonth", bcv.getElementClass().ToString());
        }

        [TestMethod]//APICS-179
        public void Test_BasicNanoTimeMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicNanoTimestampMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicSecondMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicShortMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicStringMatrix()
        {
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
            Assert.AreEqual("DT_STRING", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicString", bcv.getElementClass().ToString());
        }

        [TestMethod]
        public void Test_BasicTimeMatrix()
        {
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
        }

        [TestMethod]
        public void Test_BasicTimestampMatrix()
        {
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
        [TestMethod]
        public void test_BasicDecimal32Matrix()
        {
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(0, 0, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("\n", re1.getString());
            Assert.AreEqual(DATA_CATEGORY.DENARY, re1.getDataCategory());
            Assert.AreEqual("dolphindb.data.BasicDecimal32", re1.getElementClass().ToString());
            Assert.AreEqual(DATA_TYPE.DT_DECIMAL32, re1.getDataType());

            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 4, 0);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0 #1 #2 #3\n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n", re2.getString());

            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 1, 2, 3 });
            list.Add(new decimal[] { 4, 5, 6 });
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 2, list, 2);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re3.getString());
            IList<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "1", "2", "3" });
            list1.Add(new String[] { "4", "5", "6" });
            BasicDecimal32Matrix re4 = new BasicDecimal32Matrix(3, 2, list1, 2);
            Console.WriteLine(re4.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re4.getString());
            String exception = null;
            try
            {
                BasicDecimal32Matrix re5 = new BasicDecimal32Matrix(4, 2, list, 2);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception);
            String exception1 = null;
            try
            {
                BasicDecimal32Matrix re6 = new BasicDecimal32Matrix(3, 3, list, 2);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception1);

            String exception2 = null;
            try
            {
                BasicDecimal32Matrix re7 = new BasicDecimal32Matrix(4, 2, list1, 2);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception2);

            String exception3 = null;
            try
            {
                BasicDecimal32Matrix re8 = new BasicDecimal32Matrix(3, 3, list1, 2);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception3);
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_scale_not_true()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 999999999, 0, -999999999 });
            list.Add(new decimal[] { 999999999, 999999999, 999999999 });
            list.Add(new decimal[] { -999999999, -999999999, -999999999 });
            String exception = null;
            try
            {
                BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3, list, -1);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,9].", exception);
            String exception1 = null;
            try
            {
                BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3, list, 10);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("Scale 10 is out of bounds, it must be in [0,9].", exception1);

            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "999999999", "0", "-999999999" });
            list1.Add(new String[] { "999999999", "999999999", "999999999" });
            list1.Add(new String[] { "-999999999", "-999999999", "-999999999" });
            String exception2 = null;
            try
            {
                BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3, list1, -1);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,9].", exception2);
            String exception3 = null;
            try
            {
                BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 3, list1, 10);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("Scale 10 is out of bounds, it must be in [0,9].", exception3);
        }
        [TestMethod]
        public void test_BasicDecimal32Matrix_decimal()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 999999999, 0, -999999999 });
            list.Add(new decimal[] { 999999999, 999999999, 999999999 });
            list.Add(new decimal[] { -999999999, -999999999, -999999999 });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0         #1        #2        \n" +
                    "999999999  999999999 -999999999\n" +
                    "0          999999999 -999999999\n" +
                    "-999999999 999999999 -999999999\n", re1.getString());

            List<decimal[]> list1 = new List<decimal[]>();
            list1.Add(new decimal[] { 99999, 0, -99999 });
            list1.Add(new decimal[] { 99999, 99999, 99999 });
            list1.Add(new decimal[] { -99999, -99999, -1999 });
            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 3, list1, 4);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0          #1         #2         \n" +
                    "99999.0000  99999.0000 -99999.0000\n" +
                    "0.0000      99999.0000 -99999.0000\n" +
                    "-99999.0000 99999.0000 -1999.0000 \n", re2.getString());

            List<decimal[]> list2 = new List<decimal[]>();
            list2.Add(new decimal[] { 1, 0, -1 });
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 1, list2, 9);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0          \n" +
                    "1.000000000 \n" +
                    "0.000000000 \n" +
                    "-1.000000000\n", re3.getString());
        }
        [TestMethod]
        public void test_BasicDecimal32Matrix_string()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "999999999", "0", "-999999999" });
            list.Add(new String[] { "999999999", "999999999", "999999999" });
            list.Add(new String[] { "-999999999", "-999999999", "-999999999" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0         #1        #2        \n" +
                    "999999999  999999999 -999999999\n" +
                    "0          999999999 -999999999\n" +
                    "-999999999 999999999 -999999999\n", re1.getString());
            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "99999", "0", "-99999" });
            list1.Add(new String[] { "99999", "99999", "99999" });
            list1.Add(new String[] { "-99999", "-99999", "-1999" });
            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(3, 3, list1, 4);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0          #1         #2         \n" +
                    "99999.0000  99999.0000 -99999.0000\n" +
                    "0.0000      99999.0000 -99999.0000\n" +
                    "-99999.0000 99999.0000 -1999.0000 \n", re2.getString());

            List<String[]> list2 = new List<String[]>();
            list2.Add(new String[] { "1", "0", "-1" });
            BasicDecimal32Matrix re3 = new BasicDecimal32Matrix(3, 1, list2, 9);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0          \n" +
                    "1.000000000 \n" +
                    "0.000000000 \n" +
                    "-1.000000000\n", re3.getString());
            List<String[]> list3 = new List<String[]>();
            list3.Add(new String[] { "-1.9999", "0", "-1.00000009", "-1.999999999999" });
            list3.Add(new String[] { "1.9999", "0", "1.00000009", "1.999999999999" });
            list3.Add(new String[] { "-0.9999", "0.01", "-0.00000009", "-0.999999999999" });
            list3.Add(new String[] { "0.9999", "-0.001", "0.00000009", "0.999999999999" });
            BasicDecimal32Matrix re4 = new BasicDecimal32Matrix(4, 4, list3, 9);
            Console.WriteLine(re4.getString());
            Assert.AreEqual("#0           #1          #2           #3          \n" +
                    "-1.999900000 1.999900000 -0.999900000 0.999900000 \n" +
                    "0.000000000  0.000000000 0.010000000  -0.001000000\n" +
                    "-1.000000090 1.000000090 -0.000000090 0.000000090 \n" +
                    "-2.000000000 2.000000000 -1.000000000 1.000000000 \n", re4.getString());

            List<String[]> list4 = new List<String[]>();
            list4.Add(new String[] { "0.49", "-123.44", "132.50", "-0.51" });
            BasicDecimal32Matrix re5 = new BasicDecimal32Matrix(4, 1, list4, 0);
            Console.WriteLine(re5.getString());
            Assert.AreEqual("#0  \n" +
                    "0   \n" +
                    "-123\n" +
                    "133 \n" +
                    "-1  \n", re5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_setNull()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(3, 1, list, 0);
            Assert.AreEqual(false, re1.isNull(0, 0));
            re1.setNull(0, 0);
            Assert.AreEqual(true, re1.isNull(0, 0));
            Assert.AreEqual("#0  \n" +
                    "    \n" +
                    "0   \n" +
                    "-111\n", re1.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_setDecimal()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 1).getString());
            re1.setDecimal(0, 0, 1.999990m);
            re1.setDecimal(0, 1, -0.99999999m);
            re1.setDecimal(1, 0, 999.9999999m);
            re1.setDecimal(1, 1, -999.99999m);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(2, 2, 6);
            re2.setDecimal(0, 0, 1.999990m);
            re2.setDecimal(0, 1, -0.99999999m);
            re2.setDecimal(1, 0,  999.9999999m);
            re2.setDecimal(1, 1,  -999.99999m);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal32("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-999.999990", 6).getString(), re2.get(1, 1).getString());

            Assert.AreEqual("1.99999", re2.getDecimal(0, 0).ToString());
            Assert.AreEqual("-1", re2.getDecimal(0, 1).ToString());
            Assert.AreEqual("1000", re2.getDecimal(1, 0).ToString());
            Assert.AreEqual("-999.99999", re2.getDecimal(1, 1).ToString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_setString()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 1).getString());
            re1.setString(0, 0, "1.999990");
            re1.setString(0, 1, "-0.99999999");
            re1.setString(1, 0, "999.9999999");
            re1.setString(1, 1, "-999.99999");
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(2, 2, 6);
            re2.setString(0, 0, "1.999990");
            re2.setString(0, 1, "-0.99999999");
            re2.setString(1, 0, "999.9999999");
            re2.setString(1, 1, "-999.99999");
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal32("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_set()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32(0, 0).getString(), re1.get(1, 1).getString());
            IScalar bd1 = new BasicDecimal32("1.999990", 6);
            IScalar bd2 = new BasicDecimal32("-0.99999999",8);
            IScalar bd3 = new BasicDecimal32("999.999999", 6);
            IScalar bd4 = new BasicDecimal32("-999.99999", 5);
            re1.set(0, 0, bd1);
            re1.set(0, 1, bd2);
            re1.set(1, 0, bd3);
            re1.set(1, 1, bd4);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal32Matrix re2 = new BasicDecimal32Matrix(2, 2, 6);
            re2.set(0, 0, bd1);
            re2.set(0, 1, bd2);
            re2.set(1, 0, bd3);
            re2.set(1, 1, bd4);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal32("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal32("999.999999", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal32("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_set_null()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "9999999.99", "-9999999.99" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 1, list, 2);
            BasicDecimal32 bd1 = new BasicDecimal32("1.99999", 9);
            bd1.setNull();
            re1.set(0, 0, bd1);
            re1.isNull(0, 0);
            BasicDecimal32 bd2 = new BasicDecimal32(-2147483648, 0);
            BasicDecimal32 bd3 = new BasicDecimal32(1, 0);
            bd3.setNull();
            re1.set(1, 0, bd2);
            re1.isNull(1, 0);
            Assert.AreEqual("", re1.get(0, 0).getString());
            Assert.AreEqual("", re1.get(1, 0).getString());
        }

        [TestMethod]
        public void test_BasicDecimal32Matrix_set_entity_not_support()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal32Matrix re1 = new BasicDecimal32Matrix(2, 2, 0);

            String exception = null;
            try
            {
                re1.set(0, 0, new BasicDecimal64("1.99999", 9));
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("the type of value must be BasicDecimal32. ", exception);
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix()
        {
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(0, 0, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("\n", re1.getString());
            Assert.AreEqual(DATA_CATEGORY.DENARY, re1.getDataCategory());
            Assert.AreEqual("dolphindb.data.BasicDecimal64", re1.getElementClass().ToString());
            Assert.AreEqual(DATA_TYPE.DT_DECIMAL64, re1.getDataType());

            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 4, 0);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0 #1 #2 #3\n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n", re2.getString());

            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 1, 2, 3 });
            list.Add(new decimal[] { 4, 5, 6 });
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 2, list, 2);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re3.getString());
            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "1", "2", "3" });
            list1.Add(new String[] { "4", "5", "6" });
            BasicDecimal64Matrix re4 = new BasicDecimal64Matrix(3, 2, list1, 2);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re3.getString());
            String exception = null;
            try
            {
                BasicDecimal64Matrix re5 = new BasicDecimal64Matrix(4, 2, list, 2);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception);
            String exception1 = null;
            try
            {
                BasicDecimal64Matrix re6 = new BasicDecimal64Matrix(3, 3, list, 2);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception1);

            String exception2 = null;
            try
            {
                BasicDecimal64Matrix re7 = new BasicDecimal64Matrix(4, 2, list1, 2);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception2);

            String exception3 = null;
            try
            {
                BasicDecimal64Matrix re8 = new BasicDecimal64Matrix(3, 3, list1, 2);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception3);
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_scale_not_true()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 999999999, 0, -999999999 });
            list.Add(new decimal[] { 999999999, 999999999, 999999999 });
            list.Add(new decimal[] { -999999999, -999999999, -999999999 });
            String exception = null;
            try
            {
                BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3, list, -1);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,18].", exception);
            String exception1 = null;
            try
            {
                BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3, list, 19);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("Scale 19 is out of bounds, it must be in [0,18].", exception1);

            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "999999999999999999", "0", "-999999999999999999" });
            list1.Add(new String[] { "999999999999999999", "999999999999999999", "999999999999999999" });
            list1.Add(new String[] { "-999999999999999999", "-999999999999999999", "-999999999999999999" });
            String exception2 = null;
            try
            {
                BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3, list1, -1);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,18].", exception2);
            String exception3 = null;
            try
            {
                BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 3, list1, 19);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("Scale 19 is out of bounds, it must be in [0,18].", exception3);
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_decimal()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 999999999999999999L, 0, -999999999999999999L });
            list.Add(new decimal[] { 999999999999999999L, 999999999999999999L, 999999999999999999L });
            list.Add(new decimal[] { -999999999999999999L, -999999999999999999l, -999999999999999999L });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0                  #1                 #2                 \n" +
                    "999999999999999999  999999999999999999 -999999999999999999\n" +
                    "0                   999999999999999999 -999999999999999999\n" +
                    "-999999999999999999 999999999999999999 -999999999999999999\n", re1.getString());

            List<decimal[]> list1 = new List<decimal[]>();
            list1.Add(new decimal[] { 99999, 0, -99999 });
            list1.Add(new decimal[] { 99999, 99999, 99999 });
            list1.Add(new decimal[] { -99999, -99999, -1999 });
            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 3, list1, 13);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0                   #1                  #2                  \n" +
                    "99999.0000000000000  99999.0000000000000 -99999.0000000000000\n" +
                    "0.0000000000000      99999.0000000000000 -99999.0000000000000\n" +
                    "-99999.0000000000000 99999.0000000000000 -1999.0000000000000 \n", re2.getString());

            List<decimal[]> list2 = new List<decimal[]>();
            list2.Add(new decimal[] { 1, 0, -1 });
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 1, list2, 18);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0                   \n" +
                    "1.000000000000000000 \n" +
                    "0.000000000000000000 \n" +
                    "-1.000000000000000000\n", re3.getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_string()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "999999999999999999", "0", "-999999999999999999" });
            list.Add(new String[] { "999999999999999999", "999999999999999999", "999999999999999999" });
            list.Add(new String[] { "-999999999999999999", "-999999999999999999", "-999999999999999999" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0                  #1                 #2                 \n" +
                    "999999999999999999  999999999999999999 -999999999999999999\n" +
                    "0                   999999999999999999 -999999999999999999\n" +
                    "-999999999999999999 999999999999999999 -999999999999999999\n", re1.getString());
            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "99999", "0", "-99999" });
            list1.Add(new String[] { "99999", "99999", "99999" });
            list1.Add(new String[] { "-99999", "-99999", "-1999" });
            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(3, 3, list1, 13);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0                   #1                  #2                  \n" +
                    "99999.0000000000000  99999.0000000000000 -99999.0000000000000\n" +
                    "0.0000000000000      99999.0000000000000 -99999.0000000000000\n" +
                    "-99999.0000000000000 99999.0000000000000 -1999.0000000000000 \n", re2.getString());

            List<String[]> list2 = new List<String[]>();
            list2.Add(new String[] { "1", "0", "-1" });
            BasicDecimal64Matrix re3 = new BasicDecimal64Matrix(3, 1, list2, 18);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0                   \n" +
                    "1.000000000000000000 \n" +
                    "0.000000000000000000 \n" +
                    "-1.000000000000000000\n", re3.getString());
            List<String[]> list3 = new List<String[]>();
            list3.Add(new String[] { "-1.9999", "0", "-1.00000000000009", "-1.999999999999999999999999" });
            list3.Add(new String[] { "1.9999", "0", "1.00000000009", "1.999999999999999999999999" });
            list3.Add(new String[] { "-0.9999", "0.01", "-0.00000000009", "-0.999999999999999999999999" });
            list3.Add(new String[] { "0.9999", "-0.001", "0.00000000000000000009", "0.999999999999999999999999" });
            BasicDecimal64Matrix re4 = new BasicDecimal64Matrix(4, 4, list3, 18);
            Console.WriteLine(re4.getString());
            Assert.AreEqual("#0                    #1                   #2                    #3                   \n" +
                    "-1.999900000000000000 1.999900000000000000 -0.999900000000000000 0.999900000000000000 \n" +
                    "0.000000000000000000  0.000000000000000000 0.010000000000000000  -0.001000000000000000\n" +
                    "-1.000000000000090000 1.000000000090000000 -0.000000000090000000 0.000000000000000000 \n" +
                    "-2.000000000000000000 2.000000000000000000 -1.000000000000000000 1.000000000000000000 \n", re4.getString());

            List<String[]> list4 = new List<String[]>();
            list4.Add(new String[] { "0.49", "-123.44", "132.50", "-0.51" });
            BasicDecimal64Matrix re5 = new BasicDecimal64Matrix(4, 1, list4, 0);
            Console.WriteLine(re5.getString());
            Assert.AreEqual("#0  \n" +
                    "0   \n" +
                    "-123\n" +
                    "133 \n" +
                    "-1  \n", re5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_setNull()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(3, 1, list, 0);
            Assert.AreEqual(false, re1.isNull(0, 0));
            re1.setNull(0, 0);
            Assert.AreEqual(true, re1.isNull(0, 0));
            Assert.AreEqual("", re1.get(0, 0).getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_setDecimal()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 1).getString());
            re1.setDecimal(0, 0, 1.999990m);
            re1.setDecimal(0, 1, -0.99999999m);
            re1.setDecimal(1, 0, 999.9999999m);
            re1.setDecimal(1, 1, -999.99999m);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(2, 2, 6);
            re2.setDecimal(0, 0, 1.999990m);
            re2.setDecimal(0, 1, -0.99999999m);
            re2.setDecimal(1, 0, 999.9999999m);
            re2.setDecimal(1, 1, -999.99999m);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal64("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-999.999990", 6).getString(), re2.get(1, 1).getString());

            Assert.AreEqual("1.99999", re2.getDecimal(0, 0).ToString());
            Assert.AreEqual("-1", re2.getDecimal(0, 1).ToString());
            Assert.AreEqual("1000", re2.getDecimal(1, 0).ToString());
            Assert.AreEqual("-999.99999", re2.getDecimal(1, 1).ToString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_setString()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 1).getString());
            re1.setString(0, 0, "1.999990");
            re1.setString(0, 1, "-0.99999999");
            re1.setString(1, 0, "999.9999999");
            re1.setString(1, 1, "-999.99999");
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(2, 2, 6);
            re2.setString(0, 0, "1.999990");
            re2.setString(0, 1, "-0.99999999");
            re2.setString(1, 0, "999.9999999");
            re2.setString(1, 1, "-999.99999");
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal64("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_set()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64(0, 0).getString(), re1.get(1, 1).getString());
            IScalar bd1 = new BasicDecimal64("1.999990", 6);
            IScalar bd2 = new BasicDecimal64("-0.99999999", 8);
            IScalar bd3 = new BasicDecimal64("999.999999", 6);
            IScalar bd4 = new BasicDecimal64("-999.99999", 5);
            re1.set(0, 0, bd1);
            re1.set(0, 1, bd2);
            re1.set(1, 0, bd3);
            re1.set(1, 1, bd4);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal64Matrix re2 = new BasicDecimal64Matrix(2, 2, 6);
            re2.set(0, 0, bd1);
            re2.set(0, 1, bd2);
            re2.set(1, 0, bd3);
            re2.set(1, 1, bd4);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal64("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal64("999.999999", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal64("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_set_null()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "999999999", "-999999999" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 1, list, 2);
            BasicDecimal64 bd1 = new BasicDecimal64("1.99999", 9);
            bd1.setNull();
            re1.set(0, 0, bd1);
            re1.isNull(0, 0);
            BasicDecimal64 bd2 = new BasicDecimal64("", 0);
            re1.set(1, 0, bd2);
            re1.isNull(1, 0);
            Assert.AreEqual("", re1.get(0, 0).getString());
            Assert.AreEqual("", re1.get(1, 0).getString());
        }

        [TestMethod]
        public void test_BasicDecimal64Matrix_set_entity_not_support()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal64Matrix re1 = new BasicDecimal64Matrix(2, 2, 0);

            String exception = null;
            try
            {
                re1.set(0, 0, new BasicDecimal128("1.99999", 9));
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("value type must be BasicDecimal64. ", exception);
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix()
        {
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(0, 0, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("\n", re1.getString());
            Assert.AreEqual(DATA_CATEGORY.DENARY, re1.getDataCategory());
            Assert.AreEqual("dolphindb.data.BasicDecimal128", re1.getElementClass().ToString());
            Assert.AreEqual(DATA_TYPE.DT_DECIMAL128, re1.getDataType());

            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 4, 0);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0 #1 #2 #3\n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n" +
                    "0  0  0  0 \n", re2.getString());

            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 1, 2, 3 });
            list.Add(new decimal[] { 4, 5, 6 });
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 2, list, 2);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re3.getString());
            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "1", "2", "3" });
            list1.Add(new String[] { "4", "5", "6" });
            BasicDecimal128Matrix re4 = new BasicDecimal128Matrix(3, 2, list1, 2);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0   #1  \n" +
                    "1.00 4.00\n" +
                    "2.00 5.00\n" +
                    "3.00 6.00\n", re3.getString());
            String exception = null;
            try
            {
                BasicDecimal128Matrix re5 = new BasicDecimal128Matrix(4, 2, list, 2);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception);
            String exception1 = null;
            try
            {
                BasicDecimal128Matrix re6 = new BasicDecimal128Matrix(3, 3, list, 2);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception1);

            String exception2 = null;
            try
            {
                BasicDecimal64Matrix re7 = new BasicDecimal64Matrix(4, 2, list1, 2);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception2);

            String exception3 = null;
            try
            {
                BasicDecimal64Matrix re8 = new BasicDecimal64Matrix(3, 3, list1, 2);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("the total size of list must be equal to rows * columns", exception3);
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_scale_not_true()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 999999999, 0, -999999999 });
            list.Add(new decimal[] { 999999999, 999999999, 999999999 });
            list.Add(new decimal[] { -999999999, -999999999, -999999999 });
            String exception = null;
            try
            {
                BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3, list, -1);
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,38].", exception);
            String exception1 = null;
            try
            {
                BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3, list, 39);
            }
            catch (Exception ex)
            {
                exception1 = ex.Message;
            }
            Assert.AreEqual("Scale 39 is out of bounds, it must be in [0,38].", exception1);

            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "999999999", "0", "-999999999" });
            list1.Add(new String[] { "999999999", "999999999", "999999999" });
            list1.Add(new String[] { "-999999999", "-999999999", "-999999999" });
            String exception2 = null;
            try
            {
                BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3, list1, -1);
            }
            catch (Exception ex)
            {
                exception2 = ex.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,38].", exception2);
            String exception3 = null;
            try
            {
                BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 3, list1, 39);
            }
            catch (Exception ex)
            {
                exception3 = ex.Message;
            }
            Assert.AreEqual("Scale 39 is out of bounds, it must be in [0,38].", exception3);
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_decimal()
        {
            IList<decimal[]> list = new List<decimal[]>();
            list.Add(new decimal[] { 79228162514264337593543950335m, 0m, -79228162514264337593543950335m });
            list.Add(new decimal[] { 79228162514264337593543950335m, 79228162514264337593543950335m, 79228162514264337593543950335m });
            list.Add(new decimal[] { -79228162514264337593543950335m, -79228162514264337593543950335m, -79228162514264337593543950335m });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0                             #1                            #2                            \n" +
                    "79228162514264337593543950335  79228162514264337593543950335 -79228162514264337593543950335\n" +
                    "0                              79228162514264337593543950335 -79228162514264337593543950335\n" +
                    "-79228162514264337593543950335 79228162514264337593543950335 -79228162514264337593543950335\n", re1.getString());

            List<decimal[]> list1 = new List<decimal[]>();
            list1.Add(new decimal[] { 9999999999999999999m, 0m, -9999999999999999999m });
            list1.Add(new decimal[] { 9999999999999999999m, 9999999999999999999m, 9999999999999999999m });
            list1.Add(new decimal[] { -9999999999999999999m, -9999999999999999999m, -9999999999999999999m });
            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 3, list1, 19);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0                                       #1                                      #2                                      \n" +
                    "9999999999999999999.0000000000000000000  9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                    "0.0000000000000000000                    9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                    "-9999999999999999999.0000000000000000000 9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n", re2.getString());

            List<decimal[]> list2 = new List<decimal[]>();
            list2.Add(new decimal[] { 1m, 0m, -1m });
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 1, list2, 38);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0                                       \n" +
                    "1.00000000000000000000000000000000000000 \n" +
                    "0.00000000000000000000000000000000000000 \n" +
                    "-1.00000000000000000000000000000000000000\n", re3.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_string()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "99999999999999999999999999999999999999", "0", "-99999999999999999999999999999999999999" });
            list.Add(new String[] { "99999999999999999999999999999999999999", "99999999999999999999999999999999999999", "99999999999999999999999999999999999999" });
            list.Add(new String[] { "-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 3, list, 0);
            Console.WriteLine(re1.getString());
            Assert.AreEqual("#0                                      #1                                     #2                                     \n" +
                    "99999999999999999999999999999999999999  99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                    "0                                       99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n" +
                    "-99999999999999999999999999999999999999 99999999999999999999999999999999999999 -99999999999999999999999999999999999999\n", re1.getString());
            List<String[]> list1 = new List<String[]>();
            list1.Add(new String[] { "9999999999999999999.9999999999999999999", "0", "-9999999999999999999" });
            list1.Add(new String[] { "9999999999999999999.0000000000000000009", "9999999999999999999", "9999999999999999999" });
            list1.Add(new String[] { "-9999999999999999999.0000000000000000009", "-9999999999999999999", "-9999999999999999999" });
            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(3, 3, list1, 19);
            Console.WriteLine(re2.getString());
            Assert.AreEqual("#0                                       #1                                      #2                                      \n" +
                    "9999999999999999999.9999999999999999999  9999999999999999999.0000000000000000009 -9999999999999999999.0000000000000000009\n" +
                    "0.0000000000000000000                    9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n" +
                    "-9999999999999999999.0000000000000000000 9999999999999999999.0000000000000000000 -9999999999999999999.0000000000000000000\n", re2.getString());

            List<String[]> list2 = new List<String[]>();
            list2.Add(new String[] { "1", "0", "-1" });
            BasicDecimal128Matrix re3 = new BasicDecimal128Matrix(3, 1, list2, 38);
            Console.WriteLine(re3.getString());
            Assert.AreEqual("#0                                       \n" +
                    "1.00000000000000000000000000000000000000 \n" +
                    "0.00000000000000000000000000000000000000 \n" +
                    "-1.00000000000000000000000000000000000000\n", re3.getString());
            List<String[]> list3 = new List<String[]>();
            list3.Add(new String[] { "-1.9999", "0", "-1.00000009", "-1.999999999999" });
            list3.Add(new String[] { "1.9999", "0", "1.00000009", "1.999999999999" });
            list3.Add(new String[] { "-0.9999", "0.01", "-0.00000009", "-0.999999999999" });
            list3.Add(new String[] { "0.9999", "-0.001", "0.00000009", "0.999999999999" });
            BasicDecimal128Matrix re4 = new BasicDecimal128Matrix(4, 4, list3, 9);
            Console.WriteLine(re4.getString());
            Assert.AreEqual("#0           #1          #2           #3          \n" +
                    "-1.999900000 1.999900000 -0.999900000 0.999900000 \n" +
                    "0.000000000  0.000000000 0.010000000  -0.001000000\n" +
                    "-1.000000090 1.000000090 -0.000000090 0.000000090 \n" +
                    "-2.000000000 2.000000000 -1.000000000 1.000000000 \n", re4.getString());

            List<String[]> list4 = new List<String[]>();
            list4.Add(new String[] { "0.49", "-123.44", "132.50", "-0.51" });
            BasicDecimal128Matrix re5 = new BasicDecimal128Matrix(4, 1, list4, 0);
            Console.WriteLine(re5.getString());
            Assert.AreEqual("#0  \n" +
                    "0   \n" +
                    "-123\n" +
                    "133 \n" +
                    "-1  \n", re5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_setNull()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(3, 1, list, 0);
            Assert.AreEqual(false, re1.isNull(0, 0));
            re1.setNull(0, 0);
            Assert.AreEqual(true, re1.isNull(0, 0));
            Assert.AreEqual("", re1.get(0, 0).getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_setDecimal()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 1).getString());
            re1.setDecimal(0, 0, 1.999990m);
            re1.setDecimal(0, 1, -0.99999999m);
            re1.setDecimal(1, 0, 999.9999999m);
            re1.setDecimal(1, 1, -999.99999m);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(2, 2, 6);
            re2.setDecimal(0, 0, 1.999990m);
            re2.setDecimal(0, 1, -0.99999999m);
            re2.setDecimal(1, 0, 999.9999999m);
            re2.setDecimal(1, 1, -999.99999m);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal128("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-999.999990", 6).getString(), re2.get(1, 1).getString());

            Assert.AreEqual("1.99999", re2.getDecimal(0, 0).ToString());
            Assert.AreEqual("-1", re2.getDecimal(0, 1).ToString());
            Assert.AreEqual("1000", re2.getDecimal(1, 0).ToString());
            Assert.AreEqual("-999.99999", re2.getDecimal(1, 1).ToString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_setString()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 1).getString());
            re1.setString(0, 0, "1.999990");
            re1.setString(0, 1, "-0.99999999");
            re1.setString(1, 0, "999.9999999");
            re1.setString(1, 1, "-999.99999");
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(2, 2, 6);
            re2.setString(0, 0, "1.999990");
            re2.setString(0, 1, "-0.99999999");
            re2.setString(1, 0, "999.9999999");
            re2.setString(1, 1, "-999.99999");
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal128("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128("1000.000000", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_set()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2, 0);
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128(0, 0).getString(), re1.get(1, 1).getString());
            IScalar bd1 = new BasicDecimal128("1.999990", 6);
            IScalar bd2 = new BasicDecimal128("-0.99999999", 8);
            IScalar bd3 = new BasicDecimal128("999.999999", 6);
            IScalar bd4 = new BasicDecimal128("-999.99999", 5);
            re1.set(0, 0, bd1);
            re1.set(0, 1, bd2);
            re1.set(1, 0, bd3);
            re1.set(1, 1, bd4);
            Assert.AreEqual("#0   #1   \n" +
                    "2    -1   \n" +
                    "1000 -1000\n", re1.getString());
            BasicDecimal128Matrix re2 = new BasicDecimal128Matrix(2, 2, 6);
            re2.set(0, 0, bd1);
            re2.set(0, 1, bd2);
            re2.set(1, 0, bd3);
            re2.set(1, 1, bd4);
            Console.WriteLine(re2.getString());
            Assert.AreEqual(6, re2.getScale());
            Assert.AreEqual(new BasicDecimal128("1.999990", 6).getString(), re2.get(0, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-1.000000", 6).getString(), re2.get(0, 1).getString());
            Assert.AreEqual(new BasicDecimal128("999.999999", 6).getString(), re2.get(1, 0).getString());
            Assert.AreEqual(new BasicDecimal128("-999.999990", 6).getString(), re2.get(1, 1).getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_set_null()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "999999999", "-999999999" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 1, list, 2);
            BasicDecimal128 bd1 = new BasicDecimal128("1.99999", 9);
            bd1.setNull();
            re1.set(0, 0, bd1);
            re1.isNull(0, 0);
            BasicDecimal128 bd2 = new BasicDecimal128(" -170141183460469231731687303715884105728", 0);
            re1.set(1, 0, bd2);
            re1.isNull(1, 0);
            Console.WriteLine(re1.getString());
            Console.WriteLine(re1.get(0, 0));

            Assert.AreEqual("", re1.get(0, 0).getString());
            Assert.AreEqual("", re1.get(1, 0).getString());
        }

        [TestMethod]
        public void test_BasicDecimal128Matrix_set_entity_not_support()
        {
            IList<String[]> list = new List<String[]>();
            list.Add(new String[] { "111", "0", "-111" });
            BasicDecimal128Matrix re1 = new BasicDecimal128Matrix(2, 2, 0);

            String exception = null;
            try
            {
                re1.set(0, 0, new BasicDecimal64("1.99999", 9));
            }
            catch (Exception ex)
            {
                exception = ex.Message;
            }
            Assert.AreEqual("value type must be BasicDecimal128. ", exception);
        }

    }
}

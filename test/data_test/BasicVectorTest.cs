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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicVectorTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private ExtendedDataOutput @out;
        private ExtendedDataInput @in;
        private SymbolBase @base;

        [TestMethod]
        public void Test_BasicByteVector()
        {
            //elements scalar null
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<byte?> args = new List<byte?>();
            args.Add(1);
            args.Add(2);
            args.Add(3);
            BasicByteVector bcv = new BasicByteVector(args);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(3, bcv.rows());
            Assert.AreEqual("2", bcv.get(1).getString());
            bcv.set(0, "");
            Assert.AreEqual("", bcv.get(0).getString());
            bcv.setByte(0, 10);
            Assert.AreEqual("10", bcv.get(0).getString());
            Assert.AreEqual(false, bcv.isNull(0));
            bcv.setNull(0);
            Assert.AreEqual(true, bcv.isNull(0));
            Assert.AreEqual("INTEGRAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_BYTE", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicByte", bcv.getElementClass().ToString());
            Console.Out.WriteLine(bcv.getList());
            bcv.set(0, "");
            Assert.AreEqual("", bcv.get(0).getString());
            bcv.set(2, "string");
            Assert.AreEqual("", bcv.get(2).getString());
            bcv.set(2, "1");
            Assert.AreEqual("1", bcv.get(2).getString());
            bcv.add(4);
            Assert.AreEqual("4", bcv.get(3).getString());
            bcv.addRange(new List<byte> { 5, 6, 7 });
            Assert.AreEqual(7, bcv.rows());
            Assert.AreEqual("7", bcv.get(6).getString());
            Assert.AreEqual(0, bcv.hashBucket(0, 1));
            int[] indices = new int[10];
            bcv.getSubVector(indices);
            Assert.AreEqual("[,2,1,4,5,6,7]", bcv.getString());
            IScalar scalar = (IScalar)conn.run("6");
            Assert.AreEqual(5, bcv.asof(scalar));
            //ExtendedDataInput extendedDataInput = (ExtendedDataInput)conn.run("1");
            //bcv.deserialize(0,1, extendedDataInput);
            //Console.Out.WriteLine(bcv);
            conn.close();
        }

        [TestMethod]
        public void Test_BasicBooleanVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<byte?> args = new List<byte?>();
            args.Add(1);
            args.Add(2);
            args.Add(3);
            BasicBooleanVector bcv = new BasicBooleanVector(args);
            Assert.AreEqual(1, bcv.columns());
            Assert.AreEqual(3, bcv.rows());
            Assert.AreEqual(true, bcv.get(0).getObject());
            Assert.AreEqual(true, bcv.get(1).getObject());
            Assert.AreEqual(true, bcv.get(2).getObject());
            Assert.AreEqual(true, bcv.getBoolean(1));
            bcv.set(0, "");
            Assert.AreEqual(false, bcv.get(0).getObject());
            bcv.setBoolean(0, true);
            Assert.AreEqual(true, bcv.get(0).getObject());
            bcv.setNull(0);
            Assert.AreEqual(true, bcv.isNull(0));
            Assert.AreEqual("LOGICAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("DT_BOOL", bcv.getDataType().ToString());
            Assert.AreEqual("dolphindb.data.BasicBoolean", bcv.getElementClass().ToString());
            Exception exception = null;
            try
            {
                bcv.getList();
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            bcv.set(0, "");
            Assert.AreEqual(false, bcv.get(0).getObject());
            bcv.set(2, "string");
            Assert.AreEqual(false, bcv.get(2).getObject());
            bcv.set(2, "true");
            Assert.AreEqual(true, bcv.get(2).getObject());
            bcv.set(2, "false");
            Assert.AreEqual(false, bcv.get(2).getObject());
            bcv.add(4);
            Assert.AreEqual(true, bcv.get(3).getObject());
            bcv.addRange(new List<byte> { 5, 6, 7 });
            Assert.AreEqual(7, bcv.rows());
            Assert.AreEqual(true, bcv.get(6).getObject());
            Assert.AreEqual(-1, bcv.hashBucket(0, 1));
            Assert.AreEqual(7, bcv.rows());
            IScalar scalar = (IScalar)conn.run("6");
            try
            {
                bcv.asof(scalar);
            } 
            catch (Exception ex)
            
            {
                string s = ex.Message;
                Assert.AreEqual(s, "BasicBooleanVector.asof not supported.");
            }
            //ExtendedDataInput extendedDataInput = (ExtendedDataInput)conn.run("1");
            //bcv.deserialize(0,1, extendedDataInput);
            //Console.Out.WriteLine(bcv);
            conn.close();
        }
        [TestMethod]
        public void Test_BasicBooleanVector_setnull()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("t = table(1000:0, [`bools], [BOOL]);share t as st;");
            BasicBooleanVector bls = new BasicBooleanVector(10);
            for (int i = 0; i < 10; i++)
            {
                bls.setNull(i);
            }
            List<string> names2 = new List<string>() { "bools" };
            List<IVector> cols2 = new List<IVector>() { bls };
            BasicTable bt2 = new BasicTable(names2, cols2);
            List<IEntity> ags = new List<IEntity>() { bt2 };
            conn.run("tableInsert{st}", ags);
            for (int i = 0; i < 10; i++)
            {
                bls.setNull(i);
            }
            conn.run("stt=select * from st");
            for (int i = 0; i < 10; i++)
            {
                conn.run("assert 1,stt.bools[" + i + "]==NULL");
            }
        }
        [TestMethod]
        public void Test_BasicDateHourVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<int?> args = new List<int?>();
            args.Add(1);
            BasicDateHourVector bcv = new BasicDateHourVector(args);
            Console.Out.WriteLine(bcv.get(0).getObject());
            Assert.IsTrue(bcv.isVector());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 00, 00), bcv.get(0).getObject());
            try
            {
                bcv.set(0, (IScalar)conn.run("6"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a dateTime scalar. ");
            }
            bcv.set(0, (IScalar)conn.run("datehour(2012.06.13 13:30:10)"));
            Assert.AreEqual(new DateTime(2012, 06, 13, 13, 00, 00), bcv.get(0).getObject());
            Console.Out.WriteLine(bcv.get(0).getObject());

            bcv.set(0, "3");
            Assert.AreEqual(new DateTime(1970, 01, 01, 03, 00, 00), bcv.get(0).getObject());
            bcv.set(0, "2016-09-08 18:38:50");
            Assert.AreEqual(new DateTime(2016, 09, 08, 18, 00, 00), bcv.get(0).getObject());

            bcv.add(new DateTime(2016, 09, 10, 13, 00, 00));
            Assert.AreEqual(2, bcv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 10, 13, 00, 00), bcv.get(1).getObject());
            bcv.add("2016-09-08 18:38:50");
            Assert.AreEqual(3, bcv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 08, 18, 00, 00), bcv.get(2).getObject());
            try
            {
                bcv.add(3);

            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a dateTime scalar. ");
            }
            Console.Out.WriteLine(bcv.getList().ToString());

            int[] arr = new int[2];
            arr[0] = 1;
            BasicDateHourVector bcv1 = new BasicDateHourVector(arr);
            Console.Out.WriteLine(bcv1.get(0).getObject());
            Console.Out.WriteLine(bcv1.get(1).getObject());
            Assert.IsTrue(bcv1.isVector());
            Assert.AreEqual(2, bcv1.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 00, 00), bcv1.get(0).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), bcv1.get(1).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 00, 00), bcv1.getDateTime(0));
            int[] indices = new int[2];
            bcv1.getSubVector(indices);
           // Console.Out.WriteLine(bcv1.getSubVector(indices).getObject());

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicDateHour", bcv.getElementClass().ToString());
            Assert.AreEqual("DT_DATEHOUR", bcv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateTimeVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            int[] arr = new int[2];
            arr[0] = 1;
            BasicDateTimeVector bdtv = new BasicDateTimeVector(arr);
            Console.Out.WriteLine(bdtv.get(0).getObject());
            Console.Out.WriteLine(bdtv.get(1).getObject());
            Assert.IsTrue(bdtv.isVector());
            Assert.AreEqual(2, bdtv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 01), bdtv.get(0).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), bdtv.get(1).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 01), bdtv.getDateTime(0));
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00), bdtv.getDateTime(1));
            try
            {
                bdtv.set(0, (IScalar)conn.run("6"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a dateTime scalar. ");
            }
            bdtv.set(0, "3");
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 03), bdtv.get(0).getObject());
            bdtv.set(0, "2016-09-08 18:38:50");
            Assert.AreEqual(new DateTime(2016, 09, 08, 18, 38, 50), bdtv.get(0).getObject());

            bdtv.add(new DateTime(2016, 09, 10, 13, 00, 00));
            Assert.AreEqual(3, bdtv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 10, 13, 00, 00), bdtv.get(2).getObject());
            bdtv.add("2016-09-08 18:38:50");
            Assert.AreEqual(4, bdtv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 08, 18, 38, 50), bdtv.get(3).getObject());
            try
            {
                bdtv.add(3);

            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a dateTime scalar. ");
            }
            Console.Out.WriteLine(bdtv.getList().ToString());
            Assert.AreEqual("TEMPORAL", bdtv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicDateTime", bdtv.getElementClass().ToString());
            Assert.AreEqual("DT_DATETIME", bdtv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<int?> args = new List<int?>();
            args.Add(1);
            BasicDateVector bcv = new BasicDateVector(args);
            Console.Out.WriteLine(bcv.get(0).getObject());
            Assert.IsTrue(bcv.isVector());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 02), bcv.get(0).getObject());
            Assert.AreEqual(new DateTime(1970, 01, 02), bcv.getDate(0));
            try
            {
                bcv.set(0, (IScalar)conn.run("6"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a date scalar. ");
            }
            bcv.set(0, (IScalar)conn.run("2018.03.01"));
            Assert.AreEqual(new DateTime(2018, 03, 01), bcv.get(0).getObject());

            bcv.set(0, "3");
            Assert.AreEqual(new DateTime(1970, 01, 04), bcv.get(0).getObject());
            bcv.set(0, "2016-09-08 18:38:50");
            Assert.AreEqual(new DateTime(2016, 09, 08), bcv.get(0).getObject());

            bcv.add(new DateTime(2016, 09, 10, 13, 00, 00));
            Assert.AreEqual(2, bcv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 10), bcv.get(1).getObject());
            bcv.add("2016-09-08 18:38:50");
            Assert.AreEqual(3, bcv.rows());
            Assert.AreEqual(new DateTime(2016, 09, 08), bcv.get(2).getObject());
            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicDate", bcv.getElementClass().ToString());
            Assert.AreEqual("DT_DATE", bcv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDoubleVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<double?> args = new List<double?>();
            args.Add(1.5);
            BasicDoubleVector bdv = new BasicDoubleVector(args);
            Console.Out.WriteLine(bdv.get(0).getObject());
            Assert.IsTrue(bdv.isVector());
            Assert.AreEqual(1, bdv.rows());
            Assert.AreEqual(1.5, ((BasicDouble)bdv.get(0)).getValue());
            Assert.AreEqual(1.5, bdv.getDouble(0));
            Assert.AreEqual(false, bdv.isNull(0));
            bdv.setNull(0);
            Assert.AreEqual(true, bdv.isNull(0));
            try
            {
                bdv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a double scalar. ");
            }

            bdv.set(0, "3.1");
            Assert.AreEqual(3.1, ((BasicDouble)bdv.get(0)).getValue());
            bdv.set(0, "2016-09-08 18:38:50");
            Assert.AreEqual(true, bdv.isNull(0));

            bdv.add(123);
            Assert.AreEqual(2, bdv.rows());
            Assert.AreEqual(123, ((BasicDouble)bdv.get(1)).getValue());

            bdv.addRange(new List<double> { 1, 2, 3 });
            Assert.AreEqual(5, bdv.rows());
            Assert.AreEqual(1, ((BasicDouble)bdv.get(2)).getValue());
            Assert.AreEqual(2, ((BasicDouble)bdv.get(3)).getValue());
            Assert.AreEqual(3, ((BasicDouble)bdv.get(4)).getValue());

            IScalar scalar = (IScalar)conn.run("2.1");
            Assert.AreEqual(3, bdv.asof(scalar));

            Assert.AreEqual("FLOATING", bdv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicDouble", bdv.getElementClass().ToString());
            Assert.AreEqual("DT_DOUBLE", bdv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicFloatVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<float?> args = new List<float?>();
            args.Add(1.5f);
            BasicFloatVector bfv = new BasicFloatVector(args);
            Console.Out.WriteLine(bfv.get(0).getObject());
            Assert.IsTrue(bfv.isVector());
            Assert.AreEqual(1, bfv.rows());
            Assert.AreEqual(1.5f, ((BasicFloat)bfv.get(0)).getValue());
            Assert.AreEqual(1.5f, bfv.getFloat(0));
            Assert.AreEqual(false, bfv.isNull(0));
            bfv.setNull(0);
            Assert.AreEqual(true, bfv.isNull(0));
            try
            {
                bfv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a float scalar. ");
            }

            bfv.set(0, "3.1");
            Console.Out.WriteLine(bfv.get(0).getObject());
            Assert.AreEqual(3.1f, bfv.getFloat(0));
            bfv.set(0, "2016-09-08 18:38:50");
            Assert.AreEqual(true, bfv.isNull(0));

            bfv.add(123f);
            Assert.AreEqual(2, bfv.rows());
            Assert.AreEqual(123f, ((BasicFloat)bfv.get(1)).getValue());

            bfv.addRange(new List<float> { 1f, 2f, 3f });
            Assert.AreEqual(5, bfv.rows());
            Assert.AreEqual(1f, ((BasicFloat)bfv.get(2)).getValue());
            Assert.AreEqual(2f, ((BasicFloat)bfv.get(3)).getValue());
            Assert.AreEqual(3f, ((BasicFloat)bfv.get(4)).getValue());

            IScalar scalar = (IScalar)conn.run("2.1f");
            Assert.AreEqual(3, bfv.asof(scalar));

            Assert.AreEqual("FLOATING", bfv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicFloat", bfv.getElementClass().ToString());
            Assert.AreEqual("DT_FLOAT", bfv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicIPAddrVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<Long2> args = new List<Long2>();
            args.Add(new Long2(0,1));
            BasicIPAddrVector biv = new BasicIPAddrVector(args);
            Console.Out.WriteLine(biv.get(0).ToString());
            Assert.IsTrue(biv.isVector());
            Assert.AreEqual(1, biv.rows());
            //Assert.AreEqual(1.5f, bfv.getFloat(0));
            Assert.AreEqual(false, biv.isNull(0));
            biv.setNull(0);
            Assert.AreEqual(true, biv.isNull(0));
            try
            {
                biv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a ipaddr scalar. ");
            }

            Assert.AreEqual("BINARY", biv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicIPAddr", biv.getElementClass().ToString());
            Assert.AreEqual("DT_IPADDR", biv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicIntVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<int?> args = new List<int?>();
            args.Add(1);
            BasicIntVector biv = new BasicIntVector(args);
            Assert.IsTrue(biv.isVector());
            Assert.AreEqual(1, biv.rows());
            Assert.AreEqual(1, biv.getInt(0));
            biv.set(0,"1234");
            Assert.AreEqual(1234, biv.getInt(0));
            biv.set(0, "qwsas");
            Assert.AreEqual(true, biv.isNull(0));
            biv.addRange(new List<int> { 0, 2, 12222222 });
            Assert.AreEqual(4, biv.rows());
            Assert.AreEqual(0, ((BasicInt)biv.get(1)).getValue());
            Assert.AreEqual(2, ((BasicInt)biv.get(2)).getValue());
            Assert.AreEqual(12222222, ((BasicInt)biv.get(3)).getValue());
            Assert.AreEqual(-1, biv.hashBucket(0,1));
            Assert.AreEqual(0, biv.hashBucket(1, 1));

            Assert.AreEqual("INTEGRAL", biv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicInt", biv.getElementClass().ToString());
            Assert.AreEqual("DT_INT", biv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicLongVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<long?> args = new List<long?>();
            args.Add(1);
            BasicLongVector blv = new BasicLongVector(args);
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows());
            Assert.AreEqual(1, blv.getLong(0));
            blv.set(0, "1234");
            Assert.AreEqual(1234, blv.getLong(0));
            blv.set(0, "qwsas");
            Assert.AreEqual(true, blv.isNull(0));
            blv.addRange(new List<long> { 0, 2, 12222222 });
            Assert.AreEqual(4, blv.rows());
            Assert.AreEqual(0, ((BasicLong)blv.get(1)).getValue());
            Assert.AreEqual(2, ((BasicLong)blv.get(2)).getValue());
            Assert.AreEqual(12222222, ((BasicLong)blv.get(3)).getValue());

            blv.add(1222);
            Assert.AreEqual(5, blv.rows());
            Assert.AreEqual(1222, ((BasicLong)blv.get(4)).getValue());
            Assert.AreEqual(-1, blv.hashBucket(0, 1));
            Assert.AreEqual(0, blv.hashBucket(1, 1));

            IScalar scalar = (IScalar)conn.run("long(100)");
            Assert.AreEqual(2, blv.asof(scalar));

            Assert.AreEqual("INTEGRAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicLong", blv.getElementClass().ToString());
            Assert.AreEqual("DT_LONG", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicMinuteVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<int?> args = new List<int?>();
            args.Add(1);
            BasicMinuteVector blv = new BasicMinuteVector(args);
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 01, 00).TimeOfDay, blv.getMinute(0));
            blv.set(0, "61");
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 01, 00).TimeOfDay, blv.getMinute(0));
            blv.set(0, "wesdsd");
            //Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, blv.getMinute(0));
            blv.set(0, "14:48m");
            Assert.AreEqual(new DateTime(1970, 01, 01, 14, 48, 00).TimeOfDay, blv.getMinute(0));

            try
            {
                blv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a minute scalar. ");
            }

            blv.add("61");
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 01, 00).TimeOfDay, blv.getMinute(1));
            blv.add("04:48m");
            Assert.AreEqual(new DateTime(1970, 01, 01, 04, 48, 00).TimeOfDay, blv.getMinute(2));
            blv.add("44:48m");
            //Assert.AreEqual(new DateTime(1970, 01, 02, 20, 48, 00).TimeOfDay, blv.getMinute(3));
            //Assert.AreEqual(new DateTime(1970, 01, 02, 20, 48, 00).TimeOfDay, blv.getString());
            blv.add("23:59m");
            Assert.AreEqual(new DateTime(1970, 01, 02, 23, 59, 00).TimeOfDay, blv.getMinute(4));
            blv.add(100);
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 40, 00).TimeOfDay, blv.getMinute(5));

            Assert.AreEqual("TEMPORAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicMinute", blv.getElementClass().ToString());
            Assert.AreEqual("DT_MINUTE", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicMonthVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<int?> args = new List<int?>();
            args.Add(1);
            BasicMonthVector blv = new BasicMonthVector(args);
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows()); 
            Console.Out.WriteLine(blv.getString());
           Assert.AreEqual(new DateTime(0001, 02, 01), blv.getMonth(0));
            
            blv.add("2012.06M");
            Assert.AreEqual(new DateTime(2012, 06, 01), blv.getMonth(1));
            blv.set(0, "11");
            Assert.AreEqual(new DateTime(0001, 12, 01), blv.getMonth(0));
            blv.set(0, "wesdsd");
            Console.Out.WriteLine(blv.getString());
            Assert.AreEqual("[,2012.6ÔÂ]", blv.getString());
            try
            {
                blv.set(0, (IScalar)conn.run("2018"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a month scalar. ");
            }

            blv.add("61");
            Assert.AreEqual(new DateTime(0005, 02, 01), blv.getMonth(2));
            blv.add("2012.06M");
            Assert.AreEqual(new DateTime(2012, 06, 01), blv.getMonth(3));
            blv.add("2012.15M");
            Assert.AreEqual(new DateTime(2013, 03, 01), blv.getMonth(4));
            blv.add("9999.12M");
            Assert.AreEqual(new DateTime(9999, 12, 01), blv.getMonth(5));
            //blv.add(10);
            //Assert.AreEqual(new DateTime(1970, 10, 01), blv.getMonth(5));

            Assert.AreEqual("TEMPORAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicMonth", blv.getElementClass().ToString());
            Assert.AreEqual("DT_MONTH", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicNanoTimeVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<long?> args = new List<long?>();
            args.Add(100000);
            BasicNanoTimeVector blv = new BasicNanoTimeVector(args);
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows()); 
            DateTime dt = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            long tickCount = dt.Ticks;
            Assert.AreEqual(new DateTime(tickCount + 1000L).TimeOfDay, blv.getNanoTime(0));

            blv.set(0, "11000");
            DateTime dt1 = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            long tickCount1 = dt1.Ticks;
            Assert.AreEqual(new DateTime(tickCount1 + 0110L).TimeOfDay, blv.getNanoTime(0));

            blv.set(0, "wesdsd");
            DateTime dt2 = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            long tickCount2 = dt2.Ticks;
            //Assert.AreEqual(new DateTime(tickCount2 + 0000L).TimeOfDay, blv.getNanoTime(0));
            blv.set(0, "13:30:10.008007006");
            DateTime dt3 = new DateTime(1970, 1, 1, 13, 30, 10, 008);
            long tickCount3 = dt3.Ticks;
            Assert.AreEqual(new DateTime(tickCount3 + 0070L).TimeOfDay, blv.getNanoTime(0));
            try
            {
                blv.set(0, (IScalar)conn.run("2018"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a nanoTime scalar. ");
            }

            blv.add("161");
            DateTime dt4 = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            long tickCount4 = dt4.Ticks;
            Assert.AreEqual(new DateTime(tickCount4 + 0001L).TimeOfDay, blv.getNanoTime(1));

            blv.add("13:30:10.008007006");
            DateTime dt5 = new DateTime(1970, 1, 1, 13, 30, 10, 008);
            long tickCount5 = dt5.Ticks;
            Assert.AreEqual(new DateTime(tickCount5 + 0070L).TimeOfDay, blv.getNanoTime(2));
            try {
                blv.add("24:30:10.008007006");
            }
            catch (Exception e) { 
                string s = e.Message;
                Assert.AreEqual("Input string was not in a correct format.", s);
            }

            //blv.add("new DateTime(1970, 1, 1)");
            //DateTime dt7 = new DateTime(1970, 1, 2, 00, 30, 10, 008);
            //long tickCount7 = dt7.Ticks;
            //Assert.AreEqual(new DateTime(tickCount7 + 0070L).TimeOfDay, blv.getNanoTime(2));

            Assert.AreEqual("TEMPORAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicNanoTime", blv.getElementClass().ToString());
            Assert.AreEqual("DT_NANOTIME", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicNanoTimestampVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<long?> args = new List<long?>();
            args.Add(10000000);
            BasicNanoTimestampVector blv = new BasicNanoTimestampVector(args);
            Console.Out.WriteLine(blv.get(0).getString());
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows());
            DateTime dt = new DateTime(1970, 1, 1, 00, 00, 00, 010);
            Assert.AreEqual(new DateTime(dt.Ticks + 0000L), blv.getNanoTimestamp(0));

            blv.set(0, "11000");
            DateTime dt1 = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            Assert.AreEqual(new DateTime(dt1.Ticks + 0110L), blv.getNanoTimestamp(0));

            blv.set(0, "wesdsd");
            DateTime dt2 = new DateTime(1970, 1, 1, 00, 00, 00, 000);
            //Assert.AreEqual(new DateTime(dt2.Ticks + 0000L), blv.getNanoTimestamp(0));
            blv.set(0, "13:30:10.008007006");
            string year = DateTime.Now.Year.ToString();
            string month = DateTime.Now.Month.ToString();
            string day = DateTime.Now.Day.ToString();
            DateTime dt3 = new DateTime(int.Parse(year), int.Parse(month), int.Parse(day), 13, 30, 10, 008);
            Assert.AreEqual(new DateTime(dt3.Ticks + 0070L), blv.getNanoTimestamp(0));
            try
            {
                blv.set(0, (IScalar)conn.run("2018"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a nanoTimeStamp scalar. ");
            }

            blv.add("13:30:10.008007006");
            Console.Out.WriteLine(blv.get(0).getString());
            string year1 = DateTime.Now.Year.ToString();
            string month1 = DateTime.Now.Month.ToString();
            string day1 = DateTime.Now.Day.ToString();
            DateTime dt4 = new DateTime(int.Parse(year1), int.Parse(month1), int.Parse(day1), 13, 30, 10, 008);
            Assert.AreEqual(new DateTime(dt4.Ticks + 0070L), blv.getNanoTimestamp(1));

            Assert.AreEqual("TEMPORAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicNanoTimestamp", blv.getElementClass().ToString());
            Assert.AreEqual("DT_NANOTIMESTAMP", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicSecondVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<int?> args = new List<int?>();
            args.Add(1);
            BasicSecondVector blv = new BasicSecondVector(args);
            Assert.IsTrue(blv.isVector());
            Assert.AreEqual(1, blv.rows());
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 01).TimeOfDay, blv.getSecond(0));
            blv.set(0, "61");
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 01, 01).TimeOfDay, blv.getSecond(0));
            blv.set(0, "wesdsd");
            //Assert.AreEqual(new DateTime(1970, 01, 01, 00, 00, 00).TimeOfDay, blv.getSecond(0));
            blv.set(0, "13:30:10");
            Assert.AreEqual(new DateTime(1970, 01, 01, 13, 30, 10).TimeOfDay, blv.getSecond(0));

            try
            {
                blv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a second scalar. ");
            }

            blv.add("61");
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 01, 01).TimeOfDay, blv.getSecond(1));
            blv.add("13:30:10");
            Assert.AreEqual(new DateTime(1970, 01, 01, 13, 30, 10).TimeOfDay, blv.getSecond(2));
           

            blv.add("23:59:59");
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay, blv.getSecond(3));
            blv.add(100);
            Assert.AreEqual(new DateTime(1970, 01, 01, 00, 01, 40).TimeOfDay, blv.getSecond(4));
            try
            {
                blv.add("24:30:10");
            }
            catch (Exception e) { 
                string s = e.Message;
                Assert.AreEqual("Input string was not in a correct format.", s);
            }
            Assert.AreEqual("TEMPORAL", blv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicSecond", blv.getElementClass().ToString());
            Assert.AreEqual("DT_SECOND", blv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicShortVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<short?> args = new List<short?>();
            args.Add(1);
            BasicShortVector biv = new BasicShortVector(args);
            Assert.IsTrue(biv.isVector());
            Assert.AreEqual(1, biv.rows());
            Assert.AreEqual(1, biv.getShort(0));
            biv.set(0, "1234");
            Assert.AreEqual(1234, biv.getShort(0));
            biv.set(0, "qwsas");
            Assert.AreEqual(true, biv.isNull(0));
            try
            {
                biv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a short scalar. ");
            }

            biv.add(13);
            Assert.AreEqual(13, biv.getShort(1));

            biv.addRange(new List<short> { 0, 2, 33 });
            Assert.AreEqual(5, biv.rows());
            Assert.AreEqual(0, biv.getShort(2));
            Assert.AreEqual(2, biv.getShort(3));
            Assert.AreEqual(33, biv.getShort(4));
            Assert.AreEqual(-1, biv.hashBucket(0, 1));
            Assert.AreEqual(0, biv.hashBucket(1, 1));

            IScalar scalar = (IScalar)conn.run("3h");
            Assert.AreEqual(3, biv.asof(scalar));

            Assert.AreEqual("INTEGRAL", biv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicShort", biv.getElementClass().ToString());
            Assert.AreEqual("DT_SHORT", biv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicStringVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            List<string> args = new List<string>();
            args.Add("1");
            BasicStringVector biv = new BasicStringVector(args,true);
            BasicStringVector biv1 = new BasicStringVector(args, false);
            Assert.IsTrue(biv.isVector());
            Assert.AreEqual(1, biv.rows());
            Assert.AreEqual("1", biv.getString(0));
            try
            {
                biv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a blob scalar. ");
            }
            biv.set(0, (IScalar)conn.run("blob(\"hello\")"));
            Assert.AreEqual("hello", biv.getString(0));

            biv.setString(0, "1234");
            Assert.AreEqual("1234", biv.getString(0));
            biv.setNull(0);
            Assert.AreEqual(true, biv.isNull(0));
            biv.set(0, "1234");
            Assert.AreEqual("1234", biv.getString(0));

            biv.add((IScalar)conn.run("blob(\"hello\")"));
            //Assert.AreEqual("hello", biv.getString(1));

            biv.addRange(new List<string> { "1qaz", "2wsx", "1!@#$%^&*()_+QWERTYUIOP{}|\\\":LKJHGFDSAZXCVBNM<>?/.,;'[]\\=-0987654321`~" });
            Assert.AreEqual(5, biv.rows());
            Assert.AreEqual("1qaz", biv.getString(2));
            Assert.AreEqual("2wsx", biv.getString(3));
            Assert.AreEqual("1!@#$%^&*()_+QWERTYUIOP{}|\\\":LKJHGFDSAZXCVBNM<>?/.,;'[]\\=-0987654321`~", biv.getString(4));

            Console.Out.WriteLine(biv.getSubVector(new int[] { 1, 2, 3 }).ToString());

            int i = 0;
            Stream outStream = new MemoryStream();
            ExtendedDataOutput out1 = new BigEndianDataOutputStream(outStream);
            biv.serialize(0,1, out1);
            biv1.serialize(0, 1, out1);
            biv.serialize(new byte[] { 1, 2, 3 }, 3, 0, 3, out i);

            Stream inStream = new MemoryStream();
            ExtendedDataInput in1 = new BigEndianDataInputStream(inStream);
            Exception ex1 = null;
            try
            {
                biv.deserialize(0, 1, in1);

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.AreEqual("The method or operation is not implemented.", ex1.Message);

            IScalar scalar = (IScalar)conn.run("blob(\"1\")");
            Console.Out.WriteLine(((BasicString)scalar).getBytes());
            Assert.AreEqual(-1, biv.asof(scalar));
            try
            {
                biv.asof((IScalar)conn.run("2018.03.01"));

            }
            catch (Exception ex)
            {
                string s = ex.Message;
                Assert.AreEqual(s, "value must be a blob scalar. ");

            }
            Assert.AreEqual(1, biv.getUnitLength());
            Assert.AreEqual("LITERAL", biv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicString", biv.getElementClass().ToString());
            Assert.AreEqual("DT_BLOB", biv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicSymbolVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            string[] v1 = new string[100];
            for (int i = 0; i < 100; i++)
            {
                v1[i] = "A" + i.ToString();
            }
            BasicSymbolVector biv = new BasicSymbolVector(v1);
            Assert.IsTrue(biv.isVector());
            Assert.AreEqual(100, biv.rows());
            Assert.AreEqual("A0", biv.getString(0));
            BasicSymbolVector biv1 = new BasicSymbolVector(@base, 1000);
            BasicSymbolVector biv2 = new BasicSymbolVector(@base, new int[10],true);
            biv.set(0, "1!@#$%^&*()_+QWERTYUIOP{}|\\\":LKJHGFDSAZXCVBNM<>?/.,;'[]\\=-0987654321`~");
            Assert.AreEqual("1!@#$%^&*()_+QWERTYUIOP{}|\\\":LKJHGFDSAZXCVBNM<>?/.,;'[]\\=-0987654321`~", biv.getString(0));
            Console.Out.WriteLine(biv.getEntity(0));
            Console.Out.WriteLine(biv.hashBucket(0,1));
            IVector v2 = (BasicSymbolVector)conn.run("symbol(take(`aaa `bbb `ccc, 300))");
            Console.Out.WriteLine(biv.combine(v2).getString());
            try
            {
                biv.set(0, (IScalar)conn.run("2018.03.01"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The value must be a string scalar. ");
            }
            biv.set(0, (IScalar)conn.run("`hello"));
            Assert.AreEqual("hello", biv.getString(0));

            biv.set(0, "A1234");
            Assert.AreEqual("A1234", biv.getString(0));
            biv.setNull(0);
            Assert.AreEqual(true, biv.isNull(0));

            try
            {
                biv.add((IScalar)conn.run("`A1234"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The method or operation is not implemented.");
            }

            try
            {
                biv.addRange((IScalar)conn.run("`A1234"));
            }
            catch (Exception ex)

            {
                string s = ex.Message;
                Assert.AreEqual(s, "The method or operation is not implemented.");
            }
            
            biv.asof((IScalar)conn.run("6"));
            
            try
            {
                biv.deserialize(0, 1, @in);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            biv.serialize(0, 0, @out);//--------
            Assert.AreEqual(4, biv.getUnitLength());           
            try
            {
                //bcv.serialize(0,1,1, @out., 1,@out);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }


            Console.Out.WriteLine(biv.getSubVector(new int[] { 1, 2, 3 }).ToString());

            Assert.AreEqual("LITERAL", biv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicString", biv.getElementClass().ToString());
            Assert.AreEqual("DT_SYMBOL", biv.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicTimeVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            IList<int?> args = new List<int?>();
            args.Add(1);
            BasicTimeVector bcv = new BasicTimeVector(args);
            Console.Out.WriteLine(bcv.get(0).getObject());
            Assert.IsTrue(bcv.isVector());
            Assert.AreEqual(1, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 001).TimeOfDay, bcv.getTime(0));

            bcv.set(0, "123");
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 123).TimeOfDay, bcv.get(0).getObject());
            bcv.set(0, "10:58:02.002");
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, bcv.get(0).getObject());

            bcv.add(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay);
            Assert.AreEqual(2, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, bcv.get(1).getObject());
            bcv.add(new DateTime(1970, 1, 1, 23, 59, 59, 999).TimeOfDay);
            Assert.AreEqual(3, bcv.rows());
            Assert.AreEqual(new DateTime(1970, 1, 1, 23, 59, 59, 999).TimeOfDay, bcv.get(2).getObject());
            bcv.add(3);
            Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 003).TimeOfDay, bcv.get(3).getObject());

            Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.BasicTime", bcv.getElementClass().ToString());
            Assert.AreEqual("DT_TIME", bcv.getDataType().ToString());
            conn.close();
        }
        [TestMethod]
        public void Test_BasicArrayVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            int[] v1 = new int[100];
            string[] v2 = new string[100];
            for (int i = 0; i < 100; i++)
            {
                v1[i] = i % 10;
                v2[i] = "A" + i.ToString();
            }
            BasicIntVector col1 = new BasicIntVector(v1);
            BasicSymbolVector col2 = new BasicSymbolVector(v2);

            List<IVector> cols = new List<IVector>() { col1};
            BasicArrayVector bcv = new BasicArrayVector(cols);
            try
            {
                bcv.setNull(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.isNull(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.add(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.addRange(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.set(0, (IScalar)conn.run("6"));

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.getElementClass();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.set(0,"1");

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.get(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.getUnitLength();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }

            //Console.Out.WriteLine(bcv.get(0).getObject());
            //Assert.IsTrue(bcv.isVector());
            //Assert.AreEqual(1, bcv.rows());
            //Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 001).TimeOfDay, bcv.getTime(0));

            ////bcv.set(0, "123");
            ////Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 123).TimeOfDay, bcv.get(0).getObject());
            //bcv.set(0, "10:58:02.002");
            //Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, bcv.get(0).getObject());

            //bcv.add(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay);
            //Assert.AreEqual(2, bcv.rows());
            //Assert.AreEqual(new DateTime(1970, 1, 1, 10, 58, 02, 002).TimeOfDay, bcv.get(1).getObject());
            //bcv.add(new DateTime(1970, 1, 1, 23, 59, 59, 999).TimeOfDay);
            //Assert.AreEqual(3, bcv.rows());
            //Assert.AreEqual(new DateTime(1970, 1, 1, 23, 59, 59, 999).TimeOfDay, bcv.get(2).getObject());
            //bcv.add(3);
            //Assert.AreEqual(new DateTime(1970, 1, 1, 00, 00, 00, 003).TimeOfDay, bcv.get(3).getObject());

            //Assert.AreEqual("TEMPORAL", bcv.getDataCategory().ToString());
            //Assert.AreEqual("dolphindb.data.BasicTime", bcv.getElementClass().ToString());
            //Assert.AreEqual("DT_TIME", bcv.getDataType().ToString());
            conn.close();
        }
        [TestMethod]
        public void Test_BasicAnyVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            BasicAnyVector bcv = (BasicAnyVector)conn.run("[1,`hello]");
            Console.Out.WriteLine(bcv.get(0).getObject());
            Assert.IsTrue(bcv.isVector());
            Assert.AreEqual(2, bcv.rows()); 
            bcv.set(0, (IScalar)conn.run("6"));
            Assert.AreEqual(false, bcv.isNull(0));
            bcv.setNull(0);
            Assert.AreEqual(true, bcv.isNull(0));
            bcv.set(0,"string");
            Assert.AreEqual("string", bcv.get(0).getObject());
            Console.Out.WriteLine(bcv.getList());//-------------
            int[] indices = new int[10];
            bcv.getSubVector(indices);
            Assert.AreEqual("(string,hello)", bcv.getString());
            try
            {
                bcv.add(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.addRange(0);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.asof((IScalar)conn.run("6"));

            }
            catch (Exception ex)
            {
                Assert.AreEqual("BasicAnyVector.asof not supported.", ex.Message);
            }
            try
            {
                bcv.deserialize(0,1,@in);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.serialize(0,1,@out);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.getUnitLength();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                //bcv.serialize(0,1,1, @out., 1,@out);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                bcv.append((IScalar)conn.run("6"));

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }
            try
            {
                IVector v = (BasicBooleanVector)conn.run("true false");
                bcv.append(v);

            }
            catch (Exception ex)
            {
                Assert.AreEqual("The method or operation is not implemented.", ex.Message);
            }

            Assert.AreEqual("MIXED", bcv.getDataCategory().ToString());
            Assert.AreEqual("dolphindb.data.IEntity", bcv.getElementClass().ToString());
            Assert.AreEqual("DT_ANY", bcv.getDataType().ToString());
            conn.close();
        }
        //[TestMethod]
        //public void Test_BasicDecimal64Vector1()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(10, 5);
        //    Console.Out.WriteLine(bdv.get(0).getObject());
        //    List<double> args = new List<double>();
        //    args.Add(1.0053);
        //    BasicDecimal64Vector bdv3 = new BasicDecimal64Vector(args, 3);
        //    Console.Out.WriteLine(bdv3.get(0).getObject());
        //    Console.Out.WriteLine(bdv3.get(0).getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(10,4);
        //    Assert.AreEqual("0.0000", bdv.get(0).getString());
        //    List<double> args = new List<double>();
        //    args.Add(1.0053);
        //    BasicDecimal64Vector bdv1 = new BasicDecimal64Vector(args,0);
        //    Assert.AreEqual(new BasicDecimal64(1, 0), bdv1.get(0).getObject());
        //    Assert.AreEqual("1", bdv1.get(0).getString());

        //    BasicDecimal64Vector bdv2 = new BasicDecimal64Vector(args, 1);
        //    Assert.AreEqual(new BasicDecimal64(1.0053, 1), bdv2.get(0).getObject());
        //    BasicDecimal64Vector bdv3 = new BasicDecimal64Vector(args, 3);
        //    Assert.AreEqual("1.005", bdv3.get(0).getObject().ToString());
        //    BasicDecimal64Vector bdv4 = new BasicDecimal64Vector(args, 5);
        //    Assert.AreEqual(new BasicDecimal64(1.0053, 5), bdv4.get(0).getObject());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_add()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    bdv.add(123);
        //    Assert.AreEqual("123.00000", bdv.get(0).getString());
        //    bdv.add(123.123456);
        //    Assert.AreEqual("123.12345", bdv.get(1).getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_addRange()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    List<double> args = new List<double>();
        //    args.Add(1.0053);
        //    args.Add(123);
        //    args.Add(123.123456);
        //    bdv.addRange(args);
        //    Assert.AreEqual("[1.00530,123.00000,123.12345]", bdv.getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_append()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal64(234.3333,2)");
        //    bdv.append(scalar);
        //    Assert.AreEqual("[234.33000]", bdv.getString());

        //    IVector v = (IVector)conn.run("decimal64(symbol(`3000`234),2)");
        //    bdv.append(v);
        //    Assert.AreEqual("[234.33000,3000.00000,234.00000]", bdv.getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_getDataCategory()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual("DENARY", bdv.getDataCategory().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_setScale()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal64(234.3333,2)");
        //    bdv.append(scalar);
        //    Assert.AreEqual("[234.33000]", bdv.getString());
        //    Assert.AreEqual(5, bdv.get(0).getScale());

        //    bdv.setScale(4);
        //    Assert.AreEqual(4, bdv.get(0).getScale());
        //    Assert.AreEqual("[234.3300]", bdv.getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_deserialize()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    try
        //    { bdv.deserialize(0, 0,@in);

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("The method or operation is not implemented.", e.Message);
        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_getDataType()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual("DT_DECIMAL64", bdv.getDataType().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_getElementClass()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual("dolphindb.data.BasicDecimal64", bdv.getElementClass().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_getUnitLength()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual(8, bdv.getUnitLength());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_set()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(2, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal64(234.3333,2)");
        //    try
        //    {
        //        bdv.set(0, scalar);

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("Value's scale is not the same as the vector's!", e.Message);
        //    }
        //    IScalar scalar1 = (IScalar)conn.run("decimal64(234.3333,5)");
        //    bdv.set(0, scalar1);
        //    Assert.AreEqual("234.33330", bdv.get(0).getObject().ToString());
        //    IScalar scalar2 = (IScalar)conn.run("decimal64(234.3333,5)");
        //    scalar2.setNull();
        //    Assert.AreEqual(true, scalar2.isNull());
        //    bdv.set(0, scalar2);
        //    Assert.AreEqual("", bdv.get(0).getObject().ToString());
        //    try
        //    {
        //        bdv.set(0, "string");

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("The method or operation is not implemented.", e.Message);
        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_setNull()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(2, 5);
        //    IScalar scalar1 = (IScalar)conn.run("decimal64(234.3333,5)");
        //    bdv.set(0, scalar1);
        //    bdv.setNull(0);
        //    //Assert.AreEqual(true, bdv.isNull(0));//APICS-190
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_serialize()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    try
        //    {
        //        bdv.serialize(0, 0, @out);

        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e.Message);
        //        Assert.AreEqual("Decimal64 does not support arrayVector", e.Message);

        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_serialize2()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual(5, bdv.getExtraParamForType());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal64Vector_getExtraParamForType()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal64Vector bdv = new BasicDecimal64Vector(0, 5);
        //    Assert.AreEqual(5, bdv.getExtraParamForType());
        //    conn.close();
        //}

        //[TestMethod]
        //public void Test_BasicDecimal32Vector()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(10, 5);
        //    Assert.AreEqual("0.00000", bdv.get(0).getObject().ToString());
        //    List<double> args = new List<double>();
        //    args.Add(1.0053);
        //    BasicDecimal32Vector bdv1 = new BasicDecimal32Vector(args, 0);
        //    Assert.AreEqual("1", bdv1.get(0).getObject().ToString());
        //    BasicDecimal32Vector bdv2 = new BasicDecimal32Vector(args, 1);
        //    Assert.AreEqual("1.0", bdv2.get(0).getObject().ToString());
        //    BasicDecimal32Vector bdv3 = new BasicDecimal32Vector(args, 3);
        //    Assert.AreEqual("1.005", bdv3.get(0).getString());
        //    BasicDecimal32Vector bdv4 = new BasicDecimal32Vector(args, 5);
        //    Assert.AreEqual("1.00530", bdv4.get(0).getObject().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_add()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    bdv.add(123);
        //    //Assert.AreEqual((double)123, bdv.get(0).getObject());
        //    bdv.add(123.123456);
        //    //Assert.AreEqual((double)123.12345, bdv.get(0).getObject());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_addRange()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    List<double> args = new List<double>();
        //    args.Add(1.0053);
        //    args.Add(123);
        //    args.Add(123.123456);
        //    bdv.addRange(args);
        //    Assert.AreEqual("[1.00530,123.00000,123.12345]", bdv.getString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_append()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal32(234.3333,2)");
        //    bdv.append(scalar);
        //    Assert.AreEqual("[234.33000]", bdv.getString());

        //    IVector v = (IVector)conn.run("decimal32(symbol(`3000`234),2)");
        //    bdv.append(v);
        //    //Assert.AreEqual("[234.33000,3000.00000,234.00000]", bdv.getString());//APICS-188
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_getDataCategory()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    Assert.AreEqual("DENARY", bdv.getDataCategory().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_setScale()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal32(234.3333,2)");
        //    bdv.append(scalar);
        //    Assert.AreEqual("[234.33000]", bdv.getString());
        //    Assert.AreEqual(5, bdv.get(0).getScale());

        //    bdv.setScale(4);
        //    Assert.AreEqual(4, bdv.get(0).getScale());
        //    //Assert.AreEqual("[234.3300]", bdv.getString());//APICS-189
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_deserialize()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    try
        //    {
        //        bdv.deserialize(0, 0, @in);

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("The method or operation is not implemented.", e.Message);
        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_getDataType()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    Assert.AreEqual("DT_DECIMAL32", bdv.getDataType().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_getElementClass()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    Assert.AreEqual("dolphindb.data.BasicDecimal32", bdv.getElementClass().ToString());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_getUnitLength()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    Assert.AreEqual(4, bdv.getUnitLength());
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_set()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(2, 5);
        //    IScalar scalar = (IScalar)conn.run("decimal64(234.3333,2)");
        //    try
        //    {
        //        bdv.set(0, scalar);

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("Value's scale is not the same as the vector's!", e.Message);
        //    }
        //    IScalar scalar1 = (IScalar)conn.run("decimal32(234.3333,5)");
        //    bdv.set(0, scalar1);
        //    Assert.AreEqual("234.33330", bdv.get(0).getObject().ToString());
        //    IScalar scalar2 = (IScalar)conn.run("decimal32(234.3333,5)");
        //    scalar2.setNull();
        //    bdv.set(0, scalar2);
        //    // Assert.AreEqual(-2147483648.00000, bdv.get(0).getObject());//APICS-190
        //    try
        //    {
        //        bdv.set(0, "string");

        //    }
        //    catch (Exception e)
        //    {
        //        Assert.AreEqual("The method or operation is not implemented.", e.Message);
        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_setNull()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(2, 5);
        //    IScalar scalar1 = (IScalar)conn.run("decimal32(234.3333,5)");
        //    bdv.set(0, scalar1);
        //    bdv.setNull(0);
        //    //Assert.AreEqual(true, bdv.isNull(0));//APICS-190
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_serialize()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    try
        //    {
        //        bdv.serialize(0, 0, @out);

        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e.Message);
        //        //Assert.AreEqual("The method or operation is not implemented.", e.Message);//APICS-191

        //    }
        //    conn.close();
        //}
        //[TestMethod]
        //public void Test_BasicDecimal32Vector_getExtraParamForType()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT);
        //    BasicDecimal32Vector bdv = new BasicDecimal32Vector(0, 5);
        //    Assert.AreEqual(5, bdv.getExtraParamForType());
        //    conn.close();
        //}


        [TestMethod]
        public void Test_BasicMonthVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicMonthVector bmv1 = (BasicMonthVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_MONTH);
            BasicMonthVector bmv2 = new BasicMonthVector(new int[] { 1, 2, 3, 4, 5 });
            object list = bmv2.getList();
            Console.WriteLine(list.ToString());
            BasicMonthVector bmv3 = new BasicMonthVector(1);
            bmv3.set(0, "2018.03M");
            Assert.AreEqual("[2018.3ÔÂ]", bmv3.getString());
            bmv3.add("2018");
            bmv3.set(0, "-1998.-11M");
            Assert.AreEqual("", bmv3.getEntity(0).getString());
            try
            {
                bmv3.add("-1998.-11M");
            }
            catch(Exception e)
            {

            }
            Assert.AreEqual(2, bmv3.rows());
        }

        [TestMethod]
        public void Test_BasicUuidVector_Remain()
        {
            List<Long2> values = new List<Long2>();
            values.Add(new Long2(1, 3));
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicUuidVector buv1 = (BasicUuidVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_UUID);
            BasicUuidVector buv2 = new BasicUuidVector(values);
            Console.WriteLine(buv2.getString());
            try
            {
                buv1.set(0, new BasicInt(1));
            }
            catch(Exception e)
            {
                Assert.IsNotNull(e.Message);
            }
            Console.WriteLine(buv2.getElementClass());
        }

        [TestMethod]
        public void Test_BasicTimestampVector_Remain()
        {
            List<long?> longs = new List<long?>();
            longs.Add(3);
            BasicTimestampVector btv1 = new BasicTimestampVector(longs);
            Console.WriteLine(btv1.getString());
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicTimestampVector btv2 = (BasicTimestampVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_TIMESTAMP);
            try
            {
                btv2.set(0, new BasicInt(1));
            }catch(Exception e)
            {
                Assert.IsNotNull(e.Message);
            }
            btv1.setNull(0);
            DateTime dt1 = btv1.getTimestamp(0);
            Console.WriteLine(dt1);
            btv1.set(0, new BasicTimestamp(3));
            DateTime dt2 = btv1.getTimestamp(0);
            Console.WriteLine(dt2);
            Console.WriteLine(btv1.getElementClass());
            Console.WriteLine(btv1.getList());
            btv1.set(0, "4");
            Console.WriteLine(btv1.getString());
            btv1.set(0, "01/04/1970 00:00:00");
            Console.WriteLine(btv1.getString());
            btv1.set(0, "ugyau");
            Console.WriteLine(btv1.getString());
            DateTime dt3 = new DateTime(1970, 01, 01, 01, 00, 00);
            btv1.add(dt3);
            Console.WriteLine(btv1.getString());
            btv1.add("gsghabgja");
            try
            {
                btv1.add(new Long2(1,2));
            }catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
            btv1.add("01/05/1970 00:00:00");
            Console.WriteLine(btv1.getString());
        }

        [TestMethod]
        public void Test_BasicStringVector_Remain()
        {
            BasicStringVector bsv1 = new BasicStringVector(new string[] { "jdgjhgajk", "uhshga" });
            try
            {
                bsv1.set(0, new BasicInt(1));
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
            bsv1.setNull(0);
            Assert.AreEqual("", bsv1.getString(0));
            Assert.AreEqual(true, bsv1.isNull(0));

            BasicStringVector bsv2 = new BasicStringVector(new string[] { null });
            Assert.AreEqual(true, bsv2.isNull(0));
            List<string> data3 = new List<string>() { "jdgjhgajk", "uhshga" };
            BasicStringVector bsv3 = new BasicStringVector(data3, true);

            List<string> values = (List<string>)bsv1.getList();
            List<byte[]> blobvalues = (List<byte[]>)bsv3.getList();
            for (int i = 0; i < values.Count; i++)
            {
                Console.WriteLine(values[i]);
                Console.WriteLine(blobvalues[i]);
            }

            bsv1.set(0, new BasicString("shgiuhsgjs"));
            Assert.AreEqual("shgiuhsgjs", bsv1.getString(0));
            List<string> v = new List<string>() { "abc", "cba" };
            bsv1.addRange(v);
            Console.WriteLine(bsv1.getString());
            bsv1.asof(new BasicString("hsdguyagh"));
            byte[][] data = new byte[][] { new byte[] { 1, 3, 4, 5 }, new byte[] { 9, 7, 8, 6 } };
            bsv3.append(new BasicStringVector(data));
            Console.WriteLine(bsv3.getString());
            byte[] resultes = bsv3.getBytes(2);
            Console.WriteLine(resultes);
            List<byte[]> res = bsv3.getDataByteArray();
            for (int i = 0; i < values.Count; i++)
            {
                Console.WriteLine(res[i]);
            }

            Console.WriteLine(bsv3.asof(new BasicString(new byte[] { 1, 7, 4 }, true)));
            Console.WriteLine(bsv3.asof(new BasicString(new byte[] { 1, 3, 4, 5 }, true)));
            Console.WriteLine(bsv3.asof(new BasicString(new byte[] { 7, 2, 4, 5, 8 }, true)));
            BasicStringVector bsv4 = new BasicStringVector(new string[] { "ABC", "DEF", "GHI" });
            Console.WriteLine(bsv4.asof(new BasicString("GHS")));
            Console.WriteLine(bsv4.asof(new BasicString("ASFDAS")));
            Console.WriteLine(bsv4.asof(new BasicString("ggad")));

            try
            {
                bsv4.getBytes(0);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }

            try
            {
                bsv4.getExtraParamForType();
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
            }
        }

        [TestMethod]
        public void Test_BasicDateVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicDateVector bdv1 = (BasicDateVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_DATE);
            BasicDateVector bdv2 = new BasicDateVector(1);
            bdv2.setNull(0);
            Assert.AreEqual(DateTime.MinValue, bdv2.getDate(0));
        }


        [TestMethod]
        public void Test_BasicDateTimeVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicDateTimeVector bdv1 = (BasicDateTimeVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_DATETIME);
            BasicDateTimeVector bdv2 = new BasicDateTimeVector(1);
            bdv2.setNull(0);
            Assert.AreEqual(DateTime.MinValue, bdv2.getDateTime(0));
        }

        [TestMethod]
        public void Test_BasicDateHourVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicDateHourVector bdv1 = (BasicDateHourVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_DATEHOUR);
            BasicDateHourVector bdv2 = new BasicDateHourVector(1);
            bdv2.setNull(0);
            Assert.AreEqual(DateTime.MinValue, bdv2.getDateTime(0));
        }

        [TestMethod]
        public void Test_BasicTimeVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicTimeVector btv1 = (BasicTimeVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_TIME);
            BasicTimeVector btv2 = new BasicTimeVector(1);
            btv2.setNull(0);
            Assert.AreEqual(TimeSpan.MinValue, btv2.getTime(0));
            btv2.set(0, "10:58:02.abc.244");
            btv2.set(0, "adf.002");
            Assert.IsTrue(btv2.isNull(0));
            btv2.add(DateTime.Now);
            Console.WriteLine(btv2.getEntity(1).getString());
            btv2.add("10:58:02.492");
            Assert.AreEqual("10:58:02.4920000", btv2.getEntity(2).getString());
            btv2.add("ghahgka");
            Assert.AreEqual(btv2.rows(), 3);
        }

        [TestMethod]
        public void Test_BasicNanoTimestampVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicNanoTimestampVector bntv1 = (BasicNanoTimestampVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_NANOTIMESTAMP);
            BasicNanoTimestampVector bntv2 = new BasicNanoTimestampVector(1);
            bntv2.setNull(0);
            Assert.AreEqual(DateTime.MinValue, bntv2.getNanoTimestamp(0));
            BasicNanoTimestampVector bntv3 = new BasicNanoTimestampVector(new long[] { 4, 7, 16 });
            List<long> lists = (List<long>)bntv3.getList();
            for(int i = 0; i < lists.Count; i++)
            {
                Console.WriteLine(lists[i]);
            }
            bntv2.set(0, "gajakgj.13254");
            bntv2.set(0, "kdlhd.jhshs");
            Assert.AreEqual(DateTime.MinValue, bntv2.getNanoTimestamp(0));
            bntv3.add(DateTime.Now);
            Console.WriteLine(bntv3.getEntity(3).getString());
            bntv3.add("45467");
            bntv3.add(".");
            bntv3.add("gajakgj.13254");
            bntv3.add("kdlhd.jhshs");
            Assert.AreEqual(4, bntv3.rows());
        }

        [TestMethod]
        public void Test_BasicAnyVector_Remain()
        {
            IScalar[] entities = {
                null,
                new BasicInt(1),
                new BasicInt(2),
                new BasicInt(3),
                new BasicInt(4),
                new BasicInt(5),
                new BasicInt(6),
                new BasicInt(7),
                new BasicInt(8),
                new BasicInt(9)
            };
            BasicAnyVector ba1 = new BasicAnyVector(11);
            for (int i = 0; i < entities.Length-1; i++)
            {
                ba1.set(i, entities[i]);
            }
            BasicInt bi = new BasicInt(1);
            bi.setNull();
            ba1.set(9, bi);
            ba1.set(10, bi);
            BasicAnyVector ba2 = (BasicAnyVector)ba1.getSubVector(new int[] { 1, 3 });
            Assert.IsTrue(ba1.isNull(0));
            Assert.IsTrue(ba1.isNull(9));
            Assert.IsFalse(ba1.isNull(3));
            ba1.set(0, bi);
            Console.WriteLine(ba1.getString());
            BasicAnyVector ba3 = new BasicAnyVector(10);
            for (int i = 0; i < entities.Length - 1; i++)
            {
                ba3.set(i, entities[i]);
            }
            ba3.set(0, bi);
            ba3.set(9, bi);
            Console.WriteLine(ba3.getString());
        }

        [TestMethod]
        public void Test_BasicMinuteVector_Remain()
        {
            BasicEntityFactory bef = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicMinuteVector bmv1 = (BasicMinuteVector)bef.createPairWithDefaultValue(DATA_TYPE.DT_MINUTE);
            BasicMinuteVector bmv2 = new BasicMinuteVector(1);
            bmv2.setNull(0);
            Assert.AreEqual(TimeSpan.MinValue, bmv2.getMinute(0));
            List<int> ints = (List<int>)bmv2.getList();
            bmv2.set(0, "-20.-14m");
            Assert.AreEqual(TimeSpan.MinValue, bmv2.getMinute(0));
            try
            {
                bmv2.add("-20.-14m");
            }
            catch(Exception e)
            {

            }
            Assert.AreEqual(1, bmv2.rows());
        }


        [TestMethod]
        public void Test_BasicByteVector_Remain()
        {
            BasicByteVector bbv1 = new BasicByteVector(new byte[]{1,3,4,5,6,74,9,0 });
            Assert.AreEqual(4, bbv1.getByte(2));
            bbv1.setNull(0);
            Assert.AreEqual(128, bbv1.getByte(0));
            try
            {
                bbv1.asof(new BasicString("ghagka"));
            }
            catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
            try
            {
                bbv1.deserialize(8, 10, @in);
                bbv1.deserialize(0, 6, @in);
                bbv1.deserialize(0, 0, @in);
            }
            catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
            BasicByte bb = (BasicByte)bbv1.getEntity(0);
            Console.WriteLine(bb.getString());
            try
            {
                bbv1.getExtraParamForType();
            }catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
            BasicByteVector bbv2 = new BasicByteVector(10);
            bbv2.set(0, "1");
            Console.WriteLine(bbv2.getString());
            int a = bbv2.hashBucket(0, 4);
            Assert.AreEqual(1, a);
            bbv2.set(0, "0");
            Assert.AreEqual(-1, bbv2.hashBucket(1, 4));
        }


        [TestMethod]
        public void Test_BasicBooleanVector_Remain()
        {
            BasicBooleanVector bbv1 = new BasicBooleanVector(new byte[] { 1, 0, 1});
            BasicBoolean bb = new BasicBoolean(false);
            bb.setNull();
            bbv1.set(0, bb);
            try
            {
                bbv1.deserialize(2, 3, @in);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }

            try
            {
                bbv1.getExtraParamForType();
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }

            BasicBooleanVector bbv2 = new BasicBooleanVector(10);
            bbv2.setBoolean(0, true);
            Assert.AreEqual("True", bbv2.get(0).getString());
            bbv2.set(1, "false");
            Assert.AreEqual("False", bbv2.get(1).getString());
            bbv2.set(1, "true");
            Assert.AreEqual("True", bbv2.get(1).getString());
        }


        [TestMethod]
        public void Test_BasicEntityFactory_Remain()
        {
            BasicEntityFactory bef1 = (BasicEntityFactory)BasicEntityFactory.instance();
            BasicIntMatrix bim1 = (BasicIntMatrix)bef1.createMatrixWithDefaultValue(DATA_TYPE.DT_INT, 2, 2);
            Assert.IsNotNull(bim1);
            BasicIntMatrix bim2 = (BasicIntMatrix)bef1.createMatrixWithDefaultValue(DATA_TYPE.DT_OBJECT, 1, 1);
            Assert.IsNull(bim2);
            try
            {
                BasicIntVector biv1 = (BasicIntVector)bef1.createVectorWithDefaultValue(DATA_TYPE.DT_INT_ARRAY, 2);
            }
            catch(Exception e)
            {
                Assert.AreEqual("If there is no value, an empty ArrayVector can only be created. ", e.Message);
                Console.WriteLine(e.Message);
            }
            BasicIntVector biv2 = (BasicIntVector)bef1.createVectorWithDefaultValue(DATA_TYPE.DT_OBJECT, 1);
            Assert.IsNull(biv2);
            BasicIntVector biv3 = (BasicIntVector)bef1.createPairWithDefaultValue(DATA_TYPE.DT_OBJECT);
            Assert.IsNull(biv3);
            IEntity e1 = bef1.createEntity(DATA_FORM.DF_CHART, DATA_TYPE.DT_INT, @in, false);
            Assert.IsNull(e1);
            try
            {
                IEntity e2 = bef1.createEntity(DATA_FORM.DF_VECTOR, DATA_TYPE.DT_OBJECT, @in, false);
            }catch(Exception e)
            {
                Assert.AreEqual("Data type DT_OBJECT is not supported yet.", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicNanotimeVector_Remain()
        {
            BasicNanoTimeVector bnv1 = new BasicNanoTimeVector(1);
            bnv1.setNull(0);
            Assert.AreEqual(TimeSpan.MinValue, bnv1.getNanoTime(0));
            bnv1.set(0, "13:30:10.gajhfajbf");
            Assert.AreEqual(TimeSpan.MinValue, bnv1.getNanoTime(0));
            bnv1.set(0, "jhgjs.0084675");
            Assert.AreEqual(TimeSpan.MinValue, bnv1.getNanoTime(0));
            bnv1.add(TimeSpan.FromSeconds(4));
        }

        [TestMethod]
        public void Test_BasicLongVector_Remain()
        {
            BasicLongVector bLv1 = new BasicLongVector(new long[] { -3, 2, 3, 4 });
            Console.WriteLine(bLv1.hashBucket(0, 4));
            try
            {
                bLv1.deserialize(3, 4, @in);
            }catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicIntVector_Remain()
        {
            BasicIntVector biv = new BasicIntVector(new int[] { 1, -2, 3, 4, 5 });
            biv.setNull(0);
            Assert.AreEqual(-1, biv.hashBucket(0, 4));
            Console.WriteLine(biv.hashBucket(1, 4));
            try
            {
                biv.deserialize(3, 4, @in);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }


        [TestMethod]
        public void Test_BasicShortVector_Remain()
        {
            BasicShortVector bsv = new BasicShortVector(new short[] { 1, -2, 3, 4, 5 });
            bsv.setNull(0);
            Assert.AreEqual(-1, bsv.hashBucket(0, 4));
            Console.WriteLine(bsv.hashBucket(1, 4));
            try
            {
                bsv.deserialize(3, 4, @in);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}

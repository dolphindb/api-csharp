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
using dolphindb.route;
using System.Numerics;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicScalarTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_BasicBoolean()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicBoolean sc = (BasicBoolean)conn.run("true");
            sc.setNull();
            Assert.AreEqual(true, sc.getValue());
            Assert.AreEqual(null, sc.getNumber());
            sc.setObject(0);
            Assert.AreEqual(false, sc.getObject());
            Assert.AreEqual(false, sc.getValue());
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.GetHashCode());
            Assert.AreEqual(0, sc.CompareTo(sc));
            Exception ex = null;
            Exception ex1 = null;

            try
            {
                sc.hashBucket(1);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            try
            {
                sc.getTemporal().ToString();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);

            Assert.AreEqual("LOGICAL", sc.getDataCategory().ToString());
            Assert.AreEqual("DT_BOOL", sc.getDataType().ToString());
            conn.close();

        }
        [TestMethod]
        public void Test_BasicBoolean_1()
        {
            BasicBoolean bl = new BasicBoolean(true);
            Assert.AreEqual(true, bl.getValue());
            Assert.AreEqual(true, bl.getObject());
            Assert.AreEqual(true, bl.getNumber().boolValue());
            BasicBoolean b2 = new BasicBoolean(false);
            Assert.AreEqual(false, b2.getValue());
        }
        [TestMethod]
        public void Test_BasicBoolean_Null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicBoolean bl = (BasicBoolean)conn.run("bool(NULL)");
            Assert.AreEqual(null, bl.getNumber());
        }

        [TestMethod]
        public void Test_BasicByte()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicByte sc = (BasicByte)conn.run("'a'");
            sc.setNull();
            Assert.AreEqual(128, sc.getValue());
            Assert.AreEqual(null, sc.getNumber());
            sc.setObject(1);
            byte a = 1;
            Assert.AreEqual(a, sc.getObject());
            Assert.AreEqual("1", sc.getString());
            Assert.AreEqual(false,sc.Equals('c'));
            Assert.AreEqual(1, sc.GetHashCode());
            Assert.AreEqual(0, sc.CompareTo(sc));
            Assert.AreEqual(0, sc.hashBucket(1));
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(1, sc.hashBucket(10));

            Exception ex = null;
            try
            {
               sc.getTemporal().ToString();

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            
            Assert.AreEqual("INTEGRAL", sc.getDataCategory().ToString());
            Assert.AreEqual("DT_BYTE", sc.getDataType().ToString());
            conn.close();

        }
        [TestMethod]
        public void Test_BasicByte_Null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicByte bl = (BasicByte)conn.run("char(NULL)");
            Assert.AreEqual(null, bl.getNumber());
        }
        [TestMethod]
        public void Test_BasicDate()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDate sc = (BasicDate)conn.run("2018.03.14");
            Assert.AreEqual(new DateTime(2018, 03, 14), sc.getValue());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(2018, 03, 14), sc.getValue());
            Assert.AreEqual(new DateTime(2018, 03, 14), sc.getTemporal());
            sc.setObject(new DateTime(2018, 11, 14));
            Assert.AreEqual(new DateTime(2018, 11, 14), sc.getValue());
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            conn.close();
        }
        [TestMethod]
        public void Test_BasicDate_1()
        {
            BasicDate sc = new BasicDate(new DateTime(1969, 12, 31));
            Assert.AreEqual(new DateTime(1969, 12, 31), sc.getValue());
            BasicDate sc1 = new BasicDate(new DateTime(1918, 12, 31));
            Assert.AreEqual(new DateTime(1918, 12, 31), sc1.getValue());
            BasicDate sc2 = new BasicDate(new DateTime(2999, 12, 31));
            Assert.AreEqual(new DateTime(2999, 12, 31), sc2.getValue());
            BasicDate sc3 = new BasicDate(new DateTime(1970, 01, 01));
            Assert.AreEqual(new DateTime(1970, 01, 01), sc3.getValue());
        }

        [TestMethod]
        public void Test_BasicDateHour()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDateHour sc = (BasicDateHour)conn.run("datehour(2012.06.15)");
            Assert.AreEqual(new DateTime(2012, 06, 15, 00, 00, 00), sc.getValue());
            Assert.AreEqual("2012.06.15T00", sc.getString());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(2012, 06, 15, 00, 00, 00), sc.getValue());
            sc.setObject(new DateTime(1970, 01, 01, 01, 00, 00));
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 00, 00), sc.getTemporal());
            Assert.AreEqual(1, sc.getInternalValue());
            Assert.AreEqual(true, sc.Equals(sc));
            sc.setObject(new DateTime(2018, 11, 14, 00, 00, 00));
            Assert.AreEqual(new DateTime(2018, 11, 14), sc.getValue());
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            conn.close();

        }
        [TestMethod]
        public void Test_BasicDateHour_1()
        {
            BasicDateHour sc = new BasicDateHour(new DateTime(1969, 12, 31, 01, 01, 01));
            Assert.AreEqual(new DateTime(1969, 12, 31, 01, 00, 00), sc.getValue());
            BasicDateHour sc1 = new BasicDateHour(new DateTime(1954, 12, 31,23, 01, 01));
            Assert.AreEqual(new DateTime(1954, 12, 31, 23, 00, 00), sc1.getValue());
            BasicDateHour sc2 = new BasicDateHour(new DateTime(2038, 01, 01, 23, 01, 01));
            Assert.AreEqual(new DateTime(2038, 01, 01, 23, 00, 00), sc2.getValue());
            BasicDateHour sc3 = new BasicDateHour(new DateTime(1970, 01, 01, 23, 01, 01));
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 00, 00), sc3.getValue());
        }

        [TestMethod]
        public void Test_BasicDateTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDateTime sc = (BasicDateTime)conn.run("2018.03.14T11:28:04");
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), sc.getValue());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(2018, 03, 14, 11, 28, 4), sc.getValue());
            sc.setObject(new DateTime(1970, 01, 01, 01, 00, 00));
            Assert.AreEqual(new DateTime(1970, 01, 01, 01, 00, 00), sc.getTemporal());
            Assert.AreEqual(3600, sc.getInternalValue());
            Assert.AreEqual(true, sc.Equals(sc));
            sc.setObject(new DateTime(2018, 11, 14, 00, 00, 00));
            Assert.AreEqual(new DateTime(2018, 11, 14), sc.getValue());
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            conn.close();
        }

        [TestMethod]
        public void Test_BasicDateTime_1()
        {
            BasicDateTime sc = new BasicDateTime(new DateTime(1969, 12, 31, 01, 01, 01));
            Assert.AreEqual(new DateTime(1969, 12, 31, 01, 01, 01), sc.getValue());
            BasicDateTime sc1 = new BasicDateTime(new DateTime(1918, 12, 31, 23, 01, 01));
            Assert.AreEqual(new DateTime(1918, 12, 31, 23, 01, 01), sc1.getValue());
            BasicDateTime sc2 = new BasicDateTime(new DateTime(2037, 12, 31, 23, 59, 59));
            Assert.AreEqual(new DateTime(2037, 12, 31, 23, 59, 59), sc2.getValue());
            BasicDateTime sc3 = new BasicDateTime(new DateTime(1970, 01, 01, 23, 59, 59));
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59), sc3.getValue());
        }

        [TestMethod]
        public void Test_BasicDouble()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDouble sc = (BasicDouble)conn.run("1.3");
            Assert.AreEqual(1.3, sc.getObject());
            sc.setObject(null);
            Assert.AreEqual(0, sc.getValue());
            Assert.AreEqual(0, sc.GetHashCode());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            Assert.AreEqual("FLOATING", sc.getDataCategory().ToString());
            Exception ex1 = null;
            try
            {
                sc.getTemporal();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            conn.close();

        }

        [TestMethod]
        public void Test_BasicDouble_1()
        {
            BasicDouble sc = new BasicDouble(0);
            Assert.AreEqual(0, sc.getValue());
            BasicDouble sc1 = new BasicDouble(1222222222222.777777777777);
            Assert.AreEqual(1222222222222.777777777777, sc1.getValue());
            BasicDouble sc2 = new BasicDouble(-1222222222222.777777773337777);
            Assert.AreEqual(-1222222222222.777777773337777, sc2.getValue());
        }

        [TestMethod]
        public void Test_BasicFloat()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicFloat sc = (BasicFloat)conn.run("1.3f");
            Assert.AreEqual(1.3f, sc.getObject());
            sc.setObject(null);
            Assert.AreEqual(0, sc.getValue());
            Assert.AreEqual(0, sc.GetHashCode());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            Assert.AreEqual("FLOATING", sc.getDataCategory().ToString());
            Exception ex1 = null;
            try
            {
                sc.getTemporal();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            conn.close();
        }

        [TestMethod]
        public void Test_BasicFloat_1()
        {
            BasicFloat sc = new BasicFloat(0);
            Assert.AreEqual(0, sc.getValue());
            BasicFloat sc1 = new BasicFloat(1222222222222.777777777777f);
            Assert.AreEqual(1222222222222.777777777777f, sc1.getValue());
            BasicFloat sc2 = new BasicFloat(-1222222222222.777777773337777f);
            Assert.AreEqual(-1222222222222.777777773337777f, sc2.getValue());
        }

        [TestMethod]
        public void Test_BasicIPAddr()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicIPAddr sc = (BasicIPAddr)conn.run("ipaddr()");
            Assert.AreEqual("", sc.getString());
            BasicIPAddr sc1 = (BasicIPAddr)conn.run("ipaddr(\"192.168.1.253\")");
            Assert.AreEqual("192.168.1.253", sc1.getString());
            Assert.AreEqual(false, sc1.Equals(sc));
            conn.close();
        }

        [TestMethod]
        public void Test_BasicIPAddr_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicIPAddr sc = new BasicIPAddr((long)0, (long)0);
            Assert.AreEqual("", sc.getString());
            BasicIPAddr sc1 = new BasicIPAddr((long)192, (long)253); ;
            Assert.AreEqual("0::c0:0:0:0:fd", sc1.getString());
            Assert.AreEqual(false, sc1.Equals(sc));
            conn.close();
        }

        [TestMethod]
        public void Test_BasicInt()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicInt sc = (BasicInt)conn.run("123");
            Assert.AreEqual(123, sc.getObject());
            sc.setObject(null);
            Assert.AreEqual(0, sc.getValue());
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual(0, sc.CompareTo(sc));
            Assert.AreEqual("INTEGRAL", sc.getDataCategory().ToString());
            Exception ex1 = null;
            try
            {
                sc.getTemporal();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            conn.close();

        }
        [TestMethod]
        public void Test_BasicInt128()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicInt128 sc = (BasicInt128)conn.run("int128('e1671797c52e15f763380b45e841ec32')");
            Assert.AreEqual("e1671797c52e15f763380b45e841ec32", sc.getString());
            Assert.AreEqual(false, sc.isNull());

            //sc.setObject(null);
            Assert.AreEqual(-1508499790, sc.hashCode());
            Assert.AreEqual(0, sc.hashBucket(-1));

            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("BINARY", sc.getDataCategory().ToString());
            Exception ex1 = null;
            try
            {
                sc.getTemporal();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            conn.close();

        }

        [TestMethod]
        public void Test_BasicLong()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicLong sc = (BasicLong)conn.run("222222222222222l");
            Assert.AreEqual(222222222222222L, sc.getValue());
            Assert.AreEqual(false, sc.isNull());

            sc.setObject(null);
            Assert.AreEqual(0, sc.hashBucket(-1));

            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("INTEGRAL", sc.getDataCategory().ToString());
            Exception ex1 = null;
            try
            {
                sc.getTemporal();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            conn.close();

        }

        [TestMethod]
        public void Test_BasicMinute()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicMinute sc = (BasicMinute)conn.run("10:47m");
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 47, 0).TimeOfDay, sc.getValue());
            Assert.AreEqual(false, sc.isNull());

            sc.setObject(null);
            Assert.AreEqual(0, sc.hashBucket(-1));

            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("TEMPORAL", sc.getDataCategory().ToString());
            Assert.AreEqual("10:47:00", sc.getTemporal().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicMinute_1()
        {
            BasicMinute sc = new BasicMinute(new DateTime(1970, 01, 01, 10, 47, 0).TimeOfDay);
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 47, 0).TimeOfDay, sc.getValue());
            BasicMinute sc1 = new BasicMinute(new DateTime(2099, 01, 01, 23, 59, 59).TimeOfDay);
            Assert.AreEqual(new DateTime(2099, 01, 01, 23, 59, 0).TimeOfDay, sc1.getValue());
            BasicMinute sc2 = new BasicMinute(new DateTime(1888, 01, 01, 23, 59, 23).TimeOfDay);
            Assert.AreEqual(new DateTime(1888, 01, 01, 23, 59, 0).TimeOfDay, sc2.getValue());
        }

        [TestMethod]
        public void Test_BasicMonth()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicMonth sc = (BasicMonth)conn.run("2018.03M");
            Assert.AreEqual(new DateTime(2018, 3, 1, 0, 0, 0), sc.getValue());
            Assert.AreEqual(false, sc.isNull());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(2018, 3, 1, 0, 0, 0), sc.getValue());
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(new DateTime(2018, 3, 1, 0, 0, 0), sc.getTemporal());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("TEMPORAL", sc.getDataCategory().ToString());
            conn.close();

        }

        [TestMethod]
        public void Test_BasicMonth_1()
        {
            BasicMonth sc = new BasicMonth(new DateTime(1970, 01, 01, 10, 47, 0));
            Assert.AreEqual(new DateTime(1970, 01, 01, 0, 0, 0), sc.getValue());
            BasicMonth sc1 = new BasicMonth(new DateTime(2099, 01, 01, 23, 59, 59));
            Assert.AreEqual(new DateTime(2099, 01, 01, 0, 0, 0), sc1.getValue());
            BasicMonth sc2 = new BasicMonth(new DateTime(1888, 01, 01, 23, 59, 23));
            Assert.AreEqual(new DateTime(1888, 01, 01, 0, 0, 0), sc2.getValue());
        }

        [TestMethod]
        public void Test_BasicNanoTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            DateTime dt = new DateTime(1970, 1, 1, 15, 41, 45, 123);
            long tickCount = dt.Ticks;
            BasicNanoTime sc = (BasicNanoTime)conn.run("15:41:45.123456789");
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay, sc.getValue());
            Assert.AreEqual(false, sc.isNull());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(tickCount + 4567L).TimeOfDay, sc.getValue());

            sc.setObject(new DateTime(tickCount + 1111L).TimeOfDay);
            Assert.AreEqual(new DateTime(tickCount + 1111L).TimeOfDay, sc.getValue());
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(new DateTime(tickCount + 1111L).TimeOfDay, sc.getTemporal());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("TEMPORAL", sc.getDataCategory().ToString());
            conn.close();

        }

        [TestMethod]
        public void Test_BasicNanoTime_1()
        {
            BasicNanoTime sc = new BasicNanoTime(1000L);
            Assert.AreEqual("00:00:00.000001000", sc.getString());
            BasicNanoTime sc1 = new BasicNanoTime(-1000L);
            Assert.AreEqual("23:59:59.999999000", sc1.getString());
            BasicNanoTime sc2 = new BasicNanoTime(0L);
            Assert.AreEqual("00:00:00.000000000", sc2.getString());
            BasicNanoTime sc3 = new BasicNanoTime(9999999999L);
            Assert.AreEqual("00:00:09.999999999", sc3.getString());
        }

        [TestMethod]
        public void Test_BasicSecond()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicSecond sc = (BasicSecond)conn.run("10:48:03");
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, sc.getValue());
            Assert.AreEqual(false, sc.isNull());
            sc.setObject(null);
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, sc.getValue());

            sc.setObject(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay);
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay, sc.getValue());
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(new DateTime(1970, 01, 01, 23, 59, 59).TimeOfDay, sc.getTemporal());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("TEMPORAL", sc.getDataCategory().ToString());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicSecond_1()
        {
            BasicSecond sc = new BasicSecond(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay);
            Assert.AreEqual(new DateTime(1970, 01, 01, 10, 48, 03).TimeOfDay, sc.getValue());
            BasicSecond sc1 = new BasicSecond(new DateTime(1934, 01, 01, 10, 48, 03).TimeOfDay);
            Assert.AreEqual(new DateTime(1934, 01, 01, 10, 48, 03).TimeOfDay, sc1.getValue());
            BasicSecond sc2 = new BasicSecond(new DateTime(2199, 12, 01, 10, 48, 03).TimeOfDay);
            Assert.AreEqual(new DateTime(2199, 12, 01, 10, 48, 03).TimeOfDay, sc2.getValue());
            BasicSecond sc3 = new BasicSecond(new DateTime(2022, 01, 01, 10, 48, 03).TimeOfDay);
            Assert.AreEqual(new DateTime(2199, 12, 01, 10, 48, 03).TimeOfDay, sc3.getValue());
        }

        [TestMethod]
        public void Test_BasicShort()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicShort sc = (BasicShort)conn.run("45h");
            Assert.AreEqual(45, sc.getValue());
            Assert.AreEqual(false, sc.isNull());
            sc.setObject(null);
            Assert.AreEqual(0, sc.getValue());
            sc.setObject(99);
            Assert.AreEqual(99, sc.getValue());
            Assert.AreEqual(0, sc.hashBucket(-1));
            Assert.AreEqual(sc.GetHashCode(), sc.GetHashCode());
            Assert.AreEqual(true, sc.Equals(sc));
            Assert.AreEqual(false, sc.Equals('c'));
            Assert.AreEqual("INTEGRAL", sc.getDataCategory().ToString());
            Assert.AreEqual(0, sc.CompareTo(sc));
            try
            {
                sc.getTemporal().ToString();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("Imcompatible data type", ex.Message);
            }
            conn.close();

        }

        [TestMethod]
        public void Test_BasicShort_1()
        {
            BasicShort sc = new BasicShort(32767);
            Assert.AreEqual(32767, sc.getValue());
            BasicShort sc1 = new BasicShort(-32768);
            Assert.AreEqual(-32768, sc1.getValue());
            BasicShort sc2 = new BasicShort(0);
            Assert.AreEqual(0, sc2.getValue());
        }

        [TestMethod]
        public void Test_BasicSet()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            Exception ex = null;
            try
            {
                BasicSet sc1 = new BasicSet(DATA_TYPE.DT_VOID, 1);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            Exception ex1 = null;
            try
            {
                BasicSet sc1 = new BasicSet(DATA_TYPE.DT_ANY, 1);

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            Exception ex2 = null;
            try
            {
                BasicSet sc1 = new BasicSet(DATA_TYPE.DT_DICTIONARY,1);

            }
            catch (Exception e)
            {
                ex2 = e;
            }
            Assert.IsNotNull(ex2);
            BasicSet sc = new BasicSet(DATA_TYPE.DT_INT, 100);
            //sc.put((IScalar)conn.run("'1'"), (IEntity)conn.run("1:2"));
            Console.Out.WriteLine(sc.getDataForm());
            Console.Out.WriteLine(sc.getDataType());
            //Console.Out.WriteLine(sc.ContainsKey((IScalar)conn.run("'1'")));

            Console.Out.WriteLine(sc.rows());
            Console.Out.WriteLine(sc.columns());
            Exception ex3 = null;
            try
            {
                sc.getObject();

            }
            catch (Exception e)
            {
                ex3 = e;
            }
            Assert.IsNotNull(ex3);
            //Exception ex4 = null;
            //try
            //{
            //    sc.ExtendedDataOutput();

            //}
            //catch (Exception e)
            //{
            //    ex4 = e;
            //}
            //Assert.IsNotNull(ex4);
            conn.close();
        }

        [TestMethod]
        public void Test_BasicString()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicString sc = new BasicString("string", false);
            BasicString scc = new BasicString("string", true);
            Assert.AreEqual("string", sc.getValue());
            Assert.AreEqual(false, sc.isNull());
            Assert.AreEqual(false, sc.Equals(123));
            BasicString sc2 = new BasicString("string", false);
            Assert.AreEqual(true, sc.Equals(sc2));
            Assert.AreEqual(false, scc.Equals(sc2));
            BasicString sc3 = new BasicString("string", true);
            Assert.AreEqual(false, sc.Equals(sc3));
            Assert.AreEqual(true, scc.Equals(sc3));
            BasicString sc4 = new BasicString("string123", false);
            BasicString sc5 = new BasicString("string123", true);
            Assert.AreEqual(false, sc.Equals(sc4));
            Assert.AreEqual(false, scc.Equals(sc5));
            Console.Out.WriteLine(scc.GetHashCode());
            Console.Out.WriteLine(scc.CompareTo(sc));
            sc.setObject("12345678");
            scc.setObject("12345678");
            Assert.AreEqual("12345678", sc.getValue());
            Assert.AreEqual("12345678", scc.getValue());
            Console.Out.WriteLine(BasicString.hashBucket("123456789012", 1));
            Console.Out.WriteLine(BasicString.hashBucket("1234567890123", 1));
            Console.Out.WriteLine(BasicString.hashBucket("12345678901234", 1));
            Console.Out.WriteLine(BasicString.hashBucket("123456789012345", 1));
            Console.Out.WriteLine(BasicString.hashBucket("-string!@@  \\u0000\\/@@%$&123456789012345", 1));
            Console.Out.WriteLine(BasicString.hashBucket("\u0000", 1));
            Console.Out.WriteLine(BasicString.hashBucket("\u0000\u0001", 1));
            Console.Out.WriteLine(BasicString.hashBucket("\u0000\u0001\u007f\u0000\u0000\u0000", 1));
            Console.Out.WriteLine(BasicString.hashBucket("\u08ff", 1));
            Console.Out.WriteLine(scc.hashBucket(1));
            //Assert.AreEqual(0, sc.getValue());
            //sc.setObject(99);
            //Assert.AreEqual(99, sc.getValue());
            //Assert.AreEqual(0, sc.hashBucket(-1));
            //Assert.AreEqual(99, sc.GetHashCode());
            //Assert.AreEqual(true, sc.Equals(sc));
            //Assert.AreEqual(false, sc.Equals('c'));
            //Assert.AreEqual("LITERAL", sc.getDataCategory().ToString());
            //Assert.AreEqual(0, sc.CompareTo(sc));
            try
            {
                sc.getNumber().ToString();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("Imcompatible data type", ex.Message);
            }
            try
            {
                sc.getTemporal().ToString();

            }
            catch (Exception ex)
            {
                Assert.AreEqual("Imcompatible data type", ex.Message);
            }
            conn.close();

        }

        [TestMethod]
        public void Test_BasicString_setNull()
        {
            BasicString sc1 = new BasicString("string123", false);
            BasicString sc2 = new BasicString("string123", true);
            sc1.setNull();
            sc2.setNull();
            Assert.AreEqual(true, sc1.isNull());
            Assert.AreEqual(true, sc2.isNull());
        }

        [TestMethod]
        public void Test_BasicString_illegal_string()
        {
            BasicString sc1 = new BasicString("string123\0", false);
            BasicString sc2 = new BasicString("string123\0", true);
            Assert.AreEqual("string123\0", sc1.getString());
            Assert.AreEqual("string123\0", sc1.getString());
            BasicString sc3 = new BasicString("", false);
            BasicString sc4 = new BasicString("", true);
            Assert.AreEqual("", sc3.getString());
            Assert.AreEqual("", sc4.getString());
        }

        [TestMethod]
        public void Test_BasicInt_getNumber_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicInt bi = (BasicInt)conn.run("int(NULL)");
            Assert.IsNull(bi.getNumber());
            conn.close();
        }

        [TestMethod]
        public void Test_BasicInt_hashBucket()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicInt b1 = (BasicInt)conn.run("int(NULL)");
            Assert.AreEqual(-1, b1.hashBucket(0));

            BasicInt b2 = new BasicInt(-2);
            Console.Out.WriteLine(b2.hashBucket(1));
        }

        [TestMethod]
        public void Test_BasicInt128_fromString()
        {
            string error = null;
            try
            {
                BasicInt128 bi128 = BasicInt128.fromString("abcde");
            }
            catch(Exception e)
            {
                error = e.Message;
            }
            Assert.IsNotNull(error);
        }

        [TestMethod]
        public void Test_BasicInt128_notSupport()
        {
            BasicInt128 bi128 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32");
            try
            {
                bi128.getNumber();
            }catch(Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                bi128.getObject();
            }catch(Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

            try
            {
                bi128.setObject("123456");
            }catch(Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        [TestMethod]
        public void Test_BasicInt128_Equals()
        {
            BasicInt128 b1 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32");
            BasicInt128 b2 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32");
            BasicInt128 b3 = BasicInt128.fromString("e1671797c78d63f763380b45e841ec32");
            string a = null;
            BasicInt128 b4 = null;
            Assert.AreEqual(false, b1.Equals(a));
            Assert.AreEqual(false, b1.Equals(b3));
            Assert.AreEqual(false, b1.Equals(b4));
            Assert.AreEqual(true, b2.Equals(b1));
        }

        [TestMethod]
        public void Test_BasicInt128_parseLongFromScale16()
        {
            BasicInt128 b1 = BasicInt128.fromString("e1671797c52A15f763380D45e841ec32");
            Console.WriteLine(b1.getString());
            try
            {
                BasicInt128 b2 = BasicInt128.fromString("Z1671797G52A15f76338PO45e841ec32");
            }
            catch(Exception e)
            {
                Assert.IsNotNull(e);
            }
        }

        [TestMethod]
        public void Test_BasicLong_RemainMethod()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicLong bl = (BasicLong)conn.run("long(NULL)");
            Assert.IsNull(bl.getNumber());
            BasicLong b1 = new BasicLong(2);
            BasicLong b2 = new BasicLong(10);
            BasicLong b3 = new BasicLong(-10);
            Console.WriteLine(b1.CompareTo(b2));

            //Assert.AreEqual(-1, b1.hashBucket(0));
            Console.WriteLine(b3.hashBucket(2));
        }

        [TestMethod]
        public void Test_Ipaddr_Remain()
        {
            BasicIPAddr bip = BasicIPAddr.fromString("1215a");
            Assert.IsNull(bip);

            BasicIPAddr b1 = BasicIPAddr.parseIP4("1");
            Assert.IsNull(b1);

            BasicIPAddr b2 = BasicIPAddr.parseIP4("a.");
            Assert.IsNull(b2);

            BasicIPAddr b3 = BasicIPAddr.parseIP4("D.");
            Assert.IsNull(b3);

            BasicIPAddr b4 = BasicIPAddr.parseIP4("192.168.124.176.888");
            Assert.IsNull(b4);

            BasicIPAddr b5 = BasicIPAddr.parseIP6(":");
            Assert.IsNull(b5);

            BasicIPAddr b6 = BasicIPAddr.parseIP6("+-");
            Assert.IsNull(b6);

            BasicIPAddr b7 = BasicIPAddr.parseIP6("-");
            Assert.IsNull(b7);


            BasicIPAddr b8 = BasicIPAddr.parseIP4("192.168.1.116");
            BasicInt bi1 = new BasicInt(1);
            BasicInt bi2 = null;
            Assert.IsFalse(b8.Equals(bi2));
            Assert.IsFalse(b8.Equals(bi1));
        }

        [TestMethod]
        public void Test_Number_Remain()
        {
            Number nb = new Number(false);
            Assert.AreEqual(nb.byteValue(), 0);
            Number nb2 = new Number(1.2);
            Assert.AreEqual(nb2.byteValue(), 1);
            Number nb3 = new Number(1.2f);
            Assert.AreEqual(nb3.byteValue(), 1);
            Number nb4 = new Number(12L);
            Assert.AreEqual(nb4.byteValue(), 12L);
            Number nb5 = new Number(1);
            Assert.AreEqual(1, nb5.byteValue());
        }

        [TestMethod]
        public void Test_Long2_Remain()
        {
            Long2 l21 = new Long2(1, 2);
            int a = 0;
            Long2 l22 = null;
            Long2 l23 = new Long2(1, 3);
            Long2 l24 = new Long2(2, 2);
            Long2 l25 = new Long2(1, 2);
            Assert.IsFalse(l21.equals(a));
            Assert.IsFalse(l21.equals(l22));
            Assert.IsFalse(l21.Equals(l23));
            Assert.IsFalse(l21.Equals(l24));
            Assert.IsTrue(l21.Equals(l25));
        }

        [TestMethod]
        public void Test_BasicUuid_Remain()
        {
            try
            {
                BasicUuid bu1 = BasicUuid.fromString("gaas-jjh");
            }catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }

            BasicUuid bu2 = new BasicUuid(4, 7);
            Assert.IsFalse(bu2.Equals(new BasicInt(1)));
            Assert.IsFalse(bu2.Equals(null));
        }

        [TestMethod]
        public void Test_BasicTimeStamp_Remain()
        {
            BasicTimestamp bst1 = new BasicTimestamp(6);
            bst1.setNull();
            Assert.AreEqual(DateTime.MinValue, bst1.getValue());
            BasicTimestamp bst2 = new BasicTimestamp(12);
            Console.WriteLine(bst2.getTemporal());
            Assert.IsFalse(bst2.Equals(new BasicString("hgh")));
            Assert.IsFalse(bst2.Equals(null));
            bst2.setObject(null);
            bst2.setObject(3);
            bst2.setObject(DateTime.Now);
            Console.WriteLine(bst2.getTemporal());
        }



        [TestMethod]
        public void Test_BasicNanoTimeStamp_Remain()
        {
            BasicNanoTimestamp bnst1 = new BasicNanoTimestamp(6);
            bnst1.setNull();
            Assert.AreEqual(DateTime.MinValue, bnst1.getValue());
            BasicNanoTimestamp bnst2 = new BasicNanoTimestamp(12);
            Console.WriteLine(bnst2.getObject());
            Console.WriteLine(bnst2.getTemporal());
            Assert.IsFalse(bnst2.Equals(new BasicString("hgh")));
            Assert.IsFalse(bnst2.Equals(null));
            bnst2.setObject(null);
            bnst2.setObject(3);
            bnst2.setObject(DateTime.Now);
            Console.WriteLine(bnst2.getTemporal());
        }



        [TestMethod]
        public void Test_BasicTime_Remain()
        {
            BasicTime bt1 = new BasicTime(6);
            Assert.AreEqual(DATA_CATEGORY.TEMPORAL, bt1.getDataCategory());
            bt1.setNull();
            Assert.AreEqual(TimeSpan.MinValue, bt1.getValue());
            BasicTime bt2 = new BasicTime(12);
            Console.WriteLine(bt2.getObject());
            Console.WriteLine(bt2.getTemporal());
            Assert.IsFalse(bt2.Equals(new BasicString("hgh")));
            Assert.IsFalse(bt2.Equals(null));
            bt2.setObject(null);
            bt2.setObject(3);
            bt2.setObject(TimeSpan.FromSeconds(1.5));
            Console.WriteLine(bt2.getTemporal());
            try
            {
                BasicTime.checkTimeSpanToTime(TimeSpan.FromDays(4));
            }catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicShort_Remain()
        {
            BasicShort bs1 = new BasicShort(6);
            bs1.setNull();
            Assert.IsNull(bs1.getNumber());
            Assert.AreEqual(-1, bs1.hashBucket(0));
            bs1.setObject(2);
            Console.WriteLine(bs1.hashBucket(3));
        }


        [TestMethod]
        public void Test_BasicMonth_Remain()
        {
            BasicMonth bm1 = new BasicMonth(2012, 11);
            bm1.setNull();
            Assert.AreEqual(DateTime.MinValue, bm1.getValue());
            bm1.setObject(null);
            bm1.setObject(3);
            bm1.setObject(DateTime.Now);
            Console.WriteLine(bm1.getValue());
        }

        [TestMethod]
        public void Test_BasicLong_Remain()
        {
            BasicLong bl1 = new BasicLong(4);
            bl1.setNull();
            Assert.AreEqual(-1, bl1.hashBucket(0));
            bl1.setObject(0);
            Assert.AreEqual(0, bl1.hashBucket(1));
        }

        [TestMethod]
        public void Test_BasicDateHour_Remain()
        {
            BasicDateHour bdh1 = new BasicDateHour(4);
            Assert.AreEqual(DATA_CATEGORY.TEMPORAL, bdh1.getDataCategory());
            bdh1.setNull();
            Assert.AreEqual(DateTime.MinValue, bdh1.getValue());
        }

        [TestMethod]
        public void Test_BasicMinute_Remain()
        {
            BasicMinute bm1 = new BasicMinute(71);
            bm1.setNull();
            Assert.AreEqual(TimeSpan.MinValue, bm1.getValue());
            bm1.setObject(null);
            bm1.setObject(3);
            bm1.setObject(TimeSpan.FromSeconds(1.5));
            Console.WriteLine(bm1.getValue());
        }

        [TestMethod]
        public void Test_BasicSecond_Remain()
        {
            BasicSecond bs = new BasicSecond(3);
            bs.setNull();
            Assert.AreEqual(TimeSpan.MinValue, bs.getObject());
            Assert.AreEqual(TimeSpan.MinValue, bs.getValue());
            try
            {
                BasicSecond.checkTimeSpanToSecond(TimeSpan.FromDays(4));
            }catch(Exception e)
            {
                Assert.AreEqual("To convert BasicSecond, TimeSpan's days must equal zero. ", e.Message);
                Console.WriteLine(e.Message);
            }
        }


        [TestMethod]
        public void Test_BasicInt128_Remain()
        {
            BasicInt128 bi1 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32");
            BasicInt128 bi2 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32");
            Assert.IsTrue(bi1.equals(bi2));
            BasicInt128 bi3 = BasicInt128.fromString("e4682397c52e15f7aa1270b45e841ec3");
            Assert.IsFalse(bi1.equals(bi3));
            BasicInt128 bi4 = null;
            Assert.IsFalse(bi1.equals(bi4));
            BasicInt bi = new BasicInt(1);
            Assert.IsFalse(bi1.equals(bi));
            try
            {
                BasicInt128 bi5 = BasicInt128.fromString("+e4682397c52e15f7aa1270b45e841ec");
            }
            catch(Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }


            try
            {
                BasicInt128 bi5 = BasicInt128.fromString("=e4682397c52e15f7aa1270b45e841ec");
            }
            catch (Exception e)
            {
                Assert.IsNotNull(e);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicFloat_Remain()
        {
            BasicFloat bf1 = new BasicFloat(1.1f);
            bf1.setNull();
            Assert.IsNull(bf1.getNumber());
            BasicFloat bf2 = new BasicFloat(float.NaN);
            BasicFloat bf3 = new BasicFloat(float.NegativeInfinity);
            BasicFloat bf4 = new BasicFloat(float.PositiveInfinity);
            Console.WriteLine(bf2.getString());
            Console.WriteLine(bf3.getString());
            Console.WriteLine(bf4.getString());
            BasicFloat bf5 = new BasicFloat(0.00000004674f);
            Console.WriteLine(bf5.getString());
            BasicFloat bf6 = new BasicFloat(-0.1f);
            Console.WriteLine(bf6.getString());
            BasicFloat bf7 = new BasicFloat(4756874.4476f);
            Console.WriteLine(bf7.getString());
            BasicFloat bf8 = new BasicFloat(0.1f);
            Console.WriteLine(bf8.getString());
        }

        [TestMethod]
        public void Test_BasicDouble_Remain()
        {
            BasicDouble bd1 = new BasicDouble(1.1);
            bd1.setNull();
            Assert.IsNull(bd1.getNumber());
            BasicDouble bd2 = new BasicDouble(double.NaN);
            BasicDouble bd3 = new BasicDouble(double.NegativeInfinity);
            BasicDouble bd4 = new BasicDouble(double.PositiveInfinity);
            Console.WriteLine(bd2.getString());
            Console.WriteLine(bd3.getString());
            Console.WriteLine(bd4.getString());
            BasicDouble bd5 = new BasicDouble(0.00000004674);
            Console.WriteLine(bd5.getString());
            BasicDouble bd6 = new BasicDouble(-0.1);
            Console.WriteLine(bd6.getString());
            BasicDouble bd7 = new BasicDouble(4756874.4476);
            Console.WriteLine(bd7.getString());
            BasicDouble bd8 = new BasicDouble(0.1);
            Console.WriteLine(bd8.getString());
        }

        [TestMethod]
        public void Test_BasicString_Remain()
        {
            BasicString bs1 = new BasicString(new byte[] { 1, 2, 3, 4 }, false);
            Console.WriteLine(bs1.getString());
            BasicString bs2 = new BasicString(new byte[] { 1, 2, 3, 4 }, true);
            Assert.IsFalse(bs2.isNull());
            BasicString bs3 = new BasicString(new byte[0], true);
            Assert.IsTrue(bs3.isNull());
            BasicString bs4 = new BasicString(new byte[] { 1, 2, 3, 4 }, true);
            Assert.IsFalse(bs4.Equals(new BasicString(new byte[] { 1, 2, 3, 5 }, true)));
            BasicString bs5 = new BasicString("abcd");
            Console.WriteLine(bs5.CompareTo(new BasicString("def")));
            try
            {
                bs5.getBytes();
            }catch(Exception e)
            {
                Assert.AreEqual("The value must be a string scalar. ", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicTemporal_Remain()
        {
            BasicNanoTime bn1 = new BasicNanoTime(1);
            bn1.setNull();
            Assert.AreEqual(TimeSpan.MinValue, bn1.getValue());
            try
            {
                BasicNanoTime bn2 = new BasicNanoTime(TimeSpan.FromDays(1));
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                Assert.AreEqual("To convert BasicNanoTime, TimeSpan's days must equal zero. ", e.Message);
            }
        }

        [TestMethod]
        public void Test_BasicDecimal32()
        {
            BasicDecimal32 dc1 = new BasicDecimal32(2147483647, 0);
            Assert.AreEqual("2147483647", dc1.getString());
            Assert.AreEqual(2147483647, dc1.getNumber().decimalValue());
            BasicDecimal32 dc2 = new BasicDecimal32(214748364, 1);
            Assert.AreEqual("214748364.0", dc2.getString());
            BasicDecimal32 dc3 = new BasicDecimal32(0, 1);
            Assert.AreEqual("0.0", dc3.getString());
            BasicDecimal32 dc4 = new BasicDecimal32(0, 0);
            Assert.AreEqual("0", dc4.getString());
            BasicDecimal32 dc5 = new BasicDecimal32(1, 5);
            Assert.AreEqual("1.00000", dc5.getString());

            BasicDecimal32 dc6 = new BasicDecimal32("214748364.00001", 1);
            Assert.AreEqual("214748364.0", dc6.getString());
            BasicDecimal32 dc7 = new BasicDecimal32("214748364.11111", 0);
            Assert.AreEqual("214748364", dc7.getString());
            BasicDecimal32 dc8 = new BasicDecimal32("0.00", 1);
            Assert.AreEqual("0.0", dc8.getString());
            BasicDecimal32 dc9 = new BasicDecimal32("0.00", 0);
            Assert.AreEqual("0", dc9.getString());
            BasicDecimal32 dc10 = new BasicDecimal32("1.000000000001", 5);
            Assert.AreEqual("1.00000", dc10.getString());
            BasicDecimal32 dc11 = new BasicDecimal32("1.000000000001", 9);
            Assert.AreEqual("1.000000000", dc11.getString());
            BasicDecimal32 dc12 = new BasicDecimal32("1.1", 0);
            Assert.AreEqual("1", dc12.getString());
            BasicDecimal32 dc13 = new BasicDecimal32("1.1", 1);
            Assert.AreEqual("1.1", dc13.getString());
            BasicDecimal32 dc14 = new BasicDecimal32("0", 1);
            Assert.AreEqual("0.0", dc14.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32_value_string()
        {
            BasicDecimal32 decimal32 = new BasicDecimal32("1232", -1);
            Assert.AreEqual("1232", decimal32.getString());
            Assert.AreEqual(0, decimal32.getScale());

            BasicDecimal32 Decimal32 = new BasicDecimal32("999999999", 0);
            Assert.AreEqual("999999999", Decimal32.getString());
            BasicDecimal32 Decimal32_1 = new BasicDecimal32("-999999999", 0);
            Assert.AreEqual("-999999999", Decimal32_1.getString());

            BasicDecimal32 Decimal321 = new BasicDecimal32("0.000001", 4);
            Assert.AreEqual("0.0000", Decimal321.getString());
            BasicDecimal32 Decimal32_11 = new BasicDecimal32("-0.000001", 4);
            Assert.AreEqual("0.0000", Decimal32_11.getString());

            BasicDecimal32 Decimal322 = new BasicDecimal32("0.999999999", 8);
            Assert.AreEqual("1.00000000", Decimal322.getString());
            BasicDecimal32 Decimal32_12 = new BasicDecimal32("-0.999999999", 8);
            Assert.AreEqual("-1.00000000", Decimal32_12.getString());

            BasicDecimal32 Decimal323 = new BasicDecimal32("0.000000001", 9);
            Assert.AreEqual("0.000000001", Decimal323.getString());
            BasicDecimal32 Decimal32_13 = new BasicDecimal32("-0.000000001", 9);
            Assert.AreEqual("-0.000000001", Decimal32_13.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32_value_string_no_scale()
        {
            BasicDecimal32 Decimal32 = new BasicDecimal32("999999999");
            Assert.AreEqual("999999999", Decimal32.getString());
            Assert.AreEqual(0, Decimal32.getScale());

            BasicDecimal32 Decimal32_1 = new BasicDecimal32("-999999999");
            Assert.AreEqual("-999999999", Decimal32_1.getString());
            Assert.AreEqual(0, Decimal32_1.getScale());

            BasicDecimal32 Decimal321 = new BasicDecimal32("1232", -1);
            Assert.AreEqual("1232", Decimal321.getString());
            Assert.AreEqual(0, Decimal321.getScale());

            BasicDecimal32 Decimal322 = new BasicDecimal32("0.000001");
            Assert.AreEqual("0.000001", Decimal322.getString());
            Assert.AreEqual(6, Decimal322.getScale());

            BasicDecimal32 Decimal32_12 = new BasicDecimal32("-0.000001");
            Assert.AreEqual("-0.000001", Decimal32_12.getString());
            Assert.AreEqual(6, Decimal32_12.getScale());


            BasicDecimal32 Decimal323 = new BasicDecimal32("0.000000001");
            Assert.AreEqual("0.000000001", Decimal323.getString());
            Assert.AreEqual(9, Decimal323.getScale());

            BasicDecimal32 Decimal32_13 = new BasicDecimal32("-0.000000001");
            Assert.AreEqual("-0.000000001", Decimal32_13.getString());
            Assert.AreEqual(9, Decimal32_13.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal32_string_not_support()
        {

            String ex = null;
            try
            {
                BasicDecimal32 deci = new BasicDecimal32((2 / 1000000.0).ToString(), 1);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("decimal data form is not correct: 2E-06", ex);
        }

        [TestMethod]
        public void test_BasicDecimal32_value_string_scale_9_overflow()
        {
            String re = null;
            try
            {
                BasicDecimal32 Decimal32_2 = new BasicDecimal32("9.99999999", 9);
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual(re, "Decimal math overflow!");

            String re1 = null;
            try
            {
                BasicDecimal32 Decimal32_3 = new BasicDecimal32("-9.99999999", 9);
            }
            catch (Exception e)
            {
                re1 = e.Message;
            }
            Assert.AreEqual(re1, "Decimal math overflow!");
        }

        [TestMethod]
        public void test_BasicDecimal32_value_string_scale_10()
        {
            String ex = null;
            try
            {
                BasicDecimal32 Decimal32 = new BasicDecimal32("1232", 10);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale 10 is out of bounds, it must be in [0,9].", ex);
        }


        [TestMethod]
        public void test_BasicDecimal32_value_string_2()
        {
            BasicDecimal32 Decimal32 = new BasicDecimal32("0", 4);
            Assert.AreEqual("0.0000", Decimal32.getString());

            BasicDecimal32 re1 = new BasicDecimal32("-0.5900", 0);
            Assert.AreEqual("-1", re1.getString());

            BasicDecimal32 re2 = new BasicDecimal32("0.6900", 0);
            Assert.AreEqual("1", re2.getString());

            BasicDecimal32 re3 = new BasicDecimal32("0.4900", 0);
            Assert.AreEqual("0", re3.getString());

            BasicDecimal32 re4 = new BasicDecimal32("-0.4949", 2);
            Assert.AreEqual("-0.49", re4.getString());

            BasicDecimal32 re5 = new BasicDecimal32("-0.4950", 2);
            Assert.AreEqual("-0.50", re5.getString());

            BasicDecimal32 re6 = new BasicDecimal32("-0.4960", 2);
            Assert.AreEqual("-0.50", re6.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32_run()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            BasicDecimal32 re1 = (BasicDecimal32)conn.run("decimal32('1.003',4)");
            Assert.AreEqual("1.0030", re1.getString());
            Assert.AreEqual(1.0030m, re1.getNumber().decimalValue());

            BasicDecimal32 re2 = (BasicDecimal32)conn.run("decimal32(-12.332,6)");
            Assert.AreEqual("-12.332000", re2.getString());
            Assert.AreEqual(-12.332m, re2.getNumber().decimalValue());
            Assert.AreEqual("DT_DECIMAL32", re2.getDataType().ToString());

            BasicDecimal32 re3 = (BasicDecimal32)conn.run("decimal32(0,6)");
            Assert.AreEqual("0.000000", re3.getString());
            Assert.AreEqual(0, re3.getNumber().decimalValue());
            Assert.AreEqual("DT_DECIMAL32", re3.getDataType().ToString());

            BasicDecimal32 re4 = (BasicDecimal32)conn.run("decimal32(NULL,6)");
            Assert.AreEqual("", re4.getString());
            Assert.AreEqual(true, re4.isNull());
            Assert.AreEqual(-2147.483648m, re4.getNumber().decimalValue());
            Assert.AreEqual("DT_DECIMAL32", re4.getDataType().ToString());
            conn.close();
        }

        [TestMethod]
        public void test_BasicDecimal32_getTemporal()
        {
            String re = null;
            try
            {
                BasicDecimal32 Decimal32 = new BasicDecimal32("1.23231", 7);
                Decimal32.getTemporal();
            }
            catch (Exception ex) { 
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("The method or operation is not implemented.") || re.Contains("未实现该方法或操作。"));
        }

        [TestMethod]
        public void Test_BasicDecimal32_getDataCategory()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("1.1234567", 5);
            Assert.AreEqual("DENARY", dc1.getDataCategory().ToString());
        }

        [TestMethod]
        public void Test_BasicDecimal32_getDataType()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("1.1234567", 5);
            Assert.AreEqual("DT_DECIMAL32", dc1.getDataType().ToString());
        }

        [TestMethod]
        public void Test_BasicDecimal32_setNull()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("1.1234567", 5);
            Assert.AreEqual("1.12346", dc1.getString());
            dc1.setNull();
            Assert.AreEqual(true, dc1.isNull());
        }

        [TestMethod]
        public void test_BasicDecimal32_setRawData()
        {
            BasicDecimal32 Decimal32 = new BasicDecimal32("1.23231", 2);
            Assert.AreEqual(123, Decimal32.getRawData());
            Decimal32.setRawData(100);
            Assert.AreEqual(100, Decimal32.getRawData());
            Assert.AreEqual("1.00", Decimal32.getString());
            Decimal32.setRawData(2147483647);
            Assert.AreEqual(2147483647, Decimal32.getRawData());
            Assert.AreEqual("21474836.47", Decimal32.getString());

            BasicDecimal32 Decimal321 = new BasicDecimal32("1.23231", 8);
            Assert.AreEqual("123231000", Decimal321.getRawData().ToString());
            Decimal321.setRawData(0);
            Assert.AreEqual(000000000, Decimal321.getRawData());
            Assert.AreEqual("0.00000000", Decimal321.getString());
            Decimal321.setRawData(-2147483648);
            Assert.AreEqual(-2147483648, Decimal321.getRawData());
            Assert.AreEqual("", Decimal321.getString());
        }

        [TestMethod]
        public void Test_BasicDecimal32_getNumber()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Assert.AreEqual(123m, dc1.getNumber().decimalValue());
            Assert.AreEqual(123m, dc1.getDecimalValue());
            dc1.setNull();
            Assert.AreEqual(-2147483648m, dc1.getNumber().decimalValue());
            Assert.AreEqual(-2147483648m, dc1.getDecimalValue());

            BasicDecimal32 dc2 = new BasicDecimal32("-0.0000000005", 9);
            Assert.AreEqual(-0.000000001m, dc2.getNumber().decimalValue());
            Assert.AreEqual(-0.000000001m, dc2.getDecimalValue());

            BasicDecimal32 dc3 = new BasicDecimal32("9999.99999", 4);
            Assert.AreEqual(10000.0000m, dc3.getNumber().decimalValue());
            Assert.AreEqual(10000.0000m, dc3.getDecimalValue());
        }

        [TestMethod]
        public void Test_BasicDecimal32_getScale()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Assert.AreEqual(0, dc1.getScale());
            BasicDecimal32 dc2 = new BasicDecimal32("1.23345666", 9);
            Assert.AreEqual(9, dc2.getScale());
        }

        [TestMethod]
        public void Test_BasicDecimal32_getObject()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Assert.AreEqual("123", dc1.getObject().ToString());
            BasicDecimal32 dc2 = new BasicDecimal32("1.00000001", 9);
            Assert.AreEqual("1.000000010", dc2.getObject().ToString());
        }
        [TestMethod]
        public void Test_BasicDecimal32_getString()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Assert.AreEqual("123", dc1.getString());
            dc1.setNull();
            Assert.AreEqual("", dc1.getString());
            BasicDecimal32 dc2 = new BasicDecimal32("123.2", 5);
            Assert.AreEqual("123.20000", dc2.getString());
        }

        [TestMethod]
        public void Test_BasicDecimal32_ToString()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Assert.AreEqual("123", dc1.toString());
            dc1.setNull();
            Assert.AreEqual("", dc1.toString());
            BasicDecimal32 dc2 = new BasicDecimal32("123.2", 5);
            Assert.AreEqual("123.20000", dc2.toString());
        }


        [TestMethod]
        public void Test_BasicDecimal32_hashBucket()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            try
            {
                dc1.hashBucket(1);
            }
            catch (Exception e)
            {
                string s = e.Message;
                Assert.AreEqual(true, s.Contains("The method or operation is not implemented.") || s.Contains("未实现该方法或操作。"));
            }
        }

        [TestMethod]
        public void Test_BasicDecimal32_setObject()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Console.WriteLine(dc1.getString());
            dc1.setObject("1.222");
            Console.WriteLine(dc1.getString());
            Assert.AreEqual("1", dc1.getObject().ToString());

            BasicDecimal32 dc2 = new BasicDecimal32("123.2", 0);
            String re = null;
            try
            {
                dc2.setObject((long)3333);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("the type of value must be string.", re);
        }

        [TestMethod]
        public void Test_BasicDecimal32_writeScalarToOutputStream()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            Stream outStream = new MemoryStream();
            ExtendedDataOutput out1 = new BigEndianDataOutputStream(outStream);
            dc1.write(out1);
        }

        [TestMethod]
        public void Test_BasicDecimal32_Equals()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            BasicDecimal32 dc2 = new BasicDecimal32("123.0", 2);
            BasicDecimal32 dc3 = new BasicDecimal32("123.0", 2);
            Assert.AreEqual(true, dc1.Equals(dc1));
            Assert.AreEqual(false, dc1.Equals(dc2));
            dc2.setNull();
            dc3.setNull();
            Assert.AreEqual(true, dc2.Equals(dc3));
        }
        [TestMethod]
        public void Test_BasicDecimal32_GetHashCode()
        {
            BasicDecimal32 dc1 = new BasicDecimal32("123.2", 0);
            BasicDecimal32 dc2 = new BasicDecimal32("123.0", 2);
            BasicDecimal32 dc3 = new BasicDecimal32("123.0", 2);
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
            dc1.setNull();
            dc2.setNull();
            dc3.setNull();
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
        }

        [TestMethod]
        public void Test_BasicDecimal32_createBasicDecimal32()
        {
            BasicDecimal32 dc1 = Utils.createBasicDecimal32(-999999999, 0);
            Assert.AreEqual("-999999999", dc1.getString());

            BasicDecimal32 dc2 = Utils.createBasicDecimal32(999999999, 9);
            Assert.AreEqual("0.999999999", dc2.getString());

            BasicDecimal32 dc3 = Utils.createBasicDecimal32(-999999999, 2);
            Assert.AreEqual("-9999999.99", dc3.getString());

            BasicDecimal32 dc4 = Utils.createBasicDecimal32(0, 5);
            Assert.AreEqual("0.00000", dc4.getString());

            BasicDecimal32 dc5 = Utils.createBasicDecimal32(100000000, 1);
            Assert.AreEqual("10000000.0", dc5.getString());
        }

        [TestMethod]
        public void Test_BasicDecimal64()
        {
            //BasicDecimal64 dc1 = new BasicDecimal64(2444422222222222, 1);
            //Assert.AreEqual(2444422222222222.0, dc1.getValue());
            //BasicDecimal64 dc2 = new BasicDecimal64(2444422222222222, 0);
            //Assert.AreEqual(2444422222222222, dc2.getValue());
            //BasicDecimal64 dc3 = new BasicDecimal64(0, 1);
            //Assert.AreEqual(0.0, dc3.getValue());
            //BasicDecimal64 dc4 = new BasicDecimal64(0, 0);
            //Assert.AreEqual(0, dc4.getValue());
            //BasicDecimal64 dc5 = new BasicDecimal64(1, 5);
            //Assert.AreEqual(1.00000, dc5.getValue());

            BasicDecimal64 dc6 = new BasicDecimal64("2444422222222222.00001", 1);
            Assert.AreEqual("2444422222222222.0", dc6.getString());
            BasicDecimal64 dc7 = new BasicDecimal64("2444422222222222.11111", 0);
            Assert.AreEqual("2444422222222222", dc7.getString());
            BasicDecimal64 dc8 = new BasicDecimal64("0.00", 1);
            Assert.AreEqual("0.0", dc8.getString());
            BasicDecimal64 dc9 = new BasicDecimal64("0.00", 0);
            Assert.AreEqual("0", dc9.getString());
            BasicDecimal64 dc10 = new BasicDecimal64("1.000000000001", 5);
            Assert.AreEqual("1.00000", dc10.getString());
            BasicDecimal64 dc11 = new BasicDecimal64("1.000000000001", 9);
            Assert.AreEqual("1.000000000", dc11.getString());
            BasicDecimal64 dc12 = new BasicDecimal64("1.1", 0);
            Assert.AreEqual("1", dc12.getString());
            BasicDecimal64 dc13 = new BasicDecimal64("1.1", 1);
            Assert.AreEqual("1.1", dc13.getString());
            BasicDecimal64 dc14 = new BasicDecimal64("0", 1);
            Assert.AreEqual("0.0", dc14.getString());
        }

        [TestMethod]
        public void Test_BasicDecimal64_value_string()
        {
            BasicDecimal64 Decimal64 = new BasicDecimal64("0", 4);
            Assert.AreEqual("0.0000", Decimal64.getString());

            BasicDecimal64 re1 = new BasicDecimal64("-0.5900", 0);
            Assert.AreEqual("-1", re1.getString());

            BasicDecimal64 re2 = new BasicDecimal64("0.6900", 0);
            Assert.AreEqual("1", re2.getString());

            BasicDecimal64 re3 = new BasicDecimal64("0.4900", 0);
            Assert.AreEqual("0", re3.getString());

            BasicDecimal64 re4 = new BasicDecimal64("-0.4949", 2);
            Assert.AreEqual("-0.49", re4.getString());

            BasicDecimal64 re5 = new BasicDecimal64("-0.4950", 2);
            Assert.AreEqual("-0.50", re5.getString());

            BasicDecimal64 re6 = new BasicDecimal64("-0.4960", 2);
            Assert.AreEqual("-0.50", re6.getString());
        }

        [TestMethod]
        public void test_BasicDecimal64_value_string()
        {
            BasicDecimal64 Decimal64 = new BasicDecimal64("999999999999999999", 0);
            Assert.AreEqual("999999999999999999", Decimal64.getString());
            BasicDecimal64 Decimal64_1 = new BasicDecimal64("-999999999999999999", 0);
            Assert.AreEqual("-999999999999999999", Decimal64_1.getString());

            BasicDecimal64 Decimal641 = new BasicDecimal64("1232", -1);
            Assert.AreEqual("1232", Decimal641.getString());
            Assert.AreEqual(0, Decimal641.getScale());

            BasicDecimal64 Decimal642 = new BasicDecimal64("0.000001", 4);
            Assert.AreEqual("0.0000", Decimal642.getString());
            BasicDecimal64 Decimal64_12 = new BasicDecimal64("-0.000001", 4);
            Assert.AreEqual("0.0000", Decimal64_12.getString());

            BasicDecimal64 Decimal643 = new BasicDecimal64("0.000000000000000001", 18);
            Assert.AreEqual("0.000000000000000001", Decimal643.getString());
            BasicDecimal64 Decimal64_13 = new BasicDecimal64("-0.000000000000000001", 18);
            Assert.AreEqual("-0.000000000000000001", Decimal64_13.getString());
            BasicDecimal64 Decimal64_4 = new BasicDecimal64("0.999999999999999999", 18);
            Console.WriteLine(Decimal64_4.getString());
            Assert.AreEqual("0.999999999999999999", Decimal64_4.getString());
            BasicDecimal64 Decimal64_5 = new BasicDecimal64("-0.999999999999999999", 18);
            Assert.AreEqual("-0.999999999999999999", Decimal64_5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal64_value_decimal()
        {
            BasicDecimal64 Decimal64 = new BasicDecimal64(999999999999999999m, 0);
            Assert.AreEqual("999999999999999999", Decimal64.getString());
            BasicDecimal64 Decimal64_1 = new BasicDecimal64(-999999999999999999m, 0);
            Assert.AreEqual("-999999999999999999", Decimal64_1.getString());

            String ex = null;
            try
            {
                BasicDecimal64 Decimal641 = new BasicDecimal64(1232m, -1);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,18].", ex);

            BasicDecimal64 Decimal642 = new BasicDecimal64(0.000001m, 4);
            Assert.AreEqual("0.0000", Decimal642.getString());
            BasicDecimal64 Decimal64_12 = new BasicDecimal64(-0.0004561m, 4);
            Assert.AreEqual("-0.0005", Decimal64_12.getString());

            BasicDecimal64 Decimal643 = new BasicDecimal64(0.00000000000000000155m, 18);
            Assert.AreEqual("0.000000000000000002", Decimal643.getString());
            BasicDecimal64 Decimal64_13 = new BasicDecimal64(-0.000000000000000001m, 18);
            Assert.AreEqual("-0.000000000000000001", Decimal64_13.getString());

            BasicDecimal64 Decimal64_4 = new BasicDecimal64(0.999999999999999999m, 18);
            Console.WriteLine(Decimal64_4.getString());
            Assert.AreEqual("0.999999999999999999", Decimal64_4.getString());
            BasicDecimal64 Decimal64_5 = new BasicDecimal64(-0.999999999999999999m, 18);
            Assert.AreEqual("-0.999999999999999999", Decimal64_5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal32_value_decimal()
        {
            BasicDecimal32 Decimal32 = new BasicDecimal32(999999999m, 0);
            Assert.AreEqual(999999999, Decimal32.getDecimalValue());
            BasicDecimal32 Decimal32_1 = new BasicDecimal32(-999999999m, 0);
            Assert.AreEqual(-999999999, Decimal32_1.getDecimalValue());

            String ex = null;
            try
            {
                BasicDecimal32 Decimal321 = new BasicDecimal32(1232, -1);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,9].", ex);

            BasicDecimal32 Decimal322 = new BasicDecimal32(0.000561m, 4);
            Assert.AreEqual(0.0006m, Decimal322.getDecimalValue());
            BasicDecimal32 Decimal32_12 = new BasicDecimal32(-0.000001m, 4);
            Assert.AreEqual(0.0000m, Decimal32_12.getDecimalValue());

            BasicDecimal32 Decimal323 = new BasicDecimal32(0.000000001234m, 9);
            Assert.AreEqual(0.000000001m, Decimal323.getDecimalValue());
            BasicDecimal32 Decimal32_13 = new BasicDecimal32(-0.00000000166m, 9);
            Assert.AreEqual(-0.000000002m, Decimal32_13.getDecimalValue());
            BasicDecimal32 Decimal32_4 = new BasicDecimal32(0.999999999m, 9);
            Console.WriteLine(Decimal32_4.getDecimalValue());
            Assert.AreEqual(0.999999999m, Decimal32_4.getDecimalValue());
            BasicDecimal32 Decimal32_5 = new BasicDecimal32(-0.999999999m, 9);
            Assert.AreEqual(-0.999999999m, Decimal32_5.getDecimalValue());
        }

        [TestMethod]
        public void test_BasicDecimal64_value_string_no_scale()
        {
            BasicDecimal64 Decimal64 = new BasicDecimal64("999999999999999999");
            Assert.AreEqual("999999999999999999", Decimal64.getString());
            Assert.AreEqual(0, Decimal64.getScale());

            BasicDecimal64 Decimal64_1 = new BasicDecimal64("-999999999999999999");
            Assert.AreEqual("-999999999999999999", Decimal64_1.getString());
            Assert.AreEqual(0, Decimal64_1.getScale());

            BasicDecimal64 Decimal641 = new BasicDecimal64("1232", -1);
            Assert.AreEqual("1232", Decimal641.getString());
            Assert.AreEqual(0, Decimal641.getScale());

            BasicDecimal64 Decimal642 = new BasicDecimal64("0.000001");
            Assert.AreEqual("0.000001", Decimal642.getString());
            Assert.AreEqual(6, Decimal642.getScale());

            BasicDecimal64 Decimal64_12 = new BasicDecimal64("-0.000001");
            Assert.AreEqual("-0.000001", Decimal64_12.getString());
            Assert.AreEqual(6, Decimal64_12.getScale());


            BasicDecimal64 Decimal643 = new BasicDecimal64("0.000000000000000001");
            Assert.AreEqual("0.000000000000000001", Decimal643.getString());
            Assert.AreEqual(18, Decimal643.getScale());

            BasicDecimal64 Decimal64_13 = new BasicDecimal64("-0.000000000000000001");
            Assert.AreEqual("-0.000000000000000001", Decimal64_13.getString());
            Assert.AreEqual(18, Decimal64_13.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal64_string_not_support()
        {

            String ex = null;
            try
            {
                BasicDecimal64 deci = new BasicDecimal64((2 / 1000000.0).ToString(), 1);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("decimal data form is not correct: 2E-06", ex);
        }

        [TestMethod]
        public void test_BasicDecimal64_value_string_scale_18_overflow()
        {
            String re = null;
            try
            {
                BasicDecimal64 Decimal64_2 = new BasicDecimal64("9.9999999999999999999", 18);
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual("Decimal math overflow!", re);

            String re1 = null;
            try
            {
                BasicDecimal64 Decimal64_3 = new BasicDecimal64("-9.9999999999999999999", 18);
            }
            catch (Exception e)
            {
                re1 = e.Message;
            }
            Assert.AreEqual("Decimal math overflow!", re1);
        }
        [TestMethod]
        public void test_BasicDecimal64_value_string_scale_19()
        {
            String ex = null;
            try
            {
                BasicDecimal64 Decimal64 = new BasicDecimal64("1232", 19);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale 19 is out of bounds, it must be in [0,18].", ex);
        }



        //[TestMethod]
        //public void test_BasicDecimal64_getString1()
        //{
        //    BasicDecimal64 Decimal64_a = new BasicDecimal64((decimal)103, 6);
        //    //Assert.AreEqual("103.000000", Decimal64_a.getString());

        //    BasicDecimal32 Decimal32_a = new BasicDecimal32(103, 6);
        //    Assert.AreEqual("103.000000", Decimal32_a.getString());

        //    BasicDecimal64 Decimal64_b = new BasicDecimal64((long)-103, 6);
        //    Assert.AreEqual("-103.000000", Decimal64_b.getString());

        //    BasicDecimal64 Decimal64_6 = new BasicDecimal64("1.23231", 0);
        //    Assert.AreEqual("1", Decimal64_6.getString());

        //}

        //[TestMethod]
        //public void test_BasicDecimal64_getNumber()
        //{
        //    BasicDecimal64 Decimal64_a = new BasicDecimal64((long)103, 6);
        //    Assert.AreEqual((long)103, Decimal64_a.getNumber());

        //    BasicDecimal64 Decimal64_b = new BasicDecimal64((long)-103, 6);
        //    Console.Out.WriteLine(Decimal64_b.getNumber());
        //    Assert.AreEqual((long)-103, Decimal64_b.getNumber().longValue());

        //    BasicDecimal64 Decimal64_6 = new BasicDecimal64("1.23231", 18);
        //    Assert.AreEqual(1.23231, Decimal64_6.getNumber().doubleValue(), 5);
        //}

        [TestMethod]
        public void test_BasicDecimal64_run()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal64 re1 = (BasicDecimal64)conn.run("decimal64('1.003',4)");
            Assert.AreEqual("1.0030", re1.getString());
            Assert.AreEqual(1.003m, re1.getNumber().decimalValue());

            BasicDecimal64 re2 = (BasicDecimal64)conn.run("decimal64(-12.332,6)");
            Assert.AreEqual("-12.332000", re2.getString());
            Assert.AreEqual(-12.332m, re2.getNumber().decimalValue());

            BasicDecimal64 re3 = (BasicDecimal64)conn.run("decimal64(0,6)");
            Assert.AreEqual("0.000000", re3.getString());
            Assert.AreEqual(0m, re3.getNumber().decimalValue());

            BasicDecimal64 re4 = (BasicDecimal64)conn.run("decimal64(NULL,6)");
            Assert.AreEqual("", re4.getString());
            Assert.AreEqual(true, re4.isNull());
            Assert.AreEqual(-9223372036854.775808m, re4.getNumber().decimalValue());
            conn.close();
        }

        [TestMethod]
        public void test_BasicDecimal64_setNULL()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal64 re1 = (BasicDecimal64)conn.run("decimal64(1,6)");
            re1.setNull();
            Assert.AreEqual(true, re1.isNull());
            conn.close();
        }

        [TestMethod]
        public void test_BasicDecimal64_setRawData()
        {
            BasicDecimal64 Decimal64 = new BasicDecimal64("1.26431", 2);
            Assert.AreEqual(126, Decimal64.getRawData());
            Decimal64.setRawData(100);
            Assert.AreEqual(100, Decimal64.getRawData());
            Assert.AreEqual("1.00", Decimal64.getString());
            Decimal64.setRawData(9223372036854775807);
            Assert.AreEqual(9223372036854775807, Decimal64.getRawData());
            Assert.AreEqual("92233720368547758.07", Decimal64.getString());

            BasicDecimal64 Decimal641 = new BasicDecimal64("0.26431", 18);
            Assert.AreEqual("264310000000000000", Decimal641.getRawData().ToString());
            Decimal641.setRawData(0);
            Assert.AreEqual(0000000000000000000, Decimal641.getRawData());
            Assert.AreEqual("0.000000000000000000", Decimal641.getString());
            Decimal641.setRawData(-9223372036854775808);
            Assert.AreEqual(-9223372036854775808, Decimal641.getRawData());
            Assert.AreEqual("", Decimal641.getString());
        }

        [TestMethod]
        public void test_BasicDecimal64_getTemporal()
        {
            BasicDecimal64 Decimal64_5 = new BasicDecimal64("1.23231", 18);
            String re = null;
            try
            {
                Decimal64_5.getTemporal();
            }
            catch (Exception ex) {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("The method or operation is not implemented.") || re.Contains("未实现该方法或操作。"));

        }


        [TestMethod]
        public void Test_BasicDecimal64_getDataCategory()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("1.1234567", 5);
            Assert.AreEqual("DENARY", dc1.getDataCategory().ToString());

        }
        [TestMethod]
        public void Test_BasicDecimal64_getDataType()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("1.1234567", 5);
            Assert.AreEqual("DT_DECIMAL64", dc1.getDataType().ToString());
        }

        [TestMethod]
        public void Test_BasicDecimal64_setNull()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("1.1234567", 5);
            Assert.AreEqual("1.12346", dc1.getString());
            dc1.setNull();
            Assert.AreEqual(true, dc1.isNull());
        }

        [TestMethod]
        public void Test_BasicDecimal64_getNumber()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            Assert.AreEqual(123, dc1.getNumber().decimalValue());
            Assert.AreEqual(123, dc1.getDecimalValue());
            dc1.setNull();
            Assert.AreEqual((decimal)(long.MinValue), dc1.getNumber().decimalValue());

            BasicDecimal64 dc2 = new BasicDecimal64("123.00000000000000088", 15);
            Assert.AreEqual(123.000000000000001m, dc2.getNumber().decimalValue());
            Assert.AreEqual(123.000000000000001m, dc2.getDecimalValue());

            BasicDecimal64 dc3 = new BasicDecimal64("-0.1", 18);
            Assert.AreEqual(-0.100000000000000000m, dc3.getNumber().decimalValue());
            Assert.AreEqual(-0.100000000000000000m, dc3.getDecimalValue());
        }

        //[TestMethod]
        //public void Test_BasicDecimal64_getScale()
        //{
        //    BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
        //    Assert.AreEqual(0, dc1.getScale());
        //    BasicDecimal64 dc2 = new BasicDecimal64(1233456667777, 9);
        //    Assert.AreEqual(9, dc2.getScale());
        //}

        [TestMethod]
        public void Test_BasicDecimal64_getObject()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            Assert.AreEqual("123", dc1.getObject().ToString());
            BasicDecimal64 dc2 = new BasicDecimal64("1.00000001", 9);
            Assert.AreEqual("1.000000010", dc2.getObject().ToString());
        }

        [TestMethod]
        public void Test_BasicDecimal64_getString()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            Assert.AreEqual("123", dc1.getString());
            dc1.setNull();
            Assert.AreEqual("", dc1.getString());
            BasicDecimal64 dc2 = new BasicDecimal64("123.2", 5);
            Assert.AreEqual("123.20000", dc2.getString());
        }

        [TestMethod]
        public void Test_BasicDecimal64_toString()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            Assert.AreEqual("123", dc1.ToString());
            dc1.setNull();
            Assert.AreEqual("", dc1.ToString());
            BasicDecimal64 dc2 = new BasicDecimal64("123.2", 5);
            Assert.AreEqual("123.20000", dc2.ToString());
        }

        [TestMethod]
        public void Test_BasicDecimal64_hashBucket()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            try
            {
                dc1.hashBucket(1);
            }
            catch (Exception e)
            {
                string s = e.Message;
                Assert.AreEqual(true, s.Contains("The method or operation is not implemented.") || s.Contains("未实现该方法或操作。"));
            }

        }

        [TestMethod]
        public void Test_BasicDecimal64_setObject()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);

            Assert.AreEqual("123", dc1.getObject().ToString());
            dc1.setObject("1222");
            Assert.AreEqual("1222", dc1.getObject());
            dc1.setObject("1.222");
            Assert.AreEqual("1", dc1.getObject().ToString());
            dc1.setObject("12220");
            Assert.AreEqual("12220", dc1.getObject().ToString());
            dc1.setObject("3333");
            Assert.AreEqual("3333", dc1.getObject().ToString());

            BasicDecimal64 dc2 = new BasicDecimal64("123.2", 2);
            Assert.AreEqual("123.20", dc2.getObject().ToString());
            dc2.setObject("1222");
            Assert.AreEqual("1222.00", dc2.getObject().ToString());
            dc2.setObject("1.222");
            Assert.AreEqual("1.22", dc2.getObject().ToString());
            dc2.setObject("3333");
            Assert.AreEqual("3333.00", dc2.getObject().ToString());

            BasicDecimal64 dc3 = new BasicDecimal64("123.2", 0);

            String re = null;
            try
            {
                dc3.setObject((long)3333);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("the type of value must be string.", re);
        }

        [TestMethod]
        public void Test_BasicDecimal64_Equals()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            BasicDecimal64 dc2 = new BasicDecimal64("123.0", 2);
            BasicDecimal64 dc3 = new BasicDecimal64("123.0", 2);
            Assert.AreEqual(true, dc1.Equals(dc1));
            Assert.AreEqual(false, dc1.Equals(dc2));
            dc2.setNull();
            dc3.setNull();
            Assert.AreEqual(true, dc2.Equals(dc3));
        }

        [TestMethod]
        public void Test_BasicDecimal64_GetHashCode()
        {
            BasicDecimal64 dc1 = new BasicDecimal64("123.2", 0);
            BasicDecimal64 dc2 = new BasicDecimal64("123.0", 2);
            BasicDecimal64 dc3 = new BasicDecimal64("123.0", 2);
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
            dc1.setNull();
            dc2.setNull();
            dc3.setNull();
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
        }

        [TestMethod]
        public void Test_BasicDecimal64_createBasicDecimal64()
        {
            BasicDecimal64 dc1 = Utils.createBasicDecimal64(-999999999, 0);
            Assert.AreEqual("-999999999", dc1.getString());

            BasicDecimal64 dc2 = Utils.createBasicDecimal64(999999999999999999, 10);
            Assert.AreEqual("99999999.9999999999", dc2.getString());

            BasicDecimal64 dc3 = Utils.createBasicDecimal64(-999999999999999999, 18);
            Assert.AreEqual("-0.999999999999999999", dc3.getString());

            BasicDecimal64 dc4 = Utils.createBasicDecimal64(-999999999, 12);
            Assert.AreEqual("-0.000999999999", dc4.getString());

            BasicDecimal64 dc5 = Utils.createBasicDecimal64(99999000000000000, 1);
            Assert.AreEqual("9999900000000000.0", dc5.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_value_string()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("1232", 4);
            Assert.AreEqual("1232.0000", Decimal128.getString());

            BasicDecimal128 re1 = new BasicDecimal128("-0.5900", 0);
            Assert.AreEqual("-1", re1.getString());

            BasicDecimal128 re2 = new BasicDecimal128("0.6900", 0);
            Assert.AreEqual("1", re2.getString());

            BasicDecimal128 re3 = new BasicDecimal128("0.4900", 0);
            Assert.AreEqual("0", re3.getString());

            BasicDecimal128 re4 = new BasicDecimal128("-0.4949", 2);
            Assert.AreEqual("-0.49", re4.getString());

            BasicDecimal128 re5 = new BasicDecimal128("-0.4950", 2);
            Assert.AreEqual("-0.50", re5.getString());

            BasicDecimal128 re6 = new BasicDecimal128("-0.4960", 2);
            Assert.AreEqual("-0.50", re6.getString());

            BasicDecimal128 re7 = new BasicDecimal128("9.99090", -1);
            Assert.AreEqual("9.99090", re7.ToString());
            Assert.AreEqual(5, re7.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal128_value_string_1()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("121.23", 6);
            Assert.AreEqual("121.230000", Decimal128.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_value_decimal()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128(1234m, 4);
            Assert.AreEqual("1234.0000", Decimal128.getString());

            BasicDecimal128 re1 = new BasicDecimal128(-1234m, 0);
            Assert.AreEqual("-1234", re1.getString());

            BasicDecimal128 re2 = new BasicDecimal128(79228162514264337593543950335m, 0);
            Assert.AreEqual("79228162514264337593543950335", re2.getString());

            BasicDecimal128 re3 = new BasicDecimal128(-79228162514264337593543950335m, 0);
            Assert.AreEqual("-79228162514264337593543950335", re3.getString());

            BasicDecimal128 re4 = new BasicDecimal128(0.0000000000000000000000000001m, 38);
            Assert.AreEqual("0.00000000000000000000000000010000000000", re4.getString());

            BasicDecimal128 re5 = new BasicDecimal128(-0.0000000000000000000000000001m, 38);
            Assert.AreEqual("-0.00000000000000000000000000010000000000", re5.getString());

            String ex = null;
            try
            {
                BasicDecimal128 Decimal1281 = new BasicDecimal128(1232m, -1);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale -1 is out of bounds, it must be in [0,38].", ex);
        }
        [TestMethod]
        public void test_BasicDecimal128_scale_0()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("123.2", 0);
            Assert.AreEqual("123", Decimal128.getString());
            Assert.AreEqual(123m, Decimal128.getDecimalValue());
            BasicDecimal128 Decimal1281 = new BasicDecimal128("-123.2", 0);
            Assert.AreEqual("-123", Decimal1281.getString());
            Assert.AreEqual(-123, Decimal1281.getDecimalValue());
            BasicDecimal128 Decimal1282 = new BasicDecimal128("0", 0);
            Assert.AreEqual("0", Decimal1282.getString());
            Assert.AreEqual(0, Decimal1282.getDecimalValue());
        }

        [TestMethod]
        public void test_BasicDecimal128_scale_18()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("1.2312", 18);
            Assert.AreEqual("1.231200000000000000", Decimal128.getString());
            Assert.AreEqual(1.2312m, Decimal128.getNumber().decimalValue());
            BasicDecimal128 Decimal1281 = new BasicDecimal128("-1.2312", 18);
            Assert.AreEqual("-1.231200000000000000", Decimal1281.getString());
            Assert.AreEqual(-1.2312m, Decimal1281.getNumber().decimalValue());
        }

        [TestMethod]
        public void test_BasicDecimal128_scale_19()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("1.2312", 19);
            Assert.AreEqual("1.2312000000000000000", Decimal128.getString());
            Assert.AreEqual(1.2312000000000000000m, Decimal128.getNumber().decimalValue());
            BasicDecimal128 Decimal1281 = new BasicDecimal128("-1.2312", 19);
            Assert.AreEqual("-1.2312000000000000000", Decimal1281.getString());
            Assert.AreEqual(-1.2312000000000000000m, Decimal1281.getNumber().decimalValue());
            BasicDecimal128 Decimal1282 = new BasicDecimal128("1.2312000000000000001123456", 19);
            Assert.AreEqual("1.2312000000000000001", Decimal1282.getString());
            Assert.AreEqual(1.2312000000000000001m, Decimal1282.getNumber().decimalValue());
        }
        [TestMethod]
        public void test_BasicDecimal128_scale_30()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("99999999.999999999999999999999999999999", 30);
            Assert.AreEqual("99999999.999999999999999999999999999999", Decimal128.getString());
            BasicDecimal128 Decimal1281 = new BasicDecimal128("99999999.000000000000000000000000000009", 30);
            Assert.AreEqual("99999999.000000000000000000000000000009", Decimal1281.getString());
            BasicDecimal128 Decimal1282 = new BasicDecimal128("-99999999.999999999999999999999999999999", 30);
            Assert.AreEqual("-99999999.999999999999999999999999999999", Decimal1282.getString());
            BasicDecimal128 Decimal1283 = new BasicDecimal128("-99999999.000000000000000000000000000009", 30);
            Assert.AreEqual("-99999999.000000000000000000000000000009", Decimal1283.getString());
        }
        [TestMethod]
        public void test_BasicDecimal128_scale_37()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("9.9999999999999999999999999999999999999", 37);
            Assert.AreEqual("9.9999999999999999999999999999999999999", Decimal128.getString());
            BasicDecimal128 Decimal1281 = new BasicDecimal128("9.9999999000000000000000000000000000009", 37);
            Assert.AreEqual("9.9999999000000000000000000000000000009", Decimal1281.getString());
            BasicDecimal128 Decimal1282 = new BasicDecimal128("-9.9999999999999999999999999999999999999", 37);
            Assert.AreEqual("-9.9999999999999999999999999999999999999", Decimal1282.getString());
            BasicDecimal128 Decimal1283 = new BasicDecimal128("-9.9999999000000000000000000000000000009", 37);
            Assert.AreEqual("-9.9999999000000000000000000000000000009", Decimal1283.getString());
            BasicDecimal128 Decimal1284 = new BasicDecimal128("0", 37);
            Assert.AreEqual("0.0000000000000000000000000000000000000", Decimal1284.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_string_not_support()
        {
            
            String ex = null;
            try
            {
                BasicDecimal128 deci = new BasicDecimal128((2 / 1000000.0).ToString(), 38);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("decimal data form is not correct: 2E-06", ex);
        }

        [TestMethod]
        public void test_BasicDecimal128_scale_not_true()
        {
            String ex = null;
            try
            {
                BasicDecimal128 Decimal128 = new BasicDecimal128("9.9999999999999999999999999999999999999", 39);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Scale 39 is out of bounds, it must be in [0,38].", ex);
            String ex2 = null;  

            try
            {
                BasicDecimal128 Decimal128 = new BasicDecimal128("9.999", -2);
            }
            catch (Exception E)
            {
                ex2 = E.Message;
            }
            Assert.AreEqual("Scale -2 is out of bounds, it must be in [0,38].", ex2);
        }

        [TestMethod]
        public void test_BasicDecimal128_dataValue_not_true()
        {
            String ex = null;
            try
            {
                BasicDecimal128 tmp_128 = new BasicDecimal128("-170141183460469231731687303715884105729", 5);
            }
            catch (Exception E)
            {
                ex = E.Message;
            }
            Assert.AreEqual("Decimal math overflow!", ex);
            String ex1 = null;
            try
            {
                BasicDecimal128 tmp_128 = new BasicDecimal128("170141183460469231731687303715884105729", 5);
            }
            catch (Exception E)
            {
                ex1 = E.Message;
            }
            Assert.AreEqual("Decimal math overflow!", ex1);
        }

        [TestMethod]
        public void test_BasicDecimal128_getString()
        {
            BasicDecimal128 Decimal128_a = new BasicDecimal128("103", 6);
            Assert.AreEqual("103.000000", Decimal128_a.getString());

            BasicDecimal128 Decimal128_b = new BasicDecimal128("-103", 6);
            Assert.AreEqual("-103.000000", Decimal128_b.getString());

            BasicDecimal128 Decimal128_c = new BasicDecimal128("0", 6);
            Assert.AreEqual("0.000000", Decimal128_c.getString());

            BasicDecimal128 Decimal128 = new BasicDecimal128("1.23", 6);
            Assert.AreEqual("1.230000", Decimal128.getString());

            BasicDecimal128 Decimal128_2 = new BasicDecimal128("0.003", 6);
            Assert.AreEqual("0.003000", Decimal128_2.getString());

            BasicDecimal128 Decimal128_3 = new BasicDecimal128("-1.23", 6);
            Assert.AreEqual("-1.230000", Decimal128_3.getString());

            BasicDecimal128 Decimal128_4 = new BasicDecimal128("-0.003", 6);
            Assert.AreEqual("-0.003000", Decimal128_4.getString());

            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 2);
            Assert.AreEqual("1.23", Decimal128_5.getString());

            BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 2);
            Assert.AreEqual("-1.23", Decimal128_6.getString());

            BasicDecimal128 Decimal128_7 = new BasicDecimal128("-1.23231", 0);
            Assert.AreEqual("-1", Decimal128_7.getString());

            BasicDecimal128 Decimal128_8 = new BasicDecimal128("1.23231", 0);
            Assert.AreEqual("1", Decimal128_8.getString());

            BasicDecimal128 Decimal128_9 = new BasicDecimal128("1.23231", 37);
            Assert.AreEqual("1.2323100000000000000000000000000000000", Decimal128_9.getString());

            BasicDecimal128 Decimal128_10 = new BasicDecimal128("-1.23231", 37);
            Assert.AreEqual("-1.2323100000000000000000000000000000000", Decimal128_10.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_ToString()
        {
            BasicDecimal128 Decimal128_a = new BasicDecimal128("103", 6);
            Assert.AreEqual("103.000000", Decimal128_a.ToString());

            BasicDecimal128 Decimal128_b = new BasicDecimal128("-103", 6);
            Assert.AreEqual("-103.000000", Decimal128_b.ToString());

            BasicDecimal128 Decimal128_c = new BasicDecimal128("0", 6);
            Assert.AreEqual("0.000000", Decimal128_c.ToString());

            BasicDecimal128 Decimal128 = new BasicDecimal128("1.23", 6);
            Assert.AreEqual("1.230000", Decimal128.ToString());

            BasicDecimal128 Decimal128_2 = new BasicDecimal128("0.003", 6);
            Assert.AreEqual("0.003000", Decimal128_2.ToString());

            BasicDecimal128 Decimal128_3 = new BasicDecimal128("-1.23", 6);
            Assert.AreEqual("-1.230000", Decimal128_3.ToString());

            BasicDecimal128 Decimal128_4 = new BasicDecimal128("-0.003", 6);
            Assert.AreEqual("-0.003000", Decimal128_4.ToString());

            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 2);
            Assert.AreEqual("1.23", Decimal128_5.ToString());

            BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 2);
            Assert.AreEqual("-1.23", Decimal128_6.ToString());

            BasicDecimal128 Decimal128_7 = new BasicDecimal128("-1.23231", 0);
            Assert.AreEqual("-1", Decimal128_7.ToString());

            BasicDecimal128 Decimal128_8 = new BasicDecimal128("1.23231", 0);
            Assert.AreEqual("1", Decimal128_8.ToString());

            BasicDecimal128 Decimal128_9 = new BasicDecimal128("1.23231", 37);
            Assert.AreEqual("1.2323100000000000000000000000000000000", Decimal128_9.ToString());

            BasicDecimal128 Decimal128_10 = new BasicDecimal128("-1.23231", 37);
            Assert.AreEqual("-1.2323100000000000000000000000000000000", Decimal128_10.ToString());
        }


        [TestMethod]
        public void test_BasicDecimal128_getNumber()
        {
            BasicDecimal128 Decimal128_a = new BasicDecimal128("103", 6);
            Assert.AreEqual(103.000000m, Decimal128_a.getNumber().decimalValue());
            Assert.AreEqual(103.000000m, Decimal128_a.getDecimalValue());

            BasicDecimal128 Decimal128_b = new BasicDecimal128("-103", 6);
            Assert.AreEqual(-103.000000m, Decimal128_b.getNumber().decimalValue());
            Assert.AreEqual(-103.000000m, Decimal128_b.getDecimalValue());

            BasicDecimal128 Decimal128_c = new BasicDecimal128("0", 6);
            Assert.AreEqual(0.000000m, Decimal128_c.getNumber().decimalValue());
            Assert.AreEqual(0.000000m, Decimal128_c.getDecimalValue());


            BasicDecimal128 Decimal128 = new BasicDecimal128("1.23", 6);
            Assert.AreEqual(1.230000m, Decimal128.getNumber().decimalValue());
            Assert.AreEqual(1.230000m, Decimal128.getDecimalValue());

            BasicDecimal128 Decimal128_2 = new BasicDecimal128("0.003", 6);
            Assert.AreEqual(0.003000m, Decimal128_2.getNumber().decimalValue());

            BasicDecimal128 Decimal128_3 = new BasicDecimal128("-1.23", 6);
            Assert.AreEqual(-1.230000m, Decimal128_3.getNumber().decimalValue());

            BasicDecimal128 Decimal128_4 = new BasicDecimal128("-0.003", 6);
            Assert.AreEqual(-0.003000m, Decimal128_4.getNumber().decimalValue());

            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 2);
            Assert.AreEqual(1.23m, Decimal128_5.getNumber().decimalValue());

            BasicDecimal128 Decimal128_6 = new BasicDecimal128("-1.23231", 2);
            Assert.AreEqual(-1.23m, Decimal128_6.getNumber().decimalValue());

            BasicDecimal128 Decimal128_7 = new BasicDecimal128("-1.53231", 0);
            Assert.AreEqual(-2m, Decimal128_7.getNumber().decimalValue());
            Assert.AreEqual(-2m, Decimal128_7.getDecimalValue());

            BasicDecimal128 Decimal128_8 = new BasicDecimal128("1.73231", 0);
            Assert.AreEqual(2m, Decimal128_8.getNumber().decimalValue());
            Assert.AreEqual(2m, Decimal128_8.getDecimalValue());

            BasicDecimal128 Decimal128_9 = new BasicDecimal128("-1.23231", 28);
            Assert.AreEqual(-1.2323100000000000000000000000m, Decimal128_9.getNumber().decimalValue());
            Assert.AreEqual(-1.2323100000000000000000000000m, Decimal128_9.getDecimalValue());


            BasicDecimal128 Decimal128_10 = new BasicDecimal128("1.23231", 28);
            Assert.AreEqual(1.2323100000000000000000000000m, Decimal128_10.getNumber().decimalValue());
            Assert.AreEqual(1.2323100000000000000000000000m, Decimal128_10.getDecimalValue());
        }

        [TestMethod]
        public void test_BasicDecimal128_run()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = (BasicDecimal128)conn.run("decimal128('1.003',4)");
            Assert.AreEqual("1.0030", re1.getString());
            Assert.AreEqual(1.003m, re1.getNumber().decimalValue());
            conn.close();
        }

        [TestMethod]
        public void test_BasicDecimal128_run2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = (BasicDecimal128)conn.run("decimal128(-12.332,6)");
            Assert.AreEqual("-12.332000", re1.getString());
            //Assert.AreEqual(new BigDecimal("-12.332"), re1.getNumber());
            conn.close();
        }

        [TestMethod]
        public void test_BasicDecimal128_run3()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = (BasicDecimal128)conn.run("decimal128(0,6)");
            Assert.AreEqual("0.000000", re1.getString());
           // Assert.AreEqual(new BigDecimal("0"), re1.getNumber());
        }

        [TestMethod]
        public void test_BasicDecimal128_run_NULL()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = (BasicDecimal128)conn.run("decimal128(NULL,6)");
            Assert.AreEqual("", re1.getString());
            Assert.AreEqual(true, re1.isNull());
            String re = null;
            try {
                re1.getNumber().decimalValue();
            } catch (Exception e) {
                re = e.Message;
            }
            Assert.AreEqual(true, re.Contains("Value was either too large or too small for a Decimal.") || re.Contains("值对于 Decimal 太大或太小。"));
        }

        [TestMethod]
        public void test_BasicDecimal128_run_NULL_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = (BasicDecimal128)conn.run("decimal64(NULL,18) * decimal64(NULL,1)");
            Assert.AreEqual("", re1.getString());
            Assert.AreEqual(true, re1.isNull());
            Assert.AreEqual("", re1.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_setNULL()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDecimal128 re1 = new BasicDecimal128("0.003", 6);
            re1.setNull();
            Assert.AreEqual(true, re1.isNull());
            BasicDecimal128 re2 = (BasicDecimal128)conn.run("decimal128(NULL,3)");
            Assert.AreEqual(true, re2.isNull());
        }

        [TestMethod]
        public void test_BasicDecimal128_setRawData()
        {
            BasicDecimal128 Decimal128 = new BasicDecimal128("1.23231", 2);
            Assert.AreEqual(123, Decimal128.getRawData());
            Decimal128.setRawData(100);
            Assert.AreEqual(100, Decimal128.getRawData());
            Assert.AreEqual("1.00", Decimal128.getString());
            Decimal128.setRawData(-9223372036854775808);
            Assert.AreEqual(-9223372036854775808, Decimal128.getRawData());
            Assert.AreEqual("-92233720368547758.08", Decimal128.getString());

            BasicDecimal128 Decimal1281 = new BasicDecimal128("1.23231", 37);
            Assert.AreEqual("12323100000000000000000000000000000000", Decimal1281.getRawData().ToString());
            Decimal1281.setRawData(0);
            Assert.AreEqual(00000000000000000000000000000000000000, Decimal1281.getRawData());
            Assert.AreEqual("0.0000000000000000000000000000000000000", Decimal1281.getString());
            Decimal1281.setRawData(-9223372036854775808);
            Assert.AreEqual(-9223372036854775808, Decimal1281.getRawData());
            Assert.AreEqual("-0.0000000000000000009223372036854775808", Decimal1281.getString());
        }

        [TestMethod]
        public void test_BasicDecimal128_getScale()
        {
            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 2);
            Assert.AreEqual(2, Decimal128_5.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal128_getScale_0()
        {
            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 0);
            Assert.AreEqual(0, Decimal128_5.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal128_getScale_18()
        {
            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 18);
            Assert.AreEqual(18, Decimal128_5.getScale());
        }

        [TestMethod]
        public void test_BasicDecimal128_getTemporal()
        {
            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 18);
            String re = null;
            try
            {
                Decimal128_5.getTemporal();
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual(true, re.Contains("The method or operation is not implemented.") || re.Contains("未实现该方法或操作。"));
        }

        [TestMethod]
        public void test_BasicDecimal128_hashBucket()
        {
            BasicDecimal128 Decimal128_5 = new BasicDecimal128("1.23231", 9);
            String re = null;
            try
            {
                Decimal128_5.hashBucket(1);
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual(true, re.Contains("The method or operation is not implemented.") || re.Contains("未实现该方法或操作。"));
        }

        [TestMethod]
        public void Test_BasicDecimal128_Equals()
        {
            BasicDecimal128 dc1 = new BasicDecimal128("123.2", 0);
            BasicDecimal128 dc2 = new BasicDecimal128("123.0", 2);
            BasicDecimal128 dc3 = new BasicDecimal128("123.0", 2);
            Assert.AreEqual(true, dc1.Equals(dc1));
            Assert.AreEqual(false, dc1.Equals(dc2));
            dc2.setNull();
            dc3.setNull();
            Assert.AreEqual(true, dc2.Equals(dc3));
        }

        [TestMethod]
        public void Test_BasicDecimal128_GetHashCode()
        {
            BasicDecimal128 dc1 = new BasicDecimal128("123.2", 0);
            BasicDecimal128 dc2 = new BasicDecimal128("123.0", 2);
            BasicDecimal128 dc3 = new BasicDecimal128("123.0", 2);
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
            dc1.setNull();
            dc2.setNull();
            dc3.setNull();
            Assert.AreNotEqual(dc1.GetHashCode(), dc2.GetHashCode());
            Assert.AreEqual(dc2.GetHashCode(), dc3.GetHashCode());
        }

        [TestMethod]
        public void test_BasicDecimal128_createBasicDecimal128()
        {
            BasicDecimal128 Decimal128 = Utils.createBasicDecimal128(new BigInteger(1234), 4);
            Assert.AreEqual("0.1234", Decimal128.getString());

            BasicDecimal128 re1 = Utils.createBasicDecimal128(new BigInteger(-1234), 0);
            Assert.AreEqual("-1234", re1.getString());

            BasicDecimal128 re2 = Utils.createBasicDecimal128(BigInteger.Parse("10000000000000000000000000000000000000"), 0);
            Assert.AreEqual("10000000000000000000000000000000000000", re2.getString());

            BasicDecimal128 re3 = Utils.createBasicDecimal128(BigInteger.Parse("-10000000000000000000000000000000000000"), 0);
            Assert.AreEqual("-10000000000000000000000000000000000000", re3.getString());

            BasicDecimal128 re4 = Utils.createBasicDecimal128(BigInteger.Parse("-1"), 38);
            Assert.AreEqual("-0.00000000000000000000000000000000000001", re4.getString());

            BasicDecimal128 re5 = Utils.createBasicDecimal128(BigInteger.Parse("0"), 38);
            Assert.AreEqual("0.00000000000000000000000000000000000000", re5.getString());
        }
        [TestMethod]
        public void test_BasicComplex()
        {
            BasicComplex bc = new BasicComplex(25.14, 42.33);
            Assert.AreEqual(DATA_CATEGORY.BINARY, bc.getDataCategory());
            Assert.AreEqual(DATA_TYPE.DT_COMPLEX, bc.getDataType());
            Assert.AreEqual(25.14, bc.getDouble2().x, 0);
            Assert.AreEqual(42.33, bc.getDouble2().y, 0);
            Assert.AreEqual("25.14+42.33i", bc.getString());
            Assert.AreEqual(25.14, bc.getReal());
            Assert.AreEqual(42.33, bc.getImage());
            Assert.AreEqual(25.14, bc.getValue().x, 0);
            Assert.AreEqual(42.33, bc.getValue().y, 0);
            Assert.IsFalse(bc.isNull());
            Assert.IsFalse(bc.equals(null));
            bc.setNull();
            Assert.IsTrue(bc.isNull());
            Assert.AreEqual("", bc.getString());
            Assert.AreEqual(double.MinValue, bc.getDouble2().x, 0);
            Assert.AreEqual(double.MinValue, bc.getDouble2().y, 0);

            BasicComplex bc1 = new BasicComplex(-25.14, -42.33);
            Assert.AreEqual("-25.14-42.33i", bc1.getString());
        }

        [TestMethod]
        public void test_BasicComplex_not_support()
        {
            BasicComplex bc = new BasicComplex(25.14, 42.33);
            String re = null;
            try
            {
                bc.getNumber();
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual(true, re.Contains("Imcompatible data type"));

            String re1 = null;
            try
            {
                bc.getObject();
            }
            catch (Exception e)
            {
                re1 = e.Message;
            }
            Assert.AreEqual(true, re1.Contains("The method or operation is not implemented.") || re1.Contains("未实现该方法或操作。"));
            
            String re2 = null;
            try
            {
                bc.setObject(0);
            }
            catch (Exception e)
            {
                re2 = e.Message;
            }
            Assert.AreEqual(true, re2.Contains("The method or operation is not implemented.") || re2.Contains("未实现该方法或操作。"));
            
            String re3 = null;
            try
            {
                bc.getTemporal();
            }
            catch (Exception e)
            {
                re3 = e.Message;
            }
            Assert.AreEqual(true, re3.Contains("Imcompatible data type"));
        }

        [TestMethod]
        public void test_BasicComplex_Equals()
        {
            BasicComplex bc = new BasicComplex(25.14, 42.33);
            BasicComplex bc1 = new BasicComplex(-25.14, -42.33);
            Assert.IsTrue(bc.Equals(bc));
            Assert.IsFalse(bc.Equals(bc1));
        }

        [TestMethod]
        public void test_BasicComplex_hashBucket()
        {
            BasicComplex bc = new BasicComplex(25.14, 42.33);
            Assert.AreEqual(-1, bc.hashBucket(1));
            Assert.AreEqual(-1, bc.hashBucket(2));
        }
        [TestMethod]
        public void test_BasicComplex_GetHashCode()
        {
            BasicComplex bc = new BasicComplex(25.14, 42.33);
            Assert.AreEqual(2061676100, bc.GetHashCode());
        }

        [TestMethod]
        public void test_BasicPoint()
        {
            BasicPoint bc = new BasicPoint(25.14, 42.33);
            Assert.AreEqual(DATA_CATEGORY.BINARY, bc.getDataCategory());
            Assert.AreEqual(DATA_TYPE.DT_POINT, bc.getDataType());
            Assert.AreEqual(25.14, bc.getDouble2().x, 0);
            Assert.AreEqual(42.33, bc.getDouble2().y, 0);
            Assert.AreEqual("(25.14, 42.33)", bc.getString());
            Assert.AreEqual(25.14, bc.getX());
            Assert.AreEqual(42.33, bc.getY());
            Assert.AreEqual(25.14, bc.getValue().x, 0);
            Assert.AreEqual(42.33, bc.getValue().y, 0);
            Assert.IsFalse(bc.isNull());
            Assert.IsFalse(bc.equals(null));
            bc.setNull();
            Assert.IsTrue(bc.isNull());
            Assert.AreEqual("(, )", bc.getString());
            Assert.AreEqual(double.MinValue, bc.getDouble2().x, 0);
            Assert.AreEqual(double.MinValue, bc.getDouble2().y, 0);

            BasicPoint bc1 = new BasicPoint(-25.14, -42.33);
            Assert.AreEqual("(-25.14, -42.33)", bc1.getString());
        }

        [TestMethod]
        public void test_BasicPoint_not_support()
        {
            BasicPoint bc = new BasicPoint(25.14, 42.33);
            String re = null;
            try
            {
                bc.getNumber();
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual(true, re.Contains("Imcompatible data type"));

            String re1 = null;
            try
            {
                bc.getObject();
            }
            catch (Exception e)
            {
                re1 = e.Message;
            }
            Assert.AreEqual(true, re1.Contains("The method or operation is not implemented.") || re1.Contains("未实现该方法或操作。"));

            String re2 = null;
            try
            {
                bc.setObject(0);
            }
            catch (Exception e)
            {
                re2 = e.Message;
            }
            Assert.AreEqual(true, re2.Contains("The method or operation is not implemented.") || re2.Contains("未实现该方法或操作。"));

            String re3 = null;
            try
            {
                bc.getTemporal();
            }
            catch (Exception e)
            {
                re3 = e.Message;
            }
            Assert.AreEqual(true, re3.Contains("Imcompatible data type"));
        }

        [TestMethod]
        public void test_BasicPoint_Equals()
        {
            BasicPoint bc = new BasicPoint(25.14, 42.33);
            BasicPoint bc1 = new BasicPoint(-25.14, -42.33);
            Assert.IsTrue(bc.Equals(bc));
            Assert.IsFalse(bc.Equals(bc1));
        }

        [TestMethod]
        public void test_BasicPoint_hashBucket()
        {
            BasicPoint bc = new BasicPoint(25.14, 42.33);
            Assert.AreEqual(-1, bc.hashBucket(1));
            Assert.AreEqual(-1, bc.hashBucket(2));
        }
        [TestMethod]
        public void test_BasicPoint_GetHashCode()
        {
            BasicPoint bc = new BasicPoint(25.14, 42.33);
            Assert.AreEqual(2061676100, bc.GetHashCode());
        }

    }
}

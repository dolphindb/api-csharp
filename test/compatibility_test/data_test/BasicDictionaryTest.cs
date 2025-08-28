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
using static dolphindb_csharp_api_test.data_test.AbstractTest;

namespace dolphindb_csharp_api_test.compatibility_test.data_test
{
    [TestClass]
    public class BasicDictionaryTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private ExtendedDataOutput @out;


        [TestMethod]
        public void Test_BasicDictionary()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            Exception ex = null;
            try
            {
                BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_VOID, DATA_TYPE.DT_VOID, 1);

            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.IsNotNull(ex);
            Exception ex1 = null;
            try
            {
                BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_ANY, DATA_TYPE.DT_VOID, 1);

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.IsNotNull(ex1);
            Exception ex2 = null;
            try
            {
                BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_DICTIONARY, DATA_TYPE.DT_VOID, 1);

            }
            catch (Exception e)
            {
                ex2 = e;
            }
            Assert.IsNotNull(ex2);
            BasicDictionary sc = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_INT, 100);
            sc.put((IScalar)conn.run("'1'"), (IEntity)conn.run("1:2"));
            Assert.AreEqual("DF_DICTIONARY", sc.getDataForm().ToString());
            Assert.AreEqual("DT_INT", sc.getDataType().ToString());
            Assert.AreEqual("INTEGRAL", sc.getDataCategory().ToString());
            Assert.AreEqual(false, sc.ContainsKey((IScalar)conn.run("'1'")));
            Assert.AreEqual(0, sc.rows());
            Assert.AreEqual(1, sc.columns());
            conn.close();
        }
        [TestMethod]
        public void Test_BasicDictionary_put()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDictionary sc = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_DATE);
            sc.put((IScalar)conn.run("1"), (IEntity)conn.run("2013.06.13"));
            Assert.AreEqual("True", sc.isDictionary().ToString());
            BasicDate v = (BasicDate)sc.get(new BasicInt(1));
            Assert.AreEqual(new DateTime(2013, 06, 13), v.getValue());
            conn.close();
        }
        [TestMethod]
        public void Test_BasicDictionary_String()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDictionary sc = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_ANY);
            Console.Out.WriteLine(sc.put((IScalar)conn.run("1"), (IEntity)conn.run("2013.06.13 1234")));
            sc.put((IScalar)conn.run("1"), (IEntity)conn.run("2013.06.13 1234"));
            Assert.AreEqual("1->(2013.06.13,1234)\n", sc.String.ToString());
            sc.put((IScalar)conn.run("2"), (IEntity)conn.run("2013.06.13 1234"));
            Assert.AreEqual("1->(2013.06.13,1234)\n2->(2013.06.13,1234)\n", sc.String.ToString());

            BasicAnyVector v = (BasicAnyVector)conn.run("[1 2 3,3.4 3.5 3.6]");
            BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_VOID);
            Console.Out.WriteLine(sc1.put((IScalar)conn.run("1"), v));
            Assert.AreEqual("{}->{}", sc1.String.ToString());

            BasicInt v2 = (BasicInt)conn.run("1");
            BasicDictionary sc2 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_INT);
            Console.Out.WriteLine(sc2.put((IScalar)conn.run("1"), v2));
            Assert.AreEqual("{1}->{1}", sc2.String.ToString());
            Console.Out.WriteLine(sc2.put((IScalar)conn.run("2"), v2));
            Assert.AreEqual("{1,2}->{1,1}", sc2.String.ToString());

            conn.close();
        }
        [TestMethod]
        public void Test_BasicDictionary_write()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_DICTIONARY);
            @out = null;
            Exception ex1 = null;
            try
            {
                sc1.write(@out);

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.AreEqual("Can't streamlize the dictionary with value type DT_DICTIONARY", ex1.Message);
            BasicDictionary sc2 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_INT);
            sc2.put((IScalar)conn.run("1"), (BasicInt)conn.run("1"));
            Stream outStream = new MemoryStream();
            ExtendedDataOutput out1 = new BigEndianDataOutputStream(outStream);
            sc2.write(out1);
            Assert.AreEqual("{1}->{1}", sc2.String.ToString());
            conn.close();
        }
        [TestMethod]
        public void Test_BasicDictionary_writeCompressed()
        {
            BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_DICTIONARY);
            @out = null;
            Exception ex1 = null;
            try
            {
                sc1.writeCompressed(@out);

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.AreEqual("The method or operation is not implemented.", ex1.Message);
        }
        [TestMethod]
        public void Test_BasicDictionary_getString()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_INT);
            Console.Out.WriteLine(sc1.put((IScalar)conn.run("1"), (BasicInt)conn.run("1")));
            Exception ex1 = null;
            try
            {
                sc1.getString();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.AreEqual("The method or operation is not implemented.",ex1.Message);
            conn.close();
        }
        [TestMethod]
        public void Test_BasicDictionary_getObject()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicDictionary sc1 = new BasicDictionary(DATA_TYPE.DT_INT, DATA_TYPE.DT_INT);
            Console.Out.WriteLine(sc1.put((IScalar)conn.run("1"), (BasicInt)conn.run("1")));
            Exception ex1 = null;
            try
            {
                sc1.getObject();

            }
            catch (Exception e)
            {
                ex1 = e;
            }
            Assert.AreEqual("The method or operation is not implemented.", ex1.Message);
            conn.close();
        }

    }
}

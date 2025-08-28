using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using dolphindb.route;
using dolphindb_config;

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class Domain_Test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;

        [TestMethod]
        public void Test_ListDomain_Create()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicIntVector biv1 = new BasicIntVector(new int[] { 1, 2, 3, 4 });
            try
            {
                ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.INTEGRAL);
            }
            catch(Exception e)
            {
                Assert.AreEqual("The input list must be a tuple.", e.Message);
                Console.WriteLine(e.Message);
            }

        }

        [TestMethod]
        public void Test_ListDomain_DATA_TYPE_DT_ANY()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello]");
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            
        }
        [TestMethod]
        public void Test_ListDomain_getPartitionKey()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IScalar tt = new BasicInt(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_ListDomain_getPartitionKey_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.LITERAL);
            IScalar tt = new BasicString("hello");
            Assert.AreEqual(1, Ld1.getPartitionKey(tt));
            IScalar tt1 = new BasicString("QQQ");
            Assert.AreEqual(2, Ld1.getPartitionKey(tt1));
        }

        [TestMethod]
        public void Test_ListDomain_getPartitionKey_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IScalar tt = new BasicMonth(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }


        [TestMethod]
        public void Test_ListDomain_getPartitionKeys()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ"});
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_ListDomain_getPartitionKeys_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.LITERAL);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            Assert.AreEqual(1, Ld1.getPartitionKeys(tt)[0]);
            Assert.AreEqual(2, Ld1.getPartitionKeys(tt)[1]);
        }

        [TestMethod]
        public void Test_ListDomain_getPartitionKeys_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IVector tt = new BasicMonthVector(new int[] { 1, 2 });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKey()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IScalar tt = new BasicInt(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKey_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IScalar tt = new BasicString("hello");
            Assert.AreEqual(0, Ld1.getPartitionKey(tt));
            IScalar tt1 = new BasicString("QQQ");
            Assert.AreEqual(-1, Ld1.getPartitionKey(tt1));
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKey_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IScalar tt = new BasicMonth(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKeys()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKeys_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[ `hello, `QQQ]");
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            Assert.AreEqual(0, Ld1.getPartitionKeys(tt)[0]);
            Assert.AreEqual(-1, Ld1.getPartitionKeys(tt)[1]);
        }

        [TestMethod]
        public void Test_RangeDomain_getPartitionKeys_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            RangeDomain Ld1 = new RangeDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IVector tt = new BasicMonthVector(new int[] { 1, 2 });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKey()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IScalar tt = new BasicInt(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKey_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IScalar tt = new BasicString("hello");
            Assert.AreEqual(0, Ld1.getPartitionKey(tt));
            IScalar tt1 = new BasicString("QQQ");
            Assert.AreEqual(0, Ld1.getPartitionKey(tt1));
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKey_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IScalar tt = new BasicMonth(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKeys()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKeys_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[ `hello, `QQQ]");
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            Assert.AreEqual(0, Ld1.getPartitionKeys(tt)[0]);
            Assert.AreEqual(0, Ld1.getPartitionKeys(tt)[1]);
        }

        [TestMethod]
        public void Test_HashDomain_getPartitionKeys_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            HashDomain Ld1 = new HashDomain(1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IVector tt = new BasicMonthVector(new int[] { 1, 2 });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKey()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IScalar tt = new BasicInt(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKey_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IScalar tt = new BasicString("hello");
            Assert.AreEqual(76235, Ld1.getPartitionKey(tt));
            IScalar tt1 = new BasicString("QQQ");
            Assert.AreEqual(15193, Ld1.getPartitionKey(tt1));
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKey_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1,`hello, `QQQ]");
            Console.WriteLine(biv1.getDataCategory());
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IScalar tt = new BasicMonth(1);
            String re = null;
            try
            {
                Ld1.getPartitionKey(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKeys()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.MIXED);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Data category incompatible.", re);
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKeys_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicStringVector biv1 = (BasicStringVector)conn.run("[ `hello, `QQQ]");
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.LITERAL);
            IVector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
            Assert.AreEqual(76235, Ld1.getPartitionKeys(tt)[0]);
            Assert.AreEqual(15193, Ld1.getPartitionKeys(tt)[1]);
        }

        [TestMethod]
        public void Test_ValueDomain_getPartitionKeys_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicAnyVector biv1 = (BasicAnyVector)conn.run("[1, `hello, `QQQ]");
            ValueDomain Ld1 = new ValueDomain(biv1, DATA_TYPE.DT_ANY, DATA_CATEGORY.TEMPORAL);
            IVector tt = new BasicMonthVector(new int[] { 1, 2 });
            String re = null;
            try
            {
                Ld1.getPartitionKeys(tt);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", re);
        }
    }
}

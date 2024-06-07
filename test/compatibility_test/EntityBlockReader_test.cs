using dolphindb;
using dolphindb.data;
using dolphindb.io;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;
using System;

namespace dolphindb_csharp_api_test.compatibility_test
{
    [TestClass]
    public class EntityBlockReader_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private ExtendedDataOutput output;

        [TestMethod]
        public void Test_EntityBlockReader_getObject()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            BasicTable table = (BasicTable)bt.getObject();
            table = (BasicTable)bt.getObject();
        }
        [TestMethod]
        public void Test_EntityBlockReader_skipALL()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            bt.skipAll();
            Assert.AreEqual(false, bt.hasNext());
        }
        [TestMethod]
        public void Test_EntityBlockReader_hasNext()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(true, bt.hasNext());
        }
        [TestMethod]
        public void Test_EntityBlockReader_getDataForm()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual("DF_TABLE", bt.getDataForm().ToString());
        }
        [TestMethod]
        public void Test_EntityBlockReader_getDataCategory()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual("MIXED", bt.getDataCategory().ToString());
        }
        [TestMethod]
        public void Test_EntityBlockReader_getDataType()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual("DT_ANY", bt.getDataType().ToString());
        }
        [TestMethod]
        public void Test_EntityBlockReader_rows()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(0, bt.rows());
        }
        [TestMethod]
        public void Test_EntityBlockReader_columns()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(0, bt.columns());
        }
        [TestMethod]
        public void Test_EntityBlockReader_getString()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(null, bt.getString());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isScalar()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isScalar());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isVector()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isVector());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isPair()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isPair());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isTable()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(true, bt.isTable());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isMatrix()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isMatrix());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isDictionary()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isDictionary());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isChart()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isChart());
        }
        [TestMethod]
        public void Test_EntityBlockReader_isChunk()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(false, bt.isChunk());
        }

    }
}

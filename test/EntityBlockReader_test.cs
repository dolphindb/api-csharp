using dolphindb;
using dolphindb.data;
using dolphindb.io;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;

namespace dolphindb_csharp_api_test
{
    [TestClass]
    public class EntityBlockReader_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_getObject()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            BasicTable table = (BasicTable)bt.getObject();
            table = (BasicTable)bt.getObject();
        }
        [TestMethod]
        public void Test_skipALL()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            bt.skipAll();
            Assert.AreEqual(false, bt.hasNext());
        }
        [TestMethod]
        public void Test_hasNext()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null, 4, 4, 10000);
            Assert.AreEqual(true, bt.hasNext());
        }
    }
}

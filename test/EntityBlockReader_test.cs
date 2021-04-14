using dolphindb;
using dolphindb.data;
using dolphindb.io;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace dolphindb_csharp_api_test
{
    [TestClass]
    public class EntityBlockReader_test
    {
        private readonly string SERVER = "127.0.0.1";
        private readonly int PORT = 8848;

        [TestMethod]
        public void Test_getObject()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            EntityBlockReader bt = (EntityBlockReader)db.run("table(1..100000 as id)", (ProgressListener)null,4,4,10000);
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

using dolphindb;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace dolphindb_csharp_api_test
{
    /// <summary>
    /// 连接池测试
    /// </summary>
    [TestClass]
    public class ExclusiveDBConnectionPool_test
    {
        private readonly string SERVER = "127.0.0.1";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";

        [TestMethod]
        public void Test_ExclusiveDBConnectionPool()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 10, true, true);
            Assert.AreEqual(10, pool.getConnectionCount());
        }
        [TestMethod]
        public void Test_execute()
        {
            ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD,20, true, true);
            List<IDBTask> tasks = new List<IDBTask>(20);
            for(int i = 0; i < 20; i++)
            {
                BasicDBTask task = new BasicDBTask("table(1 2 3 as id, 4 5 6 as value);");
                tasks.Add(task);
            }
            pool.execute(tasks);
            for (int i = 0; i < 20; i++)
            {
                bool flag = tasks[i].isSuccessful();
            }
        }
    }
}

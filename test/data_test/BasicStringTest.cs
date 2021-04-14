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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicStringTest
    {
        private readonly string SERVER = "127.0.0.1";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";
        [TestMethod]
        public void blob()
        {
           




            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            
            
            db.run("a=table(100:0, `id`value`memo, [INT, DOUBLE, BLOB])");
            db.run("insert into a values(10,0.5, '[{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"}]')");
            BasicTable tb = (BasicTable)db.run("a");
            Assert.IsTrue(tb.isTable());
            Assert.AreEqual(1, tb.rows());
            Assert.AreEqual(3, tb.columns());
            Assert.AreEqual("[{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"}]", ((BasicString)tb.getColumn(2).get(0)).getValue());

            
            
          

            db.close();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
using dolphindb;
using dolphindb.route;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class PartitionedTableAppender_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;


        [TestMethod]
        public void createPartitionedTableAppender()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool(SERVER, PORT, USER, PASSWORD, 5, true, true);
            IDBTask conn = new BasicDBTask("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`id`valuie,[STRING,INT]);pt=db.createPartitionedTable(t,`pt,`id);");
            pool.execute(conn);


            PartitionedTableAppender appender = new PartitionedTableAppender("dfs://demohash", "pt", "id", pool);
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>(2);
            BasicStringVector id = new BasicStringVector(3);
            id.setString(0, "ORCA");
            id.setString(1, "YHOO");
            id.setString(2, "Ford");
            cols.Add(id);

            BasicIntVector value = new BasicIntVector(3);
            value.setInt(0, 10);
            value.setInt(1, 11);
            value.setInt(2, 12);
            cols.Add(value);

            int res = appender.append(new BasicTable(colNames, cols));
            Assert.AreEqual(3, res);


        }
    }
}

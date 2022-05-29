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

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class PartitionedTableAppender_test
    {
        [TestMethod]
        public void createPartitionedTableAppender()
        {

            IDBConnectionPool pool = new ExclusiveDBConnectionPool("192.168.1.37", 8848, "admin", "123456", 5, true, true);
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

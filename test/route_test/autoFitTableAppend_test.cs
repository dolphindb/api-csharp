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
using System.Data;
using System.Threading;


namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class autoFittableAppend_test
    {
        private readonly string SERVER = "192.168.1.37";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";

        [TestMethod]
        public void append_test()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT,USER,PASSWORD);
            conn.run("dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`股票代码`股票日期`买方报价`卖方报价`时间戳`备注`timespan,[STRING,MONTH,DOUBLE,DOUBLE,DATE,STRING,TIME]);pt=db.createPartitionedTable(t,`pt,`股票代码);");
            autoFitTableAppender aft = new autoFitTableAppender("dfs://demohash", "pt", false, conn);

            DataTable dt = new DataTable();
            List<DataColumn> cols = new List<DataColumn>(){
                new DataColumn("股票代码",Type.GetType("System.String")),
                new DataColumn("股票日期",Type.GetType("System.DateTime")),
                new DataColumn("买方报价", Type.GetType("System.Double")),
                new DataColumn("卖方报价", Type.GetType("System.Double")),
                new DataColumn("时间戳", Type.GetType("System.DateTime")),
                new DataColumn("备注", Type.GetType("System.String")),
                new DataColumn("timespan",Type.GetType("System.TimeSpan"))
            };
            dt.Columns.AddRange(cols.ToArray());
            for (int i = 0; i < 1000; i++)
            {
                DataRow dr = dt.NewRow();
                dr["股票代码"] = new string[] { "GGG", "MSSS", "FBBBB" }[i % 3];
                dr["股票日期"] = DateTime.Now.Date;
                dr["买方报价"] = 22222.5544;
                dr["卖方报价"] = 3333.33322145;
                dr["时间戳"] = new DateTime(2021, 1, 26, 15, 1, 2);
                dr["备注"] = "备注" + i.ToString();
                dr["timespan"] = new TimeSpan(0, 15, 15, 14, 123);
                dt.Rows.Add(dr);
            }
            IEntity res = aft.append(dt);
            //Assert.AreEqual(res, null);
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, USER, PASSWORD);
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                try
                {
                    BasicLong x = (BasicLong)db.run("exec count(*) from loadTable(\"dfs://demohash\",`pt)");
                    if (x.getValue() == 1000)
                    {
                        break;
                    }
                }
                catch(Exception ex)
                {
                    BasicInt x = (BasicInt)db.run("exec count(*) from loadTable(\"dfs://demohash\",`pt)");
                    if (x.getValue() == 1000)
                    {
                        break;
                    }
                }

            }
            BasicLong re = (BasicLong)db.run("exec count(*) from loadTable(\"dfs://demohash\",`pt)");
            Assert.AreEqual(re.getValue(), 1000);
            db.run("dropDatabase(\"dfs://demohash\")");
            db.close();


        }
    }
}

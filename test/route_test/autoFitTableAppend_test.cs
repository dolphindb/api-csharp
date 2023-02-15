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
using dolphindb_config;



namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class autoFittableAppend_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void append_test()
        {
        
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER,PASSWORD);
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
        //[TestMethod] not support
        public void Test_autoFittableAppend_dfs_table_decimal64()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8)])\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            autoFitTableAppender aft = new autoFitTableAppender("dfs://tableAppend_test", "pt", false, conn);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
            DataTable dt = bt.toDataTable();

            IEntity res = aft.append(dt);//APICS-198
            
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test\",`pt)");
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), bt.getColumn(i).get(j).getObject());

                }
            }
            conn.close();
        }
        //[TestMethod]APICS-199:autoFittableAppend not support memory table
        public void Test_autoFittableAppend_imemory_table_decimal64()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `id`col0`col1`col2`col3, [INT,DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8)]) as table1";
            conn.run(script);
            autoFitTableAppender aft = new autoFitTableAppender("", "table1", false, conn);   //APICS-199
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal64(0..10000,0) as col0, decimal64(0..10000,2) as col1, decimal64(0..10000,7) as col2, decimal64(0..10000,8) as col3);t2;");
            DataTable dt = bt.toDataTable();

            IEntity res = aft.append(dt);//APICS-198

            BasicTable re = (BasicTable)conn.run("select * from table1)");
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), bt.getColumn(i).get(j).getObject());

                }
            }
            conn.close();
        }
        //[TestMethod]not support
        public void Test_autoFittableAppend_dfs_table_decimal32()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableAppend_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0,`id`col0`col1`col2`col3,[INT,DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)])\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            autoFitTableAppender aft = new autoFitTableAppender("dfs://tableAppend_test", "pt", false, conn);
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000%99,0) as col0, decimal32(0..10000%99,2) as col1, decimal32(0..10000%99,6) as col2, decimal32(0..10000%99,7) as col3);t2;");
            DataTable dt = bt.toDataTable();

            IEntity res = aft.append(dt);//APICS-198

            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppend_test\",`pt)");
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), bt.getColumn(i).get(j).getObject());

                }
            }
            conn.close();
        }
        //[TestMethod]APICS-199:autoFittableAppend not support memory table
        public void Test_autoFittableAppend_imemory_table_decimal32()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, `col0`col1`col2`col3, [DECIMAL32(0),DECIMAL32(2),DECIMAL32(3),DECIMAL32(4)]) as table1";
            conn.run(script);
            autoFitTableAppender aft = new autoFitTableAppender("", "table1", false, conn);   //APICS-199
            BasicTable bt = (BasicTable)conn.run("t2 = table(int(0..10000) as id,decimal32(0..10000%99,0) as col0, decimal32(0..10000%99,2) as col1, decimal32(0..10000%99,6) as col2, decimal32(0..10000%99,7) as col3);t2;");
            DataTable dt = bt.toDataTable();

            IEntity res = aft.append(dt);//APICS-198

            BasicTable re = (BasicTable)conn.run("select * from table1)");
            for (int i = 0; i < re.columns(); i++)
            {
                for (int j = 0; j < re.rows(); j++)
                {
                    Assert.AreEqual(re.getColumn(i).get(j).getObject(), bt.getColumn(i).get(j).getObject());

                }
            }
            conn.close();
        }
    }
}

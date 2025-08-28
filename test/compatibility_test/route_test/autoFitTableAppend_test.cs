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



namespace dolphindb_csharp_api_test.compatibility_test.route_test
{
    [TestClass]
    public class autoFittableAppend_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        static void compareBasicTable(BasicTable table, BasicTable newTable)
        {
            Assert.AreEqual(table.rows(), newTable.rows());
            Assert.AreEqual(table.columns(), newTable.columns());
            int cols = table.columns();
            for (int i = 0; i < cols; i++)
            {
                AbstractVector v1 = (AbstractVector)table.getColumn(i);
                AbstractVector v2 = (AbstractVector)newTable.getColumn(i);
                if (!v1.Equals(v2))
                {
                    for (int j = 0; j < table.rows(); j++)
                    {
                        int failCase = 0;
                        AbstractScalar e1 = (AbstractScalar)table.getColumn(i).get(j);
                        Console.WriteLine(e1.toString());

                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        Console.WriteLine(e2.toString());

                        if (e1.getString().Equals(e2.getString()) == false)
                        {
                            Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }

        }

        [TestMethod]
        public void append_test()
        {
        
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER,PASSWORD);
            conn.run("try{\n undef(`t,SHARED)\n }\n catch(ex){\n };\n dbPath = \"dfs://demohash\";if(existsDatabase(dbPath))    dropDatabase(dbPath); db = database(dbPath, HASH,[STRING, 2]);t= table(100:0,`股票代码`股票日期`买方报价`卖方报价`时间戳`备注`timespan,[STRING,MONTH,DOUBLE,DOUBLE,DATE,STRING,TIME]);pt=db.createPartitionedTable(t,`pt,`股票代码);");
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

        [TestMethod]
        public void Test_AutoFitTableAppender_dfs_allDateType()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`cint)\n";
            script += "pt.append!(t)";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            autoFitTableAppender aftu = new autoFitTableAppender("dfs://tableAppenderTest", "pt", false, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from pt;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from pt where cint = 10;");
            compareBasicTable(bt, act);
            conn.close();
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_dfs_allDateType_1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            //script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = table(intv,uuidv,ippaddrv,int128v)\n";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`intv)\n";
            script += "pt1 = db.createPartitionedTable(t,`pt1,`intv)\n";
            script += "pt.append!(t)";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("select * from t");
            autoFitTableAppender aftu = new autoFitTableAppender("dfs://tableAppenderTest", "pt1", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt1);");
            Assert.AreEqual(100, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt);");
            compareBasicTable(act, ua);
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_DimensionTable_allDateType()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10)\n";
            script += "pt = db.createTable(t,`pt)\n";
            script += "pt.append!(t)";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            autoFitTableAppender aftu = new autoFitTableAppender("dfs://tableAppenderTest", "pt", false, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from pt;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from pt where cint = 10;");
            compareBasicTable(bt, act);
            conn.close();
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_DimensionTable_allDateType_1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            //script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = table(intv,uuidv,ippaddrv,int128v)\n";
            script += "dbPath = \"dfs://tableAppenderTest\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "db=database(dbPath,VALUE, 1..10)\n";
            script += "pt = db.createTable(t,`pt)\n";
            script += "pt1 = db.createTable(t,`pt1)\n";
            script += "pt.append!(t)";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("select * from t");
            autoFitTableAppender aftu = new autoFitTableAppender("dfs://tableAppenderTest", "pt1", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt1);");
            Assert.AreEqual(100, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://tableAppenderTest\",`pt);");
            compareBasicTable(act, ua);
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_memory_allDateType()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            autoFitTableAppender aftu = new autoFitTableAppender("", "st", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_memory_allDateType_1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            //script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = table(intv,uuidv,ippaddrv,int128v)\n";
            script += "t1 = table(100:0,`intv`uuidv`ippaddrv`int128v,[INT,UUID,IPADDR,INT128])\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("select * from t");
            autoFitTableAppender aftu = new autoFitTableAppender("", "t1", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from t1;");
            Assert.AreEqual(100, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from t");
            compareBasicTable(act, ua);
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_streamTable_allDateType() 
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "t = streamTable(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "share t as st;";
            conn.run(script);
        BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
        autoFitTableAppender aftu = new autoFitTableAppender("", "st", true,conn);
        aftu.append(bt);
        BasicTable ua = (BasicTable)conn.run("select * from st;");
        Assert.AreEqual(4, ua.rows());
        BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
        compareBasicTable(bt, act);
        conn.run("undef(`st, SHARED)");
    }
        [TestMethod]
        public void Test_AutoFitTableAppender_streamTable_allDateType_1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            //script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = streamTable(intv,uuidv,ippaddrv,int128v)\n";
            script += "t1 = table(100:0,`intv`uuidv`ippaddrv`int128v,[INT,UUID,IPADDR,INT128])\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("select * from t");
            autoFitTableAppender aftu = new autoFitTableAppender("", "t1", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from t1;");
            Assert.AreEqual(100, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from t");
            compareBasicTable(act, ua);
        }

        [TestMethod]
        public void Test_AutoFitTableAppender_keyedTable_allDateType()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = null;
            script = "cbool = true false false;\n";
            script += "cchar = 'a' 'b' 'c';\n";
            script += "cshort = 122h 32h 45h;\n";
            script += "cint = 1 4 9;\n";
            script += "clong = 17l 39l 72l;\n";
            script += "cdate = 2013.06.13 2015.07.12 2019.08.15;\n";
            script += "cmonth = 2011.08M 2014.02M 2019.07M;\n";
            script += "ctime = 04:15:51.921 09:27:16.095 11:32:28.387;\n";
            script += "cminute = 03:25m 08:12m 10:15m;\n";
            script += "csecond = 01:15:20 04:26:45 09:22:59;\n";
            script += "cdatetime = 1976.09.10 02:31:42 1987.12.13 11:58:31 1999.12.10 20:49:23;\n";
            script += "ctimestamp = 1997.07.20 21:45:16.339 2002.11.26 12:40:31.783 2008.08.10 23:54:27.629;\n";
            script += "cnanotime = 01:25:33.365869429 03:47:25.364828475 08:16:22.748395721;\n";
            script += "cnanotimestamp = 2005.09.23 13:30:35.468385940 2007.12.11 14:54:38.949792731 2009.09.30 16:39:51.973463623;\n";
            script += "cfloat = 7.5f 0.79f 8.27f;\n";
            script += "cdouble = 5.7 7.2 3.9;\n";
            script += "cstring = \"hello\" \"hi\" \"here\";\n";
            script += "cdatehour = datehour(2012.06.15 15:32:10.158 2012.06.15 17:30:10.008 2014.09.29 23:55:42.693);\n";
            script += "t = keyedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour)");
            autoFitTableAppender aftu = new autoFitTableAppender("", "st", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
        }
        [TestMethod]
        public void Test_AutoFitTableAppender_keyedTable_allDateType_1()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "n=100;\n";
            script += "intv = 1..100;\n";
            script += "uuidv = rand(rand(uuid(), 10) join take(uuid(), 4), n);\n";
            script += "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n)\n";
            script += "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n";
            //script += "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            //script += "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n";
            script += "t = keyedTable(`intv,intv,uuidv,ippaddrv,int128v)\n";
            script += "t1 = keyedTable(`intv,100:0,`intv`uuidv`ippaddrv`int128v,[INT,UUID,IPADDR,INT128])\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("select * from t");
            autoFitTableAppender aftu = new autoFitTableAppender("", "t1", true, conn);
            aftu.append(bt);
            BasicTable ua = (BasicTable)conn.run("select * from t1;");
            Assert.AreEqual(100, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from t");
            compareBasicTable(act, ua);
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

        [TestMethod]
        public void Test_autoFitTableAppender_allDataType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v" };
            int rowNum = 0;
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicBooleanVector(rowNum));
            cols.Add(new BasicByteVector(rowNum));
            cols.Add(new BasicShortVector(rowNum));
            cols.Add(new BasicLongVector(rowNum));
            cols.Add(new BasicDoubleVector(rowNum));
            cols.Add(new BasicFloatVector(rowNum));
            cols.Add(new BasicDateVector(rowNum));
            cols.Add(new BasicMonthVector(rowNum));
            cols.Add(new BasicTimeVector(rowNum));
            cols.Add(new BasicMinuteVector(rowNum));
            cols.Add(new BasicSecondVector(rowNum));
            cols.Add(new BasicDateTimeVector(rowNum));
            cols.Add(new BasicTimestampVector(rowNum));
            cols.Add(new BasicNanoTimeVector(rowNum));
            cols.Add(new BasicNanoTimestampVector(rowNum));
            cols.Add(new BasicSymbolVector(rowNum));
            cols.Add(new BasicStringVector(rowNum));
            cols.Add(new BasicUuidVector(rowNum));
            cols.Add(new BasicDateHourVector(rowNum));
            cols.Add(new BasicIPAddrVector(rowNum));
            cols.Add(new BasicInt128Vector(rowNum));
            conn.run("try{\n undef(`t,SHARED)\n }\n catch(ex){\n };\n dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2]);\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128]);\n pt=db.createPartitionedTable(t,`pt,`stringv);");
            autoFitTableAppender aft = new autoFitTableAppender("dfs://empty_table", "pt", false, conn);  
            aft.append(new BasicTable(colNames, cols));
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            conn.close();
        }
    }
}

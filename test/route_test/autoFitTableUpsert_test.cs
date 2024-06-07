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
using dolphindb_csharpapi_net_core.src.route;

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class autoFitTableUpsert_test
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
                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        if (!e1.toString().Equals(e2.toString()))
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
        public void Test_autoFitTableUpsert_keyedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "st", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(3,ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 9;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_keyedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "st", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_indexedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "st", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 9;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_indexedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "st", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from st;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from st where cint = 10;");
            compareBasicTable(bt, act);
            conn.run("undef(`st, SHARED)");

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_partitionedTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";

            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(2787877,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://partitionedTable", "pt", conn, false, new String[] { "cint" }, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 9;");
            compareBasicTable(bt, act);
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_PartitionedTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(-2799897897,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://partitionedTable", "pt", conn, false, new String[] { "cint" }, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 10;");
            compareBasicTable(bt, act);
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DimensionTable_allDateType_update()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt);\n";
            script += "pt.append!(t);\n";

            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(9.6767676722227,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://partitionedTable", "pt", conn, false, new String[] { "cint" }, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from pt;");
            Assert.AreEqual(3, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from pt where cint = 9;");
            compareBasicTable(bt, act);
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DimensionTable_allDateType_insert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt,);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://partitionedTable", "pt", conn, false, new String[] { "cint" }, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from pt;");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from pt where cint = 10;");
            compareBasicTable(bt, act);
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_dbUrl_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t\n";
            conn.run(script);
            Exception exception = null;
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("pt", "pt", conn, false, new String[] { "cint" }, null);

            }
            catch (Exception e )
            {

                exception=e;
            }
            Assert.IsNotNull(exception);
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_tableName_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t\n";
            conn.run(script);
            Exception exception = null;
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "@123456", conn, false, new String[] { "cint" }, null);

            }
            catch (Exception e)
            {

                exception = e;
            }
            Assert.IsNotNull(exception);
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DBConnection_not_login()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)\n";
            script += "db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)\n";
            script += "pt = db.createPartitionedTable(t, `pt, `qty)\n";
            conn.run(script);
            Exception exception = null;
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn1, false, null, null);

            }
            catch (Exception e)
            {

                exception = e;
                Console.WriteLine(exception.Message);
            }
            Assert.AreEqual("schema(loadTable(\"dfs://tableUpsert_test\", \"pt\")) => <NotAuthenticated>Not granted to read table dfs://tableUpsert_test/pt",exception.Message);
            conn.close();
            conn1.close();
        }
        //[TestMethod]APICS-193:asynchronousTask not support true about autoFitTableUpsert
        public void Test_autoFitTableUpsert_DBConnection_asynchronousTask_true()
        {
            DBConnection conn = new DBConnection(true, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0, `id`value,[ INT, INT[]])\n";
            script += "db  = database(dbPath, RANGE,1 10000,,'TSDB')\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id,,`id)\n";
            conn.run(script);
            BasicIntVector bi = new BasicIntVector(new int[] { 1, 100, 9999 });
            BasicArrayVector ba = new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY);
            ba.append(bi);
            ba.append(bi);
            ba.append(bi);
            List<string> colNames = new List<string> { "id", "value" };
            List<IVector> cols = new List<IVector> { bi, ba };
            BasicTable bt = new BasicTable(colNames, cols);
            string[] keyColName = new string[] { "id" };
            //APICS - 193
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt");
            //Assert.AreEqual(3, res.rows());
            //Assert.AreEqual(2, res.columns());
            //Assert.AreEqual(1, res.getColumn(0).get(0).getObject());
            //Assert.AreEqual(1, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(0)).getValue());
            //Assert.AreEqual(100, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(1)).getValue());
            //Assert.AreEqual(9999, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(2)).getValue());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DBConnection_useSSL_true()
        {
            DBConnection conn = new DBConnection(false, true, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0, `id`value,[ INT, INT[]])\n";
            script += "db  = database(dbPath, RANGE,1 10000,,'TSDB')\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id,,`id)\n";
            conn.run(script);
            BasicIntVector bi = new BasicIntVector(new int[] { 1, 100, 9999 });
            BasicArrayVector ba = new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY);
            ba.append(bi);
            ba.append(bi);
            ba.append(bi);
            List<string> colNames = new List<string> { "id", "value" };
            List<IVector> cols = new List<IVector> { bi, ba };
            BasicTable bt = new BasicTable(colNames, cols);
            string[] keyColName = new string[] { "id" };
            //APICS - 193
            //AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id" }, null);
            //aftu.upsert(bt);
            //BasicTable res = (BasicTable)conn.run("select * from pt");
            //Assert.AreEqual(3, res.rows());
            //Assert.AreEqual(2, res.columns());
            //Assert.AreEqual(1, res.getColumn(0).get(0).getObject());
            //Assert.AreEqual(1, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(0)).getValue());
            //Assert.AreEqual(100, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(1)).getValue());
            //Assert.AreEqual(9999, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(2)).getValue());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_PartitionedTable_allDateType_insert_compress_true()
        {
            DBConnection conn = new DBConnection(false, false, true);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "cbool = true false false;\n";
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
            script += "cblob = blob(\"dolphindb\" \"gaussdb\" \"goldendb\")\n";
            script += "cdecimal32 = decimal32(12 17 135.2,2)\n";
            script += "cdecimal64 = decimal64(18 24 33.878,4)\n";
            script += "cdecimal128 = decimal128(18 24 33.878,10)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128)");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://partitionedTable", "pt", conn, false, new String[] { "cint" }, null);
            aftu.upsert(bt);
            BasicTable ua = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt);");
            Assert.AreEqual(4, ua.rows());
            BasicTable act = (BasicTable)conn.run("select * from loadTable(\"dfs://partitionedTable\",`pt) where cint = 10;");
            compareBasicTable(bt, act);
            conn.close();
        }

        [TestMethod]
        public void Test_autoFitTableUpsert_DBConnection_compress_true()
        {
            DBConnection conn = new DBConnection(false, false, true);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(1000:0, `id`value,[ INT, INT[]])\n";
            script += "db  = database(dbPath, RANGE,1 10000,,'TSDB')\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id,,`id)\n";
            conn.run(script);
            BasicIntVector bi = new BasicIntVector(new int[] { 1, 100, 9999 });
            BasicArrayVector ba = new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY);
            ba.append(bi);
            ba.append(bi);
            ba.append(bi);
            List<string> colNames = new List<string> { "id", "value" };
            List<IVector> cols = new List<IVector> { bi, ba };
            BasicTable bt = new BasicTable(colNames, cols);
            string[] keyColName = new string[] { "id" };
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(2, res.columns());
            Assert.AreEqual(1, res.getColumn(0).get(0).getObject());
            Assert.AreEqual(1, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(0)).getValue());
            Assert.AreEqual(100, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(1)).getValue());
            Assert.AreEqual(9999, ((BasicInt)((BasicIntVector)((BasicArrayVector)res.getColumn(1)).getSubVector(0)).get(2)).getValue());
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_keyedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "kt = keyedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value);";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "kt", conn, true, null, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_keyedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "kt = keyedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value);";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "kt", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_indexedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "kt = indexedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value);";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "kt", conn, true, null, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_indexedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "kt = indexedTable(`id2,take(1..10, 100) as id, 1..100 as id2, 100..1 as value);";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("", "kt", conn, false, null, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from kt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from kt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_partitionedTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, true, new string[] { "id2" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_partitionedTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DimensionTable_ignoreNull_true()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, true, new string[] { "id2" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(0, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(99, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DimensionTable_ignoreNull_false()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from pt where value = NULL;");
            BasicTable res1 = (BasicTable)conn.run("select * from pt order by id2 ");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(100, res1.rows());
            Assert.AreEqual(1001, res1.getColumn(2).get(0).getObject());
            Assert.AreEqual(-2147483648, res1.getColumn(2).get(1).getObject());
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DFS_pkeyColNames_null()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            Exception exception = null;
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, null, null);
                aftu.upsert(bt);
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine(exception.Message);
            }
            Assert.AreEqual("Usage: upsert!(obj, newData, [ignoreNull=false], [keyColNames], [sortColumns]). keyColNames must be specified if obj is a DFS table.", exception.Message.ToString());
            conn.close();           
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_DFS_psortColumns()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, new string[] { "value" });
            aftu.upsert(bt);
            BasicTable res1 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(1001, res1.getColumn(2).get(99).getObject());
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_notMatchColumns()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(1..10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2,1 2 as id3, 1001 NULL as value);t2;");
            Exception exception = null;
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
                aftu.upsert(bt);
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine(exception.Message);
            }
            Assert.AreEqual("The input table columns doesn't match the columns of the target table.", exception.Message.ToString());
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_notMatchDataType()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(string(10), 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 as id, 1 2 as id2, 1001 NULL as value);t2;");
            Exception exception = null;
            try
            {
                AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
                aftu.upsert(bt);
            }
            catch (Exception e)
            {
                exception = e;
                Console.WriteLine(exception.Message);
            }
            Assert.AreEqual("column 0, expect category LITERAL, got category INTEGRAL", exception.Message.ToString());
            conn.close();
        }
        [TestMethod]
        public void Test_autoFitTableUpsert_upsert_more_then_1()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( 1 2 3 as id, 99 199 123 as id2, 1001 NULL NULL as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt);
            aftu.upsert(bt);
            aftu.upsert(bt);
            aftu.upsert(bt);
            BasicTable res1 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(102, res1.rows());
            BasicTable res2 = (BasicTable)conn.run("select * from pt where id2 = 99 ");
            Assert.AreEqual(1001, res2.getColumn(2).get(0).getObject());
            conn.close();
        }
        //[TestMethod]
        public void Test_autoFitTableUpsert_upsert_big_data()
        {
            DBConnection conn = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "dbPath = \"dfs://tableUpsert_test\"\n";
            script += "if(existsDatabase(dbPath))\n";
            script += "dropDatabase(dbPath)\n";
            script += "t = table(take(10, 100) as id, 1..100 as id2, 100..1 as value)\n";
            script += "db  = database(dbPath, RANGE,1 50 10000)\n";
            script += "pt = db.createTable(t,`pt).append!(t)\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t2 = table( take(1000,30000000) as id, 1..30000000 as id2, 1..30000000 as value);t2;");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://tableUpsert_test", "pt", conn, false, new string[] { "id2" }, null);
            aftu.upsert(bt);
            BasicTable res1 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(30000000, res1.rows());
            aftu.upsert(bt);
            BasicTable res2 = (BasicTable)conn.run("select * from pt ");
            Assert.AreEqual(30000000, res2.rows());
            conn.close();
        }

        [TestMethod]
        public void Test_autoFitTableUpsert_allDataType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv", "decimal32v", "decimal64v", "decimal128v" };
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
            cols.Add(new BasicStringVector(new List<string>(), true));
            cols.Add(new BasicDecimal32Vector(rowNum, 1));
            cols.Add(new BasicDecimal64Vector(rowNum, 10));
            cols.Add(new BasicDecimal128Vector(rowNum, 20));
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(1), DECIMAL64(10), DECIMAL128(20)]);\n pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://empty_table", "pt", conn, false, new string[] { "stringv" }, null);
            aftu.upsert(new BasicTable(colNames, cols));
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            conn.close();
        }

        [TestMethod]
        public void Test_autoFitTableUpsert_allDataType_array_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            var cols = new List<IVector>() { };
            var colNames = new List<String>() { "id", "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "uuidv", "datehourv", "ippaddrv", "int128v", "decimal32v", "decimal64v", "decimal128v" };
            int rowNum = 0;
            cols.Add(new BasicIntVector(rowNum));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_BOOL_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_BYTE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_SHORT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_LONG_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DOUBLE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_FLOAT_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_MONTH_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_TIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_MINUTE_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_SECOND_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATETIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_TIMESTAMP_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIME_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIMESTAMP_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_UUID_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DATEHOUR_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_IPADDR_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_INT128_ARRAY));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL32_ARRAY, 1));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL64_ARRAY, 10));
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_DECIMAL128_ARRAY, 20));
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n t= table(100:0,`id`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`decimal32v`decimal64v`decimal128v, [INT, INT[], BOOL[], CHAR[], SHORT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], DECIMAL32(1)[], DECIMAL64(10)[], DECIMAL128(20)[]]);\n pt=db.createPartitionedTable(t,`pt,`id,,`id);");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://empty_table", "pt", conn, false, new string[] { "id" }, null);
            aftu.upsert(new BasicTable(colNames, cols));
            BasicTable re = (BasicTable)conn.run("select * from loadTable(\"dfs://empty_table\", `pt)");
            Assert.AreEqual(0, re.rows());
            conn.close();
        }

        [TestMethod]
        public void test_AutoFitTableAppender_ArrayVector_decimal()
        {
            DBConnection conn = new DBConnection(false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4`col5`col6`col7" +
                    ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[],DECIMAL128(0)[],DECIMAL128(10)[],DECIMAL128(37)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector", "pt", conn, false, new string[] { "cint" }, null);

            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            colNames.Add("col5");
            colNames.Add("col6");
            colNames.Add("col7");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v32 = new BasicDecimal32Vector(3, 0);
            v32.set(0, new BasicDecimal32("15645.00", 0));
            v32.set(1, new BasicDecimal32("24635.00001", 0));
            v32.set(2, new BasicDecimal32("24635.00001", 0));
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0, 0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v321 = new BasicDecimal32Vector(3, 4);
            v321.set(0, new BasicDecimal32("15645.00", 4));
            v321.set(1, new BasicDecimal32("24635.00001", 4));
            v321.set(2, new BasicDecimal32("24635.00001", 4));
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1, 4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v640 = new BasicDecimal64Vector(3, 0);
            v640.set(0, new BasicDecimal64("15645.00", 0));
            v640.set(1, new BasicDecimal64("24635.00001", 0));
            v640.set(2, new BasicDecimal64("24635.00001", 0));
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2, 0);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v641 = new BasicDecimal64Vector(3, 4);
            v641.set(0, new BasicDecimal64("15645.00", 4));
            v641.set(1, new BasicDecimal64("24635.00001", 4));
            v641.set(2, new BasicDecimal64("24635.00001", 4));
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3, 4);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v642 = new BasicDecimal64Vector(3, 8);
            v642.set(0, new BasicDecimal64("15645.00", 8));
            v642.set(1, new BasicDecimal64("24635.00001", 8));
            v642.set(2, new BasicDecimal64("24635.00001", 8));
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4, 8);
            cols.Add(bavcol4);

            List<IVector> bdvcol5 = new List<IVector>();
            IVector v128 = new BasicDecimal128Vector(3, 0);
            v128.set(0, new BasicDecimal128("15645.00", 0));
            v128.set(1, new BasicDecimal128("24635.00001", 0));
            v128.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol5.Add(v128);
            bdvcol5.Add(v128);
            bdvcol5.Add(v128);
            BasicArrayVector bavcol5 = new BasicArrayVector(bdvcol5, 0);
            cols.Add(bavcol5);
            List<IVector> bdvcol6 = new List<IVector>();
            IVector v1281 = new BasicDecimal128Vector(3, 10);
            v1281.set(0, new BasicDecimal128("15645.00", 10));
            v1281.set(1, new BasicDecimal128("24635.00001", 10));
            v1281.set(2, new BasicDecimal128("24635.00001", 10));
            bdvcol6.Add(v1281);
            bdvcol6.Add(v1281);
            bdvcol6.Add(v1281);
            BasicArrayVector bavcol6 = new BasicArrayVector(bdvcol6, 10);
            cols.Add(bavcol6);
            List<IVector> bdvcol7 = new List<IVector>();
            IVector v1282 = new BasicDecimal128Vector(3, 37);
            v1282.set(0, new BasicDecimal128("1.00", 37));
            v1282.set(1, new BasicDecimal128("1.00001", 37));
            v1282.set(2, new BasicDecimal128("0.00001", 37));
            bdvcol7.Add(v1282);
            bdvcol7.Add(v1282);
            bdvcol7.Add(v1282);
            BasicArrayVector bavcol7 = new BasicArrayVector(bdvcol7, 37);
            cols.Add(bavcol7);

            BasicTable bt = new BasicTable(colNames, cols);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getEntity(0).getString());
            Assert.AreEqual(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getEntity(0).getString());
            Assert.AreEqual(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getEntity(0).getString());
            Assert.AreEqual(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getEntity(0).getString());
            Assert.AreEqual(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getEntity(0).getString());
            Assert.AreEqual(v128.getString(), ((BasicArrayVector)(res.getColumn("col5"))).getEntity(0).getString());
            Assert.AreEqual(v1281.getString(), ((BasicArrayVector)(res.getColumn("col6"))).getEntity(0).getString());
            Assert.AreEqual(v1282.getString(), ((BasicArrayVector)(res.getColumn("col7"))).getEntity(0).getString());
            conn.close();
        }

        [TestMethod]
        public void test_AutoFitTableAppender_ArrayVector_decimal_compress_true()
        {
            DBConnection conn = new DBConnection(false, false, true);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testArrayVector\")){\n" +
                    "    dropDatabase(\"dfs://testArrayVector\")\n" +
                    "}\n" +
                    "db = database(\"dfs://testArrayVector\",RANGE,int(1..100),,\"TSDB\")\n" +
                    "t = table(1000000:0,`cint`col0`col1`col2`col3`col4`col5`col6`col7" +
                    ",[INT,DECIMAL32(0)[],DECIMAL32(4)[],DECIMAL64(0)[],DECIMAL64(4)[],DECIMAL64(8)[],DECIMAL128(0)[],DECIMAL128(10)[],DECIMAL128(37)[]])\n" +
                    "pt = db.createPartitionedTable(t,`pt,`cint,,`cint)";
            conn.run(script);
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testArrayVector", "pt", conn, false, new string[] { "cint" }, null);

            List<String> colNames = new List<String>();
            colNames.Add("cint");
            colNames.Add("col0");
            colNames.Add("col1");
            colNames.Add("col2");
            colNames.Add("col3");
            colNames.Add("col4");
            colNames.Add("col5");
            colNames.Add("col6");
            colNames.Add("col7");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(new int[] { 12, 29, 31 }));
            List<IVector> bdvcol0 = new List<IVector>();
            IVector v32 = new BasicDecimal32Vector(3, 0);
            v32.set(0, new BasicDecimal32("15645.00", 0));
            v32.set(1, new BasicDecimal32("24635.00001", 0));
            v32.set(2, new BasicDecimal32("24635.00001", 0));
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            bdvcol0.Add(v32);
            BasicArrayVector bavcol0 = new BasicArrayVector(bdvcol0, 0);
            cols.Add(bavcol0);
            List<IVector> bdvcol1 = new List<IVector>();
            IVector v321 = new BasicDecimal32Vector(3, 4);
            v321.set(0, new BasicDecimal32("15645.00", 4));
            v321.set(1, new BasicDecimal32("24635.00001", 4));
            v321.set(2, new BasicDecimal32("24635.00001", 4));
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            bdvcol1.Add(v321);
            BasicArrayVector bavcol1 = new BasicArrayVector(bdvcol1, 4);
            cols.Add(bavcol1);
            List<IVector> bdvcol2 = new List<IVector>();
            IVector v640 = new BasicDecimal64Vector(3, 0);
            v640.set(0, new BasicDecimal64("15645.00", 0));
            v640.set(1, new BasicDecimal64("24635.00001", 0));
            v640.set(2, new BasicDecimal64("24635.00001", 0));
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            bdvcol2.Add(v640);
            BasicArrayVector bavcol2 = new BasicArrayVector(bdvcol2, 0);
            cols.Add(bavcol2);
            List<IVector> bdvcol3 = new List<IVector>();
            IVector v641 = new BasicDecimal64Vector(3, 4);
            v641.set(0, new BasicDecimal64("15645.00", 4));
            v641.set(1, new BasicDecimal64("24635.00001", 4));
            v641.set(2, new BasicDecimal64("24635.00001", 4));
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            bdvcol3.Add(v641);
            BasicArrayVector bavcol3 = new BasicArrayVector(bdvcol3, 4);
            cols.Add(bavcol3);
            List<IVector> bdvcol4 = new List<IVector>();
            IVector v642 = new BasicDecimal64Vector(3, 8);
            v642.set(0, new BasicDecimal64("15645.00", 8));
            v642.set(1, new BasicDecimal64("24635.00001", 8));
            v642.set(2, new BasicDecimal64("24635.00001", 8));
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            bdvcol4.Add(v642);
            BasicArrayVector bavcol4 = new BasicArrayVector(bdvcol4, 8);
            cols.Add(bavcol4);

            List<IVector> bdvcol5 = new List<IVector>();
            IVector v128 = new BasicDecimal128Vector(3, 0);
            v128.set(0, new BasicDecimal128("15645.00", 0));
            v128.set(1, new BasicDecimal128("24635.00001", 0));
            v128.set(2, new BasicDecimal128("24635.00001", 0));
            bdvcol5.Add(v128);
            bdvcol5.Add(v128);
            bdvcol5.Add(v128);
            BasicArrayVector bavcol5 = new BasicArrayVector(bdvcol5, 0);
            cols.Add(bavcol5);
            List<IVector> bdvcol6 = new List<IVector>();
            IVector v1281 = new BasicDecimal128Vector(3, 10);
            v1281.set(0, new BasicDecimal128("15645.00", 10));
            v1281.set(1, new BasicDecimal128("24635.00001", 10));
            v1281.set(2, new BasicDecimal128("24635.00001", 10));
            bdvcol6.Add(v1281);
            bdvcol6.Add(v1281);
            bdvcol6.Add(v1281);
            BasicArrayVector bavcol6 = new BasicArrayVector(bdvcol6, 10);
            cols.Add(bavcol6);
            List<IVector> bdvcol7 = new List<IVector>();
            IVector v1282 = new BasicDecimal128Vector(3, 37);
            v1282.set(0, new BasicDecimal128("1.00", 37));
            v1282.set(1, new BasicDecimal128("1.00001", 37));
            v1282.set(2, new BasicDecimal128("0.00001", 37));
            bdvcol7.Add(v1282);
            bdvcol7.Add(v1282);
            bdvcol7.Add(v1282);
            BasicArrayVector bavcol7 = new BasicArrayVector(bdvcol7, 37);
            cols.Add(bavcol7);

            BasicTable bt = new BasicTable(colNames, cols);
            aftu.upsert(bt);
            BasicTable res = (BasicTable)conn.run("select * from loadTable(\"dfs://testArrayVector\",\"pt\");");
            Assert.AreEqual(3, res.rows());
            Assert.AreEqual(v32.getString(), ((BasicArrayVector)(res.getColumn("col0"))).getEntity(0).getString());
            Assert.AreEqual(v321.getString(), ((BasicArrayVector)(res.getColumn("col1"))).getEntity(0).getString());
            Assert.AreEqual(v640.getString(), ((BasicArrayVector)(res.getColumn("col2"))).getEntity(0).getString());
            Assert.AreEqual(v641.getString(), ((BasicArrayVector)(res.getColumn("col3"))).getEntity(0).getString());
            Assert.AreEqual(v642.getString(), ((BasicArrayVector)(res.getColumn("col4"))).getEntity(0).getString());
            Assert.AreEqual(v128.getString(), ((BasicArrayVector)(res.getColumn("col5"))).getEntity(0).getString());
            Assert.AreEqual(v1281.getString(), ((BasicArrayVector)(res.getColumn("col6"))).getEntity(0).getString());
            Assert.AreEqual(v1282.getString(), ((BasicArrayVector)(res.getColumn("col7"))).getEntity(0).getString());
            conn.close();
        }
    }
}


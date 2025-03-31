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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cblob,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "share t as st;";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,blob(\"dolphindb\")as cblob,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";

            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(2787877,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(-2799897897,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt);\n";
            script += "pt.append!(t);\n";

            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,9 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(9.6767676722227,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createTable(t,`pt,);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            script += "share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as pt1\n";
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
            conn.run("undef(`pt1, SHARED)");
            conn.close();

        }
        [TestMethod]
        public void Test_autoFitTableUpsert_tableName_error()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            string script = null;
            script += "share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as pt1\n";
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
            conn.run("undef(`pt1, SHARED)");
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
            script += "ccomplex = complex(-11 0 22,-22.4 0 77.5)\n";
            script += "t = indexedTable(`cint,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,";
            script += "csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,";
            script += "cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128,ccomplex);";
            script += "if(existsDatabase(\"dfs://partitionedTable\")){\n";
            script += "dropDatabase(\"dfs://partitionedTable\")}\n";
            script += "db = database(\"dfs://partitionedTable\",VALUE,1..10);";
            script += "pt = db.createPartitionedTable(t,`pt,`cint);\n";
            script += "pt.append!(t);\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("table(true as cbool,'d' as cchar,86h as cshort,10 as cint,726l as clong,2021.09.23 as cdate,2021.10M as cmonth,14:55:26.903 as ctime,15:27m as cminute,14:27:35 as csecond,2018.11.11 11:11:11 as cdatetime,2010.09.29 11:35:47.295 as ctimestamp,12:25:45.284729843 as cnanotime,2018.09.15 15:32:32.734728902 as cnanotimestamp,5.7f as cfloat,0.86 as cdouble,\"single\" as cstring,datehour(2022.08.23 17:33:54.324) as cdatehour,decimal32(19,2) as cdecimal32,decimal64(27,4) as cdecimal64,decimal128(27,10) as cdecimal128, complex(27.9999,1.111) as ccomplex)");
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
            var colNames = new List<String>() { "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "uuidv", "datehourv", "ippaddrv", "int128v", "blobv", "decimal32v", "decimal64v", "decimal128v", "complexv" };
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
            cols.Add(new BasicComplexVector(rowNum));
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[STRING, 2],,\"TSDB\");\n t= table(100:0,`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v`complexv, [INT, BOOL, CHAR, SHORT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(1), DECIMAL64(10), DECIMAL128(20),COMPLEX]);\n pt=db.createPartitionedTable(t,`pt,`stringv,,`stringv);");
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
            var colNames = new List<String>() { "id", "intv", "boolv", "charv", "shortv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "uuidv", "datehourv", "ippaddrv", "int128v", "decimal32v", "decimal64v", "decimal128v", "complexv" };
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
            cols.Add(new BasicArrayVector(DATA_TYPE.DT_COMPLEX_ARRAY, 20));
            conn.run("dbPath = \"dfs://empty_table\";if(existsDatabase(dbPath)) dropDatabase(dbPath); \n db = database(dbPath, HASH,[INT, 2],,\"TSDB\");\n t= table(100:0,`id`intv`boolv`charv`shortv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`uuidv`datehourv`ippaddrv`int128v`decimal32v`decimal64v`decimal128v`complexv, [INT, INT[], BOOL[], CHAR[], SHORT[], LONG[], DOUBLE[], FLOAT[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], UUID[], DATEHOUR[], IPADDR[], INT128[], DECIMAL32(1)[], DECIMAL64(10)[], DECIMAL128(20)[], COMPLEX[]]);\n pt=db.createPartitionedTable(t,`pt,`id,,`id);");
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
        [TestMethod]
        public void Test_AutoFitTableUpsert_iotAnyVector()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol(`AAA`bbb`xxx) as value);\n select * from t ");
            Console.WriteLine(bt8.getString());
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", conn, true, new String[] { "deviceId", "timestamp" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt1);
            aftu.upsert(bt2);
            aftu.upsert(bt3);
            aftu.upsert(bt4);
            aftu.upsert(bt5);
            aftu.upsert(bt6);
            aftu.upsert(bt7);
            aftu.upsert(bt8);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(11, bt10.rows());
            Console.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", bt10.getColumn(3).getString());
        }

        [TestMethod]
        public void Test_AutoFitTableUpsert_iotAnyVector_compress_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol(`AAA`bbb`xxx) as value);\n select * from t ");
            Console.WriteLine(bt8.getString());
            DBConnection connection = new DBConnection(false, false, true);
            connection.connect(SERVER, PORT, "admin", "123456");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", connection, true, new String[] { "deviceId", "timestamp" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt1);
            aftu.upsert(bt2);
            aftu.upsert(bt3);
            aftu.upsert(bt4);
            aftu.upsert(bt5);
            aftu.upsert(bt6);
            aftu.upsert(bt7);
            aftu.upsert(bt8);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(11, bt10.rows());
            Console.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", bt10.getColumn(3).getString());
            connection.close();
        }

        //[TestMethod] SERVER有问题，后续再增加case
        public void Test_AutoFitTableUpsert_iotAnyVector_upsert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('a')] as value);\n select * from t");
            DBConnection connection = new DBConnection(false, false, true);
            connection.connect(SERVER, PORT, "admin", "123456");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", connection, true, new String[] { "deviceId" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt1);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(1, bt10.rows());
            Console.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("", bt10.getColumn(3).getString());
        }

        [TestMethod]
        public void Test_AutoFitTableUpsert_iotAnyVector_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
            List<String> colNames = new List<String>();
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(0));
            cols.Add(new BasicTimestampVector(0));
            cols.Add(new BasicStringVector(0));
            cols.Add(new BasicIntVector(0));
            BasicTable bt9 = new BasicTable(colNames, cols);
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", conn, true, new String[] { "deviceId", "timestamp" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt1);
            aftu.upsert(bt2);
            aftu.upsert(bt3);
            aftu.upsert(bt4);
            aftu.upsert(bt5);
            aftu.upsert(bt6);
            aftu.upsert(bt7);
            aftu.upsert(bt8);
            aftu.upsert(bt9);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(9, bt10.rows());
            Console.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("[, , , , , , , , ]", bt10.getColumn(3).getString());
        }

        [TestMethod]
        public void Test_AutoFitTableUpsert_iotAnyVector_null_compress_true()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  `loc1`loc2`loc3 as location, symbol([NULL,`bbb,`AAA]) as value);\n select * from t limit 1 ");
            List<String> colNames = new List<String>();
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>();
            cols.Add(new BasicIntVector(0));
            cols.Add(new BasicTimestampVector(0));
            cols.Add(new BasicStringVector(0));
            cols.Add(new BasicIntVector(0));
            BasicTable bt9 = new BasicTable(colNames, cols);
            DBConnection connection = new DBConnection(false, false, true);
            connection.connect(SERVER, PORT, "admin", "123456");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", connection, true, new String[] { "deviceId", "timestamp" }, null);
            aftu.upsert(bt);
            aftu.upsert(bt1);
            aftu.upsert(bt2);
            aftu.upsert(bt3);
            aftu.upsert(bt4);
            aftu.upsert(bt5);
            aftu.upsert(bt6);
            aftu.upsert(bt7);
            aftu.upsert(bt8);
            aftu.upsert(bt9);
            BasicTable bt10 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\",`pt);");
            Assert.AreEqual(9, bt10.rows());
            Console.WriteLine(bt10.getColumn(3).getString());
            Assert.AreEqual("[, , , , , , , , ]", bt10.getColumn(3).getString());
            connection.close();
        }

        [TestMethod]
        public void test_AutoFitTableUpsert_iotAny_write_illegal_string()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable table1 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\", `pt) ;\n");
            Console.WriteLine("table1:" + table1.getString());

            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType", "pt", conn, true, new String[] { "deviceId", "timestamp" }, null);
            List<String> colNames = new List<String>();
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>();
            BasicIntVector deviceId = new BasicIntVector(0);
            deviceId.add(1);
            deviceId.add(2);
            deviceId.add(3);
            cols.Add(deviceId);
            BasicTimestampVector timestamp = new BasicTimestampVector(0);
            timestamp.add(new DateTime(2023, 10, 1, 9, 0, 0));
            timestamp.add(new DateTime(2023, 10, 1, 9, 0, 1));
            timestamp.add(new DateTime(2023, 10, 1, 9, 0, 2));
            cols.Add(timestamp);
            BasicStringVector location = new BasicStringVector(0);
            location.add("symbol1AM\0\0ZN");
            location.add("\0symbol1AP\0PL\0");
            location.add("symbol1AM\0");

            //location.add("symbol1");
            //location.add("symbol1A");
            //location.add("symbol1AM");
            cols.Add(location);
            BasicStringVector value = new BasicStringVector(0);
            value.add("blob1am\0zn");
            value.add("\0blob1am\0zn");
            value.add("blob1amzn\0");

            //value.add("symbol1");
            //value.add("symbol1A");
            //value.add("symbol1AM");
            cols.Add(value);
            BasicTable tmpTable = new BasicTable(colNames, cols);
            Console.WriteLine("tmpTable:" + tmpTable.getString());
            aftu.upsert(tmpTable);
            BasicTable table2 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\", `pt) ;\n");
            Console.WriteLine("table2:" + table2.getString());
            Assert.AreEqual("blob1am��zn", table2.getColumn(3).get(0).getString());
            Assert.AreEqual("��blob1am��zn", table2.getColumn(3).get(1).getString());
            Assert.AreEqual("blob1amzn��", table2.getColumn(3).get(2).getString());
        }

        [TestMethod]
        public void Test_AutoFitTableUpsert_iotAnyVector_big_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType1\")) dropDatabase(\"dfs://testIOT_allDateType1\")\n" +
                    "     create database \"dfs://testIOT_allDateType1\" partitioned by   RANGE(1000000*(0..10)),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType1\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType1\",\"pt\");\n";
            conn.run(script);
            BasicTable bt = (BasicTable)conn.run("t=table(take(1..1000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(char(1..100000),1000000) as value);\n select * from t");
            BasicTable bt1 = (BasicTable)conn.run("t=table(take(1000001..2000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(short(1..100000),1000000) as value);\n select * from t");
            BasicTable bt2 = (BasicTable)conn.run("t=table(take(2000001..3000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(int(1..100000),1000000) as value);\n select * from t");
            BasicTable bt3 = (BasicTable)conn.run("t=table(take(3000001..4000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(long(1..100000),1000000) as value);\n select * from t");
            BasicTable bt4 = (BasicTable)conn.run("t=table(take(4000001..5000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(true false null,1000000) as value);\n select * from t");
            BasicTable bt5 = (BasicTable)conn.run("t=table(take(5000001..6000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33f 0 4.44f,1000000) as value);\n select * from t");
            BasicTable bt6 = (BasicTable)conn.run("t=table(take(6000001..7000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(-2.33 0 4.44,1000000) as value);\n select * from t");
            BasicTable bt7 = (BasicTable)conn.run("t=table(take(7000001..8000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, take(\"bb\"+string(0..100000), 1000000) as value);\n select * from t");
            BasicTable bt8 = (BasicTable)conn.run("t=table(take(8000001..9000000,1000000) as deviceId, take(now()+(0..100), 1000000)  as timestamp,  take(\"bb\"+string(0..100), 1000000) as location, symbol(take(NULL`bbb`AAA,1000000)) as value);\n select * from t");
            AutoFitTableUpsert aftu = new AutoFitTableUpsert("dfs://testIOT_allDateType1", "pt", conn, true, new String[] { "deviceId", "timestamp" }, null);
            DateTime startTime = DateTime.Now;
            aftu.upsert(bt);
            aftu.upsert(bt1);
            aftu.upsert(bt2);
            aftu.upsert(bt3);
            aftu.upsert(bt4);
            aftu.upsert(bt5);
            aftu.upsert(bt6);
            aftu.upsert(bt7);
            aftu.upsert(bt8);
            DateTime endTime = DateTime.Now;
            TimeSpan duration = endTime - startTime;
            Console.WriteLine("操作耗时: " + duration.TotalSeconds + " 秒");
            BasicTable bt10 = (BasicTable)conn.run("select count(*) from loadTable(\"dfs://testIOT_allDateType1\",`pt);");
            Assert.AreEqual("9000000", bt10.getColumn(0).get(0).getString());
            //Console.WriteLine(bt10.getString());
            conn.close();
        }

    }
}


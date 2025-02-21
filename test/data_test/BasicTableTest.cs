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
using System.Configuration;
using dolphindb_config;
using dolphindb_csharpapi_net_core.src;
using dolphindb.route;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicTableTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_BasicTable_Remain()
        {
            List<string> names = new List<string>();
            List<IVector> cols = new List<IVector>();
            BasicTable bt1 = new BasicTable(names, cols);
            Assert.AreEqual(bt1.rows(), 0);

            List<string> names2 = new List<string>() { "id"};
            List<IVector> cols2 = new List<IVector>() { new BasicIntVector(new int[] { 1,2,3,4})};
            BasicTable bt2 = new BasicTable(names2, cols2);
            Assert.AreEqual(bt2.getColumn("name"), null);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_null()
        {
            List<string> names1 = new List<string>();
            List<IVector> cols1 = new List<IVector>();
            BasicTable bt1 = new BasicTable(names1, cols1);
            Assert.IsNull(bt1.toDataTable());
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_bool_null()
        {
            List<string> names2 = new List<string>() { "bool" };
            BasicBooleanVector blv1 = new BasicBooleanVector(new byte[] { 0 });
            blv1.setNull(0);
            List<IVector> cols2 = new List<IVector>() { blv1 };
            BasicTable bt2 = new BasicTable(names2, cols2);
            DataTable dt1 = bt2.toDataTable();
            Assert.AreEqual(DBNull.Value, dt1.Rows[0]["bool"]);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_byte_null()
        {
            List<string> names3 = new List<string>() { "byte" };
            BasicByteVector blv2 = new BasicByteVector(new byte[] { 0 });
            blv2.setNull(0);
            List<IVector> cols3 = new List<IVector>() { blv2 };
            BasicTable bt3 = new BasicTable(names3, cols3);
            DataTable dt2 = bt3.toDataTable();
            Assert.AreEqual(DBNull.Value, dt2.Rows[0]["byte"]);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_short_null()
        {
            List<string> names4 = new List<string>() { "short" };
            BasicShortVector bsv = new BasicShortVector(1);
            bsv.setNull(0);
            List<IVector> cols4 = new List<IVector>() { bsv };
            BasicTable bt4 = new BasicTable(names4, cols4);
            DataTable dt3 = bt4.toDataTable();
            Assert.AreEqual(DBNull.Value, dt3.Rows[0]["short"]);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_int_null()
        {
            List<string> names5 = new List<string>() { "int" };
            BasicIntVector biv = new BasicIntVector(1);
            biv.setNull(0);
            List<IVector> cols5 = new List<IVector>() { biv };
            BasicTable bt5 = new BasicTable(names5, cols5);
            DataTable dt4 = bt5.toDataTable();
            Assert.AreEqual(DBNull.Value, dt4.Rows[0]["int"]);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_float_null()
        {
            List<string> names6 = new List<string>() { "float" };
            BasicFloatVector bfv = new BasicFloatVector(1);
            bfv.setNull(0);
            List<IVector> cols6 = new List<IVector>() { bfv };
            BasicTable bt6 = new BasicTable(names6, cols6);
            DataTable dt5 = bt6.toDataTable();
            Assert.AreEqual(DBNull.Value, dt5.Rows[0]["float"]);
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_BOOL()
        {
            string script = @"table(take(true false NULL, 10) as tBOOL)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("True", dt.Rows[0]["tBOOL"].ToString());
            Assert.AreEqual("False", dt.Rows[1]["tBOOL"].ToString());
            Assert.AreEqual("", dt.Rows[2]["tBOOL"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_CHAR()
        {
            string script = @"table(char(1..2) join char() as tCHAR)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("1", dt.Rows[0]["tCHAR"].ToString());
            Assert.AreEqual("2", dt.Rows[1]["tCHAR"].ToString());
            Assert.AreEqual("", dt.Rows[2]["tCHAR"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_SHORT()
        {
            string script = @"table(short(-1 0 10) as tSHORT)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("-1", dt.Rows[0]["tSHORT"].ToString());
            Assert.AreEqual("0", dt.Rows[1]["tSHORT"].ToString());
            Assert.AreEqual("10", dt.Rows[2]["tSHORT"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_INT()
        {
            string script = @"table( int(-1 0 10) as tINT)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("-1", dt.Rows[0]["tINT"].ToString());
            Assert.AreEqual("0", dt.Rows[1]["tINT"].ToString());
            Assert.AreEqual("10", dt.Rows[2]["tINT"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_LONG()
        {
            string script = @"table(long(-1 0 10) as tLONG)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("-1", dt.Rows[0]["tLONG"].ToString());
            Assert.AreEqual("0", dt.Rows[1]["tLONG"].ToString());
            Assert.AreEqual("10", dt.Rows[2]["tLONG"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_DATE()
        {
            string script = @"table(2000.01.02 1967.01.02 2099.01.02  as tDATE)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(new DateTime(2000, 01, 02), dt.Rows[0]["tDATE"]);
            Assert.AreEqual(new DateTime(1967, 01, 02), dt.Rows[1]["tDATE"]);
            Assert.AreEqual(new DateTime(2099, 01, 02), dt.Rows[2]["tDATE"]);
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_MONTH()
        {
            string script = @"table(2000.01M + 1..10 as tMONTH)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual(new DateTime(2000, 02, 01), dt.Rows[0]["tMONTH"]);
            Assert.AreEqual(new DateTime(2000, 03, 01), dt.Rows[1]["tMONTH"]);
            Assert.AreEqual(new DateTime(2000, 04, 01), dt.Rows[2]["tMONTH"]);
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_TIME()
        {
            string script = @"table( 13:30:10.008 + 1..10 as tTIME)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("13:30:10.0090000", dt.Rows[0]["tTIME"].ToString());
            Assert.AreEqual("13:30:10.0100000", dt.Rows[1]["tTIME"].ToString());
            Assert.AreEqual("13:30:10.0110000", dt.Rows[2]["tTIME"].ToString());
        }
        [TestMethod]
        public void Test_BasicTable_toDataTable_MINUTE()
        {
            string script = @"table(13:30m + 1..10 as tMINUTE)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("13:31:00", dt.Rows[0]["tMINUTE"].ToString());
            Assert.AreEqual("13:32:00", dt.Rows[1]["tMINUTE"].ToString());
            Assert.AreEqual("13:33:00", dt.Rows[2]["tMINUTE"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_SECOND()
        {
            string script = @"table(13:30:10 + 1..10 as tSECOND)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("13:30:11", dt.Rows[0]["tSECOND"].ToString());
            Assert.AreEqual("13:30:12", dt.Rows[1]["tSECOND"].ToString());
            Assert.AreEqual("13:30:13", dt.Rows[2]["tSECOND"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_DATETIME()
        {
            string script = @"table(2012.06.13T13:30:11 1969.06.13T13:30:12 1970.06.13T13:30:13 as tDATETIME)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(new DateTime(2012, 06, 13, 13, 30, 11), dt.Rows[0]["tDATETIME"]);
            Assert.AreEqual(new DateTime(1969, 06, 13, 13, 30, 12), dt.Rows[1]["tDATETIME"]);
            Assert.AreEqual(new DateTime(1970, 06, 13, 13, 30, 13), dt.Rows[2]["tDATETIME"]);
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_TIMESTAMP()
        {
            string script = @"table( 2012.06.13T13:30:10.008 1969.06.13T13:30:10.008 1970.06.13T13:30:10.008 as tTIMESTAMP)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual(new DateTime(2012, 06, 13, 13, 30, 10, 008), dt.Rows[0]["tTIMESTAMP"]);
            Assert.AreEqual(new DateTime(1969, 06, 13, 13, 30, 10, 008), dt.Rows[1]["tTIMESTAMP"]);
            Assert.AreEqual(new DateTime(1970, 06, 13, 13, 30, 10, 008), dt.Rows[2]["tTIMESTAMP"]);
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_NANOTIME()
        {
            string script = @"table(09:00:01.000100001 + 1..10 as tNANOTIME)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("09:00:01.0001000", dt.Rows[0]["tNANOTIME"].ToString());
            Assert.AreEqual("09:00:01.0001000", dt.Rows[1]["tNANOTIME"].ToString());
            Assert.AreEqual("09:00:01.0001000", dt.Rows[2]["tNANOTIME"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_NANOTIMESTAMP()
        {
            string script = @"table(2016.12.30T09:00:01.000100001 1970.01.01T09:00:01.000100001 1969.12.30T09:00:01.000100001 as tNANOTIMESTAMP)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(3, dt.Rows.Count);
            Assert.AreEqual("2016-12-30 09:00:01", ((DateTime)dt.Rows[0]["tNANOTIMESTAMP"]).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual("1970-01-01 09:00:01", ((DateTime)dt.Rows[1]["tNANOTIMESTAMP"]).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual("1969-12-30 09:00:01", ((DateTime)dt.Rows[2]["tNANOTIMESTAMP"]).ToString("yyyy-MM-dd HH:mm:ss"));
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_SYMBOL()
        {
            string script = @"data = table(1:0, [`tSYMBOL],[SYMBOL]); tSYMBOL = table(take(`A`B`C`D, 10) as tSYMBOL) ; data.append!(tSYMBOL);data;";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("A", dt.Rows[0]["tSYMBOL"].ToString());
            Assert.AreEqual("B", dt.Rows[1]["tSYMBOL"].ToString());
            Assert.AreEqual("C", dt.Rows[2]["tSYMBOL"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_STRING()
        {
            string script = @"table(take(`A`B`C`D, 10) as tSTRING)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("A", dt.Rows[0]["tSTRING"].ToString());
            Assert.AreEqual("B", dt.Rows[1]["tSTRING"].ToString());
            Assert.AreEqual("C", dt.Rows[2]["tSTRING"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_UUID()
        {
            string script = "table(take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\") join uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee88\") join uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee89\"), 10) as tUUID)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee87", dt.Rows[0]["tUUID"].ToString());
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee88", dt.Rows[1]["tUUID"].ToString());
            Assert.AreEqual("5d212a78-cc48-e3b1-4235-b4d91473ee89", dt.Rows[2]["tUUID"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_DATEHOUR()
        {
            string script = "table(take(datehour(1969.06.13T13:30:10.008) join datehour(2012.06.14T13:30:10.008) join datehour(2012.06.15T13:30:10.008), 10) as tDATEHOUR)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("1969-06-13 13:00:00", ((DateTime)dt.Rows[0]["tDATEHOUR"]).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual("2012-06-14 13:00:00", ((DateTime)dt.Rows[1]["tDATEHOUR"]).ToString("yyyy-MM-dd HH:mm:ss"));
            Assert.AreEqual("2012-06-15 13:00:00", ((DateTime)dt.Rows[2]["tDATEHOUR"]).ToString("yyyy-MM-dd HH:mm:ss"));
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_IPADDR()
        {
            string script = "table(take(ipaddr(\"192.168.1.13\") join ipaddr(\"192.168.1.14\")join ipaddr(\"192.168.1.15\"), 10) as tIPADDR)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("192.168.1.13", dt.Rows[0]["tIPADDR"].ToString());
            Assert.AreEqual("192.168.1.14", dt.Rows[1]["tIPADDR"].ToString());
            Assert.AreEqual("192.168.1.15", dt.Rows[2]["tIPADDR"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_INT128()
        {
            string script = "table(take(int128(\"e1671797c52e15f763380b45e841ec32\") join int128(\"e1671797c52e15f763380b45e841ec33\")join int128(\"e1671797c52e15f763380b45e841ec34\"), 10) as tINT128)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("e1671797c52e15f763380b45e841ec32", dt.Rows[0]["tINT128"].ToString());
            Assert.AreEqual("e1671797c52e15f763380b45e841ec33", dt.Rows[1]["tINT128"].ToString());
            Assert.AreEqual("e1671797c52e15f763380b45e841ec34", dt.Rows[2]["tINT128"].ToString());
            db.close();
        }

        [TestMethod]
        public void Test_BasicTable_toDataTable_COMPLEX()
        {
            string script = "table(take( complex(12.50,-1.48) join complex(0,0) join complex(-12.50,1.48), 10) as tCOMPLEX)";
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicTable tb = (BasicTable)db.run(script);
            DataTable dt = tb.toDataTable();
            Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreEqual("12.5-1.48i", dt.Rows[0]["tCOMPLEX"].ToString());
            Assert.AreEqual("0+0i", dt.Rows[1]["tCOMPLEX"].ToString());
            Assert.AreEqual("-12.5+1.48i", dt.Rows[2]["tCOMPLEX"].ToString());
            db.close();
        }

    }
}

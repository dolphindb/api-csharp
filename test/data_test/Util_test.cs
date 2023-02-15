using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
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
using dolphindb_config;


namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class Util_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void test_fillSchema_DataTable()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
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
            for (int i = 0; i < 10; i++)
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
            Dictionary<string, DATA_TYPE> var1 = new Dictionary<string, DATA_TYPE>();
            var1.Add("时间戳", DATA_TYPE.DT_DATE);            
            BasicTable bt1 = Utils.fillSchema(dt, var1);
            Assert.AreEqual(bt1.getColumn("时间戳").getDataType(), var1["时间戳"]);
            Assert.AreEqual(((BasicDate)bt1.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 0, 0, 0));

            Dictionary<string, DATA_TYPE> var2 = new Dictionary<string, DATA_TYPE>();
            var2.Add("时间戳", DATA_TYPE.DT_MONTH);
            BasicTable bt2 = Utils.fillSchema(dt, var2);
            Assert.AreEqual(bt2.getColumn("时间戳").getDataType(), var2["时间戳"]);
            Assert.AreEqual(((BasicMonth)bt2.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 1, 0, 0, 0));


            Dictionary<string, DATA_TYPE> var3 = new Dictionary<string, DATA_TYPE>();
            var3.Add("时间戳", DATA_TYPE.DT_TIME);
            var3.Add("timespan", DATA_TYPE.DT_TIME);
            BasicTable bt3 = Utils.fillSchema(dt, var3);
            Assert.AreEqual(bt3.getColumn("时间戳").getDataType(), var3["时间戳"]);
            Assert.AreEqual(((BasicTime)bt3.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            Assert.AreEqual(bt3.getColumn("timespan").getDataType(), var3["timespan"]);
            Assert.AreEqual(((BasicTime)bt3.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 123));


            Dictionary<string, DATA_TYPE> var4 = new Dictionary<string, DATA_TYPE>();
            var4.Add("时间戳", DATA_TYPE.DT_MINUTE);
            var4.Add("timespan", DATA_TYPE.DT_MINUTE);
            BasicTable bt4 = Utils.fillSchema(dt, var4);
            Assert.AreEqual(bt4.getColumn("时间戳").getDataType(), var4["时间戳"]);
            Assert.AreEqual(((BasicMinute)bt4.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 0));
            Assert.AreEqual(bt4.getColumn("timespan").getDataType(), var4["timespan"]);
            Assert.AreEqual(((BasicMinute)bt4.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 0, 0));


            Dictionary<string, DATA_TYPE> var5 = new Dictionary<string, DATA_TYPE>();
            var5.Add("时间戳", DATA_TYPE.DT_SECOND);
            var5.Add("timespan", DATA_TYPE.DT_SECOND);
            BasicTable bt5 = Utils.fillSchema(dt, var5);
            Assert.AreEqual(bt5.getColumn("时间戳").getDataType(), var5["时间戳"]);
            Assert.AreEqual(((BasicSecond)bt5.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            Assert.AreEqual(bt5.getColumn("timespan").getDataType(), var5["timespan"]);
            Assert.AreEqual(((BasicSecond)bt5.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 0));


            Dictionary<string, DATA_TYPE> var6 = new Dictionary<string, DATA_TYPE>();
            var6.Add("时间戳", DATA_TYPE.DT_DATETIME);
            BasicTable bt6 = Utils.fillSchema(dt, var6);
            Assert.AreEqual(bt6.getColumn("时间戳").getDataType(), var6["时间戳"]);
            Assert.AreEqual(((BasicDateTime)bt6.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

            Dictionary<string, DATA_TYPE> var7 = new Dictionary<string, DATA_TYPE>();
            var7.Add("时间戳", DATA_TYPE.DT_TIMESTAMP);
            BasicTable bt7 = Utils.fillSchema(dt, var7);
            Assert.AreEqual(bt7.getColumn("时间戳").getDataType(), var7["时间戳"]);
            Assert.AreEqual(((BasicTimestamp)bt7.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));


            Dictionary<string, DATA_TYPE> var8 = new Dictionary<string, DATA_TYPE>();
            var8.Add("时间戳", DATA_TYPE.DT_TIMESTAMP);
            BasicTable bt8 = Utils.fillSchema(dt, var8);
            Assert.AreEqual(bt8.getColumn("时间戳").getDataType(), var8["时间戳"]);
            Assert.AreEqual(((BasicTimestamp)bt8.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

            Dictionary<string, DATA_TYPE> var9 = new Dictionary<string, DATA_TYPE>();
            var9.Add("时间戳", DATA_TYPE.DT_NANOTIME);
            var9.Add("timespan", DATA_TYPE.DT_NANOTIME);
            BasicTable bt9 = Utils.fillSchema(dt, var9);
            Assert.AreEqual(bt9.getColumn("时间戳").getDataType(), var9["时间戳"]);
            Assert.AreEqual(((BasicNanoTime)bt9.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            Assert.AreEqual(bt9.getColumn("timespan").getDataType(), var9["timespan"]);
            Assert.AreEqual(((BasicNanoTime)bt9.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 123));

            Dictionary<string, DATA_TYPE> var10 = new Dictionary<string, DATA_TYPE>();
            var10.Add("时间戳", DATA_TYPE.DT_NANOTIMESTAMP);
            BasicTable bt10 = Utils.fillSchema(dt, var10);
            Assert.AreEqual(bt10.getColumn("时间戳").getDataType(), var10["时间戳"]);
            Assert.AreEqual(((BasicNanoTimestamp)bt10.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

        }
        [TestMethod]
        public void test_fillSchema_BasicTable()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
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
            for (int i = 0; i < 10; i++)
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
            Dictionary<string, DATA_TYPE> var1 = new Dictionary<string, DATA_TYPE>();
            var1.Add("时间戳", DATA_TYPE.DT_DATE);
            BasicTable btt = new BasicTable(dt);
            BasicTable bt = btt;
            BasicTable bt1 = Utils.fillSchema(bt, var1);
            //Assert.AreEqual(bt1.getColumn("时间戳").getDataType(), var1["时间戳"]);
            //Assert.AreEqual(((BasicDate)bt1.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 0, 0, 0));

            Dictionary<string, DATA_TYPE> var2 = new Dictionary<string, DATA_TYPE>();
            var2.Add("时间戳", DATA_TYPE.DT_MONTH);
            BasicTable bt2 = Utils.fillSchema(bt, var2);
            //Assert.AreEqual(bt2.getColumn("时间戳").getDataType(), var2["时间戳"]);
            //Assert.AreEqual(((BasicMonth)bt2.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 1, 0, 0, 0));


            Dictionary<string, DATA_TYPE> var3 = new Dictionary<string, DATA_TYPE>();
            var3.Add("时间戳", DATA_TYPE.DT_TIME);
            var3.Add("timespan", DATA_TYPE.DT_TIME);
            BasicTable bt3 = Utils.fillSchema(bt, var3);
            //Assert.AreEqual(bt3.getColumn("时间戳").getDataType(), var3["时间戳"]);
            //Assert.AreEqual(((BasicTime)bt3.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            //Assert.AreEqual(bt3.getColumn("timespan").getDataType(), var3["timespan"]);
            //Assert.AreEqual(((BasicTime)bt3.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 123));


            Dictionary<string, DATA_TYPE> var4 = new Dictionary<string, DATA_TYPE>();
            var4.Add("时间戳", DATA_TYPE.DT_MINUTE);
            var4.Add("timespan", DATA_TYPE.DT_MINUTE);
            BasicTable bt4 = Utils.fillSchema(bt, var4);
            //Assert.AreEqual(bt4.getColumn("时间戳").getDataType(), var4["时间戳"]);
            //Assert.AreEqual(((BasicMinute)bt4.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 0));
            //Assert.AreEqual(bt4.getColumn("timespan").getDataType(), var4["timespan"]);
            //Assert.AreEqual(((BasicMinute)bt4.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 0, 0));


            Dictionary<string, DATA_TYPE> var5 = new Dictionary<string, DATA_TYPE>();
            var5.Add("时间戳", DATA_TYPE.DT_SECOND);
            var5.Add("timespan", DATA_TYPE.DT_SECOND);
            BasicTable bt5 = Utils.fillSchema(bt, var5);
            //Assert.AreEqual(bt5.getColumn("时间戳").getDataType(), var5["时间戳"]);
            //Assert.AreEqual(((BasicSecond)bt5.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            //Assert.AreEqual(bt5.getColumn("timespan").getDataType(), var5["timespan"]);
            //Assert.AreEqual(((BasicSecond)bt5.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 0));


            Dictionary<string, DATA_TYPE> var6 = new Dictionary<string, DATA_TYPE>();
            var6.Add("时间戳", DATA_TYPE.DT_DATETIME);
            BasicTable bt6 = Utils.fillSchema(bt, var6);
            //Assert.AreEqual(bt6.getColumn("时间戳").getDataType(), var6["时间戳"]);
            //Assert.AreEqual(((BasicDateTime)bt6.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

            Dictionary<string, DATA_TYPE> var7 = new Dictionary<string, DATA_TYPE>();
            var7.Add("时间戳", DATA_TYPE.DT_TIMESTAMP);
            BasicTable bt7 = Utils.fillSchema(bt, var7);
            //Assert.AreEqual(bt7.getColumn("时间戳").getDataType(), var7["时间戳"]);
            //Assert.AreEqual(((BasicTimestamp)bt7.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));


            Dictionary<string, DATA_TYPE> var8 = new Dictionary<string, DATA_TYPE>();
            var8.Add("时间戳", DATA_TYPE.DT_TIMESTAMP);
            BasicTable bt8 = Utils.fillSchema(bt, var8);
            //Assert.AreEqual(bt8.getColumn("时间戳").getDataType(), var8["时间戳"]);
            //Assert.AreEqual(((BasicTimestamp)bt8.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

            Dictionary<string, DATA_TYPE> var9 = new Dictionary<string, DATA_TYPE>();
            var9.Add("时间戳", DATA_TYPE.DT_NANOTIME);
            var9.Add("timespan", DATA_TYPE.DT_NANOTIME);
            BasicTable bt9 = Utils.fillSchema(bt, var9);
            //Assert.AreEqual(bt9.getColumn("时间戳").getDataType(), var9["时间戳"]);
            //Assert.AreEqual(((BasicNanoTime)bt9.getColumn("时间戳").get(0)).getValue(), new TimeSpan(15, 1, 2));
            //Assert.AreEqual(bt9.getColumn("timespan").getDataType(), var9["timespan"]);
            //Assert.AreEqual(((BasicNanoTime)bt9.getColumn("timespan").get(0)).getValue(), new TimeSpan(0, 15, 15, 14, 123));

            Dictionary<string, DATA_TYPE> var10 = new Dictionary<string, DATA_TYPE>();
            var10.Add("时间戳", DATA_TYPE.DT_NANOTIMESTAMP);
            BasicTable bt10 = Utils.fillSchema(bt, var10);
            //Assert.AreEqual(bt10.getColumn("时间戳").getDataType(), var10["时间戳"]);
            //Assert.AreEqual(((BasicNanoTimestamp)bt10.getColumn("时间戳").get(0)).getValue(), new DateTime(2021, 1, 26, 15, 1, 2));

        }
        [TestMethod]
        public void test_Utils_getDataTypeString()
        {
            Assert.AreEqual("BOOL", Utils.getDataTypeString(DATA_TYPE.DT_BOOL));
            Assert.AreEqual("BYTE", Utils.getDataTypeString(DATA_TYPE.DT_BYTE));
            Assert.AreEqual("SHORT", Utils.getDataTypeString(DATA_TYPE.DT_SHORT));
            Assert.AreEqual("INT", Utils.getDataTypeString(DATA_TYPE.DT_INT));
            Assert.AreEqual("LONG", Utils.getDataTypeString(DATA_TYPE.DT_LONG));
            Assert.AreEqual("FLOAT", Utils.getDataTypeString(DATA_TYPE.DT_FLOAT));
            Assert.AreEqual("DOUBLE", Utils.getDataTypeString(DATA_TYPE.DT_DOUBLE));
            Assert.AreEqual("NANOTIME", Utils.getDataTypeString(DATA_TYPE.DT_NANOTIME));
            Assert.AreEqual("NANOTIMESTAMP", Utils.getDataTypeString(DATA_TYPE.DT_NANOTIMESTAMP));
            Assert.AreEqual("TIMESTAMP", Utils.getDataTypeString(DATA_TYPE.DT_TIMESTAMP));
            Assert.AreEqual("DATE", Utils.getDataTypeString(DATA_TYPE.DT_DATE));
            Assert.AreEqual("MONTH", Utils.getDataTypeString(DATA_TYPE.DT_MONTH));
            Assert.AreEqual("TIME", Utils.getDataTypeString(DATA_TYPE.DT_TIME));
            Assert.AreEqual("MINUTE", Utils.getDataTypeString(DATA_TYPE.DT_MINUTE));
            Assert.AreEqual("SECOND", Utils.getDataTypeString(DATA_TYPE.DT_SECOND));
            Assert.AreEqual("DATETIME", Utils.getDataTypeString(DATA_TYPE.DT_DATETIME));
            Assert.AreEqual("INT128", Utils.getDataTypeString(DATA_TYPE.DT_INT128));
            Assert.AreEqual("IPADDR", Utils.getDataTypeString(DATA_TYPE.DT_IPADDR));
            Assert.AreEqual("UUID", Utils.getDataTypeString(DATA_TYPE.DT_UUID));
            Assert.AreEqual("STRING", Utils.getDataTypeString(DATA_TYPE.DT_STRING));
            Assert.AreEqual("DT_SYMBOL", Utils.getDataTypeString(DATA_TYPE.DT_SYMBOL));
            Assert.AreEqual("25", Utils.getDataTypeString(DATA_TYPE.DT_ANY));
            //Assert.AreEqual("DECIMAL32", Utils.getDataTypeString(DATA_TYPE.DT_DECIMAL32));
            //Assert.AreEqual("DECIMAL64", Utils.getDataTypeString(DATA_TYPE.DT_DECIMAL64));
            //Assert.AreEqual("39", Utils.getDataTypeString(DATA_TYPE.DT_DECIMAL128));
        }
        [TestMethod]
        public void test_Utils_getDolphinDBType()
        {
            Assert.AreEqual("DT_BOOL", Utils.getDolphinDBType(Type.GetType("System.Boolean")).ToString());
            Assert.AreEqual("DT_BYTE", Utils.getDolphinDBType(Type.GetType("System.Byte")).ToString());
            Assert.AreEqual("DT_DOUBLE", Utils.getDolphinDBType(Type.GetType("System.Double")).ToString());
            Assert.AreEqual("DT_DATETIME", Utils.getDolphinDBType(Type.GetType("System.DateTime")).ToString());
            Assert.AreEqual("DT_SECOND", Utils.getDolphinDBType(Type.GetType("System.TimeSpan")).ToString());
            Assert.AreEqual("DT_SHORT", Utils.getDolphinDBType(Type.GetType("System.Int16")).ToString());
            Assert.AreEqual("DT_INT", Utils.getDolphinDBType(Type.GetType("System.Int32")).ToString());
            Assert.AreEqual("DT_LONG", Utils.getDolphinDBType(Type.GetType("System.Int64")).ToString());
           // Assert.AreEqual("DT_STRING", Utils.getDolphinDBType(Type.GetType("System.Decimal")).ToString());//
        }
        [TestMethod]
        public void test_Utils_typeToCategory()
        {
            Assert.AreEqual("LOGICAL", Utils.typeToCategory(DATA_TYPE.DT_BOOL).ToString());
            Assert.AreEqual("INTEGRAL", Utils.typeToCategory(DATA_TYPE.DT_BYTE).ToString());
            Assert.AreEqual("INTEGRAL", Utils.typeToCategory(DATA_TYPE.DT_SHORT).ToString());
            Assert.AreEqual("INTEGRAL", Utils.typeToCategory(DATA_TYPE.DT_INT).ToString());
            Assert.AreEqual("INTEGRAL", Utils.typeToCategory(DATA_TYPE.DT_LONG).ToString());
            Assert.AreEqual("FLOATING", Utils.typeToCategory(DATA_TYPE.DT_FLOAT).ToString());
            Assert.AreEqual("FLOATING", Utils.typeToCategory(DATA_TYPE.DT_DOUBLE).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_NANOTIME).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_NANOTIMESTAMP).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_TIMESTAMP).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_DATE).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_MONTH).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_TIME).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_MINUTE).ToString());
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_SECOND).ToString());//APICS-202
            Assert.AreEqual("TEMPORAL", Utils.typeToCategory(DATA_TYPE.DT_DATETIME).ToString());
            Assert.AreEqual("BINARY", Utils.typeToCategory(DATA_TYPE.DT_INT128).ToString());
            Assert.AreEqual("BINARY", Utils.typeToCategory(DATA_TYPE.DT_IPADDR).ToString());
            Assert.AreEqual("BINARY", Utils.typeToCategory(DATA_TYPE.DT_UUID).ToString());
            Assert.AreEqual("LITERAL", Utils.typeToCategory(DATA_TYPE.DT_STRING).ToString());
            Assert.AreEqual("LITERAL", Utils.typeToCategory(DATA_TYPE.DT_SYMBOL).ToString());
            Assert.AreEqual("MIXED", Utils.typeToCategory(DATA_TYPE.DT_ANY).ToString());
            //Assert.AreEqual("DENARY", Utils.typeToCategory(DATA_TYPE.DT_DECIMAL32).ToString());
            //Assert.AreEqual("DENARY", Utils.typeToCategory(DATA_TYPE.DT_DECIMAL64).ToString());
            //Assert.AreEqual("SYSTEM", Utils.typeToCategory(DATA_TYPE.DT_DECIMAL128).ToString());
        }
        [TestMethod]
        public void test_Utils_toDate_Scalar()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toDate((IEntity)conn.run("1234567890"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.", exception.Message);
            Assert.AreEqual("2012.06.15", Utils.toDate((IEntity)conn.run("datehour(2012.06.15 15:32:10.158)")).ToString());

        }
        [TestMethod]
        public void test_Utils_toDate_vector()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toDate((IEntity)conn.run("1:2"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.", exception.Message);
            IEntity Tb = (IEntity)conn.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
            Assert.AreEqual(new DateTime(2018, 03, 14, 0, 0, 0), ((BasicDate)((BasicDateVector)Utils.toDate(Tb)).get(0)).getValue());
            IEntity Tb1 = (IEntity)conn.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.AreEqual(new DateTime(2018, 03, 15, 0, 0, 0), ((BasicDate)((BasicDateVector)Utils.toDate(Tb1)).get(1)).getValue());
            IEntity Tb2 = (IEntity)conn.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.AreEqual(new DateTime(2018, 03, 15, 0, 0, 0), ((BasicDate)((BasicDateVector)Utils.toDate(Tb2)).get(1)).getValue());
            IEntity Tb3 = (IEntity)conn.run("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])");
            Assert.AreEqual(new DateTime(2012, 06, 15, 0, 0, 0), ((BasicDate)((BasicDateVector)Utils.toDate(Tb3)).get(1)).getValue());
        }
        [TestMethod]
        public void test_Utils_toMonth_Scalar()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toMonth((IEntity)conn.run("1234567890"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.", exception.Message);
            Assert.AreEqual("2012.6月", Utils.toMonth((IEntity)conn.run("datehour(2012.06.15 15:32:10.158)")).ToString());
            Assert.AreEqual("2017.4月", Utils.toMonth((IEntity)conn.run("2017.04.02")).ToString());
        }
        [TestMethod]
        public void test_Utils_toMonth_vector()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toMonth((IEntity)conn.run("1:2"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.", exception.Message);
            IEntity Tb = (IEntity)conn.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
            Assert.AreEqual(new DateTime(2018, 03, 01), ((BasicMonth)((BasicMonthVector)Utils.toMonth(Tb)).get(0)).getValue());
            IEntity Tb1 = (IEntity)conn.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.AreEqual(new DateTime(2018, 03, 01), ((BasicMonth)((BasicMonthVector)Utils.toMonth(Tb1)).get(1)).getValue());
            IEntity Tb2 = (IEntity)conn.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.AreEqual(new DateTime(2018, 03, 01), ((BasicMonth)((BasicMonthVector)Utils.toMonth(Tb2)).get(1)).getValue());
            IEntity Tb3 = (IEntity)conn.run("datehour([2012.06.15 15:32:10.158,2012.06.15 17:30:10.008,2012.06.15 17:30:10.008])");
            Assert.AreEqual(new DateTime(2012, 06, 01), ((BasicMonth)((BasicMonthVector)Utils.toMonth(Tb3)).get(1)).getValue());
            IEntity Tb4 = (IEntity)conn.run("2018.03.01 2017.04.02 2016.05.03");
            Assert.AreEqual(new DateTime(2016, 05, 01), ((BasicMonth)((BasicMonthVector)Utils.toMonth(Tb4)).get(2)).getValue());

        }
        [TestMethod]
        public void test_Utils_toDateHour_Scalar()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toDateHour((IEntity)conn.run("1234567890"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.", exception.Message);
        }

        [TestMethod]
        public void test_Utils_toDateHour_vector()
        {

            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            Exception exception = null;
            try
            {
                Utils.toDateHour((IEntity)conn.run("1:2"));
            }
            catch (Exception e)
            {
                exception = e;
            }
            Assert.AreEqual("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.", exception.Message);
            IEntity Tb = (IEntity)conn.run("2018.03.14T15:41:45.123222321 2018.03.14T15:41:45.123222421 2018.03.14T15:41:45.123222521");
            Assert.AreEqual(new DateTime(2018, 03, 14, 15, 00, 00), ((BasicDateHour)((BasicDateHourVector)Utils.toDateHour(Tb)).get(0)).getValue());
            IEntity Tb1 = (IEntity)conn.run("2018.03.14T10:57:01.001 2018.03.15T10:58:02.002 2018.03.16T10:59:03.003");
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 00, 00), ((BasicDateHour)((BasicDateHourVector)Utils.toDateHour(Tb1)).get(1)).getValue());
            IEntity Tb2 = (IEntity)conn.run("2018.03.14T10:57:01 2018.03.15T10:58:02 2018.03.16T10:59:03");
            Assert.AreEqual(new DateTime(2018, 03, 15, 10, 00, 00), ((BasicDateHour)((BasicDateHourVector)Utils.toDateHour(Tb2)).get(1)).getValue());

        }

        [TestMethod]
        public void Test_CountDays()
        {
            Assert.AreEqual(int.MinValue, Utils.countDays(2018, 0, 14));
            Assert.AreEqual(int.MinValue, Utils.countDays(2018, 0, -3));
            Assert.AreEqual(int.MinValue, Utils.countDays(2018, 19, 14));
            Assert.AreEqual(int.MinValue, Utils.countDays(2012, 11, 31));
            Assert.AreEqual(15674, Utils.countDays(2012, 11, 30));
            Assert.AreEqual(17865, Utils.countDays(2018, 11, 30));
        }

        [TestMethod]
        public void Test_Category()
        {
            Assert.AreEqual(DATA_CATEGORY.NOTHING, Utils.typeToCategory(DATA_TYPE.DT_VOID));
            Assert.AreEqual(DATA_CATEGORY.BINARY, Utils.getCategory(DATA_TYPE.DT_INT128));
            Assert.AreEqual(DATA_CATEGORY.BINARY, Utils.getCategory(DATA_TYPE.DT_UUID));
            Assert.AreEqual(DATA_CATEGORY.BINARY, Utils.getCategory(DATA_TYPE.DT_IPADDR));
            Assert.AreEqual(DATA_CATEGORY.MIXED, Utils.getCategory(DATA_TYPE.DT_ANY));
            Assert.AreEqual(DATA_CATEGORY.NOTHING, Utils.getCategory(DATA_TYPE.DT_VOID));
        }

        [TestMethod]
        public void Test_castDateTime()
        {
            BasicInt bi = new BasicInt(1);
            try
            {
                Utils.castDateTime(bi, DATA_TYPE.DT_LONG);
            }catch(Exception e)
            {
                Assert.AreEqual("The target date/time type supports MONTH/DATE only for time being.", e.Message);
                Console.WriteLine(e.Message);
            }
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            BasicTable bt = (BasicTable)conn.run("table([1,2,3] as id, [4,5,6] as age)");
            try
            {
                Utils.castDateTime(bt, DATA_TYPE.DT_LONG);
            }
            catch (Exception e)
            {
                Assert.AreEqual("The source data must be a temporal scalar/vector.", e.Message);
                Console.WriteLine(e.Message);
            }
        }

        [TestMethod]
        public void Test_divide()
        {
            Assert.AreEqual(int.MinValue, Utils.divide(int.MinValue, 4));
            Assert.AreEqual(-2, Utils.divide(-4, 2));
            Assert.AreEqual(-2, Utils.divide(-4, 3));


            Assert.AreEqual(long.MinValue, Utils.divide(long.MinValue, 4L));
            Assert.AreEqual(-2, Utils.divide(-4L, 2L));
            Assert.AreEqual(-2, Utils.divide(-4L, 3L));
        }

        [TestMethod]
        public void Test_countMonthsAndcreateObject()
        {
            Assert.AreEqual(4811, Utils.countMonths(-573067));
            Assert.AreEqual(4811, Utils.countMonths(-573066));

            //try
            //{
            //    Utils.createObject(DATA_TYPE.DT_DECIMAL32, new decimal(452), 10);
            //}catch(Exception e)
            //{
            //    Assert.AreEqual("Failed to insert data,  Cannot convert DECIMAL32", e.Message);
            //    Console.WriteLine(e.Message);
            //}

            try
            {
                Utils.createObject(DATA_TYPE.DT_SHORT, false);
            }catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. Cannot convert bool toSHORT", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                long l = 154212124242442453;
                Utils.createObject(DATA_TYPE.DT_INT, l, "int");
            }catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. int cannot be converted because it exceeds the range of INT", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                long l = 154212124242442453;
                Utils.createObject(DATA_TYPE.DT_SHORT, l, "short");
            }
            catch (Exception e)
            {
                Assert.AreEqual("Failed to insert data. short cannot be converted because it exceeds the range of SHORT", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                long l = 154212124242442453;
                Utils.createObject(DATA_TYPE.DT_BYTE, l, "byte");
            }
            catch (Exception e)
            {
                Assert.AreEqual("Failed to insert data. byte cannot be converted because it exceeds the range of BYTE", e.Message);
                Console.WriteLine(e.Message);
            }

            //try
            //{
            //    long l = 154212124242442453;
            //    Utils.createObject(DATA_TYPE.DT_DECIMAL32, l, "long");
            //}
            //catch(Exception e)
            //{
            //    Assert.AreEqual("Failed to insert data. Cannot convert long to DECIMAL32", e.Message);
            //    Console.WriteLine(e.Message);
            //}
            //int a = 4;
            //BasicDecimal32 bd32_1 = (BasicDecimal32)Utils.createObject(DATA_TYPE.DT_DECIMAL32, a, 2);
            //BasicDecimal64 bd64_1 = (BasicDecimal64)Utils.createObject(DATA_TYPE.DT_DECIMAL64, a, 2);
            //Assert.AreEqual(bd32_1.getString(), "4.00");
            //Assert.AreEqual(bd64_1.getString(), "4.00");

            try
            {
                string s = "ishgha";
                Utils.createObject(DATA_TYPE.DT_INT, s);
            }
            catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. Cannot convert string to INT", e.Message);
                Console.WriteLine(e.Message);
            }

            //long la = 4;
            //BasicDecimal32 bd32_2 = (BasicDecimal32)Utils.createObject(DATA_TYPE.DT_DECIMAL32, la, 2);
            //BasicDecimal64 bd64_2 = (BasicDecimal64)Utils.createObject(DATA_TYPE.DT_DECIMAL64, la, 2);
            //Assert.AreEqual(bd32_2.getString(), "4.00");
            //Assert.AreEqual(bd64_2.getString(), "4.00");

            float f = 4.1f;
            //BasicDecimal32 bd32_3 = (BasicDecimal32)Utils.createObject(DATA_TYPE.DT_DECIMAL32, f, 2);
            //BasicDecimal64 bd64_3 = (BasicDecimal64)Utils.createObject(DATA_TYPE.DT_DECIMAL64, f, 2);
            //Assert.AreEqual(bd32_3.getString(), "4.10");
            //Assert.AreEqual(bd64_3.getString(), "4.10");

            try
            {
                Utils.createObject(DATA_TYPE.DT_BOOL, f, 2);
            }catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. Cannot convert float to BOOL", e.Message);
                Console.WriteLine(e.Message);
            }

            double d1 = 4.1;
            double d2 = double.MaxValue;
            try
            {
                Utils.createObject(DATA_TYPE.DT_FLOAT, d2, 0);
            }catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. Cannot convert double to FLOAT", e.Message);
                Console.WriteLine(e.Message);
            }

            try
            {
                Utils.createObject(DATA_TYPE.DT_STRING, d1, 0);
            }catch(Exception e)
            {
                Assert.AreEqual("Failed to insert data. Cannot convert double to STRING", e.Message);
                Console.WriteLine(e.Message);
            }
            //BasicDecimal32 bd32_4 = (BasicDecimal32)Utils.createObject(DATA_TYPE.DT_DECIMAL32, d1, 2);
            //BasicDecimal64 bd64_4 = (BasicDecimal64)Utils.createObject(DATA_TYPE.DT_DECIMAL64, d1, 2);
            //Assert.AreEqual(bd32_4.getString(), "4.10");
            //Assert.AreEqual(bd64_4.getString(), "4.10");

            //try
            //{
            //    Utils.createObject(DATA_TYPE.DT_DECIMAL32, DateTime.Now);
            //}catch(Exception e)
            //{
            //    Assert.AreEqual("Failed to insert data. Cannot convert DateTime to DECIMAL32", e.Message);
            //    Console.WriteLine(e.Message);
            //}


            //try
            //{
            //    Utils.createObject(DATA_TYPE.DT_DECIMAL32, TimeSpan.FromSeconds(546135));
            //}
            //catch (Exception e)
            //{
            //    Assert.AreEqual("Failed to insert data. Cannot convert TimeSpan to DECIMAL32", e.Message);
            //    Console.WriteLine(e.Message);
            //}

            try
            {
                Utils.createObject<int>(DATA_TYPE.DT_INT, new int[] { 1, 2, 3 }, 2);
            }catch(Exception e)
            {
                Assert.AreEqual("System.Int32[]must convert to array vector", e.Message);
                Console.WriteLine(e.Message);
            }
        }
    }
}

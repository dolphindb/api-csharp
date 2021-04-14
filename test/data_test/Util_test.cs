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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class Util_test
    {
        private readonly string SERVER = "127.0.0.1";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";
        [TestMethod]
        public void test_fillSchema()
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
                dr["timespan"] = new TimeSpan(25, 15, 15, 14, 123);
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
    }
}

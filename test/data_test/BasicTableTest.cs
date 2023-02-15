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

namespace dolphindb_csharpapi_net_core_test.data_test
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
        public void Test_BasicTable_toDataTable()
        {
            List<string> names1 = new List<string>();
            List<IVector> cols1 = new List<IVector>();
            BasicTable bt1 = new BasicTable(names1, cols1);
            Assert.IsNull(bt1.toDataTable());
            
            List<string> names2 = new List<string>() { "bool" };
            BasicBooleanVector blv1 = new BasicBooleanVector(new byte[] { 0 });
            blv1.setNull(0);
            List<IVector> cols2 = new List<IVector>() { blv1 };
            BasicTable bt2 = new BasicTable(names2, cols2);
            DataTable dt1 = bt2.toDataTable();
            Assert.AreEqual(DBNull.Value, dt1.Rows[0]["bool"]);


            List<string> names3 = new List<string>() { "byte" };
            BasicByteVector blv2 = new BasicByteVector(new byte[] { 0 });
            blv2.setNull(0);
            List<IVector> cols3 = new List<IVector>() { blv2 };
            BasicTable bt3 = new BasicTable(names3, cols3);
            DataTable dt2 = bt3.toDataTable();
            Assert.AreEqual(DBNull.Value, dt2.Rows[0]["byte"]);


            List<string> names4 = new List<string>() { "short" };
            BasicShortVector bsv = new BasicShortVector(1);
            bsv.setNull(0);
            List<IVector> cols4 = new List<IVector>() { bsv };
            BasicTable bt4 = new BasicTable(names4, cols4);
            DataTable dt3 = bt4.toDataTable();
            Assert.AreEqual(DBNull.Value, dt3.Rows[0]["short"]);


            List<string> names5 = new List<string>() { "int" };
            BasicIntVector biv = new BasicIntVector(1);
            biv.setNull(0);
            List<IVector> cols5 = new List<IVector>() { biv };
            BasicTable bt5 = new BasicTable(names5, cols5);
            DataTable dt4 = bt5.toDataTable();
            Assert.AreEqual(DBNull.Value, dt4.Rows[0]["int"]);


            List<string> names6 = new List<string>() { "float" };
            BasicFloatVector bfv = new BasicFloatVector(1);
            bfv.setNull(0);
            List<IVector> cols6 = new List<IVector>() { bfv };
            BasicTable bt6 = new BasicTable(names6, cols6);
            DataTable dt5 = bt6.toDataTable();
            Assert.AreEqual(DBNull.Value, dt5.Rows[0]["float"]);
        }
    }
}

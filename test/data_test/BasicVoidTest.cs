
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb_config;
using dolphindb;
using dolphindb.data;
using System.Collections.Generic;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicVoidTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        [TestMethod]
        public void Test_Void()
        {
            dolphindb.data.Void vo = new dolphindb.data.Void();
            Assert.AreEqual(true, vo.Equals(vo));
            Assert.AreEqual(false, vo.Equals(null));
            Assert.AreEqual(false, vo.Equals(1));
        }


        [TestMethod]
        public void test_BasicVoidVector_run()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("n = 1000;void1 = table(1..n as id);");
            BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
            Assert.AreEqual(1000, table.rows());
            Assert.AreEqual("", table.getColumn("value1").get(0).getString());
            Assert.AreEqual("[, , , , , , , , , ,...]", table.getColumn("value1").getString());
            Assert.AreEqual(1000, table.getColumn("value1").rows());
            Assert.AreEqual(DATA_CATEGORY.NOTHING, table.getColumn("value1").getDataCategory());
            Assert.AreEqual(DATA_TYPE.DT_VOID, table.getColumn("value1").getDataType());
            Assert.AreEqual(typeof(Void), table.getColumn("value1").getElementClass());
            Assert.AreEqual(new Void(), table.getColumn("value1").getEntity(0));
            Assert.AreEqual("[, ]", table.getColumn("value1").getSubVector(new int[]{ 1,2}).getString());
            Assert.AreEqual(true, table.getColumn("value1").isNull(0));
            conn.run("t2 = table(100:0, `id`value1`value2`value3`value4, [INT,INT,DOUBLE,LONG,STRING]);t2.append!(tmp)");
            BasicTable table1 = (BasicTable)conn.run("select * from t2");
            Assert.AreEqual(1000, table1.rows());
        }

        [TestMethod]
        public void test_BasicVoidVector_add()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("n = 1000;void1 = table(1..n as id);");
            BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
            Assert.AreEqual("", table.getColumn("value1").get(0).getString());
            table.getColumn("value1").add( new BasicInt(1));
            Assert.AreEqual(1001, table.getColumn("value1").rows());
            table.getColumn("value1").addRange(new BasicInt(1));
            Assert.AreEqual(1002, table.getColumn("value1").rows());
            table.getColumn("value1").append(new BasicInt(1));
            Assert.AreEqual(1003, table.getColumn("value1").rows());
            table.getColumn("value1").append(new BasicIntVector(5));
            Assert.AreEqual(1008, table.getColumn("value1").rows());
        }

        [TestMethod]
        public void test_BasicVoidVector_upload()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("n = 5;void1 = table(1..n as id);");
            BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
            Assert.AreEqual(5, table.rows());
            Assert.AreEqual("", table.getColumn("value1").get(0).getString());
            BasicIntVector idv = (BasicIntVector)table.getColumn("id");
            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("id", idv);
            upObj.Add("value1", table.getColumn("value1"));
            upObj.Add("value2", table.getColumn("value2"));
            upObj.Add("value3", table.getColumn("value3"));
            upObj.Add("value4", table.getColumn("value4"));
            conn.upload(upObj);
            IEntity value1 = conn.run("value1");
            IEntity value2 = conn.run("value2");
            IEntity value3 = conn.run("value3");
            IEntity value4 = conn.run("value4");
            Assert.AreEqual("[, , , , ]", value1.getString());
            Assert.AreEqual("[, , , , ]", value2.getString());
            Assert.AreEqual("[, , , , ]", value3.getString());
            Assert.AreEqual("[, , , , ]", value4.getString());
        }

        [TestMethod]
        public void test_BasicVoidVector_upload_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("n = 1000;void1 = table(1..n as id);");
            BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
            Assert.AreEqual(1000, table.rows());
            Assert.AreEqual("", table.getColumn("value1").get(0).getString());
            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("table_upload_table", (IEntity)table);
            conn.upload(upObj);
            BasicTable table1 = (BasicTable)conn.run("table_upload_table");
            Assert.AreEqual(1000, table1.rows());
            Assert.AreEqual(5, table1.columns());
            Assert.AreEqual("", table1.getColumn("value1").get(0).getString());
        }
        [TestMethod]
        public void test_void_tableInsert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("n = 1000;void1 = table(1..n as id);");
            BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
            Assert.AreEqual(1000, table.rows());
            List<IEntity> args = new List<IEntity>() { table };
            conn.run("tb = table(100:0, `id`value1`value2`value3`value4, [INT,INT,DOUBLE,LONG,STRING]);");
            BasicInt re = (BasicInt)conn.run("tableInsert{tb}", args);
            Assert.AreEqual(1000, re.getInt());
            BasicTable table1 = (BasicTable)conn.run("select * from tb");
            Assert.AreEqual(1000, table1.rows());
            Assert.AreEqual(5, table1.columns());
            Assert.AreEqual("", (((BasicIntVector)table1.getColumn("value1")).get(0)).getString());
        }

    }
}

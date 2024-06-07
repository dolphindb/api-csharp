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

namespace dolphindb_csharp_api_test.compatibility_test.data_test
{
    [TestClass]
    public class UploadTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

    //    [TestMethod]
    //    public void Test_upload_tableInsert_imemory_table_decimal64_compress_false()
    //    {
    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL64(4)]);share t as st");
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal64Vector bd64 = new BasicDecimal64Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal64Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{st}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_dfs_table_decimal64_compress_false()
    //    {
    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        string script = null;
    //        script += "dbPath = \"dfs://tableInsert_test\"\n";
    //        script += "if(existsDatabase(dbPath))\n";
    //        script += "dropDatabase(dbPath)\n";
    //        script += "t = table(1000:0,`id`a,[INT,DECIMAL64(4)])\n";
    //        script += "db  = database(dbPath, RANGE,1 50 10000)\n";
    //        script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
    //        conn.run(script);
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal64Vector bd64 = new BasicDecimal64Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal64Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", "dfs://tableInsert_test", "pt"), args);
    //        BasicTable st = (BasicTable)conn.run("select * from pt");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.close();

    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_imemory_table_decimal32_compress_false()
    //    {

    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL32(4)]);share t as st");
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal32Vector bd64 = new BasicDecimal32Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal32Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{st}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();

    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_dfs_table_decimal32_compress_false()
    //    {

    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        string script = null;
    //        script += "dbPath = \"dfs://tableInsert_test\"\n";
    //        script += "if(existsDatabase(dbPath))\n";
    //        script += "dropDatabase(dbPath)\n";
    //        script += "t = table(1000:0,`id`a,[INT,DECIMAL32(4)])\n";
    //        script += "db  = database(dbPath, RANGE,1 50 10000)\n";
    //        script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
    //        conn.run(script);
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal32Vector bd64 = new BasicDecimal32Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal32Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", "dfs://tableInsert_test", "pt"), args);
    //        BasicTable st = (BasicTable)conn.run("select * from pt");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_insert_into_imemory_table_decimal64_compress_false()
    //    {
    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL64(4)]);share t as st");
    //        conn.run(String.Format("insert into st values({0},{1})", 1, (double)1.1));
    //        conn.run(String.Format("insert into st values({0},{1})", 2, (double)1.24));
    //        conn.run(String.Format("insert into st values({0},{1})", 3, (double)1.33));
    //        conn.run(String.Format("insert into st values({0},{1})", 4, (double)1.440111));
    //        conn.run(String.Format("insert into st values({0},{1})", 5, (double)6.5300001));
    //        conn.run(String.Format("insert into st values({0},{1})", 6, (double)7.441));
    //        conn.run(String.Format("insert into st values({0},{1})", 7, (double)8.8));
    //        conn.run(String.Format("insert into st values({0},{1})", 8, (double)8.99));
    //        conn.run(String.Format("insert into st values({0},{1})", 9, (double)7.645));

    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_insert_into_imemory_table_decimal32_compress_false()
    //    {
    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL32(4)]);share t as st");
    //        conn.run(String.Format("insert into st values({0},{1})", 1, (double)1.1));
    //        conn.run(String.Format("insert into st values({0},{1})", 2, (double)1.24));
    //        conn.run(String.Format("insert into st values({0},{1})", 3, (double)1.33));
    //        conn.run(String.Format("insert into st values({0},{1})", 4, (double)1.440111));
    //        conn.run(String.Format("insert into st values({0},{1})", 5, (double)6.5300001));
    //        conn.run(String.Format("insert into st values({0},{1})", 6, (double)7.441));
    //        conn.run(String.Format("insert into st values({0},{1})", 7, (double)8.8));
    //        conn.run(String.Format("insert into st values({0},{1})", 8, (double)8.99));
    //        conn.run(String.Format("insert into st values({0},{1})", 9, (double)7.645));
         
    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_imemory_table_decimal64_compress_true()
    //    {
    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL64(4)]);share t as st");
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal64Vector bd64 = new BasicDecimal64Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal64Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{st}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_dfs_table_decimal64_compress_true()
    //    {
    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        string script = null;
    //        script += "dbPath = \"dfs://tableInsert_test\"\n";
    //        script += "if(existsDatabase(dbPath))\n";
    //        script += "dropDatabase(dbPath)\n";
    //        script += "t = table(1000:0,`id`a,[INT,DECIMAL64(4)])\n";
    //        script += "db  = database(dbPath, RANGE,1 50 10000)\n";
    //        script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
    //        conn.run(script);
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal64Vector bd64 = new BasicDecimal64Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal64Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", "dfs://tableInsert_test", "pt"), args);
    //        BasicTable st = (BasicTable)conn.run("select * from pt");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.close();

    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_imemory_table_decimal32_compress_true()
    //    {

    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL32(4)]);share t as st");
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal32Vector bd64 = new BasicDecimal32Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal32Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{st}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();

    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_dfs_table_decimal32_compress_true()
    //    {

    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        string script = null;
    //        script += "dbPath = \"dfs://tableInsert_test\"\n";
    //        script += "if(existsDatabase(dbPath))\n";
    //        script += "dropDatabase(dbPath)\n";
    //        script += "t = table(1000:0,`id`a,[INT,DECIMAL32(4)])\n";
    //        script += "db  = database(dbPath, RANGE,1 50 10000)\n";
    //        script += "pt = db.createPartitionedTable(t,`pt,`id).append!(t)\n";
    //        conn.run(script);
    //        BasicIntVector bi = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    //        BasicDecimal32Vector bd64 = new BasicDecimal32Vector(new List<double>() { 1.1, 1.24, 1.33 }, 4);
    //        bd64.add(1.440111);
    //        bd64.addRange(new List<double>() { 6.5300001, 7.441 });
    //        bd64.append(new BasicDecimal32Vector(new List<double>() { 8.8, 8.99, 7.645 }, 4));
    //        List<IVector> cols = new List<IVector>() { bi, bd64 };
    //        List<string> names = new List<string>() { "id", "name" };
    //        BasicTable bt = new BasicTable(names, cols);
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}", "dfs://tableInsert_test", "pt"), args);
    //        BasicTable st = (BasicTable)conn.run("select * from pt");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_insert_into_imemory_table_decimal64_compress_true()
    //    {
    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL64(4)]);share t as st");
    //        conn.run(String.Format("insert into st values({0},{1})", 1, (double)1.1));
    //        conn.run(String.Format("insert into st values({0},{1})", 2, (double)1.24));
    //        conn.run(String.Format("insert into st values({0},{1})", 3, (double)1.33));
    //        conn.run(String.Format("insert into st values({0},{1})", 4, (double)1.440111));
    //        conn.run(String.Format("insert into st values({0},{1})", 5, (double)6.5300001));
    //        conn.run(String.Format("insert into st values({0},{1})", 6, (double)7.441));
    //        conn.run(String.Format("insert into st values({0},{1})", 7, (double)8.8));
    //        conn.run(String.Format("insert into st values({0},{1})", 8, (double)8.99));
    //        conn.run(String.Format("insert into st values({0},{1})", 9, (double)7.645));

    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal64)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal64)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal64)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal64)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal64)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal64)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal64)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal64)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal64)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_insert_into_imemory_table_decimal32_compress_true()
    //    {
    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("t = table(1000:0,`id`a,[INT,DECIMAL32(4)]);share t as st");
    //        conn.run(String.Format("insert into st values({0},{1})", 1, (double)1.1));
    //        conn.run(String.Format("insert into st values({0},{1})", 2, (double)1.24));
    //        conn.run(String.Format("insert into st values({0},{1})", 3, (double)1.33));
    //        conn.run(String.Format("insert into st values({0},{1})", 4, (double)1.440111));
    //        conn.run(String.Format("insert into st values({0},{1})", 5, (double)6.5300001));
    //        conn.run(String.Format("insert into st values({0},{1})", 6, (double)7.441));
    //        conn.run(String.Format("insert into st values({0},{1})", 7, (double)8.8));
    //        conn.run(String.Format("insert into st values({0},{1})", 8, (double)8.99));
    //        conn.run(String.Format("insert into st values({0},{1})", 9, (double)7.645));

    //        BasicTable st = (BasicTable)conn.run("select * from st");
    //        Assert.AreEqual(9, st.rows());
    //        Assert.AreEqual(2, st.columns());
    //        Assert.AreEqual(1.1000, ((BasicDecimal32)st.getColumn(1).get(0)).getValue());
    //        Assert.AreEqual(1.2400, ((BasicDecimal32)st.getColumn(1).get(1)).getValue());
    //        Assert.AreEqual(1.3300, ((BasicDecimal32)st.getColumn(1).get(2)).getValue());
    //        Assert.AreEqual(1.4401, ((BasicDecimal32)st.getColumn(1).get(3)).getValue());
    //        Assert.AreEqual(6.5300, ((BasicDecimal32)st.getColumn(1).get(4)).getValue());
    //        Assert.AreEqual(7.4410, ((BasicDecimal32)st.getColumn(1).get(5)).getValue());
    //        Assert.AreEqual(8.8000, ((BasicDecimal32)st.getColumn(1).get(6)).getValue());
    //        Assert.AreEqual(8.9900, ((BasicDecimal32)st.getColumn(1).get(7)).getValue());
    //        Assert.AreEqual(7.6450, ((BasicDecimal32)st.getColumn(1).get(8)).getValue());
    //        conn.run("undef(`st, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_compress_false_big_data()
    //    {
    //        DBConnection conn = new DBConnection(false, false, false, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("share table(100:0, `col0`col1`col2`col3`col4`col5`col6`col7, [DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8),DECIMAL32(0),DECIMAL32(2),DECIMAL32(6),DECIMAL32(7)]) as table1");
    //        BasicTable bt = (BasicTable)conn.run("table(decimal64(0..9999999,0) as col0, decimal64(0..9999999,2) as col1, decimal64(0..9999999,7) as col2, decimal64(0..9999999,8) as col3,decimal32(0..9999999%99,0) as col4, decimal32(0..9999999%99,2) as col5, decimal32(0..9999999%99,6) as col6, decimal32(0..9999999%99,7) as col7)");
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{table1}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from table1");
    //        for (int i = 0; i < st.columns(); i++)
    //        {
    //            for (int j = 0; j < st.rows(); j++)
    //            {
    //                Assert.AreEqual(st.getColumn(i).get(j).getObject(), bt.getColumn(i).get(j).getObject());

    //            }
    //        }
    //        Assert.AreEqual(st.getColumn(0).get(0).getObject(), bt.getColumn(0).get(0).getObject());
    //        conn.run("undef(`table1, SHARED)");
    //        conn.close();
    //    }
    //    [TestMethod]
    //    public void Test_upload_tableInsert_compress_true_big_data()
    //    {
    //        DBConnection conn = new DBConnection(false, false, true, false);
    //        conn.connect(SERVER, PORT, USER, PASSWORD);
    //        conn.run("share table(100:0, `col0`col1`col2`col3`col4`col5`col6`col7, [DECIMAL64(0),DECIMAL64(2),DECIMAL64(7),DECIMAL64(8),DECIMAL32(0),DECIMAL32(2),DECIMAL32(6),DECIMAL32(7)]) as table1");
    //        BasicTable bt = (BasicTable)conn.run("table(decimal64(0..9999999,0) as col0, decimal64(0..9999999,2) as col1, decimal64(0..9999999,7) as col2, decimal64(0..9999999,8) as col3,decimal32(0..9999999%99,0) as col4, decimal32(0..9999999%99,2) as col5, decimal32(0..9999999%99,6) as col6, decimal32(0..9999999%99,7) as col7)");
    //        List<IEntity> args = new List<IEntity>() { bt };
    //        conn.run("tableInsert{table1}", args);
    //        BasicTable st = (BasicTable)conn.run("select * from table1");
    //        for (int i = 0; i < st.columns(); i++)
    //        {
    //            for (int j = 0; j < st.rows(); j++)
    //            {
    //                Assert.AreEqual(st.getColumn(i).get(j).getObject().ToString(), bt.getColumn(i).get(j).getObject().ToString());

    //            }
    //        }
    //        Assert.AreEqual(st.getColumn(0).get(0).getObject(), bt.getColumn(0).get(0).getObject());
    //        conn.run("undef(`table1, SHARED)");
    //        conn.close();
    //    }

    }
}

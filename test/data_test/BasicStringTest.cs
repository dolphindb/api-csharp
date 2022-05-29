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

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicStringTest
    {
        private readonly string SERVER = "127.0.0.1";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";
        [TestMethod]
        public void blob_scalar()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            BasicString str = (BasicString)db.run("mt=table(take(1..1000000,1000000) as id,take(`fyf``ftf`ddtjtydtyty`通常都是,1000000) as str,take(`fyf``ftf`ddtjtydtyty`通常都是,1000000) as str1," +
                "take(`fyf``ftf`ddtjtydtyty`通常都是,1000000) as str2);blob(mt.toStdJson())");
            db.close();
        }

        [TestMethod]
        public void blob_imemory_table_download()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
        
            db.run("a=table(100:0, `id`value`memo, [INT, DOUBLE, BLOB])");
            db.run("insert into a values(10,0.5, '[{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"}]')");
            BasicTable tb = (BasicTable)db.run("a");
            Assert.IsTrue(tb.isTable());
            Assert.AreEqual(1, tb.rows());
            Assert.AreEqual(3, tb.columns());
            Assert.AreEqual("[{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"},{\"name\":\"shily\",\"sex\":\"女\",\"age\":\"23\"}]", ((BasicString)tb.getColumn(2).get(0)).getValue());

            db.close();
        }


        [TestMethod]
        public void blob_imemory_table_upload()
        {

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            db.run("share table(100:0, `id`memo, [INT,  BLOB])as st");
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("str");
            List<IVector> cols = new List<IVector>(2);
            BasicIntVector id = new BasicIntVector(2);
            id.setInt(1, 1);
            id.setInt(0, 1);

            BasicStringVector str = new BasicStringVector(2);
            String tmp = "534656 unuk6.太阳能与看见了和和 规划局广告费 hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金dd，/5额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技hudfgdg TYF  FEW 867R 8T至于金额及计划一 至于科技";
            str.setString(0, tmp);
            str.setString(1, tmp);
            cols.Add(id);
            cols.Add(str);
            BasicTable t = new BasicTable(colNames, cols);
            List<IEntity> arg = new List<IEntity>() { t };
            db.run("tableInsert{st}", arg);

            BasicTable st = (BasicTable)db.run("st");
            // Console.WriteLine(((BasicTable)st).getColumn(2).get(0).getValue());
        }
  
    [TestMethod]
    public void blob_imemory_table_upload_download_bigdata()
    {

        DBConnection db = new DBConnection();
        db.connect(SERVER, PORT);
        db.run("share table(blob(`d`f) as blob)as st");
        List<String> colNames = new List<String>(1);
        colNames.Add("str");
        List<IVector> cols = new List<IVector>(1);
        BasicStringVector vec = new BasicStringVector(1000);
        BasicString str = (BasicString)db.run("mt=table(take(1..1000000,1000) as id,take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str,take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str1," +
                  "take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str2);mt.toStdJson()");
        String tmp = str.getString();
        for(int i = 0; i < 1000; i++)
            {
                vec.setString(i,tmp+i.ToString());
            }
        cols.Add(vec);
        BasicTable tb = new BasicTable(colNames, cols);
        List<IEntity> arg = new List<IEntity>() { tb };
       // for (int i = 0; i < 100; i++)
      //  {
            db.run("tableInsert{st}", arg);
       // }

        BasicTable st = (BasicTable)db.run("st");
        Assert.AreEqual(1000, tb.rows());
            Assert.AreEqual(1, tb.columns());
            Assert.AreEqual(tmp + 8.ToString(), ((BasicTable)st).getColumn(0).get(10).getString());
    }


      /*  [TestMethod]
        public void blob_dfs_table_upload_download()
        {
            String dbPath = "'dfs://blob_test'";
            String tbName = "`pt";

            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT);
            String script = "st= table(100:0, `id`memo, [INT,  BLOB]);" +
                "login(`admin,`123456);" +
                "if(existsDatabase(" + dbPath + ")){dropDatabase(" + dbPath + ")};" +
                "db=database(" + dbPath + ",HASH,[INT,5],,'TSDB');" +
                "db.createPartitionedTable(st," + tbName + ",`id,,`id)";
            Console.Write(script);
            db.run(script);
          
            List<String> colNames = new List<String>(2);
            colNames.Add("id");
            colNames.Add("str");
            List<IVector> cols = new List<IVector>(2);
            BasicIntVector id = new BasicIntVector(1000);
            for (int i = 0; i < 1000; i++)
            {
                id.setInt(i, i);
            }

            BasicStringVector vec = new BasicStringVector(1000);
            BasicString str = (BasicString)db.run("mt=table(take(1..1000000,1000) as id,take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str,take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str1," +
                      "take(`fyf``ftf`ddtjtydtyty`通常都是,1000) as str2);mt.toStdJson()");
            String tmp = str.getString();
            for (int i = 0; i < 1000; i++)
            {
                vec.setString(i, tmp + i.ToString());
            }
            cols.Add(id);
            cols.Add(vec);
            BasicTable t = new BasicTable(colNames, cols);
            List<IEntity> arg = new List<IEntity>() {t};
            db.run(String.Format("tableInsert{{loadTable('{0}','{1}')}}","dfs://blob_test", tbName),arg);


           // BasicTable st = (BasicTable)db.run("st");
           // Console.WriteLine(((BasicTable)st).getColumn(2).get(0).getValue());
        }*/
    }
}

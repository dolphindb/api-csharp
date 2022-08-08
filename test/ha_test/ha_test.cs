using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb;
using dolphindb.data;

namespace dolphindb_csharp_api_test.ha_test
{
    [TestClass]
    public class ha_test
    {
        private readonly string SERVER = "192.168.1.167";
        private readonly int PORT = 18921;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";
        private readonly string CREATE_DFS_SCRIPT = "t = table(1:0, `date`time`sym`qty`price, [DATE,TIME,SYMBOL,INT,DOUBLE])" + 
                                                    "haStreamTable(2, t, `trades, 100000000)";
        private readonly string DBNAME = "ha_test_db";
        private readonly string TBNAME = "trades";
        private readonly string WRITE_FUNCTION_SCRIPT = "def saveData(hatb, data){return tableInsert(hatb, data)}";
        private readonly string[] HA_SITES = new string[] { "192.168.1.167:18921", "192.168.1.167:18922", "192.168.1.167:18923" };
        private readonly string[] SYMBOLS = new string[] {"appl", "goog", "ms"};
        [TestMethod]
        public void testKeepWritingTask()
        {
            
            //DBConnection conn = new DBConnection();
            //conn.connect(SERVER, PORT, USER, PASSWORD, WRITE_FUNCTION_SCRIPT, true, HA_SITES);
            //int count = 0;
            //Random rnd = new Random();
            //string funcScript = String.Format("saveData{{{0}}}", TBNAME);
            //Console.WriteLine(funcScript);
            //for (int i = 0; i <86400000; i++)
            //{
            //    IList<IEntity> args = new List<IEntity>();
            //    DateTime dt = new DateTime(2021, 1, 1);
            //    BasicDateVector bdv = new BasicDateVector(new int[] { Utils.countDays(2021,4,7)});
            //    BasicTimeVector btv = new BasicTimeVector(new int[] {i});
            //    BasicStringVector bsv = new BasicStringVector(new string[] { SYMBOLS[rnd.Next(3)] });
            //    BasicIntVector bqv = new BasicIntVector(new int[] { rnd.Next(80,100) });
            //    BasicDoubleVector bpv = new BasicDoubleVector(new double[] {rnd.NextDouble() });
            //    List<String> colNames = new List<string>() { "date", "time", "sym", "qty", "price" };
            //    List<IVector> cols = new List<IVector>() {bdv, btv, bsv, bqv, bpv};
            //    BasicTable table1 = new BasicTable(colNames, cols);
            //    args.Add(table1);

            //    BasicInt re = (BasicInt)conn.run(funcScript, args);
            //    Console.WriteLine(re.getInt());
            //}
        }
    }
}

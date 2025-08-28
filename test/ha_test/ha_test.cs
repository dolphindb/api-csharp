using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace dolphindb_csharp_api_test.ha_test
{
    [TestClass]
    public class ha_test
    {     
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

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
using dolphindb.data;
using dolphindb_config;

namespace dolphindb_csharp_api_test.compatibility_test.data_test
{
    [TestClass]
    public class BasicArrayVectorTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string DATA_DIR = MyConfigReader.DATA_DIR;

        static void compareArrayVector(BasicArrayVector t1, BasicArrayVector t2)
        {
            int row = t1.rows();
            for(int i = 0; i < row; ++i)
            {
                IVector v1 = t1.getSubVector(i);
                IVector v2 = t2.getSubVector(i);
                for(int j = 0; j < v1.rows(); ++j)
                {
                    int failCase = 0;
                    AbstractScalar e1 = (AbstractScalar)v1.get(j);
                    AbstractScalar e2 = (AbstractScalar)v2.get(j);
                    if (e1.Equals(e2) == false)
                    {
                        Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                        failCase++;
                    }
                    Assert.AreEqual(0, failCase);
                }
            }
        }

        static void compareArrayVectorWithAnyVector(BasicArrayVector t1, BasicAnyVector t2)
        {
            Assert.AreEqual(t1.rows(), t2.rows());
            int len = t1.rows();
            for(int i = 0; i < len; i++)
            {
                AbstractVector v1 = (AbstractVector)t1.getSubVector(i);
                AbstractVector v2 = (AbstractVector)t2.getEntity(i);
                int subLen = v1.rows();
                if (v1.Equals(v2)==false)
                {
                    if (v1.ToString().Equals(v2.ToString()) == false)
                    {
                        for (int j = 0; j < subLen; j++)
                        {
                            int failCase = 0;
                            AbstractScalar e1 = (AbstractScalar)v1.get(j);
                            AbstractScalar e2 = (AbstractScalar)v2.get(j);
                            if (e1.Equals(e2) == false)
                            {
                                Console.WriteLine("subVector " + i + ", index " + j + " expected: " + e2.getString() + " actual: " + e1.getString());
                                failCase++;
                            }
                            Assert.AreEqual(0, failCase);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void read_error_data()
        {
            DBConnection db = new DBConnection();
            db.connect(SERVER, PORT, "admin", "123456");
            BasicTable data = (BasicTable)db.run(String.Format("loadText('{0}api_error_data.csv')", DATA_DIR));
            //BasicTable data = (BasicTable)db.run("table(take('hc2210''hc22101', 100).symbol() as col1, take('hc2210''hc22101', 100).symbol() as col2)");
            int cols = data.columns();
            int rows = data.rows();
            for (int i = 0; i < cols; ++i)
            {
                IVector vec = data.getColumn(i);
                
                if (vec.getDataType() == DATA_TYPE.DT_SYMBOL)
                {
                    for (int j = 0; j < rows; ++j)
                    {
                        string t = ((BasicSymbolVector)vec).getString(j);
                    }
                }
            }
            db.close();
        }

        [TestMethod]
        public void Test_BasicArrayVector_Remain()
        {
            try
            {
                BasicArrayVector bav1 = new BasicArrayVector(new int[0], new BasicIntVector(new int[] { 1, 2, 3, 4 }));
            }
            catch(Exception e)
            {
                Assert.AreEqual("Index must be an array of size greater than 0.", e.Message);
                Console.WriteLine(e.Message);
            }

            BasicIntVector bi1 = new BasicIntVector(0);
            List<IVector> value1 = new List<IVector>();
            value1.Add(bi1);
            BasicArrayVector ba2 = new BasicArrayVector(value1);
        }

    }
}

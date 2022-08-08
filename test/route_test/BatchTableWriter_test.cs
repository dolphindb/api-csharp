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
using dolphindb.route;
using dolphindb_config;

namespace dolphindb_csharp_api_test.route_test
{
    [TestClass]
    public class BatchTableWriter_test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;

        static void compareBasicTable(BasicTable table, BasicTable newTable)
        {
            Assert.AreEqual(table.rows(), newTable.rows());
            Assert.AreEqual(table.columns(), newTable.columns());
            int cols = table.columns();
            for (int i = 0; i < cols; i++)
            {
                AbstractVector v1 = (AbstractVector)table.getColumn(i);
                AbstractVector v2 = (AbstractVector)newTable.getColumn(i);
                if (!v1.Equals(v2))
                {
                    for (int j = 0; j < table.rows(); j++)
                    {
                        int failCase = 0;
                        AbstractScalar e1 = (AbstractScalar)table.getColumn(i).get(j);
                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        if (e1.Equals(e2) == false)
                        {
                            Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }

        }

        static void compareBasicTable16(BasicTable table, BasicTable newTable)
        {
            Assert.AreEqual(table.rows(), newTable.rows());
            Assert.AreEqual(table.columns(), newTable.columns());
            int cols = table.columns();
            for (int i = 0; i < cols; i++)
            {
                AbstractVector v1 = (AbstractVector)table.getColumn(i);
                AbstractVector v2 = (AbstractVector)newTable.getColumn(i);
                if (!v1.Equals(v2))
                {
                    for (int j = 0; j < table.rows(); j++)
                    {
                        int failCase = 0;
                        AbstractScalar e1 = (AbstractScalar)table.getColumn(i).get(j);
                        AbstractScalar e2 = (AbstractScalar)newTable.getColumn(i).get(j);
                        if (!e1.getString().Equals(e2.getString()))
                        {
                            Console.WriteLine("Column " + i + ", row " + j + " expected: " + e1.getString() + " actual: " + e2.getString());
                            failCase++;
                        }
                        Assert.AreEqual(0, failCase);
                    }

                }
            }

        }

        [TestMethod]
        public void Test_batchTableWriter_BasicBoolean()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [BOOL]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicBoolean(true) });
                    btw.insert("table1", "", x);
                }
                else if(i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicBoolean(false) });
                    btw.insert("table1", "", x);
                }
            }
            for(int i=0;i<10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take([true, false], 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicByte()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [CHAR]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicByte((byte)(i%99)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(char(0..999999%99), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicShort()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SHORT]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicShort((short)(i % 99)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(short(0..999999%99), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicInt()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..999999 as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicLong()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [LONG]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicLong(i) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(long(0..999999) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicDate()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DATE]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDate(i) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(date(0..999999) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicMonth()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [MONTH]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 20; i < 100; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicMonth(i) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(80))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 80);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(month(20..99) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [TIME]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicTime(new DateTime(1970, 01, 01, i%24, 00, 00, 123).TimeOfDay) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00:00.123, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicMinute()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [MINUTE]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicMinute(new DateTime(1970, 01, 01, i % 24, 00, 00, 000).TimeOfDay) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00m, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicSecond()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SECOND]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicSecond(new DateTime(1970, 01, 01, i % 24, 00, 00, 000).TimeOfDay) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:00:00, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicDateTime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DATETIME]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDateTime(new DateTime(2012, 01, 01, i % 24, 00, 00)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:00:00, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicTimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [TIMESTAMP]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicTimestamp(new DateTime(2012, 01, 01, i % 24, 00, 00, 123)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:00:00.123, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicNanotime()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [NANOTIME]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                DateTime dt = new DateTime(2012, 1, 1, i % 24, 41, 45, 123);
                long tickCount = dt.Ticks;
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicNanoTime(new DateTime(tickCount + 4567L).TimeOfDay) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(00:41:45.123456700, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable16(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicNanotimestamp()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [NANOTIMESTAMP]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                DateTime dt = new DateTime(2012, 1, 1, i % 24, 41, 45, 123);
                long tickCount = dt.Ticks;
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicNanoTimestamp(new DateTime(tickCount + 4567L)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(temporalAdd(2012.01.01T00:41:45.123456700, 0..23, 'H'), 1000000) as col0)");
            compareBasicTable16(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }


        [TestMethod]
        public void Test_batchTableWriter_BasicDouble()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [DOUBLE]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicDouble(i+0.1) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(double(0..999999+0.1) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicFloat()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [FLOAT]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {

                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicFloat((float)(i + 0.1)) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(float(0..999999+0.1) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicString()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [STRING]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString("AA"+(i%99).ToString()) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table('AA'+string(0..999999%99) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicSymbol()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [SYMBOL]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicString("AA" + (i % 99).ToString()) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(symbol('AA'+string(0..999999%99)) as col0)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicUuid()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [UUID]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicUuid(14, 15) });
                    btw.insert("table1", "", x);
                }else if(i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicUuid(14, 20) });
                    btw.insert("table1", "", x);
                }
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(uuid(['00000000-0000-000e-0000-00000000000f','00000000-0000-000e-0000-000000000014']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicIpaddr()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [IPADDR]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicIPAddr(14, 15) });
                    btw.insert("table1", "", x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicIPAddr(14, 20) });
                    btw.insert("table1", "", x);
                }
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(ipaddr(['0::e:0:0:0:f','0::e:0:0:0:14']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_BasicInt128()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`col0], [INT128]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for (int i = 0; i < 1000000; i++)
            {
                if (i % 2 == 0)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt128(14, 15) });
                    btw.insert("table1", "", x);
                }
                else if (i % 2 == 1)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt128(14, 20) });
                    btw.insert("table1", "", x);
                }
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(1000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 1000000);
                    }
                }
            }
            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(take(int128(['000000000000000e000000000000000f','000000000000000e0000000000000014']), 1000000) as col0)");
            compareBasicTable16(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_in_memory_table_huge_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            for(int i=0; i<3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA"+(i%99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i%999+0.1), new BasicInt(i%999) });
                btw.insert("table1", "", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 3000000);
                    }
                }
            }

            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_dfs_table_huge_data()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_batchTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id);";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("dfs://test_batchTableWriter", "pt");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                btw.insert("dfs://test_batchTableWriter", "pt", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 3000000);
                    }
                }
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_batchTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 'BB'+string(0..2999999%99) as str, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, str, price");
            compareBasicTable(re, expected);
            btw.removeTable("dfs://test_batchTableWriter", "pt");
            conn.run("dropDatabase('dfs://test_batchTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_dfs_table_huge_data_delta_compression()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "dbName = 'dfs://test_batchTableWriter';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, [`id, `sym, `price, `val], [INT, STRING, DOUBLE, INT]);";
            script += "db.createPartitionedTable(dummy, `pt, `id, {id:'delta', val:'delta'});";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("dfs://test_batchTableWriter", "pt");
            for (int i = 0; i < 3000000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                btw.insert("dfs://test_batchTableWriter", "pt", x);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(3000000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 3000000);
                    }
                }
            }

            BasicTable re = (BasicTable)conn.run("select * from loadTable('dfs://test_batchTableWriter', 'pt') order by id, sym, price");
            conn.run("expected = table(0..2999999 as id, 'AA'+string(0..2999999%99) as sym, 0..2999999%999+0.1 as price, 0..2999999%999 as val)");
            BasicTable expected = (BasicTable)conn.run("select * from expected order by id, sym, price");
            compareBasicTable(re, expected);
            btw.removeTable("dfs://test_batchTableWriter", "pt");
            conn.run("dropDatabase('dfs://test_batchTableWriter')");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_write_multiple_table()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1;";
            script += "share table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table2;";
            script += "share table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table3;";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            btw.addTable("table1", "");
            btw.addTable("table2", "");
            btw.addTable("table3", "");
            for (int i = 0; i < 10000; i++)
            {
                List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t1" + (i % 99).ToString()), new BasicString("tt1" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                List<IScalar> y = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t2" + (i % 99).ToString()), new BasicString("tt2" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                List<IScalar> z = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t3" + (i % 99).ToString()), new BasicString("tt3" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                btw.insert("table1", "", x);
                btw.insert("table2", "", y);
                btw.insert("table3", "", z);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(10000) && ((BasicIntVector)result.getColumn(3)).getInt(1).Equals(10000) && ((BasicIntVector)result.getColumn(3)).getInt(1).Equals(10000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 10000);
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(1), 10000);
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(2), 10000);
                    }
                }
            }

            BasicTable re1 = (BasicTable)conn.run("table1");
            BasicTable re2 = (BasicTable)conn.run("table2");
            BasicTable re3 = (BasicTable)conn.run("table3");
            BasicTable expected1 = (BasicTable)conn.run("table(0..9999 as id, 't1'+string(0..9999%99) as sym, 'tt1'+string(0..9999%99) as str, 0..9999%999+0.1 as price, 0..9999%999 as val)");
            BasicTable expected2 = (BasicTable)conn.run("table(0..9999 as id, 't2'+string(0..9999%99) as sym, 'tt2'+string(0..9999%99) as str, 0..9999%999+0.1 as price, 0..9999%999 as val)");
            BasicTable expected3 = (BasicTable)conn.run("table(0..9999 as id, 't3'+string(0..9999%99) as sym, 'tt3'+string(0..9999%99) as str, 0..9999%999+0.1 as price, 0..9999%999 as val)");
            compareBasicTable(re1, expected1);
            compareBasicTable(re2, expected2);
            compareBasicTable(re3, expected3);
            btw.removeTable("table1");

            for (int i = 10000; i < 20000; i++)
            {
                List<IScalar> y = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t2" + (i % 99).ToString()), new BasicString("tt2" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                List<IScalar> z = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t3" + (i % 99).ToString()), new BasicString("tt3" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                btw.insert("table2", "", y);
                btw.insert("table3", "", z);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(20000) && ((BasicIntVector)result.getColumn(3)).getInt(1).Equals(20000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 10000);
                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(1), 20000);
                    }
                }
            }

            BasicTable re1_2 = (BasicTable)conn.run("table1");
            BasicTable re2_2 = (BasicTable)conn.run("table2");
            BasicTable re3_2 = (BasicTable)conn.run("table3");
            BasicTable expected1_2 = (BasicTable)conn.run("table(0..9999 as id, 't1'+string(0..9999%99) as sym, 'tt1'+string(0..9999%99) as str, 0..9999%999+0.1 as price, 0..9999%999 as val)");
            BasicTable expected2_2 = (BasicTable)conn.run("table(0..19999 as id, 't2'+string(0..19999%99) as sym, 'tt2'+string(0..19999%99) as str, 0..19999%999+0.1 as price, 0..19999%999 as val)");
            BasicTable expected3_2 = (BasicTable)conn.run("table(0..19999 as id, 't3'+string(0..19999%99) as sym, 'tt3'+string(0..19999%99) as str, 0..19999%999+0.1 as price, 0..19999%999 as val)");
            compareBasicTable(re1_2, expected1_2);
            compareBasicTable(re2_2, expected2_2);
            compareBasicTable(re3_2, expected3_2);
            btw.removeTable("table2");

            for (int i = 20000; i < 30000; i++)
            {
                List<IScalar> z = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("t3" + (i % 99).ToString()), new BasicString("tt3" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                btw.insert("table3", "", z);
            }
            for (int i = 0; i < 10; i++)
            {
                Thread.Sleep(1000);
                BasicTable result = btw.getAllStatus();
                if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(30000))
                {
                    break;
                }
                else
                {
                    if (i == 9)
                    {

                        Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(1), 30000);
                    }
                }
            }

            BasicTable re1_3 = (BasicTable)conn.run("table1");
            BasicTable re2_3 = (BasicTable)conn.run("table2");
            BasicTable re3_3 = (BasicTable)conn.run("table3");
            BasicTable expected1_3 = (BasicTable)conn.run("table(0..9999 as id, 't1'+string(0..9999%99) as sym, 'tt1'+string(0..9999%99) as str, 0..9999%999+0.1 as price, 0..9999%999 as val)");
            BasicTable expected2_3 = (BasicTable)conn.run("table(0..19999 as id, 't2'+string(0..19999%99) as sym, 'tt2'+string(0..19999%99) as str, 0..19999%999+0.1 as price, 0..19999%999 as val)");
            BasicTable expected3_3 = (BasicTable)conn.run("table(0..29999 as id, 't3'+string(0..29999%99) as sym, 'tt3'+string(0..29999%99) as str, 0..29999%999+0.1 as price, 0..29999%999 as val)");
            compareBasicTable(re1_3, expected1_3);
            compareBasicTable(re2_3, expected2_3);
            compareBasicTable(re3_3, expected3_3);
            btw.removeTable("table3");

            conn.run("undef(`table1, SHARED)");
            conn.run("undef(`table2, SHARED)");
            conn.run("undef(`table3, SHARED)");
            conn.close();
        }

        [TestMethod]
        public void Test_batchTableWriter_addTable_removeTable_multiple_times()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "";
            script += "share table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]) as table1";
            conn.run(script);
            BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
            
            for (int j = 1; j <= 10; j++)
            {
                btw.addTable("table1", "");
                int start = 10000 * (j - 1);
                int end = 10000 * j;
                for (int i = start; i < end; i++)
                {
                    List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
                    btw.insert("table1", "", x);
                }
                for (int i = 0; i < 10; i++)
                {
                    Thread.Sleep(1000);
                    BasicTable result = btw.getAllStatus();
                    if (((BasicIntVector)result.getColumn(3)).getInt(0).Equals(10000))
                    {
                        break;
                    }
                    else
                    {
                        if (i == 9)
                        {
                            Assert.AreEqual(((BasicIntVector)result.getColumn(3)).getInt(0), 10000);
                        }
                    }
                }
                Assert.AreEqual(((BasicInt)conn.run("exec count(*) from table1")).getInt(), 10000 * j);
                btw.removeTable("table1");
            }

            BasicTable re = (BasicTable)conn.run("table1");
            BasicTable expected = (BasicTable)conn.run("table(0..99999 as id, 'AA'+string(0..99999%99) as sym, 'BB'+string(0..99999%99) as str, 0..99999%999+0.1 as price, 0..99999%999 as val)");
            compareBasicTable(re, expected);
            btw.removeTable("table1");
            conn.run("undef(`table1, SHARED)");
            conn.close();
        }

        //kill dolphindb and restart
        //[TestMethod]
        //public void Test_batchTableWriter_getUnwrittenData()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, USER, PASSWORD);
        //    String script = "";
        //    script += "dbName = 'dfs://test_batchTableWriter';";
        //    script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
        //    script += "db = database(dbName, HASH, [INT, 10]);";
        //    script += "dummy = table(100:0, [`id, `sym, `str, `price, `val], [INT, SYMBOL, STRING, DOUBLE, INT]);";
        //    script += "db.createPartitionedTable(dummy, `pt, `id);";
        //    conn.run(script);
        //    BatchTableWriter btw = new BatchTableWriter(SERVER, PORT, USER, PASSWORD);
        //    btw.addTable("dfs://test_batchTableWriter", "pt");
        //    int errorIndex = 0;
        //    try
        //    {
        //        for (int i = 0; i < 300000000; i++)
        //        {
        //            List<IScalar> x = new List<IScalar>(new IScalar[] { new BasicInt(i), new BasicString("AA" + (i % 99).ToString()), new BasicString("BB" + (i % 99).ToString()), new BasicDouble(i % 999 + 0.1), new BasicInt(i % 999) });
        //            btw.insert("dfs://test_batchTableWriter", "pt", x);
        //            errorIndex++;
        //        }
        //    }
        //    catch(Exception e)
        //    {
        //        int a = 1;
        //    }
        //    Thread.Sleep(1000);
        //    BasicTable fail = btw.getUnwrittenData("dfs://test_batchTableWriter", "pt");
        //    bool reStart = false;
        //    while (!reStart)
        //    {
        //        try
        //        {
        //            reStart = conn.tryReconnect();
        //        }
        //        catch(Exception e)
        //        {
        //        }
        //    }
        //    Thread.Sleep(10000);
        //    DBConnection newConn = new DBConnection();
        //    bool shouldNotAgain = false;
        //    while (!shouldNotAgain)
        //    {
        //        try
        //        {
        //            shouldNotAgain = newConn.connect("192.168.1.38", 18848, "admin", "123456");
        //        }
        //        catch(Exception e)
        //        {
        //        }
        //    }
        //    while (!((BasicBoolean)newConn.run("isChunkNodeInit()")).getValue());
        //    BasicTable t = (BasicTable)newConn.run("select * from loadTable('dfs://test_batchTableWriter', 'pt')");
        //    int count = t.rows();
        //    Assert.AreEqual(count, errorIndex - fail.rows());
        //}
    }
}

using System;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using dolphindb;
using dolphindb.data;
using System.Collections;
using System.Collections.Generic;

namespace dolphindb_csharp_api_test.compress_test
{
    [TestClass]
    public class DBConnection_Compression
    {
        //private readonly string SERVER = "127.0.0.1";
        private readonly string SERVER = "192.168.1.37";
        private readonly int PORT = 8848;
        private readonly string USER = "admin";
        private readonly string PASSWORD = "123456";

        static void compareBasicTable(BasicTable table, BasicTable newTable)
        {
            Assert.AreEqual(table.rows(), newTable.rows());
            Assert.AreEqual(table.columns(), newTable.columns());
            int cols = table.columns();
            for(int i = 0; i < cols; i++)
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
                        if (e1.Equals(e2)==false)
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
        public void Test_compression_one_row()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t = table(100:0, `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cuuid`cipaddr`cint128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, UUID, IPADDR, INT128])");

            List<IEntity> arguments = new List<IEntity>();

            BasicBooleanVector boolv = (BasicBooleanVector)conn1.run("[true]");
            BasicByteVector charv = (BasicByteVector)conn1.run("['a']");
            BasicShortVector shortv = (BasicShortVector)conn1.run("[12h]");
            BasicIntVector intv = (BasicIntVector)conn1.run("[1]");
            BasicLongVector longv = (BasicLongVector)conn1.run("[12l]");
            BasicDateVector datev = (BasicDateVector) conn1.run("[2012.01.01]");
            BasicMonthVector monthv = (BasicMonthVector)conn1.run("[2012.01M]");
            BasicTimeVector timev = (BasicTimeVector)conn1.run("[12:00:00.000]");
            BasicMinuteVector minutev = (BasicMinuteVector)conn1.run("[12:00m]");
            BasicSecondVector secondv = (BasicSecondVector)conn1.run("[12:00:00]");
            BasicDateTimeVector datetimev = (BasicDateTimeVector)conn1.run("[2012.06.13T13:30:10]");
            BasicTimestampVector timestampv = (BasicTimestampVector)conn1.run("[2012.06.13T13:30:10.008]");
            BasicNanoTimeVector nanotimev = (BasicNanoTimeVector)conn1.run("[13:30:10.008007006]");
            BasicNanoTimestampVector nanotimestampv = (BasicNanoTimestampVector)conn1.run("[2012.06.13T13:30:10.008007006]");
            BasicFloatVector floatv = (BasicFloatVector)conn1.run("[2.1f]");
            BasicDoubleVector doublev = (BasicDoubleVector)conn1.run("[2.1]");
            BasicStringVector symbolv = (BasicStringVector)conn1.run("symbol(['IBM'])");
            BasicStringVector stringv = (BasicStringVector)conn1.run("['IBM']");
            BasicUuidVector uuidv = (BasicUuidVector)conn1.run("[uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')]");
            BasicIPAddrVector ipaddrv = (BasicIPAddrVector)conn1.run("[ipaddr('192.168.1.13')]");
            BasicInt128Vector int128v = (BasicInt128Vector)conn1.run("[int128('e1671797c52e15f763380b45e841ec32')]");
            boolv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            charv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            shortv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            intv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            longv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            datev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            monthv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            timev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            minutev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            secondv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            datetimev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            timestampv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            nanotimev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            nanotimestampv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            floatv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            doublev.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            symbolv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            stringv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            uuidv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            ipaddrv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            int128v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<String>() { "cbool", "cchar", "cshort", "cint", "clong", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime", "cnanotimestamp", "cfloat", "cdouble", "csymbol", "cstring", "cuuid", "cipaddr", "cint128" };
            List<IVector> cols = new List<IVector>() { boolv, charv, shortv, intv, longv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, floatv, doublev, symbolv, stringv, uuidv, ipaddrv, int128v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(1, re.getInt());
            BasicTable newTable = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newTable);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_tt()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, USER, PASSWORD);
            conn1.connect(SERVER, PORT, USER, PASSWORD);
            conn.run("try{undef(`st, SHARED)}catch(ex){};share table(100:0, [`cint], [INT]) as st");

            List<IEntity> arguments = new List<IEntity>();

            BasicIntVector intv = (BasicIntVector)conn1.run("[1]");

            intv.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);


            List<String> colNames = new List<String>() { "cint" };
            List<IVector> cols = new List<IVector>() { intv };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{st}", arguments);
            Assert.AreEqual(1, re.getInt());
            BasicTable newTable = (BasicTable)conn.run("select * from st");
            compareBasicTable(tmp, newTable);
            conn.run("try{undef(`st, SHARED)}catch(ex){}");
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_int()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [INT, INT, INT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicIntVector col1v = (BasicIntVector)conn1.run("rand(-100..100," + n + ")");
            BasicIntVector col2v = (BasicIntVector)conn1.run("rand(1..10 join NULL," + n + ")");
            BasicIntVector col3v = (BasicIntVector)conn1.run("take([int()]," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_short()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SHORT, SHORT, SHORT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicShortVector col1v = (BasicShortVector)conn1.run("rand(short(-100..100)," + n + ")");
            BasicShortVector col2v = (BasicShortVector)conn1.run("rand(short(1..10 join NULL)," + n + ")");
            BasicShortVector col3v = (BasicShortVector)conn1.run("take([short()]," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_short_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SHORT, SHORT, SHORT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicShortVector col1v = (BasicShortVector)conn1.run("rand(short(-100..100)," + n + ")");
            BasicShortVector col2v = (BasicShortVector)conn1.run("rand(short(1..10 join NULL)," + n + ")");
            BasicShortVector col3v = (BasicShortVector)conn1.run("take([short()]," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_long()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [LONG, LONG, LONG])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicLongVector col1v = (BasicLongVector)conn1.run("rand(long(-100..100)," + n + ")");
            BasicLongVector col2v = (BasicLongVector)conn1.run("rand(long(1..10 join NULL)," + n + ")");
            BasicLongVector col3v = (BasicLongVector)conn1.run("take([long()]," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_long_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [LONG, LONG, LONG])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicLongVector col1v = (BasicLongVector)conn1.run("rand(long(-100..100)," + n + ")");
            BasicLongVector col2v = (BasicLongVector)conn1.run("rand(long(1..10 join NULL)," + n + ")");
            BasicLongVector col3v = (BasicLongVector)conn1.run("take([long()]," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_date()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [DATE, DATE, DATE])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDateVector col1v = (BasicDateVector)conn1.run("rand(date(-100..100)," + n + ")");
            BasicDateVector col2v = (BasicDateVector)conn1.run("rand(date(1..10 join NULL)," + n + ")");
            BasicDateVector col3v = (BasicDateVector)conn1.run("take(date()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_date_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [DATE, DATE, DATE])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDateVector col1v = (BasicDateVector)conn1.run("rand(date(-100..100)," + n + ")");
            BasicDateVector col2v = (BasicDateVector)conn1.run("rand(date(1..10 join NULL)," + n + ")");
            BasicDateVector col3v = (BasicDateVector)conn1.run("take(date()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_month()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [MONTH, MONTH, MONTH])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicMonthVector col1v = (BasicMonthVector)conn1.run("rand(month(-100..100)," + n + ")");
            BasicMonthVector col2v = (BasicMonthVector)conn1.run("rand(month(1..10 join NULL)," + n + ")");
            BasicMonthVector col3v = (BasicMonthVector)conn1.run("take(month()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_month_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [MONTH, MONTH, MONTH])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicMonthVector col1v = (BasicMonthVector)conn1.run("rand(month(-100..100)," + n + ")");
            BasicMonthVector col2v = (BasicMonthVector)conn1.run("rand(month(1..10 join NULL)," + n + ")");
            BasicMonthVector col3v = (BasicMonthVector)conn1.run("take(month()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_time()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [TIME, TIME, TIME])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicTimeVector col1v = (BasicTimeVector)conn1.run("rand(time(0..1000)," + n + ")");
            BasicTimeVector col2v = (BasicTimeVector)conn1.run("rand(time(1..1000 join NULL)," + n + ")");
            BasicTimeVector col3v = (BasicTimeVector)conn1.run("take(time()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_time_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [TIME, TIME, TIME])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicTimeVector col1v = (BasicTimeVector)conn1.run("rand(time(0..1000)," + n + ")");
            BasicTimeVector col2v = (BasicTimeVector)conn1.run("rand(time(1..1000 join NULL)," + n + ")");
            BasicTimeVector col3v = (BasicTimeVector)conn1.run("take(time()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_minute()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [MINUTE, MINUTE, MINUTE])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicMinuteVector col1v = (BasicMinuteVector)conn1.run("rand(minute(0..100)," + n + ")");
            BasicMinuteVector col2v = (BasicMinuteVector)conn1.run("rand(minute(1..100 join NULL)," + n + ")");
            BasicMinuteVector col3v = (BasicMinuteVector)conn1.run("take(minute()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_minute_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [MINUTE, MINUTE, MINUTE])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicMinuteVector col1v = (BasicMinuteVector)conn1.run("rand(minute(0..100)," + n + ")");
            BasicMinuteVector col2v = (BasicMinuteVector)conn1.run("rand(minute(1..100 join NULL)," + n + ")");
            BasicMinuteVector col3v = (BasicMinuteVector)conn1.run("take(minute()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_second()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SECOND, SECOND, SECOND])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicSecondVector col1v = (BasicSecondVector)conn1.run("rand(second(0..1000)," + n + ")");
            BasicSecondVector col2v = (BasicSecondVector)conn1.run("rand(second(1..1000 join NULL)," + n + ")");
            BasicSecondVector col3v = (BasicSecondVector)conn1.run("take(second()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_second_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SECOND, SECOND, SECOND])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicSecondVector col1v = (BasicSecondVector)conn1.run("rand(second(0..1000)," + n + ")");
            BasicSecondVector col2v = (BasicSecondVector)conn1.run("rand(second(1..1000 join NULL)," + n + ")");
            BasicSecondVector col3v = (BasicSecondVector)conn1.run("take(second()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_datetime()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [DATETIME, DATETIME, DATETIME])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDateTimeVector col1v = (BasicDateTimeVector)conn1.run("rand(datetime(-1000..1000)," + n + ")");
            BasicDateTimeVector col2v = (BasicDateTimeVector)conn1.run("rand(datetime(1..1000 join NULL)," + n + ")");
            BasicDateTimeVector col3v = (BasicDateTimeVector)conn1.run("take(datetime()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_datetime_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [DATETIME, DATETIME, DATETIME])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDateTimeVector col1v = (BasicDateTimeVector)conn1.run("rand(datetime(-1000..1000)," + n + ")");
            BasicDateTimeVector col2v = (BasicDateTimeVector)conn1.run("rand(datetime(1..1000 join NULL)," + n + ")");
            BasicDateTimeVector col3v = (BasicDateTimeVector)conn1.run("take(datetime()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_timestamp()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [TIMESTAMP, TIMESTAMP, TIMESTAMP])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicTimestampVector col1v = (BasicTimestampVector)conn1.run("rand(timestamp(-1000..1000)," + n + ")");
            BasicTimestampVector col2v = (BasicTimestampVector)conn1.run("rand(timestamp(1..1000 join NULL)," + n + ")");
            BasicTimestampVector col3v = (BasicTimestampVector)conn1.run("take(timestamp()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_timestamp_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [TIMESTAMP, TIMESTAMP, TIMESTAMP])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicTimestampVector col1v = (BasicTimestampVector)conn1.run("rand(timestamp(-1000..1000)," + n + ")");
            BasicTimestampVector col2v = (BasicTimestampVector)conn1.run("rand(timestamp(1..1000 join NULL)," + n + ")");
            BasicTimestampVector col3v = (BasicTimestampVector)conn1.run("take(timestamp()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_nanotime()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [NANOTIME, NANOTIME, NANOTIME])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicNanoTimeVector col1v = (BasicNanoTimeVector)conn1.run("rand(nanotime(0..100000)," + n + ")");
            BasicNanoTimeVector col2v = (BasicNanoTimeVector)conn1.run("rand(nanotime(1..1000 join NULL)," + n + ")");
            BasicNanoTimeVector col3v = (BasicNanoTimeVector)conn1.run("take(nanotime()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_nanotime_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("share table(100:0, [`col1, `col2, `col3], [NANOTIME, NANOTIME, NANOTIME]) as t");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicNanoTimeVector col1v = (BasicNanoTimeVector)conn1.run("rand(nanotime(0..100000)," + n + ")");
            BasicNanoTimeVector col2v = (BasicNanoTimeVector)conn1.run("rand(nanotime(1..1000 join NULL)," + n + ")");
            BasicNanoTimeVector col3v = (BasicNanoTimeVector)conn1.run("take(nanotime()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_nanotimestamp()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicNanoTimestampVector col1v = (BasicNanoTimestampVector)conn1.run("rand(nanotimestamp(-1000..1000)," + n + ")");
            BasicNanoTimestampVector col2v = (BasicNanoTimestampVector)conn1.run("rand(nanotimestamp(1..1000 join NULL)," + n + ")");
            BasicNanoTimestampVector col3v = (BasicNanoTimestampVector)conn1.run("take(nanotimestamp()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_nanotimestamp_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicNanoTimestampVector col1v = (BasicNanoTimestampVector)conn1.run("rand(nanotimestamp(-1000..1000)," + n + ")");
            BasicNanoTimestampVector col2v = (BasicNanoTimestampVector)conn1.run("rand(nanotimestamp(1..1000 join NULL)," + n + ")");
            BasicNanoTimestampVector col3v = (BasicNanoTimestampVector)conn1.run("take(nanotimestamp()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_float()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [FLOAT, FLOAT, FLOAT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicFloatVector col1v = (BasicFloatVector)conn1.run("rand(float(-1000..1000)," + n + ")");
            BasicFloatVector col2v = (BasicFloatVector)conn1.run("rand(float(1..1000 join NULL)," + n + ")");
            BasicFloatVector col3v = (BasicFloatVector)conn1.run("take(float()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_float_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [FLOAT, FLOAT, FLOAT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicFloatVector col1v = (BasicFloatVector)conn1.run("rand(float(-1000..1000)," + n + ")");
            BasicFloatVector col2v = (BasicFloatVector)conn1.run("rand(float(1..1000 join NULL)," + n + ")");
            BasicFloatVector col3v = (BasicFloatVector)conn1.run("take(float()," + n + ")");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_double()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [DOUBLE, DOUBLE, DOUBLE])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDoubleVector col1v = (BasicDoubleVector)conn1.run("rand(double(-1000..1000)," + n + ")");
            BasicDoubleVector col2v = (BasicDoubleVector)conn1.run("rand(double(1..1000 join NULL)," + n + ")");
            BasicDoubleVector col3v = (BasicDoubleVector)conn1.run("take(double()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_double_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [FLOAT, FLOAT, FLOAT])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicDoubleVector col1v = (BasicDoubleVector)conn1.run("rand(double(-1000..1000)," + n + ")");
            BasicDoubleVector col2v = (BasicDoubleVector)conn1.run("rand(double(1..1000 join NULL)," + n + ")");
            BasicDoubleVector col3v = (BasicDoubleVector)conn1.run("take(double()," + n + ")");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }catch(Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_symbol()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SYMBOL, SYMBOL, SYMBOL])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicSymbolVector col1v = (BasicSymbolVector)conn1.run("rand(symbol('AA'+string(1..1000))," + n + ")");
            BasicSymbolVector col2v = (BasicSymbolVector)conn1.run("rand(symbol('AA'+string(1..100) join string())," + n + ")");
            BasicSymbolVector col3v = (BasicSymbolVector)conn1.run("symbol(take(string()," + n + "))");
            
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_symbol_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [SYMBOL, SYMBOL, SYMBOL])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicSymbolVector col1v = (BasicSymbolVector)conn1.run("rand(symbol('AA'+string(1..1000))," + n + ")");
            BasicSymbolVector col2v = (BasicSymbolVector)conn1.run("rand(symbol('AA'+string(1..100) join string())," + n + ")");
            BasicSymbolVector col3v = (BasicSymbolVector)conn1.run("symbol(take(string()," + n + "))");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }catch(Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_string()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t = table(100:0, [`col1, `col2, `col3], [STRING, STRING, STRING])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicStringVector col1v = (BasicStringVector)conn1.run("string(rand('AA'+string(1..1000)," + n + "))");
            BasicStringVector col2v = (BasicStringVector)conn1.run("string(rand('AA'+string(1..100) join string()," + n + "))");
            BasicStringVector col3v = (BasicStringVector)conn1.run("take(string()," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_string_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [STRING, STRING, STRING])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicStringVector col1v = (BasicStringVector)conn1.run("string(rand('AA'+string(1..1000)," + n + "))");
            BasicStringVector col2v = (BasicStringVector)conn1.run("string(rand('AA'+string(1..100) join string()," + n + "))");
            BasicStringVector col3v = (BasicStringVector)conn1.run("take(string()," + n + ")");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }catch(Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();
        }

        [TestMethod]
        public void Test_compression_uuid()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [UUID, UUID, UUID])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicUuidVector col1v = (BasicUuidVector)conn1.run("rand(uuid()," + n + ")");
            BasicUuidVector col2v = (BasicUuidVector)conn1.run("rand(uuid()," + n + ")");
            BasicUuidVector col3v = (BasicUuidVector)conn1.run("take(uuid(string())," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_uuid_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [UUID, UUID, UUID])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicUuidVector col1v = (BasicUuidVector)conn1.run("rand(uuid()," + n + ")");
            BasicUuidVector col2v = (BasicUuidVector)conn1.run("rand(uuid()," + n + ")");
            BasicUuidVector col3v = (BasicUuidVector)conn1.run("take(uuid(string())," + n + ")");
            
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_ipaddr()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [IPADDR, IPADDR, IPADDR])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicIPAddrVector col1v = (BasicIPAddrVector)conn1.run("rand(ipaddr()," + n + ")");
            BasicIPAddrVector col2v = (BasicIPAddrVector)conn1.run("rand(ipaddr()," + n + ")");
            BasicIPAddrVector col3v = (BasicIPAddrVector)conn1.run("take(ipaddr(string())," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_ipaddr_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [IPADDR, IPADDR, IPADDR])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicIPAddrVector col1v = (BasicIPAddrVector)conn1.run("rand(ipaddr()," + n + ")");
            BasicIPAddrVector col2v = (BasicIPAddrVector)conn1.run("rand(ipaddr()," + n + ")");
            BasicIPAddrVector col3v = (BasicIPAddrVector)conn1.run("take(ipaddr(string())," + n + ")");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_int128()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [INT128, INT128, INT128])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicInt128Vector col1v = (BasicInt128Vector)conn1.run("rand(int128()," + n + ")");
            BasicInt128Vector col2v = (BasicInt128Vector)conn1.run("rand(int128()," + n + ")");
            BasicInt128Vector col3v = (BasicInt128Vector)conn1.run("take(int128(string())," + n + ")");
            col1v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col2v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);
            col3v.setCompressedMethod(Vector_Fields.COMPRESS_LZ4);

            List<String> colNames = new List<string>() { "col1", "col2", "col3" };
            List<IVector> cols = new List<IVector>() { col1v, col2v, col3v };
            BasicTable tmp = new BasicTable(colNames, cols);
            arguments.Add(tmp);
            BasicInt re = (BasicInt)conn.run("tableInsert{t}", arguments);
            Assert.AreEqual(re.getInt(), n);
            BasicTable newT = (BasicTable)conn.run("select * from t");
            compareBasicTable16(tmp, newT);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_int128_delta()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT);
            conn1.connect(SERVER, PORT);
            conn.run("t=table(100:0, [`col1, `col2, `col3], [INT128, INT128, INT128])");
            List<IEntity> arguments = new List<IEntity>();
            int n = 100000;
            BasicInt128Vector col1v = (BasicInt128Vector)conn1.run("rand(int128()," + n + ")");
            BasicInt128Vector col2v = (BasicInt128Vector)conn1.run("rand(int128()," + n + ")");
            BasicInt128Vector col3v = (BasicInt128Vector)conn1.run("take(int128(string())," + n + ")");
            Exception exception = null;
            try
            {
                col1v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col2v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
                col3v.setCompressedMethod(Vector_Fields.COMPRESS_DELTA);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNotNull(exception);
            conn.close();
            conn1.close();

        }

        [TestMethod]
        public void Test_compression_download_data()
        {
            DBConnection conn = new DBConnection(false, false, true);
            DBConnection conn1 = new DBConnection(false, false, false);
            conn.connect(SERVER, PORT, "admin", "123456");
            conn1.connect(SERVER, PORT, "admin", "123456");
            string script = "";
            script += "try{undef(`st, SHARED)}catch(ex){};\n";
            script += "n = 1000000;\n";
            script += "boolv = rand([true, false, NULL], n);\n";
            script += "charv = rand('A'..'Z', n);\n";
            script += "shortv = rand(-100h..100h join NULL, n);\n";
            script += "intv = rand(-100..100 join NULL, n);\n";
            script += "longv = rand(-100l..100l join NULL, n);\n";
            script += "datev = rand(1969.01.01..1970.12.31 join NULL, n);\n";
            script += "monthv = rand(1969.01M..1970.12M join NULL, n);\n";
            script += "timev = rand(00:00:00.000..23:59:59.999 join NULL, n);\n";
            script += "minutev = rand(00:00m..23:59m join NULL, n);\n";
            script += "secondv = rand(00:00:00..23:59:59 join NULL, n);\n";
            script += "datetimev = rand(datetime(-100..100 join NULL), n);\n";
            script += "timestampv = rand(timestamp(-100..100 join NULL), n);\n";
            script += "nanotimev = rand(nanotime(0..1000 join NULL), n);\n";
            script += "nanotimestampv = rand(nanotimestamp(-100..100 join NULL), n);\n";
            script += "floatv = rand(float(-100..100 join NULL), n);\n";
            script += "doublev = rand(double(-100..100 join NULL), n);\n";
            script += "symbolv = rand(symbol('AA'+string(1..100) join NULL), n);\n";
            script += "stringv = rand('AA'+string(1..100) join NULL, n);\n";
            script += "share table(boolv as cbool, charv as cchar, shortv as cshort, intv as cint, longv as clong, datev as cdate, monthv as cmonth, timev as ctime, minutev as cminute, secondv as csecond, datetimev as cdatetime, timestampv as ctimestamp, nanotimev as cnanotime, nanotimestampv as cnanotimestamp, floatv as cfloat, doublev as cdouble, symbolv as csymbol, stringv as cstring) as st;\n";
            Console.WriteLine("111");
            conn.run(script);
            Console.WriteLine("222");
            BasicTable tmp = (BasicTable)conn.run("st");
            Console.WriteLine("333");
            BasicTable newT = (BasicTable)conn1.run("st");
            Console.WriteLine("444");
            compareBasicTable16(tmp, newT);
            conn.run("undef(`st, SHARED)");
            conn.close();
            conn1.close();
        }

        
    }
}

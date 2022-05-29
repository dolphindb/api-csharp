using dolphindb;
using dolphindb.data;
using System;
using System.Collections.Generic;
using System.IO;

public class DFSTableWriting
{
    private static DBConnection conn;
    public static string HOST = "localhost";
    public static int PORT = 8848;

    private BasicTable createBasicTable()
    {
        List<string> colNames = new List<string>();
        colNames.Add("cbool");
        colNames.Add("cchar");
        colNames.Add("cshort");
        colNames.Add("cint");
        colNames.Add("clong");
        colNames.Add("cdate");
        colNames.Add("cmonth");
        colNames.Add("ctime");
        colNames.Add("cminute");
        colNames.Add("csecond");
        colNames.Add("cdatetime");
        colNames.Add("ctimestamp");
        colNames.Add("cnanotime");
        colNames.Add("cnanotimestamp");
        colNames.Add("cfloat");
        colNames.Add("cdouble");
        colNames.Add("csymbol");
        colNames.Add("cstring");
        List<IVector> cols = new List<IVector>();


        //boolean
        byte[] vbool = new byte[] { 1, 0 };
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.Add(bbv);
        //char
        byte[] vchar = new byte[] { (byte)'c', (byte)'a' };
        BasicByteVector bcv = new BasicByteVector(vchar);
        cols.Add(bcv);
        //cshort
        short[] vshort = new short[] { 32767, 29 };
        BasicShortVector bshv = new BasicShortVector(vshort);
        cols.Add(bshv);
        //cint
        int[] vint = new int[] { 2147483647, 483647 };
        BasicIntVector bintv = new BasicIntVector(vint);
        cols.Add(bintv);
        //clong
        long[] vlong = new long[] { 2147483647, 483647 };
        BasicLongVector blongv = new BasicLongVector(vlong);
        cols.Add(blongv);
        //cdate
        int[] vdate = new int[] { Utils.countDays(new DateTime(2018,2,14)), Utils.countDays(new DateTime(2018,8,15)) };
        BasicDateVector bdatev = new BasicDateVector(vdate);
        cols.Add(bdatev);
        //cmonth
        int[] vmonth = new int[] { Utils.countMonths(new DateTime(2018,2,6)), Utils.countMonths(new DateTime(2018,8,11)) };
        BasicMonthVector bmonthv = new BasicMonthVector(vmonth);
        cols.Add(bmonthv);
        //ctime
        int[] vtime = new int[] { Utils.countMilliseconds(16, 46, 05, 123), Utils.countMilliseconds(18, 32, 05, 321) };
        BasicTimeVector btimev = new BasicTimeVector(vtime);
        cols.Add(btimev);
        //cminute
        int[] vminute = new int[] { Utils.countMinutes(new TimeSpan(16,30,00)), Utils.countMinutes(new TimeSpan(9,30,00)) };
        BasicMinuteVector bminutev = new BasicMinuteVector(vminute);
        cols.Add(bminutev);
        //csecond
        int[] vsecond = new int[] { Utils.countSeconds(new TimeSpan(16,30,00)), Utils.countSeconds(new TimeSpan(16, 30, 50)) };
        BasicSecondVector bsecondv = new BasicSecondVector(vsecond);
        cols.Add(bsecondv);
        //cdatetime
        int[] vdatetime = new int[] { Utils.countSeconds(new DateTime(2018, 9, 8, 9, 30, 01)), Utils.countSeconds(new DateTime(2018, 11, 8, 16, 30, 01)) };
        BasicDateTimeVector bdatetimev = new BasicDateTimeVector(vdatetime);
        cols.Add(bdatetimev);
        //ctimestamp
        long[] vtimestamp = new long[] { Utils.countMilliseconds(2018, 11, 12, 9, 30, 01, 123), Utils.countMilliseconds(2018, 11, 12, 16, 30, 01, 123) };
        BasicTimestampVector btimestampv = new BasicTimestampVector(vtimestamp);
        cols.Add(btimestampv);
        //cnanotime
        long[] vnanotime = new long[] { Utils.countNanoseconds(new TimeSpan(9, 30, 05, 1234567)), Utils.countNanoseconds(new TimeSpan(16, 30, 05, 9876543)) };
        BasicNanoTimeVector bnanotimev = new BasicNanoTimeVector(vnanotime);
        cols.Add(bnanotimev);
        //cnanotimestamp
        long[] vnanotimestamp = new long[] { Utils.countNanoseconds(new DateTime(2018, 11, 12, 9, 30, 05, 123)), Utils.countNanoseconds(new DateTime(2018, 11, 13, 16, 30, 05, 987)) };
        BasicNanoTimestampVector bnanotimestampv = new BasicNanoTimestampVector(vnanotimestamp);
        cols.Add(bnanotimestampv);
        //cfloat
        float[] vfloat = new float[] { 2147.483647f, 483.647f };
        BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
        cols.Add(bfloatv);
        //cdouble
        double[] vdouble = new double[] { 214.7483647, 48.3647 };
        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
        cols.Add(bdoublev);
        //csymbol
        String[] vsymbol = new String[] { "GOOG", "MS" };
        BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
        cols.Add(bsymbolv);
        //cstring
        String[] vstring = new String[] { "", "test string" };
        BasicStringVector bstringv = new BasicStringVector(vstring);
        cols.Add(bstringv);
        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }

    public void writeDfsTable()
    {
        BasicTable table1 = createBasicTable();
        conn.login("admin", "123456", false);
        conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
        conn.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
        conn.run("db = database('dfs://testDatabase',RANGE,2018.01.01..2018.12.31)");
        conn.run("db.createPartitionedTable(t,'tb1','cdate')");
        conn.run("def saveData(data){ loadTable('dfs://testDatabase','tb1').tableInsert(data)}");
        List<IEntity> args = new List<IEntity>(1);
        args.Add(table1);
        conn.run("saveData", args);
        BasicTable dt = (BasicTable)conn.run("select * from loadTable('dfs://testDatabase','tb1')");
        if (dt.rows() != 2) {
            Console.WriteLine("failed");
        }
    }


    public static void Main(string[] args)
    {
        conn = new DBConnection();
        if (args.Length == 2)
        {
            try
            {
                HOST = args[0];
                PORT = int.Parse(args[1]);
            }
            catch (Exception)
            {
                Console.WriteLine("Wrong arguments");
            }
        }
        else if (args.Length != 2 && args.Length != 0)
        {
            Console.WriteLine("wrong arguments");
            return;
        }
        try
        {
            conn.connect(HOST, PORT);
        }
        catch (IOException e)
        {
            Console.WriteLine("Connection error");
            Console.WriteLine(e.ToString());
            Console.Write(e.StackTrace);
        }
        try
        {
            (new DFSTableWriting()).writeDfsTable();
        }
        catch (IOException)
        {
            Console.WriteLine("Writing error");
        }
    }
}
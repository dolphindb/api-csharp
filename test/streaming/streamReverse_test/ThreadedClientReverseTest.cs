using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using System.Threading;
using dolphindb_config;

namespace dolphindb_csharp_api_test.streamReverse_test
{
    [TestClass]
    public class ThreadedClientReverseTest
    {

        public static DBConnection streamConn;
        public static string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private string LOCALHOST = MyConfigReader.LOCALHOST;
        static private int SUB_FLAG = MyConfigReader.SUB_FLAG;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        public static DBConnection conn;
        private static ThreadedClient client;
        static private int total = 0;
        public void clear_env()
        {
            try
            {
                conn.run("a = getStreamingStat().pubTables\n" +
                "for(i in a){\n" +
                "\tstopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)\n" +
                "}");
                conn.run("def getAllShare(){\n" +
                    "\treturn select name from objs(true) where shared=1\n" +
                    "\t}\n" +
                    "\n" +
                    "def clearShare(){\n" +
                    "\tlogin(`admin,`123456)\n" +
                    "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                    "\tfor(i in allShare){\n" +
                    "\t\ttry{\n" +
                    "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                    "\t\t\t}catch(ex1){}\n" +
                    "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                    "\t}\n" +
                    "\ttry{\n" +
                    "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                    "\t}catch(ex1){}\n" +
                    "}\n" +
                    "clearShare()");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            //streamConn = new DBConnection();
            //streamConn.connect(SERVER, PORT, "admin", "123456");
            client = new ThreadedClient(LOCALHOST, 0);
            try { client.unsubscribe(SERVER, PORT, "pub"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "Trades"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action1"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action2"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action3"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema"); } catch (Exception ) { }
            clear_env();
        }
        [TestCleanup]
        public void TestCleanup()
        {
            try { client.unsubscribe(SERVER, PORT, "pub"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "Trades"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action1"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action2"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "pub", "action3"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema"); } catch (Exception ) { }
            clear_env();
            try
            {
                conn.run("login(`admin,`123456);" +
                        "try{dropStreamTable('pub')}catch(ex){};" +
                        "try{dropStreamTable('sub1')}catch(ex){};" +
                        "try{dropStreamTable('sub2')}catch(ex){};" +
                        "try{deleteUser(`test1)}catch(ex){};" +
                        "userlist=getUserList();" +
                        "grouplist=getGroupList();" +
                        "loop(deleteUser,userlist);" +
                        "loop(deleteGroup,grouplist)");
            }
            catch (Exception )
            {
            }
            try { client.close(); } catch (Exception ) { }
            conn.close();
            //streamConn.close();
        }

        public void PrepareStreamTable1(DBConnection conn, String tableName)
        {
            try
            {
                String script = String.Format("share(streamTable(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"{0}\")", tableName);
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void PrepareStreamTable_array(DBConnection conn, String dateType, String tableName)
        {
            try
            {
                String script = "share(streamTable(1000000:0, `permno`dateType, [INT," + dateType + ")," + tableName + ")";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareUser1(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser1\", \"123456\");grant(\"testuser1\", TABLE_READ, \"*\")}\n rpc(getControllerAlias(), test_user)";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareUser2(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser2\", \"123456\")}\n rpc(getControllerAlias(), test_user)";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareUser3(DBConnection conn)
        {
            try
            {
                String script = "def test_user(){createUser(\"testuser3\", \"123456\")}\n rpc(getControllerAlias(), test_user)";
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareStreamTable2(DBConnection conn, String tableName)
        {
            try
            {
                String script = String.Format("share(streamTable(1000000:0, `permno`timestamp`ticker`price1`vol1, [INT, TIMESTAMP, SYMBOL, DOUBLE, INT]), \"{0}\")", tableName);
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void PrepareStreamTable3(DBConnection conn, String tableName)
        {
            try
            {
                String script = String.Format("share(streamTable(1000:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`uuidv`datehourv`ippaddrv`int128v`blobv`decimal32v`decimal64v`decimal128v, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL, STRING, UUID, DATEHOUR, IPADDR, INT128, BLOB, DECIMAL32(3), DECIMAL64(4), DECIMAL128(4)]), \"{0}\")", tableName);
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void WriteStreamTable1(DBConnection conn, String tableName, int batch)
        {
            try
            {
                String str = "";
                str += "tmp = table({1}:{1},  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
                str += "tmp[`permno] = rand(1000, {1});";
                str += "tmp[`timestamp] = take(now(), {1});";
                str += "tmp[`ticker] = rand(\"A\"+string(1..1000), {1});";
                str += "tmp[`price1] = rand(100, {1});";
                str += "tmp[`price2] = rand(100, {1});";
                str += "tmp[`price3] = rand(100, {1});";
                str += "tmp[`price4] = rand(100, {1});";
                str += "tmp[`price5] = rand(100, {1});";
                str += "tmp[`vol1] = rand(100, {1});";
                str += "tmp[`vol2] = rand(100, {1});";
                str += "tmp[`vol3] = rand(100, {1});";
                str += "tmp[`vol4] = rand(100, {1});";
                str += "tmp[`vol5] = rand(100, {1});";
                str += "{0}.append!(tmp);";
                String script = String.Format(str, tableName, batch.ToString());
                conn.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void writeStreamTable_all_dateType(long dataRows,String tableName)
        {
            try
            {
            String script = "n = " + dataRows + ";\n" +
            "boolv = bool(rand([true, false, NULL],n));\n" +
            "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n))\n" +
            "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n)); \n" +
            "intv = int(take(take(-100..1000, 990) join take(int(), 4), n)); \n" +
            "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n)); \n" +
            "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n)); \n" +
            "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n)); \n" + "" +
            "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n)); \n" +
            "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n)); \n" +
            "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n)); \n" +
            "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n)); \n" +
            "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n)); \n" +
            "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n)); \n" +
            "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n)); \n" +
            "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n)); \n" +
            "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n)); \n" +
            "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n); \n" +
            "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n); \n" +
            "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n); \n" +
            "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n)); \n" +
            " ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n); \n" +
            " int128v = rand(rand(int128(), 1000) join take(int128(), 4), n); \n" +
            "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n))); \n" +
            "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n); \n" +
            "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n); \n" +
            "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3); \n" +
            "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3); \n" +
            "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3); \n" +
            "t = table(boolv, charv, intv, shortv, longv, floatv, doublev, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, symbolv, stringv, uuidv, datehourv, ippaddrv, int128v, blobv,  decimal32v, decimal64v,decimal128v);\n" +
            tableName + ".append!(t);\n";

                DBConnection conn1 = new DBConnection();
                conn1.connect(SERVER, PORT, "admin", "123456");
                conn1.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void PrepareStreamTable_array(String dataType)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "[]]) as Trades;\n" +
            "permno = take(1..1000,1000); \n" +
            "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n" +
            "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n" +
            "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n" +
            "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n" +
            "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n" + "" +
            "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n" +
            "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n" +
            "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n" +
            "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n" +
            "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n" +
            "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n" +
            "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n" +
            "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n" +
            "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n" +
            "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n" +
            "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n" +
            "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n" +
            "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n" +
            "share table(permno,dateType_" + dataType + ") as pub_t\n" +
            "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "[]]) as sub1;\n";
                DBConnection conn1 = new DBConnection();
                conn1.connect(SERVER, PORT, "admin", "123456");
                conn1.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void PrepareStreamTable_array_1()
        {
            String script = "permno = take(1..1000,1000); \n" +
            "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n" +
            "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n" +
            "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n" +
            "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n" +
            "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n" + "" +
            "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n" +
            "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n" +
            "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n" +
            "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n" +
            "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n" +
            "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n" +
            "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n" +
            "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n" +
            "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n" +
            "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n" +
            "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n" +
            "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n" +
            "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n" +
            "dateType_DECIMAL32 =   array(DECIMAL32(3)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
            "dateType_DECIMAL64 =   array(DECIMAL64(7)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
            "dateType_DECIMAL128 =   array(DECIMAL128(19)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
            "share streamTable(permno as permno,dateType_BOOL as boolv, dateType_CHAR  as charv, dateType_SHORT  as shortv, dateType_INT  as intv, dateType_LONG  as longv, dateType_DOUBLE  as doublev, dateType_FLOAT  as floatv, dateType_DATE  as datev, dateType_MONTH  as monthv, dateType_TIME  as timev, dateType_MINUTE  as minutev, dateType_SECOND  as secondv, dateType_DATETIME  as datetimev, dateType_TIMESTAMP  as timestampv, dateType_NANOTIME  as nanotimev, dateType_NANOTIMESTAMP  as nanotimestampv, dateType_DATEHOUR  as datehourv, dateType_UUID  as uuidv, dateType_IPADDR  as ipaddrv, dateType_INT128  as int128v, dateType_DECIMAL32 as decimal32v, dateType_DECIMAL64 as decimal64v, dateType_DECIMAL128 as decimal128v) as pub;\n" +
            "share streamTable(1000000:0, `permno`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v`decimal32v`decimal64v`decimal128v, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],DECIMAL32(3)[],DECIMAL64(7)[],DECIMAL128(19)[]]) as sub1;\n";
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "admin", "123456");
            conn1.run(script);
        }
        public void PrepareStreamTableDecimal_array(String dataType, int scale)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")[]]) as Trades;\n" +
                "permno = take(1..1000,1000); \n" +
                "dateType_DECIMAL32 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
                "dateType_DECIMAL64 =   array(DECIMAL64(4)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
                "dateType_DECIMAL128 =   array(DECIMAL128(8)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000*10) + 0.254, 3), 10)); \n" +
                "share table(permno,dateType_" + dataType + ") as pub_t\n" +
                "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")[]]) as sub1;\n";
                DBConnection conn1 = new DBConnection();
                conn1.connect(SERVER, PORT, "admin", "123456");
                conn1.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareStreamTable_allDateType(String dataType, int rows)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "]) as Trades;\n" +
                "setStreamTableFilterColumn(Trades, `permno); \n" +
                "permno = take(1..10," + rows + "); \n" +
                "dateType_COMPLEX =  rand(complex(rand(100, 1000), rand(100, 1000)) join NULL,  " + rows + "); \n" +
                "share table(permno,dateType_" + dataType + ") as pub_t\n" +
                "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "]) as sub1;\n";
                DBConnection conn1 = new DBConnection();
                conn1.connect(SERVER, PORT, "admin", "123456");
                conn1.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }

        public void PrepareStreamTableDecimal(String dataType, int scale)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")]) as Trades;\n" +
                "permno = take(1..1000,1000); \n" +
                "dateType_DECIMAL32 =  decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), 1000), 3); \n" +
                "dateType_DECIMAL64 =   decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), 1000), 10); \n" +
                "dateType_DECIMAL128 =   decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), 1000), 13); \n" +
                "share table(permno,dateType_" + dataType + ") as pub_t\n" +
                "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")]) as sub1;\n";
                DBConnection conn1 = new DBConnection();
                conn1.connect(SERVER, PORT, "admin", "123456");
                conn1.run(script);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
        }
        public void PrepareStreamTable_StreamDeserializer_array_allDataType()
        {

            String script = "share streamTable(10000:0, `permno`sym`blob`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v`complexv, [TIMESTAMP,SYMBOL,BLOB,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]]) as outTables;\n" +
            "permno = take(1..1000,1000); \n" +
            "timestampv = 2018.12.01T01:21:23.000 + 1..1000; \n" +
            "dateType_INT =  array(INT[]).append!(cut(take(-100..100 join NULL, 1000*10), 10)); \n" +
            "dateType_BOOL =  array(BOOL[]).append!(cut(take([true, false, NULL], 1000*10), 10)); \n" +
            "dateType_CHAR =  array(CHAR[]).append!(cut(take(char(-10..10 join NULL), 1000*10), 10)); \n" +
            "dateType_SHORT =  array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000*10), 10)); \n" +
            "dateType_LONG =  array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000*10), 10)); \n" + "" +
            "dateType_DOUBLE =  array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254, 10)); \n" +
            "dateType_FLOAT =  array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000*10) + 0.254f, 10)); \n" +
            "dateType_DATE =  array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000*10), 10)); \n" +
            "dateType_MONTH =   array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000*10), 10)); \n" +
            "dateType_TIME =  array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000*10), 10)); \n" +
            "dateType_MINUTE =  array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000*10), 10)); \n" +
            "dateType_SECOND =  array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_DATETIME =  array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000*10), 10)); \n" +
            "dateType_TIMESTAMP =  array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000*10), 10)); \n" +
            "dateType_NANOTIME =  array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_NANOTIMESTAMP =  array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000*10), 10)); \n" +
            "dateType_UUID =  array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000*10), 10)); \n" +
            "dateType_DATEHOUR =  array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000*10), 10)); \n" +
            "dateType_IPADDR =  array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000*10), 10)); \n" +
            "dateType_INT128 =  array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000*10), 10)); \n" +
            "dateType_COMPLEX =   array(COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10));; \n" +
            "dateType_POINT =  array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000*10), 10)); \n" +
            "share table(timestampv as timestamp1,dateType_BOOL as boolv, dateType_CHAR  as charv, dateType_SHORT  as shortv, dateType_INT  as intv, dateType_LONG  as longv, dateType_DOUBLE  as doublev, dateType_FLOAT  as floatv, dateType_DATE  as datev, dateType_MONTH  as monthv, dateType_TIME  as timev, dateType_MINUTE  as minutev, dateType_SECOND  as secondv, dateType_DATETIME  as datetimev, dateType_TIMESTAMP  as timestampv, dateType_NANOTIME  as nanotimev, dateType_NANOTIMESTAMP  as nanotimestampv, dateType_DATEHOUR  as datehourv, dateType_UUID  as uuidv, dateType_IPADDR  as ipaddrv, dateType_INT128  as int128v, dateType_COMPLEX as complexv) as pub_t1;\n" +
            "share table(timestampv as timestamp1,dateType_BOOL as boolv, dateType_CHAR  as charv, dateType_SHORT  as shortv, dateType_INT  as intv, dateType_LONG  as longv, dateType_DOUBLE  as doublev, dateType_FLOAT  as floatv, dateType_DATE  as datev, dateType_MONTH  as monthv, dateType_TIME  as timev, dateType_MINUTE  as minutev, dateType_SECOND  as secondv, dateType_DATETIME  as datetimev, dateType_TIMESTAMP  as timestampv, dateType_NANOTIME  as nanotimev, dateType_NANOTIMESTAMP  as nanotimestampv, dateType_DATEHOUR  as datehourv, dateType_UUID  as uuidv, dateType_IPADDR  as ipaddrv, dateType_INT128  as int128v, dateType_COMPLEX as complexv) as pub_t2;\n" +
            "d = dict(['msg1','msg2'], [pub_t1, pub_t2]);\n" +
            "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestamp1, timeColumn=`timestamp1);\n" +
            "share streamTable(1000000:0, `timestamp1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v`complexv, [TIMESTAMP,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]]) as sub1;\n" +
            "share streamTable(1000000:0, `timestamp1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v`complexv, [TIMESTAMP,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]]) as sub2;\n";
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "admin", "123456");
            conn1.run(script);
        }

        public static void checkResult(DBConnection conn)
        {
            for (int i = 0; i < 20; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt() == (1000))
                {
                    break;
                }
                Thread.Sleep(100);
            }
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(except.rows(), res.rows());
            for (int i = 0; i < 1000; i++)
            {
                Assert.AreEqual(except.getColumn(1).getEntity(i).getString(), res.getColumn(1).getEntity(i).getString());
            }
        }

        public static void checkResult1(DBConnection conn)
        {
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub2");
                if (tmpNum.getInt() == (1000) && tmpNum1.getInt() == (1000))
                {
                    break;
                }
                Thread.Sleep(2000);
            }
            conn.run("re = select * from  pub_t1 order by timestamp1;\n re1 =select * from  sub1 order by timestamp1;\n assert 1, each(eqObj,re.values(),re.values());\nassert 2, re1.rows()==1000");
            conn.run("re = select * from  pub_t2 order by timestamp1;\n re1 =select * from  sub2 order by timestamp1;\n assert 1, each(eqObj,re.values(),re.values());\n assert 2, re1.rows()==1000");
        }

        class Handler1 : MessageHandler
        {

            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        class Handler2 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub2 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler3 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub3 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler4 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {

                    SUB_FLAG += 1;
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        class Handler5 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                try
                {
                    int rows = msgs.Count;
                    BasicIntVector col1v = new BasicIntVector(rows);
                    BasicTimestampVector col2v = new BasicTimestampVector(rows);
                    BasicSymbolVector col3v = new BasicSymbolVector(rows);
                    BasicDoubleVector col4v = new BasicDoubleVector(rows);
                    BasicDoubleVector col5v = new BasicDoubleVector(rows);
                    BasicDoubleVector col6v = new BasicDoubleVector(rows);
                    BasicDoubleVector col7v = new BasicDoubleVector(rows);
                    BasicDoubleVector col8v = new BasicDoubleVector(rows);
                    BasicIntVector col9v = new BasicIntVector(rows);
                    BasicIntVector col10v = new BasicIntVector(rows);
                    BasicIntVector col11v = new BasicIntVector(rows);
                    BasicIntVector col12v = new BasicIntVector(rows);
                    BasicIntVector col13v = new BasicIntVector(rows);

                    for (int i = 0; i < rows; ++i)
                    {
                        col1v.set(i, (IScalar)msgs[i].getEntity(0));
                        col2v.set(i, (IScalar)msgs[i].getEntity(1));
                        col3v.set(i, (IScalar)msgs[i].getEntity(2));
                        col4v.set(i, (IScalar)msgs[i].getEntity(3));
                        col5v.set(i, (IScalar)msgs[i].getEntity(4));
                        col6v.set(i, (IScalar)msgs[i].getEntity(5));
                        col7v.set(i, (IScalar)msgs[i].getEntity(6));
                        col8v.set(i, (IScalar)msgs[i].getEntity(7));
                        col9v.set(i, (IScalar)msgs[i].getEntity(8));
                        col10v.set(i, (IScalar)msgs[i].getEntity(9));
                        col11v.set(i, (IScalar)msgs[i].getEntity(10));
                        col12v.set(i, (IScalar)msgs[i].getEntity(11));
                        col13v.set(i, (IScalar)msgs[i].getEntity(12));
                    }
                    BasicTable tmp = new BasicTable(new List<string> { "permno", "timestamp", "ticker", "price1", "price2", "price3", "price4", "price5", "vol1", "vol2", "vol3", "vol4", "vol5" }, new List<IVector> { col1v, col2v, col3v, col4v, col5v, col6v, col7v, col8v, col9v, col10v, col11v, col12v, col13v });
                    conn.run("append!{sub1}", new List<IEntity> { tmp });
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }

            public void doEvent(IMessage msg)
            {
                throw new NotImplementedException();
            }
        };

        public class Handler6 : MessageHandler
        {
            private StreamDeserializer deserializer_;
            private List<BasicMessage> msg1 = new List<BasicMessage>();
            private List<BasicMessage> msg2 = new List<BasicMessage>();

            public Handler6(StreamDeserializer deserializer)
            {
                deserializer_ = deserializer;
            }

            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    BasicMessage message = deserializer_.parse(msg);
                    if (message.getSym() == "msg1")
                    {
                        msg1.Add(message);
                    }
                    else if (message.getSym() == "msg2")
                    {
                        msg2.Add(message);
                    }
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }

            public List<BasicMessage> getMsg1()
            {
                return msg1;
            }

            public List<BasicMessage> getMsg2()
            {
                return msg2;
            }
        };

        public class Handler7 : MessageHandler
        {
            private List<BasicMessage> msg1 = new List<BasicMessage>();
            private List<BasicMessage> msg2 = new List<BasicMessage>();


            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    if (((BasicMessage)msg).getSym() == "msg1")
                    {
                        msg1.Add((BasicMessage)msg);
                    }
                    else if (((BasicMessage)msg).getSym() == "msg2")
                    {
                        msg2.Add((BasicMessage)msg);
                    }
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }

            public List<BasicMessage> getMsg1()
            {
                return msg1;
            }

            public List<BasicMessage> getMsg2()
            {
                return msg2;
            }
        };
        class Handler8 : MessageHandler
        {

            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {

                try
                {
                    msg.getEntity(0);
                    String script = String.Format("t= table(10000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);\n insert into t values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} );\n pt=loadTable(\"dfs://test_stream1\",`pt);\n pt.append!(t);\n", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };



        [TestMethod]
        public void Test_ThreadedClient_subscribe_host_localhost()
        {
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            Exception exception = null;
            ThreadedClient client = null;
            try
            {
                //allow to use localhost 
                client = new ThreadedClient("localhost", 10100);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNull(exception);

        }
        [TestMethod]

        public void Test_ThreadedClient_subscribe_port_null()
        {

            PrepareStreamTable1(conn, "pub1");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient();
            client.subscribe(SERVER, PORT, "pub1", "action1", handler1, 0);
        }

        [TestMethod]
        public void Test_ThreadedClient_subscribe_streamTable_not_exist()
        {
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            String exception = null;
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "trades", handler, -1, -1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The shared table trades doesn't exist"));
        }

        [TestMethod]
        public void Test_ThreadedClient_subscribe_duplicated_actionName()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0);
            String exception = null;
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "action1", handler2, 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("already be subscribed"));
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_not_specify_host()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            ThreadedClient client = new ThreadedClient(10103);
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_0()

        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_neg_1()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_less_than_existing_rows()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 500, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1500))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1500, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_larger_than_existing_rows()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
            String exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub", handler1, 1500, -1);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("Failed to subscribe to table pub. Can't find the message with offset [1500]"));
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_actionName()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_int_filter()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_symbol_filter()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_string_filter()
        {
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `ticker)";
            try
            {
                conn.run("dropStreamTable(`pub)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicStringVector filter = new BasicStringVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_date_filter()
        {
            String script1 = "";
            script1 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as pub;setStreamTableFilterColumn(pub, `date)";
            try
            {
                conn.run("dropStreamTable(`pub)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn.run(script1);
            String script2 = "";
            script2 += "share streamTable(1000:0,  `permno`date`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]) as sub1";
            conn.run(script2);
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicDateVector filter = (BasicDateVector)conn.run("2012.01.01..2012.01.10");
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, filter, -1);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where date in 2012.01.01..2012.01.10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where date in 2012.01.01..2012.01.10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_reconnect_true()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, true, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_reconnect_false()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, 0, false, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_one_client_multiple_subscription()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action2", handler2, -1, -1);
            client.subscribe(SERVER, PORT, "pub", "action3", handler3, 500, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(1000) && tmpNum3.getInt().Equals(1500))
                {
                    break;
                }
                Thread.Sleep(2000);
            }
            BasicInt sub1_num = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num.getInt());
            BasicInt sub2_num = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(1000, sub2_num.getInt());
            BasicInt sub3_num = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(1500, sub3_num.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            client.unsubscribe(SERVER, PORT, "pub", "action2");
            client.unsubscribe(SERVER, PORT, "pub", "action3");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_multiple_subscribe_unsubscribe_part()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            Handler2 handler2 = new Handler2();
            Handler3 handler3 = new Handler3();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action2", handler2, 0, -1);
            client.subscribe(SERVER, PORT, "pub", "action3", handler3, 0, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(2000) && tmpNum3.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_1 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_1.getInt());
            BasicInt sub2_num_1 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(2000, sub2_num_1.getInt());
            BasicInt sub3_num_1 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(2000, sub3_num_1.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(3000) && tmpNum3.getInt().Equals(3000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_2.getInt());
            BasicInt sub2_num_2 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_2.getInt());
            BasicInt sub3_num_2 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(3000, sub3_num_2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action2");
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum1 = (BasicInt)conn.run("exec count(*) from sub1");
                BasicInt tmpNum2 = (BasicInt)conn.run("exec count(*) from sub2");
                BasicInt tmpNum3 = (BasicInt)conn.run("exec count(*) from sub3");
                if (tmpNum1.getInt().Equals(2000) && tmpNum2.getInt().Equals(3000) && tmpNum3.getInt().Equals(4000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt sub1_num_3 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_3.getInt());
            BasicInt sub2_num_3 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_3.getInt());
            BasicInt sub3_num_3 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(4000, sub3_num_3.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action3");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn.run("undef(`sub2, SHARED)");
            conn.run("undef(`sub3, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_actionName_reconnect_false()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_default_specify_actionName_symbol_filter()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", "action1", handler1, 0, filter, -1);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        //batchSize not -1
        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_actionName_batchSize_100()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler5 handler5 = new Handler5();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler5, 0, 100);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_offset_not_0_batchSize_100()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler5 handler5 = new Handler5();
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", "sub1", handler5, 500, 100);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1500))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1500, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_specify_symbol_filter_batchSize_100()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            Handler5 handler5 = new Handler5();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            client.subscribe(SERVER, PORT, "pub", handler5, 0, filter, 100);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_schema()
        {
            try
            {
                conn.run("dropStreamTable(`outTables1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share  streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables1;\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables1, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            //schema StreamDeserializer
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
            Dictionary<string, BasicDictionary> schemas = new Dictionary<string, BasicDictionary>();
            schemas["msg1"] = outSharedTables1Schema;
            schemas["msg2"] = outSharedTables2Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(schemas);


            Handler6 handler = new Handler6(streamFilter);
            client.subscribe(SERVER, PORT, "outTables1", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables1", "mutiSchema");
            conn.run("undef(`outTables1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_colType()
        {
            try
            {
                conn.run("dropStreamTable(`outTables2)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables2;\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables2, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            //2colType
            Dictionary<string, List<DATA_TYPE>> colTypes = new Dictionary<string, List<DATA_TYPE>>();
            List<DATA_TYPE> table1ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_DOUBLE };
            colTypes["msg1"] = table1ColTypes;
            List<DATA_TYPE> table2ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE };
            colTypes["msg2"] = table2ColTypes;
            StreamDeserializer streamFilter = new StreamDeserializer(colTypes);

            Handler6 handler = new Handler6(streamFilter);
            client.subscribe(SERVER, PORT, "outTables2", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables2", "mutiSchema");
            conn.run("undef(`outTables2, SHARED)");
        }


        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_tablename()
        {
            try
            {
                conn.run("dropStreamTable(`outTables3)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables3\n";
            conn.run(script);

            string replayScript = "n = 10000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables3, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");


            //tablename
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "table1");
            tables["msg2"] = new Tuple<string, string>("", "table2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);


            Handler6 handler = new Handler6(streamFilter);
            client.subscribe(SERVER, PORT, "outTables3", "mutiSchema", handler, 0, true);

            Thread.Sleep(5000);
            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables3", "mutiSchema");
            conn.run("undef(`outTables3, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer()
        {
            try
            {
                conn.run("dropStreamTable(`outTables4)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables4;\n";
            conn.run(script);

            string replayScript = "n = 100000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);" +
                "table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);" +
                "tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));" +
                "tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));" +
                "d = dict(['msg1','msg2'], [table1, table2]);" +
                "replay(inputTables=d, outputTables=`outTables4, dateColumn=`timestampv, timeColumn=`timestampv)";
            conn.run(replayScript);

            BasicTable table1 = (BasicTable)conn.run("table1");
            BasicTable table2 = (BasicTable)conn.run("table2");

            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            BasicDictionary outSharedTables2Schema = (BasicDictionary)conn.run("table2.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            filter["msg2"] = outSharedTables2Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            client.subscribe(SERVER, PORT, "outTables4", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);

            List<BasicMessage> msg1 = handler.getMsg1();
            List<BasicMessage> msg2 = handler.getMsg2();
            Thread.Sleep(10000);
            Assert.AreEqual(table1.rows(), msg1.Count);
            Assert.AreEqual(table2.rows(), msg2.Count);
            for (int i = 0; i < table1.columns(); ++i)
            {
                IVector tableCol = table1.getColumn(i);
                for (int j = 0; j < 100000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg1[j].getEntity(i));
                }
            }
            for (int i = 0; i < table2.columns(); ++i)
            {
                IVector tableCol = table2.getColumn(i);
                for (int j = 0; j < 10000; ++j)
                {
                    Assert.AreEqual(tableCol.get(j), msg2[j].getEntity(i));
                }
            }
            client.unsubscribe(SERVER, PORT, "outTables4", "mutiSchema");
            conn.run("undef(`outTables4, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_memorytable()
        {
            try
            {
                conn.run("dropStreamTable(`outTables5)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            string replayScript = "n = 100000;t1 = table(100:0, `timestampv`sym`blob`price2, [TIMESTAMP, SYMBOL, BLOB, DOUBLE]);" +
                "share t1 as table1";
            conn.run(replayScript);
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "table1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Only stream tables can publish data.", re);
            conn.run("undef(`table1, SHARED)");

        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_streamTable_colTypeFalse2()
        {
            try
            {
                conn.run("dropStreamTable(`st1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,INT,BLOB,DOUBLE]) as st1;\n";
            conn.run(script);

            string replayScript = "insert into st1 values(NULL,NULL,NULL,NULL)\t\n";
            conn.run(replayScript);
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            String re = null;
            try
            {

                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The 2rd column must be a vector type with symbol or string. ", re);
            conn.run("undef(`st1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_streamDeserilizer_streamTable_colTypeFalse3()
        {
            try
            {
                conn.run("dropStreamTable(`st1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,INT,DOUBLE]) as st1\n";
            conn.run(script);

            //string replayScript = "insert into st1 values(NULL,NULL,NULL,NULL)\t\n";
            //conn.run(replayScript);


            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            String re = null;
            try
            {

                client.subscribe(SERVER, PORT, "st1", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The 3rd column must be a vector type with blob. ", re);
            conn.run("undef(`st1, SHARED)");
        }

        public class Handler_StreamDeserializer_array_allDateType : MessageHandler
        {
            private StreamDeserializer deserializer_;

            public Handler_StreamDeserializer_array_allDateType(StreamDeserializer deserializer)
            {
                deserializer_ = deserializer;
            }
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    BasicMessage message = deserializer_.parse(msg);
                    var cols = new List<IEntity>() { };
                    cols.Add((BasicTimestamp)message.getEntity(0));
                    cols.Add((BasicArrayVector)message.getEntity(1));
                    cols.Add((BasicArrayVector)message.getEntity(2));
                    cols.Add((BasicArrayVector)message.getEntity(3));
                    cols.Add((BasicArrayVector)message.getEntity(4));
                    cols.Add((BasicArrayVector)message.getEntity(5));
                    cols.Add((BasicArrayVector)message.getEntity(6));
                    cols.Add((BasicArrayVector)message.getEntity(7));
                    cols.Add((BasicArrayVector)message.getEntity(8));
                    cols.Add((BasicArrayVector)message.getEntity(9));
                    cols.Add((BasicArrayVector)message.getEntity(10));
                    cols.Add((BasicArrayVector)message.getEntity(11));
                    cols.Add((BasicArrayVector)message.getEntity(12));
                    cols.Add((BasicArrayVector)message.getEntity(13));
                    cols.Add((BasicArrayVector)message.getEntity(14));
                    cols.Add((BasicArrayVector)message.getEntity(15));
                    cols.Add((BasicArrayVector)message.getEntity(16));
                    cols.Add((BasicArrayVector)message.getEntity(17));
                    cols.Add((BasicArrayVector)message.getEntity(18));
                    cols.Add((BasicArrayVector)message.getEntity(19));
                    cols.Add((BasicArrayVector)message.getEntity(20));
                    cols.Add((BasicArrayVector)message.getEntity(21));
                    if (message.getSym().Equals("msg1"))
                    {
                        conn.run("tableInsert{sub1}", cols);
                    }
                    else if (message.getSym().Equals("msg2"))
                    {
                        conn.run("tableInsert{sub2}", cols);
                    }
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine("error£º" + e.Message);
                }
            }
        };

        [TestMethod]
        public void Test_ThreaedClient_subscribe_StreamDeserializer_streamTable_arrayVector_allDataType()
        {
            PrepareStreamTable_StreamDeserializer_array_allDataType();
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "pub_t1");
            tables["msg2"] = new Tuple<string, string>("", "pub_t2");

            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            Handler_StreamDeserializer_array_allDateType handler = new Handler_StreamDeserializer_array_allDateType(streamFilter);
            client.subscribe(SERVER, PORT, "outTables", "mutiSchema", handler, 0);
            checkResult1(conn);
            client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema");
        }

        public class Handler_StreamDeserializer_array_allDateType1 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    var cols = new List<IEntity>() { };
                    cols.Add((BasicTimestamp)msg.getEntity(0));
                    cols.Add((BasicArrayVector)msg.getEntity(1));
                    cols.Add((BasicArrayVector)msg.getEntity(2));
                    cols.Add((BasicArrayVector)msg.getEntity(3));
                    cols.Add((BasicArrayVector)msg.getEntity(4));
                    cols.Add((BasicArrayVector)msg.getEntity(5));
                    cols.Add((BasicArrayVector)msg.getEntity(6));
                    cols.Add((BasicArrayVector)msg.getEntity(7));
                    cols.Add((BasicArrayVector)msg.getEntity(8));
                    cols.Add((BasicArrayVector)msg.getEntity(9));
                    cols.Add((BasicArrayVector)msg.getEntity(10));
                    cols.Add((BasicArrayVector)msg.getEntity(11));
                    cols.Add((BasicArrayVector)msg.getEntity(12));
                    cols.Add((BasicArrayVector)msg.getEntity(13));
                    cols.Add((BasicArrayVector)msg.getEntity(14));
                    cols.Add((BasicArrayVector)msg.getEntity(15));
                    cols.Add((BasicArrayVector)msg.getEntity(16));
                    cols.Add((BasicArrayVector)msg.getEntity(17));
                    cols.Add((BasicArrayVector)msg.getEntity(18));
                    cols.Add((BasicArrayVector)msg.getEntity(19));
                    cols.Add((BasicArrayVector)msg.getEntity(20));
                    cols.Add((BasicArrayVector)msg.getEntity(21));
                    if (((BasicMessage)msg).getSym() == "msg1")
                    {
                        conn.run("tableInsert{sub1}", cols);
                    }
                    else if (((BasicMessage)msg).getSym() == "msg2")
                    {
                        conn.run("tableInsert{sub2}", cols);
                    }
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.StackTrace);
                }
            }
        };

        [TestMethod]
        public void Test_ThreaedClient_subscribe_StreamDeserializer_streamTable_arrayVector_allDataType_1()
        {
            PrepareStreamTable_StreamDeserializer_array_allDataType();
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "pub_t1");
            tables["msg2"] = new Tuple<string, string>("", "pub_t2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            Handler_StreamDeserializer_array_allDateType1 handler = new Handler_StreamDeserializer_array_allDateType1();
            client.subscribe(SERVER, PORT, "outTables", "mutiSchema", handler, 0, true, null, -1, (float)0.01, streamFilter);
            checkResult1(conn);
            client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema");  
        }


        //subscribe haStreamTable
        [TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_on_leader()
        {
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderHost = StreamLeaderHostTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and  name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            Console.WriteLine(StreamLeaderHost);
            Console.WriteLine(StreamLeaderPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            try
            {
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamLeaderHost, StreamLeaderPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            client.subscribe(StreamLeaderHost, StreamLeaderPort, "haSt1", handler1, 0, -1);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, haSt1.values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(StreamLeaderHost, StreamLeaderPort, "haSt1");
            conn1.run("dropStreamTable(\"haSt1\")");
            conn1.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_DBConnection_connect_follower_append_haStreamTable()
        {
            string script0 = String.Format("leader = getStreamingLeader({0});", HASTREAM_GROUPID);
            script0 += String.Format("groupSitesStr = (exec sites from getStreamingRaftGroups() where id =={0})[0];\n", HASTREAM_GROUPID);
            script0 += "groupSites = split(groupSitesStr, \",\");\n";
            script0 += "followerInfo = exec top 1 *  from rpc(getControllerAlias(), getClusterPerf) where site in groupSites and name!=leader;";
            conn.run(script0);
            BasicString StreamFollowerHostTmp = (BasicString)conn.run("(exec host from followerInfo)[0]");
            String StreamFollowerHost = StreamFollowerHostTmp.getString();
            BasicInt StreamFollowerPortTmp = (BasicInt)conn.run("(exec port from followerInfo)[0]");
            int StreamFollowerPort = StreamFollowerPortTmp.getInt();

            Console.WriteLine(StreamFollowerHost);
            Console.WriteLine(StreamFollowerPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456", "", true, HASTREAM_GROUP);
            try
            {
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            PrepareStreamTable1(conn1, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            BasicLong total = (BasicLong)conn1.run("getPersistenceMeta(haSt1).totalSize");
            Assert.AreEqual(total.getValue(), 1000);
            conn1.run("dropStreamTable('haSt1')");
            conn1.close();
            streamConn.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_on_follower()
        {
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamFollowerHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name!=\"{0}\")[0]", StreamLeader));
            String StreamFollowerHost = StreamFollowerHostTmp.getString();
            BasicInt StreamFollowerPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and name!=\"{0}\")[0]", StreamLeader));
            int StreamFollowerPort = StreamFollowerPortTmp.getInt();
            Console.WriteLine(StreamFollowerHost);
            Console.WriteLine(StreamFollowerPort.ToString());
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456", "", true, HASTREAM_GROUP);
            try
            {
                conn1.run("dropStreamTable(`haSt1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            conn1.run(String.Format("haStreamTable({0}, table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]), \"haSt1\", 100000)", HASTREAM_GROUPID));
            DBConnection conn2 = new DBConnection();
            conn2.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            PrepareStreamTable1(conn2, "sub1");
            streamConn = new DBConnection();
            streamConn.connect(StreamFollowerHost, StreamFollowerPort, "admin", "123456");
            Handler1 handler1 = new Handler1();
            client.subscribe(StreamFollowerHost, StreamFollowerPort, "haSt1", handler1, 0, -1);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);haSt1.append!(tmp); ";
            conn1.run(script);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn2.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicBoolean re = (BasicBoolean)conn2.run("each(eqObj, haSt1.values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(StreamFollowerHost, StreamFollowerPort, "haSt1");
            conn1.run("dropStreamTable(\"haSt1\")");
            conn2.run("undef(`sub1, SHARED)");
            conn1.close();
            conn2.close();
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_reconnect_false()
        {
            conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456", null, true);
            String script = "";
            script += "dbName = 'dfs://test_stream1';";
            script += "try{dropDatabase(dbName)}catch(ex){};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
            script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
            conn.run(script);
            //conn.run("undef(`pub, SHARED)");
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
            String nodeAlias = nodeAliasTmp.getString();
            BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
            String ControllerHost = ControllerHostTmp.getString();
            BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
            int ControllerPort = ControllerPortTmp.getInt();
            DBConnection conn1 = new DBConnection();
            conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("enableTablePersistence(pub, true, true, 1000000)");
            Handler8 handler8 = new Handler8();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler8, -1, false, -1);
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt tmpNum_1 = (BasicInt)conn.run("exec count(*) from pub");
            Console.Out.WriteLine(tmpNum_1.ToString());
            //Thread.Sleep(10000);
            BasicTable tmpNum_2 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_stream1\",`pt)");
            Console.Out.WriteLine("---------------11111111111111---------");
            Console.Out.WriteLine(tmpNum_2.rows());
            Console.Out.WriteLine("---------------11111111111111---------");
            //Assert.AreEqual(1000, tmpNum_2.rows() );
            try
            {
                conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
            Thread.Sleep(10000);
            conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
            Thread.Sleep(10000);
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "admin", "123456");
            Thread.Sleep(10000);
            Console.Out.WriteLine("---------------12222222222222222---------");
            BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream1\",`pt)");
            Console.Out.WriteLine(tmpNum_3.rows());
            Console.Out.WriteLine("---------------12222222222222222---------");

            Assert.AreEqual(true, tmpNum_3.rows() > tmpNum_2.rows());
            Thread.Sleep(5000);
            PrepareStreamTable1(conn, "pub");
            //client.unsubscribe(SERVER, PORT, "pub");
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        }
        class Handler_disconnect : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    Console.WriteLine("Handler");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
        };
        [TestMethod]
        public void Test_ThreaedClient_subscribe_disConnect()
        {
            String script = "";
            script += "share streamTable(10000:0,`ID`value`temperature, [INT,INT,DOUBLE]) as pubMarketData;";
            script += "insert into pubMarketData values(1,2,2.3);";
            conn.run(script);
            BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
            String nodeAlias = nodeAliasTmp.getString();
            BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
            String ControllerHost = ControllerHostTmp.getString();
            BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
            int ControllerPort = ControllerPortTmp.getInt();
            DBConnection conn1 = new DBConnection();
            conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
            ThreadedClient client = new ThreadedClient(SERVER, PORT);
            client.subscribe(SERVER, PORT, "pubMarketData", new Handler_disconnect(), -1, true);
            try
            {
                conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
            Thread.Sleep(5000);
            conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
            Thread.Sleep(5000);
            BasicTable re = (BasicTable)conn1.run(String.Format("select connectionNum from rpc(getControllerAlias(),getClusterPerf) where host= \"{0}\" and port = {1}", SERVER, PORT));
            Console.WriteLine("connectionNum is: " + re.getColumn(0).get(0).ToString());
            Thread.Sleep(10000);
            BasicTable re1 = (BasicTable)conn1.run(String.Format("select connectionNum from rpc(getControllerAlias(),getClusterPerf) where host= \"{0}\" and port = {1}", SERVER, PORT));
            Console.WriteLine("connectionNum is: " + re1.getColumn(0).get(0).ToString());
            Assert.AreEqual(true, int.Parse(re.getColumn(0).get(0).ToString()) - int.Parse(re1.getColumn(0).get(0).ToString()) < 2);

        }

        //[TestMethod]//APICS-241
        public void Test_ThreaedClient_subscribe_reconnect_true()
        {
            conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456", null, true);
            String script = "";
            script += "dbName = 'dfs://test_stream';";
            script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
            script += "db = database(dbName, HASH, [INT, 10]);";
            script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
            script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
            conn.run(script);
            //conn.run("undef(`pub, SHARED)");
            conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
            BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
            String nodeAlias = nodeAliasTmp.getString();
            BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
            String ControllerHost = ControllerHostTmp.getString();
            BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
            int ControllerPort = ControllerPortTmp.getInt();
            DBConnection conn1 = new DBConnection();
            conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("enableTablePersistence(pub, true, true, 1000000)");
            Handler8 handler8 = new Handler8();
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler8, -1, true, -1);
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt tmpNum_1 = (BasicInt)conn.run("exec count(*) from pub");
            Console.Out.WriteLine(tmpNum_1.ToString());
            Thread.Sleep(10000);
            BasicTable tmpNum_2 = (BasicTable)conn.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine("---------------11111111111111---------");
            Console.Out.WriteLine(tmpNum_2.rows());
            Console.Out.WriteLine("---------------11111111111111---------");
            Assert.AreEqual(true, 1000 > tmpNum_2.rows());
            try
            {
                conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }
            Thread.Sleep(8000);
            conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
            Thread.Sleep(8000);
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "admin", "123456");
            Console.Out.WriteLine("---------------12222222222222222---------");
            BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream\",`pt)");
            Console.Out.WriteLine(tmpNum_3.rows());
            Console.Out.WriteLine("---------------12222222222222222---------");

            Assert.AreEqual(true, tmpNum_3.rows() > tmpNum_2.rows());
            Thread.Sleep(5000);
            PrepareStreamTable1(conn, "pub");
            //client.unsubscribe(SERVER, PORT, "pub");
            //conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        }

        //[TestMethod]//High availability when stream subscription is established is temporarily not supported
        //public void Test_ThreaedClient_subscribe_reconnect_true()
        //{
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, "admin", "123456");
        //    String script = "";
        //    script += "dbName = 'dfs://test_stream';";
        //    script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
        //    script += "db = database(dbName, HASH, [INT, 10]);";
        //    script += "dummy = table(100:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);";
        //    script += "pt=db.createPartitionedTable(dummy, `pt, `permno);";
        //    conn.run(script);
        //    conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        //    BasicString nodeAliasTmp = (BasicString)conn.run("getNodeAlias()");
        //    String nodeAlias = nodeAliasTmp.getString();
        //    BasicString ControllerHostTmp = (BasicString)conn.run("rpc(getControllerAlias(), getNodeHost)");
        //    String ControllerHost = ControllerHostTmp.getString();
        //    BasicInt ControllerPortTmp = (BasicInt)conn.run("rpc(getControllerAlias(), getNodePort)");
        //    int ControllerPort = ControllerPortTmp.getInt();
        //    DBConnection conn1 = new DBConnection();
        //    conn1.connect(ControllerHost, ControllerPort, "admin", "123456");
        //    PrepareStreamTable1(conn, "pub");
        //    conn.run("enableTablePersistence(pub, true, true, 1000000)");
        //    Handler8 handler8 = new Handler8();
        //    //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
        //    WriteStreamTable1(conn, "pub", 1000);

        //    try
        //    {
        //        conn1.run(String.Format("stopDataNode(\"{0}\")", nodeAlias));
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.StackTrace);
        //    }
        //    Thread.Sleep(5000);
        //    client.subscribe(SERVER, PORT, "pub", handler8, -1, true, -1);
        //    conn1.run(String.Format("startDataNode(\"{0}\")", nodeAlias));
        //    Thread.Sleep(50000);
        //    DBConnection conn2 = new DBConnection();
        //    conn2.connect(SERVER, PORT, "admin", "123456");

        //    Console.Out.WriteLine("---------------22222222222---------");
        //    BasicTable tmpNum_3 = (BasicTable)conn2.run("select * from loadTable(\"dfs://test_stream\",`pt)");
        //    Console.Out.WriteLine(tmpNum_3.rows());
        //    Console.Out.WriteLine("---------------12222222222222222---------");

        //    Thread.Sleep(5000);


        //    client.unsubscribe(SERVER, PORT, "pub");
        //    client.close();
        //    conn.run("try{dropStreamTable(\"pub\")}catch(ex){}");
        //    conn.run("undef(`pub, SHARED)");
        //    conn.close();
        //}

        //[TestMethod]
        public void Test_ThreaedClient_subscribe_haStreamTable_reconnect_true()
        {
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader({0})", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderHostTmp = (BasicString)conn.run(String.Format("(exec host from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderHost = StreamLeaderHostTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            Handler4 handler4 = new Handler4();
            client.subscribe(StreamLeaderHost, StreamLeaderPort, "haPub1", handler4, -1, true, -1);
            for (int i = 0; i < 30; i++)
            {
                Console.WriteLine(SUB_FLAG);
                Thread.Sleep(2000);
            }
        }


        [TestMethod]
        public void Test_ThreaedClient_subscribe_user_notExist()
        {
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            String exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "a1234", "a1234");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The user name or password is incorrect"));
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_user_password_notTrue()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            Handler1 handler = new Handler1();
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            String exception = null;
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "admin", "123455");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The user name or password is incorrect"));
            client.close();
            conn.close();

        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_user_password()
        {
            PrepareUser1(conn);
            PrepareUser3(conn);
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "testuser1", "123456");
            DBConnection conn2 = new DBConnection();
            conn2.connect(SERVER, PORT, "testuser3", "123456");
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            // client.subscribe(SERVER, PORT, "pub", handler1, 0, filter, -1);
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "admin", "123456");
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)

            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            try
            {
                BasicInt num3 = (BasicInt)conn1.run("exec count(*) from sub1");
                Assert.AreEqual(expectedNum.getInt(), num3.getInt());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                String ex1 = ex.ToString();
            }
            try
            {
                BasicInt num4 = (BasicInt)conn2.run("exec count(*) from sub1");
                Assert.AreEqual(expectedNum.getInt(), num4.getInt());
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                String ex2 = ex.ToString();
            }
            //Assert.IsNotNull(exception);
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            String script1 = "def test_user(){deleteUser(\"testuser1\")}\n rpc(getControllerAlias(), test_user)";
            String script2 = "def test_user(){deleteUser(\"testuser3\")}\n rpc(getControllerAlias(), test_user)";
            conn.run(script1);
            conn.run(script2);
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
            conn1.close();
            conn2.close();
        }

        //[TestMethod]
        //public void Test_ThreaedClient_subscribe_user_not_grant()
        //{
        //    DBConnection conn1 = new DBConnection();
        //    conn1.connect(SERVER, PORT, "admin", "123456");
        //    PrepareUser2(conn1);
        //    DBConnection conn = new DBConnection();
        //    conn.connect(SERVER, PORT, "uesr123", "123456");
        //    PrepareStreamTable1(conn, "pub");
        //    conn.run("setStreamTableFilterColumn(pub, `permno)");
        //    PrepareStreamTable1(conn, "sub1");
        //    Handler1 handler1 = new Handler1();
        //    Exception exception = null;
        //    //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        //    BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });     
        //    try
        //    {
        //        //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
        //        client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "testuser2", "123456");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.ToString());
        //        exception = ex;
        //    }
        //    Assert.IsNotNull(exception);
        //    client.unsubscribe(SERVER, PORT, "pub", "sub1");
        //    conn.run("undef(`pub, SHARED)");
        //    conn.run("undef(`sub1, SHARED)");
        //    client.close();
        //    conn.close();
        //    streamConn.close();
        //}
        [TestMethod]
        public void Test_ThreaedClient_subscribe_one_rows()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1 rows first
            WriteStreamTable1(conn, "pub", 1);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1, num1.getInt());
            Handler1 handler1 = new Handler1();
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler,int batchSize=-1)
            client.subscribe(SERVER, PORT, "pub", handler1, -1);
            //write 1 rows after subscribe
            WriteStreamTable1(conn, "pub", 1);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(1))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        class Handler_array : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values({0},{1})", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace(", ,", ", NULL,").Replace("[,", "[NULL,").Replace(", ]", ", NULL]").Replace(", ", " "));
                    conn.run(script);
                    //System.Console.Out.WriteLine(script);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        class Handler : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values({0},{1})", msg.getEntity(0).getString(), msg.getEntity(1).getString());
                    conn.run(script);
                    System.Console.Out.WriteLine(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_INT()
        {
            PrepareStreamTable_array("INT");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_BOOL()
        {
            PrepareStreamTable_array("BOOL");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_CHAR()
        {
            PrepareStreamTable_array("CHAR");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_SHORT()
        {
            PrepareStreamTable_array("SHORT");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_LONG()
        {
            PrepareStreamTable_array("LONG");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DOUBLE()
        {
            PrepareStreamTable_array("DOUBLE");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_FLOAT()
        {
            PrepareStreamTable_array("FLOAT");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DATE()
        {
            PrepareStreamTable_array("DATE");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_MONTH()
        {
            PrepareStreamTable_array("MONTH");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_TIME()
        {
            PrepareStreamTable_array("TIME");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_MINUTE()
        {
            PrepareStreamTable_array("MINUTE");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_SECOND()
        {
            PrepareStreamTable_array("SECOND");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DATETIME()
        {
            PrepareStreamTable_array("DATETIME");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_TIMESTAMP()
        {
            PrepareStreamTable_array("TIMESTAMP");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_NANOTIME()
        {
            PrepareStreamTable_array("NANOTIME");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_NANOTIMESTAMP()
        {
            PrepareStreamTable_array("NANOTIMESTAMP");
            Handler_array Handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", Handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_array_UUID : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values( {0},[uuid({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    conn.run(script);
                    System.Console.Out.WriteLine(script);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_UUID()
        {
            PrepareStreamTable_array("UUID");
            Handler_array_UUID Handler_array_UUID = new Handler_array_UUID();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_UUID, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }
        class Handler_array_DATEHOUR : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values( {0},[datehour({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    conn.run(script);
                    System.Console.Out.WriteLine(script);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DATEHOUR()
        {
            PrepareStreamTable_array("DATEHOUR");
            Handler_array_DATEHOUR Handler_array_DATEHOUR = new Handler_array_DATEHOUR();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_DATEHOUR, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }
        class Handler_array_IPADDR : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values( {0},[ipaddr({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    conn.run(script);
                    System.Console.Out.WriteLine(script);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };
        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_IPADDR()
        {
            PrepareStreamTable_array("IPADDR");
            Handler_array_IPADDR Handler_array_IPADDR = new Handler_array_IPADDR();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_IPADDR, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_array_INT128 : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values( {0},[int128({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\" \"", "NULL"));
                    conn.run(script);
                    System.Console.Out.WriteLine(script);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };
        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_INT128()
        {
            PrepareStreamTable_array("INT128");
            Handler_array_INT128 Handler_array_INT128 = new Handler_array_INT128();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_INT128, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_array_COMPLEX : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    var cols = new List<IEntity>() { };
                    var colNames = new List<String>() { "permno", "dateType" };
                    BasicArrayVector dateType = new BasicArrayVector(DATA_TYPE.DT_COMPLEX_ARRAY);
                    dateType.append((IVector)msg.getEntity(1));
                    cols.Add(msg.getEntity(0));
                    cols.Add(dateType);
                    conn.run("tableInsert{sub1}", cols);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_COMPLEX()
        {
            PrepareStreamTable_array("COMPLEX");
            Handler_array_COMPLEX Handler_array_COMPLEX = new Handler_array_COMPLEX();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_COMPLEX, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_array_POINT : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    String complex1 = msg.getEntity(1).getString().Replace("(,)", "(NULL,NULL)");
                    //Console.WriteLine(complex1);
                    complex1 = complex1.Substring(1).Substring(complex1.Length - 1);
                    string[] separators = { "),(" };
                    String[] complex2 = complex1.Split(separators, StringSplitOptions.None);
                    String complex3 = null;
                    StringBuilder re1 = new StringBuilder();
                    StringBuilder re2 = new StringBuilder();
                    for (int i = 0; i < complex2.Length; i++)
                    {
                        complex3 = complex2[i];
                        String[] complex4 = complex3.Split(',');
                        re1.Append(complex4[0]);
                        re1.Append(' ');
                        re2.Append(complex4[1]);
                        re2.Append(' ');
                    }
                    complex1 = re1 + "," + re2;
                    complex1 = complex1.Replace("(", "").Replace(")", "");
                    String script = String.Format("insert into sub1 values( {0},[point({1})])", msg.getEntity(0).getString(), complex1);
                    conn.run(script);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };
        //[TestMethod] not support
        public void Test_ThreadClient_subscribe_arrayVector_POINT()
        {
            PrepareStreamTable_array("POINT");
            Handler_array_POINT Handler_array_POINT = new Handler_array_POINT();
            client.subscribe(SERVER, PORT, "Trades", Handler_array_POINT, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_COMPLEX : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    var cols = new List<IEntity>() { };
                    cols.Add(msg.getEntity(0));
                    cols.Add(msg.getEntity(1));
                    conn.run("tableInsert{sub1}", cols);

                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreadClient_subscribe_COMPLEX()
        {
            PrepareStreamTable_allDateType("COMPLEX", 1000);
            Handler_COMPLEX handler = new Handler_COMPLEX();
            client.subscribe(SERVER, PORT, "Trades", handler, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_COMPLEX_1()
        {
            PrepareStreamTable_allDateType("COMPLEX", 1);
            Handler_COMPLEX handler = new Handler_COMPLEX();
            client.subscribe(SERVER, PORT, "Trades", handler, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod] 
        public void Test_ThreadClient_subscribe_DECIMAL32()
        {
            PrepareStreamTableDecimal("DECIMAL32", 3);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "Trades", handler, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_DECIMAL64()
        {
            PrepareStreamTableDecimal("DECIMAL64", 3);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "Trades", handler, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }


        [TestMethod]
        public void Test_ThreadClient_subscribe_DECIMAL128()
        {
            PrepareStreamTableDecimal("DECIMAL128", 10);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "Trades", handler, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod] 
        public void Test_ThreadClient_subscribe_arrayVector_DECIMAL32()
        {
            PrepareStreamTableDecimal_array("DECIMAL32", 3);
            Handler_array handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DECIMAL64()
        {
            PrepareStreamTableDecimal_array("DECIMAL64", 5);
            Handler_array handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        [TestMethod]
        public void Test_ThreadClient_subscribe_arrayVector_DECIMAL128()
        {
            PrepareStreamTableDecimal_array("DECIMAL128", 10);
            Handler_array handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "Trades", handler_array, -1);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            Thread.Sleep(2000);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            client.unsubscribe(SERVER, PORT, "Trades");
        }

        class Handler_getOffset : MessageHandler
        {

            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }

            public void doEvent(IMessage msg)
            {
                try
                {
                    msg.getEntity(0);
                    String script = String.Format("insert into sub1 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                    Console.Out.WriteLine("msg.getOffset is :" + msg.getOffset());
                    Assert.AreEqual(total, msg.getOffset());
                    total++;
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreaedClient_subscribe_getOffset()

        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            Handler_getOffset handler1 = new Handler_getOffset();
            client.subscribe(SERVER, PORT, "pub", handler1, 0, -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(2000))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_msgAsTable_false()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler1 handler1 = new Handler1();
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, filter, -1, 0.01f, null, "admin", "123456",false);

            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");

            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        class Handler_msgAsTable : MessageHandler
        {
            public void batchHandler(List<IMessage> msgs)
            {
                throw new NotImplementedException();
            }
            public void doEvent(IMessage msg)
            {
                try
                {
                    List<IEntity> args1 = new List<IEntity>() { msg.getTable() };
                    conn.run("tableInsert{sub1}", args1);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        [TestMethod]
        public void Test_ThreaedClient_subscribe_msgAsTable_true_1()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            Handler_msgAsTable handler1 = new Handler_msgAsTable();
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, null, -1, 0.01f, null, "admin", "123456", true);

            String script = "";
            script += "batch=1;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");

            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            Assert.AreEqual(expectedNum.getInt(), 1);
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_msgAsTable_true_all_dateType()
        {
            PrepareStreamTable3(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `intv)");
            PrepareStreamTable3(conn, "sub1");
            Handler_msgAsTable handler1 = new Handler_msgAsTable();
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, null, -1, 0.01f, null, "admin", "123456", true);
            writeStreamTable_all_dateType(1000, "pub");
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub ");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");

            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            Assert.AreEqual(expectedNum.getInt(), 1000);
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub ).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_ThreaedClient_subscribe_msgAsTable_true_all_dateType_10000()
        {
            PrepareStreamTable3(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `intv)");
            PrepareStreamTable3(conn, "sub1");
            Handler_msgAsTable handler1 = new Handler_msgAsTable();
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, null, -1, 0.01f, null, "admin", "123456", true);
            writeStreamTable_all_dateType(10000, "pub");
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub ");
            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");

            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            Assert.AreEqual(expectedNum.getInt(), 10000);
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub ).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_msgAsTable_true_all_dateType_array_10000()
        {
            PrepareStreamTable_array_1();
            Handler_msgAsTable handler1 = new Handler_msgAsTable();
            client.subscribe(SERVER, PORT, "pub", "sub1", handler1, 0, false, null, -1, 0.01f, null, "admin", "123456", true);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub ");
            for (int i = 0; i < 1000; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt().Equals(expectedNum.getInt()))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");

            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            Assert.AreEqual(expectedNum.getInt(), 1000);
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub  order by permno).values(), (select * from sub1  order by permno).values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_StreamDeserializer_msgastable_true()
        {
            PrepareStreamTable_StreamDeserializer_array_allDataType();
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "pub_t1");
            tables["msg2"] = new Tuple<string, string>("", "pub_t2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            Handler_StreamDeserializer_array_allDateType1 handler = new Handler_StreamDeserializer_array_allDateType1();
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub", "sub1", handler, 0, false, null, -1, 0.01f, streamFilter, "admin", "123456", true);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Cannot set deserializer when msgAsTable is true. ", re);
        }
    }
}

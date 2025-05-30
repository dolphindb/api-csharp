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
using System.Collections.Concurrent;

namespace dolphindb_csharp_api_test.streamReverse_test
{
    [TestClass]
    public class PoolingClientReverseTest
    {

        public static DBConnection streamConn;
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string LOCALHOST = MyConfigReader.LOCALHOST;
        private readonly int LOCALPORT = MyConfigReader.LOCALPORT;
        static private int SUB_FLAG = MyConfigReader.SUB_FLAG;
        private string NODE1_HOST = MyConfigReader.NODE1_HOST;
        private readonly int NODE1_PORT = MyConfigReader.NODE1_PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        private readonly int TIMEOUT = 10000;
        public static DBConnection conn;
        private static PollingClient client;
        private static List<long> myLongList;
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
            client = new PollingClient(LOCALHOST, 0);
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

        public void writeStreamTable_all_dateType(long dataRows, String tableName)
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
            "setStreamTableFilterColumn(Trades, `permno);\n" +
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
        public void PrepareStreamTableDecimal_array(String dataType, int scale)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")[]]) as Trades;\n" +
                "setStreamTableFilterColumn(Trades, `permno); \n" +
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

        public void PrepareStreamTable_allDateType(String dataType, int rows)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "]) as Trades;\n" +
                "setStreamTableFilterColumn(Trades, `permno); \n" +
                "permno = take(1..10," + rows + "); \n" +
                "dateType_COMPLEX =  rand(complex(rand(100, 1000), rand(100, 1000)) join NULL,  " + rows + "); \n" +
                "dateType_POINT =  rand(point(rand(100, 1000), rand(100, 1000)) join NULL,  " + rows + "); \n" +
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
        public void PrepareStreamTableDecimal(String dataType, int scale, int rows)
        {
            try
            {
                String script = "share streamTable(1000000:0, `permno`dateType, [INT," + dataType + "(" + scale + ")]) as Trades;\n" +
                "setStreamTableFilterColumn(Trades, `permno); \n" +
                "permno = take(1..10," + rows + "); \n" +
                "dateType_DECIMAL32 =  decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), " + rows + "), 3); \n" +
                "dateType_DECIMAL64 =   decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), " + rows + "), 10); \n" +
                "dateType_DECIMAL128 =   decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), " + rows + "), 13); \n" +
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

            String script = "share streamTable(10000:0, `permno`sym`blob`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v, [TIMESTAMP,SYMBOL,BLOB,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[]]) as outTables;\n" +
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
            "share table(timestampv as timestamp1,dateType_BOOL as boolv, dateType_CHAR  as charv, dateType_SHORT  as shortv, dateType_INT  as intv, dateType_LONG  as longv, dateType_DOUBLE  as doublev, dateType_FLOAT  as floatv, dateType_DATE  as datev, dateType_MONTH  as monthv, dateType_TIME  as timev, dateType_MINUTE  as minutev, dateType_SECOND  as secondv, dateType_DATETIME  as datetimev, dateType_TIMESTAMP  as timestampv, dateType_NANOTIME  as nanotimev, dateType_NANOTIMESTAMP  as nanotimestampv, dateType_DATEHOUR  as datehourv, dateType_UUID  as uuidv, dateType_IPADDR  as ipaddrv, dateType_INT128  as int128v) as pub_t1;\n" +
            "share table(timestampv as timestamp1,dateType_BOOL as boolv, dateType_CHAR  as charv, dateType_SHORT  as shortv, dateType_INT  as intv, dateType_LONG  as longv, dateType_DOUBLE  as doublev, dateType_FLOAT  as floatv, dateType_DATE  as datev, dateType_MONTH  as monthv, dateType_TIME  as timev, dateType_MINUTE  as minutev, dateType_SECOND  as secondv, dateType_DATETIME  as datetimev, dateType_TIMESTAMP  as timestampv, dateType_NANOTIME  as nanotimev, dateType_NANOTIMESTAMP  as nanotimestampv, dateType_DATEHOUR  as datehourv, dateType_UUID  as uuidv, dateType_IPADDR  as ipaddrv, dateType_INT128  as int128v) as pub_t2;\n" +
            "d = dict(['msg1','msg2'], [pub_t1, pub_t2]);\n" +
            "replay(inputTables=d, outputTables=`outTables, dateColumn=`timestamp1, timeColumn=`timestamp1);\n" +
            "share streamTable(1000000:0, `timestamp1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v, [TIMESTAMP,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[]]) as sub1;\n" +
            "share streamTable(1000000:0, `timestamp1`boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`datehourv`uuidv`ipaddrv`int128v, [TIMESTAMP,BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[]]) as sub2;\n";
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "admin", "123456");
            conn1.run(script);
        }

        public static void checkResult(DBConnection conn)
        {

            for (int i = 0; i < 10; i++)
            {
                BasicInt tmpNum = (BasicInt)conn.run("exec count(*) from sub1");
                if (tmpNum.getInt() == (1000))
                {
                    break;
                }
                Thread.Sleep(2000);
            }
            BasicTable except = (BasicTable)conn.run("select * from  Trades where permno<=10 order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 where permno<=10 order by permno");
            Assert.AreEqual(except.rows(), res.rows());
            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(except.getColumn(1).getEntity(i).getString(), res.getColumn(1).getEntity(i).getString());
            }
        }
        public static void checkResult1(DBConnection conn, String exceptTable, String resTable)
        {
            BasicTable except = (BasicTable)conn.run(String.Format("select * from  {0}", exceptTable));
            BasicTable res = (BasicTable)conn.run(String.Format("select * from  {0}", resTable));
            Assert.AreEqual(except.rows(), res.rows());
            for (int i = 0; i < except.rows(); i++)
            {
                for (int j = 0; j < except.columns(); j++)
                {
                    Assert.AreEqual(except.getColumn(j).getEntity(i).getString(), res.getColumn(j).getEntity(i).getString());
                }
            }
        }
        public static void checkResult2(DBConnection conn)
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

        public void Handler(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values({0},{1})", msg.getEntity(0).getString(), msg.getEntity(1).getString());
                    conn.run(script);
                    System.Console.Out.WriteLine(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        public void Handler1(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        public void Handler2(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub2 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        public void Handler3(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub3 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
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

        [TestMethod]
        public void Test_PollingClient_subscribe_port_not_zero()
        {
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            PollingClient client = new PollingClient(12344);
            String exception = null;
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter, null, "a1234", "a1234");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The user name or password is incorrect"));
            client.close();
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_host_localhost()
        {
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            Exception exception = null;
            PollingClient client1 = null;
            try
            {
                //allow to use localhost 
                client1 = new PollingClient("localhost", 10100);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.IsNull(exception);
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_table_not_exist()
        {
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            String exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "trades");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The shared table trades doesn't exist"));
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_duplicated_actionName()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            client.subscribe(SERVER, PORT, "pub", "action1");
            String exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub", "action1");
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
        public void Test_PollingClient_subscribe_offset_default()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub");
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_offset_0()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_offset_neg_1()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", -1);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_offset_less_than_existing_rows()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1500, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_offset_larger_than_existing_rows()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            String exception = null;
            try
            {
                client.subscribe(SERVER, PORT, "pub", 1500);
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
        public void Test_PollingClient_subscribe_specify_actionName()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1");
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(1000, num2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_filter_int()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_filter_int_1()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where permno in 1..10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_filter_symbol()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `ticker)");
            PrepareStreamTable1(conn, "sub1");
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicSymbolVector filter = new BasicSymbolVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_filter_string()
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
            //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicStringVector filter = new BasicStringVector(new string[] { "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10" });
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where ticker in \"A\"+string(1..10)");
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where ticker in \"A\"+string(1..10)).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_filter_date()
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
            //usage: subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
            BasicDateVector filter = (BasicDateVector)conn.run("2012.01.01..2012.01.10");
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter);
            String script3 = "";
            script3 += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = rand(2012.01.01..2012.01.30, batch);tmp[`ticker] = rand(\"A\" + string(1..100), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script3);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where date in 2012.01.01..2012.01.10");
            BasicInt num2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(expectedNum.getInt(), num2.getInt());
            BasicBoolean re = (BasicBoolean)conn.run("each(eqObj, (select * from pub where date in 2012.01.01..2012.01.10).values(), sub1.values()).all()");
            Assert.AreEqual(re.getValue(), true);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_reconnect_true()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, true);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_reconnect_false()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler1(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_one_client_multiple_subscription()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            TopicPoller pool1 = client.subscribe(SERVER, PORT, "pub", "action1", 0);
            TopicPoller pool2 = client.subscribe(SERVER, PORT, "pub", "action2", -1);
            TopicPoller pool3 = client.subscribe(SERVER, PORT, "pub", "action3", 500);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages1 = pool1.poll(1000, 2000);
            List<IMessage> messages2 = pool2.poll(1000, 2000);
            List<IMessage> messages3 = pool3.poll(1000, 2000);
            Handler1(messages1);
            Handler2(messages2);
            Handler3(messages3);
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
        public void Test_PollingClient_multiple_subscribe_unsubscribe_part()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            PrepareStreamTable1(conn, "sub2");
            PrepareStreamTable1(conn, "sub3");
            //write 1000 rows first
            WriteStreamTable1(conn, "pub", 1000);
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());

            //usage: subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
            TopicPoller pool1 = client.subscribe(SERVER, PORT, "pub", "action1", 0);
            TopicPoller pool2 = client.subscribe(SERVER, PORT, "pub", "action2", 0);
            TopicPoller pool3 = client.subscribe(SERVER, PORT, "pub", "action3", 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages1 = pool1.poll(1000, 2000);
            List<IMessage> messages2 = pool2.poll(1000, 2000);
            List<IMessage> messages3 = pool3.poll(1000, 2000);
            Handler1(messages1);
            Handler2(messages2);
            Handler3(messages3);
            BasicInt sub1_num_1 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_1.getInt());
            BasicInt sub2_num_1 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(2000, sub2_num_1.getInt());
            BasicInt sub3_num_1 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(2000, sub3_num_1.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action1");
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages4 = pool1.poll(1000, 2000);
            List<IMessage> messages5 = pool2.poll(1000, 2000);
            List<IMessage> messages6 = pool3.poll(1000, 2000);
            Handler1(messages4);
            Handler2(messages5);
            Handler3(messages6);
            BasicInt sub1_num_2 = (BasicInt)conn.run("exec count(*) from sub1");
            Assert.AreEqual(2000, sub1_num_2.getInt());
            BasicInt sub2_num_2 = (BasicInt)conn.run("exec count(*) from sub2");
            Assert.AreEqual(3000, sub2_num_2.getInt());
            BasicInt sub3_num_2 = (BasicInt)conn.run("exec count(*) from sub3");
            Assert.AreEqual(3000, sub3_num_2.getInt());
            client.unsubscribe(SERVER, PORT, "pub", "action2");
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages7 = pool1.poll(1000, 2000);
            List<IMessage> messages8 = pool2.poll(1000, 2000);
            List<IMessage> messages9 = pool3.poll(1000, 2000);
            Handler1(messages7);
            Handler2(messages8);
            Handler3(messages9);
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
        public void Test_PollingClient_subscribe_user_notExist()
        {
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            String exception = null;
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")

                //subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
                client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter, null, "a1234", "a1234");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The user name or password is incorrect"));
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_user_password_notTrue()
        {
            String script = "";
            script += "try{undef(`trades, SHARED)}catch(ex){}";
            conn.run(script);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            String exception = null;
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            try
            {
                //usage: subscribe(string SERVER, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
                client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, filter, null, "admin", "123455");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("The user name or password is incorrect"));

        }

        [TestMethod]
        public void Test_PollingClient_subscribe_admin()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, true, filter, null, "admin", "123456");
            Thread.Sleep(500);
            List<IMessage> messages = pool.poll(TIMEOUT);
            Console.WriteLine(messages);
            Thread.Sleep(500);
            int messageCount = messages.Count;
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            Assert.AreEqual(expectedNum.getInt(), messageCount);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_otherUser()
        {
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "admin", "123456");
            PrepareUser1(conn1);
            PrepareStreamTable1(conn, "pub2");
            conn.run("setStreamTableFilterColumn(pub2, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub2.append!(tmp); ";
            conn.run(script);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub2", "sub1", 0, true, filter, null, "testuser1", "123456");
            Thread.Sleep(500);
            List<IMessage> messages = pool.poll(TIMEOUT);
            Thread.Sleep(500);
            int messageCount = messages.Count;
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub2 where permno in 1..10");
            Assert.AreEqual(expectedNum.getInt(), messageCount);
            client.unsubscribe(SERVER, PORT, "pub2", "sub1");
            String script1 = "def test_user(){deleteUser(\"testuser1\")}\n rpc(getControllerAlias(), test_user)";
            conn1.run(script1);
            //conn.run("undef(`pub, SHARED)");
            conn1.run("undef(`sub1, SHARED)");
            conn1.close();
        }

        [TestMethod]//can subscribe can be success when not grant   
        public void Test_PollingClient_subscribe_user_not_grant()
        {
            DBConnection conn1 = new DBConnection();
            conn1.connect(SERVER, PORT, "admin", "123456");
            PrepareUser2(conn1);
            PrepareStreamTable1(conn, "pub2");
            conn.run("setStreamTableFilterColumn(pub2, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub2.append!(tmp); ";
            conn.run(script);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub2", "sub1", 0, true, filter, null, "testuser2", "123456");
            Thread.Sleep(500);
            List<IMessage> messages = pool.poll(TIMEOUT);
            Thread.Sleep(500);
            int messageCount = messages.Count;
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub2 where permno in 1..10");
            Assert.AreEqual(expectedNum.getInt(), messageCount);
            client.unsubscribe(SERVER, PORT, "pub2", "sub1");
            String script1 = "def test_user(){deleteUser(\"testuser2\")}\n rpc(getControllerAlias(), test_user)";
            conn1.run(script1);
            //conn.run("undef(`pub, SHARED)");
            conn1.run("undef(`sub1, SHARED)");
            conn1.close();
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_schema()
        {
            try
            {
                conn.run("dropStreamTable(`outTables1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }


            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables1;\n";
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
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller poller = client.subscribe(SERVER, PORT, "outTables1", "mutiSchema", 0, true, null, streamFilter);
            Thread.Sleep(5000);
            List<IMessage> messages = poller.poll(TIMEOUT);
            List<BasicMessage> msg1 = new List<BasicMessage>();
            List<BasicMessage> msg2 = new List<BasicMessage>();
            int msg1count = 0;
            int msg2count = 0;
            foreach (IMessage message in messages)
            {

                if ((((BasicMessage)message).getSym() == "msg1"))
                {
                    msg1count++;
                }
                else if ((((BasicMessage)message).getSym() == "msg2"))
                {
                    msg2count++;
                }
            }
            Assert.AreEqual(table1.rows(), msg1count);
            Assert.AreEqual(table2.rows(), msg2count);
            int messageCount = messages.Count;
            Assert.AreEqual(table1.rows() + table2.rows(), messageCount);
            client.unsubscribe(SERVER, PORT, "outTables1", "mutiSchema");
            conn.run("undef(`outTables1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_colType()
        {
            try
            {
                conn.run("dropStreamTable(`outTables2)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables2\n";
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

            //2、colType
            Dictionary<string, List<DATA_TYPE>> colTypes = new Dictionary<string, List<DATA_TYPE>>();
            List<DATA_TYPE> table1ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_DOUBLE };
            colTypes["msg1"] = table1ColTypes;
            List<DATA_TYPE> table2ColTypes = new List<DATA_TYPE> { DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_DOUBLE };
            colTypes["msg2"] = table2ColTypes;
            StreamDeserializer streamFilter = new StreamDeserializer(colTypes);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller poller = client.subscribe(SERVER, PORT, "outTables2", "mutiSchema", 0, true, null, streamFilter);
            Thread.Sleep(5000);
            List<IMessage> messages = poller.poll(TIMEOUT);
            List<BasicMessage> msg1 = new List<BasicMessage>();
            List<BasicMessage> msg2 = new List<BasicMessage>();
            int msg1count = 0;
            int msg2count = 0;
            foreach (IMessage message in messages)
            {

                if ((((BasicMessage)message).getSym() == "msg1"))
                {
                    msg1count++;
                }
                else if ((((BasicMessage)message).getSym() == "msg2"))
                {
                    msg2count++;
                }
            }
            Assert.AreEqual(table1.rows(), msg1count);
            Assert.AreEqual(table2.rows(), msg2count);
            int messageCount = messages.Count;
            Assert.AreEqual(table1.rows() + table2.rows(), messageCount);
            client.unsubscribe(SERVER, PORT, "outTables2", "mutiSchema");
            conn.run("undef(`outTables2, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_tablename()
        {
            try
            {
                conn.run("dropStreamTable(`outTables3)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables3;\n";
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
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller poller = client.subscribe(SERVER, PORT, "outTables3", "mutiSchema", 0, true, null, streamFilter);
            Thread.Sleep(5000);
            List<IMessage> messages = poller.poll(TIMEOUT);
            List<BasicMessage> msg1 = new List<BasicMessage>();
            List<BasicMessage> msg2 = new List<BasicMessage>();
            int msg1count = 0;
            int msg2count = 0;
            foreach (IMessage message in messages)
            {

                if ((((BasicMessage)message).getSym() == "msg1"))
                {
                    msg1count++;
                }
                else if ((((BasicMessage)message).getSym() == "msg2"))
                {
                    msg2count++;
                }
            }
            Assert.AreEqual(table1.rows(), msg1count);
            Assert.AreEqual(table2.rows(), msg2count);
            int messageCount = messages.Count;
            Assert.AreEqual(table1.rows() + table2.rows(), messageCount);
            client.unsubscribe(SERVER, PORT, "outTables3", "mutiSchema");
            conn.run("undef(`outTables3, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer()
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
            PollingClient client = new PollingClient(LOCALHOST, 0);
            //TopicPoller poller = client.subscribe(SERVER, PORT, "outTables", "mutiSchema", 0, true, null);
            TopicPoller poller = client.subscribe(SERVER, PORT, "outTables4", "mutiSchema", 0, true, null, streamFilter);
            Thread.Sleep(5000);
            List<IMessage> messages = poller.poll(TIMEOUT);
            List<BasicMessage> msg1 = new List<BasicMessage>();
            List<BasicMessage> msg2 = new List<BasicMessage>();
            int msg1count = 0;
            int msg2count = 0;
            foreach (IMessage message in messages)
            {

                if ((((BasicMessage)message).getSym() == "msg1"))
                {
                    msg1count++;
                }
                else if ((((BasicMessage)message).getSym() == "msg2"))
                {
                    msg2count++;
                }
            }
            Assert.AreEqual(table1.rows(), msg1count);
            Assert.AreEqual(table2.rows(), msg2count);
            int messageCount = messages.Count;
            Assert.AreEqual(table1.rows() + table2.rows(), messageCount);
            client.unsubscribe(SERVER, PORT, "outTables4", "mutiSchema");
            conn.run("undef(`outTables4, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_memorytable()
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

            BasicTable table1 = (BasicTable)conn.run("table1");

            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("table1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            PollingClient client = new PollingClient(LOCALHOST, 0);
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "table1", "mutiSchema", 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                re = ex.Message;
            }
            Assert.AreEqual("Only stream tables can publish data.", re);
            conn.run("undef(`table1, SHARED)");

        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_streamTable_colTypeFalse2()
        {
            try
            {
                conn.run("dropStreamTable(`outTables6)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
            try
            {
                conn.run("dropStreamTable(`st1)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }

            String script = "share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,INT,BLOB,DOUBLE]) as st1\n";
            conn.run(script);

            string replayScript = "insert into st1 values(NULL,NULL,NULL,NULL)\t\n";
            conn.run(replayScript);
            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            Handler7 handler = new Handler7();
            PollingClient client = new PollingClient(LOCALHOST, 0);
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "st1", "mutiSchema", 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                re = ex.Message;
            }
            Assert.AreEqual("The 2rd column must be a vector type with symbol or string. ", re);
            conn.run("undef(`st1, SHARED)");

        }

        [TestMethod]
        public void Test_PollingClient_subscribe_streamDeserilizer_streamTable_colTypeFalse3()
        {
            try
            {
                conn.run("dropStreamTable(`outTables7)");
            }
            catch (Exception e)
            {
                Console.Out.WriteLine(e.Message);
            }
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

            BasicDictionary outSharedTables1Schema = (BasicDictionary)conn.run("st1.schema()");
            Dictionary<string, BasicDictionary> filter = new Dictionary<string, BasicDictionary>();
            filter["msg1"] = outSharedTables1Schema;
            StreamDeserializer streamFilter = new StreamDeserializer(filter);

            PollingClient client = new PollingClient(LOCALHOST, 0);
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "st1", "mutiSchema", 0, true, null, streamFilter);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                re = ex.Message;
            }
            Assert.AreEqual("The 3rd column must be a vector type with blob. ", re);
            conn.run("undef(`st1, SHARED)");
        }

        public void Handler_StreamDeserializer_array_allDateType(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];

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
                    if (msg.getSym().Equals("msg1"))
                    {
                        conn.run("tableInsert{sub1}", cols);
                    }
                    else if (msg.getSym().Equals("msg2"))
                    {
                        conn.run("tableInsert{sub2}", cols);
                    }
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_StreamDeserializer_streamTable_arrayVector_allDataType()
        {
            PrepareStreamTable_StreamDeserializer_array_allDataType();
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "pub_t1");
            tables["msg2"] = new Tuple<string, string>("", "pub_t2");

            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            TopicPoller poller = client.subscribe(SERVER, PORT, "outTables", "mutiSchema", 0, true, null, streamFilter);
            List<IMessage> messages = poller.poll(2000, 2000);
            Handler_StreamDeserializer_array_allDateType(messages);
            checkResult2(conn);
            client.unsubscribe(SERVER, PORT, "outTables", "mutiSchema");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_one_rows()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            String script = "";
            script += "batch=1;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            PollingClient client = new PollingClient(LOCALHOST, 0);
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, true, filter, null, "admin", "123456");
            Thread.Sleep(500);
            List<IMessage> messages = pool.poll(TIMEOUT);
            Thread.Sleep(500);
            int messageCount = messages.Count;
            BasicInt expectedNum = (BasicInt)conn.run("exec count(*) from pub where permno in 1..10");
            Assert.AreEqual(expectedNum.getInt(), messageCount);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_disConnect()
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
            PollingClient client = new PollingClient(SERVER, PORT);
            TopicPoller poller = client.subscribe(SERVER, PORT, "pubMarketData", -1, true);
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
            BasicTable re = (BasicTable)conn1.run(String.Format("select connectionNum from rpc(getControllerAlias(),getClusterPerf) where host= \"{0}\" and port = {1}", SERVER, PORT));
            Console.WriteLine("connectionNum is: " + re.getColumn(0).get(0).ToString());
            Thread.Sleep(10000);
            BasicTable re1 = (BasicTable)conn1.run(String.Format("select connectionNum from rpc(getControllerAlias(),getClusterPerf) where host= \"{0}\" and port = {1}", SERVER, PORT));
            Console.WriteLine("connectionNum is: " + re1.getColumn(0).get(0).ToString());
            Assert.AreEqual(true, int.Parse(re.getColumn(0).get(0).ToString()) - int.Parse(re1.getColumn(0).get(0).ToString()) < 2);
        }
        public void Handler_array(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values( {0},{1})", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace(", ,", ", NULL,").Replace("[,", "[NULL,").Replace(", ]", ", NULL]").Replace(", ", " "));
                    Console.WriteLine(script);
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_INT()
        {
            PrepareStreamTable_array("INT");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_BOOL()
        {
            PrepareStreamTable_array("BOOL");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_CHAR()
        {
            PrepareStreamTable_array("CHAR");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_SHORT()
        {
            PrepareStreamTable_array("SHORT");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_LONG()
        {
            PrepareStreamTable_array("LONG");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DOUBLE()
        {
            PrepareStreamTable_array("DOUBLE");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_FLOAT()
        {
            PrepareStreamTable_array("FLOAT");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_TIME()
        {
            PrepareStreamTable_array("TIME");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_MINUTE()
        {
            PrepareStreamTable_array("MINUTE");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_SECOND()
        {
            PrepareStreamTable_array("SECOND");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DATETIME()
        {
            PrepareStreamTable_array("DATETIME");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_TIMESTAMP()
        {
            PrepareStreamTable_array("TIMESTAMP");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_NANOTIME()
        {
            PrepareStreamTable_array("NANOTIME");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_NANOTIMESTAMP()
        {
            PrepareStreamTable_array("NANOTIMESTAMP");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        public void Handler_array_UUID(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values( {0},[uuid({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    //Console.WriteLine(script);
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_UUID()
        {
            PrepareStreamTable_array("UUID");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_UUID(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        public void Handler_array_DATEHOUR(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values( {0},[datehour({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    //Console.WriteLine(script);
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DATEHOUR()
        {
            PrepareStreamTable_array("DATEHOUR");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_DATEHOUR(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        public void Handler_array_IPADDR(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values( {0},[ipaddr({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    //Console.WriteLine(script);
                    conn.run(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_IPADDR()
        {
            PrepareStreamTable_array("IPADDR");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_IPADDR(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        public void Handler_array_INT128(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    Console.WriteLine(msg.getEntity(1).getString());
                    String script = String.Format("insert into sub1 values( {0},[int128({1})])", msg.getEntity(0).getString(), msg.getEntity(1).getString().Replace("[", "[\"").Replace("]", "\"]").Replace(", ", "\",\"").Replace("\"\"", "NULL"));
                    conn.run(script);
                    Console.WriteLine(script);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_INT128()
        {
            PrepareStreamTable_array("INT128");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_INT128(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }


        public void Handler_array_COMPLEX(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
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
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_COMPLEX()
        {
            PrepareStreamTable_array("COMPLEX");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_COMPLEX(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        public void Handler_array_POINT(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    var cols = new List<IEntity>() { };
                    var colNames = new List<String>() { "permno", "dateType" };
                    BasicArrayVector dateType = new BasicArrayVector(DATA_TYPE.DT_POINT_ARRAY);
                    dateType.append((IVector)msg.getEntity(1));
                    cols.Add(msg.getEntity(0));
                    cols.Add(dateType);
                    conn.run("tableInsert{sub1}", cols);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_POINT()
        {
            PrepareStreamTable_array("POINT");
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array_POINT(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        public void Handler_data(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    var cols = new List<IEntity>() { };
                    cols.Add(msg.getEntity(0));
                    cols.Add(msg.getEntity(1));
                    conn.run("tableInsert{sub1}", cols);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_COMPLEX()
        {
            PrepareStreamTable_allDateType("COMPLEX", 1000);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_data(messages);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_COMPLEX_1()
        {
            PrepareStreamTable_allDateType("COMPLEX", 1);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_data(messages);
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_POINT()
        {
            PrepareStreamTable_allDateType("POINT", 1000);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_data(messages);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }
        [TestMethod]
        public void Test_PollingClient_subscribe_POINT_1()
        {
            PrepareStreamTable_allDateType("POINT", 1);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_data(messages);
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }


        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL32()
        {
            PrepareStreamTableDecimal("DECIMAL32", 3, 1000);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL64()
        {
            PrepareStreamTableDecimal("DECIMAL64", 3, 1000);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }


        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL128()
        {
            PrepareStreamTableDecimal("DECIMAL128", 10, 1000);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Console.WriteLine(res.rows());
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL32_1()
        {
            PrepareStreamTableDecimal("DECIMAL32", 3, 1);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL64_1()
        {
            PrepareStreamTableDecimal("DECIMAL64", 3, 1);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }


        [TestMethod]
        public void Test_PollingClient_subscribe_DECIMAL128_1()
        {
            PrepareStreamTableDecimal("DECIMAL128", 10, 1);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler(messages);
            BasicTable except = (BasicTable)conn.run("select * from  Trades order by permno");
            BasicTable res = (BasicTable)conn.run("select * from  sub1 order by permno");
            Assert.AreEqual(1, res.rows());
            Assert.AreEqual(1, except.rows());
            Assert.AreEqual(except.getColumn(1).getEntity(0).getString(), res.getColumn(1).getEntity(0).getString());
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }


        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DECIMAL32()
        {
            PrepareStreamTableDecimal_array("DECIMAL32", 3);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DECIMAL64()
        {
            PrepareStreamTableDecimal_array("DECIMAL64", 5);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_arrayVector_DECIMAL128()
        {
            PrepareStreamTableDecimal_array("DECIMAL128", 10);
            TopicPoller poller = client.subscribe(SERVER, PORT, "Trades", "sub1", 0, true);
            conn.run("Trades.append!(pub_t);");
            //write 1000 rows after subscribe
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler_array(messages);
            checkResult(conn);
            client.unsubscribe(SERVER, PORT, "Trades", "sub1");
        }

        [TestMethod]
        public void Test_TopicPoller_poll_data_null()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller poller = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            List<IMessage> messages = poller.poll(1000, 1000);
            Handler1(messages);
            BasicTable except = (BasicTable)conn.run("select * from  pub");
            BasicTable res = (BasicTable)conn.run("select * from  sub1");
            Assert.AreEqual(0, except.rows());
            Assert.AreEqual(0, res.rows());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_TopicPoller_poll_size_neg()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller poller = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            String exception = null;
            try
            {
                List<IMessage> messages = poller.poll(1000, -100);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("Size must be greater than zero"));
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }
        [TestMethod]
        public void Test_TopicPoller_poll_size_0()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller poller = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            String exception = null;
            try
            {
                List<IMessage> messages = poller.poll(1000, 0);
            }
            catch (Exception ex)
            {
                exception = ex.ToString();
            }
            Assert.AreEqual(true, exception.Contains("Size must be greater than zero"));
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_TopicPoller_poll_timeout_0()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller poller = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = poller.poll(0, 1000);
            Handler1(messages);
            BasicTable except = (BasicTable)conn.run("select * from  pub");
            BasicTable res = (BasicTable)conn.run("select * from  sub1");
            Assert.AreEqual(1000, except.rows());
            Assert.AreEqual(0, res.rows());
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_TopicPoller_take()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller poller = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false);
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = new List<IMessage>();
            for (int i = 0; i < 1000; i++)
            {
                IMessage msg = poller.take();
                messages.Add(msg);
            }
            Handler1(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        public void Handler_getOffset(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    String script = String.Format("insert into sub1 values({0},{1},\"{2}\",{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} )", msg.getEntity(0).getString(), msg.getEntity(1).getString(), msg.getEntity(2).getString(), msg.getEntity(3).getString(), msg.getEntity(4).getString(), msg.getEntity(5).getString(), msg.getEntity(6).getString(), msg.getEntity(7).getString(), msg.getEntity(8).getString(), msg.getEntity(9).getString(), msg.getEntity(10).getString(), msg.getEntity(11).getString(), msg.getEntity(12).getString());
                    conn.run(script);
                    Console.Out.WriteLine("msg.getOffset is :" + msg.getOffset());
                    myLongList.Add(msg.getOffset());
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_getOffset()
        {
            PrepareStreamTable1(conn, "pub");
            PrepareStreamTable1(conn, "sub1");
            WriteStreamTable1(conn, "pub", 1000);
            myLongList = new List<long>();
            BasicInt num1 = (BasicInt)conn.run("exec count(*) from pub");
            Assert.AreEqual(1000, num1.getInt());
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", 0);
            //write 1000 rows after subscribe
            WriteStreamTable1(conn, "pub", 1000);
            List<IMessage> messages = pool.poll(1000, 2000);
            Handler_getOffset(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub");
            myLongList.Sort();
            int total = 0;
            Assert.AreEqual(2000, myLongList.Count);
            foreach (long item in myLongList)
            {
                Console.WriteLine(item);
                Assert.AreEqual(item, total);
                total++;
            }
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_false()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            BasicIntVector filter = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            TopicPoller pool =  client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", false);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 1000);
            Handler1(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_false_getTable()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", false);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 1000);
            IMessage msg = messages[0];
            String re = null;
            try
            {
                msg.getTable().getString();
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual("The getTable method must be used when msgAsTable is true. ", re);
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        public void Handler_msgAsTable(List<IMessage> messages)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                try
                {
                    IMessage msg = messages[i];
                    List<IEntity> args1 = new List<IEntity>() { msg.getTable() };
                    conn.run("tableInsert{sub1}", args1);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine(e.ToString());
                }
            }
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_true_1()
        {
            PrepareStreamTable1(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `permno)");
            PrepareStreamTable1(conn, "sub1");
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", true);
            String script = "";
            script += "batch=1000;tmp = table(batch:batch,  `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);tmp[`permno] = take(1..100, batch);tmp[`timestamp] = take(now(), batch);tmp[`ticker] = rand(\"A\" + string(1..1000), batch);tmp[`price1] = rand(100, batch);tmp[`price2] = rand(100, batch);tmp[`price3] = rand(100, batch);tmp[`price4] = rand(100, batch);tmp[`price5] = rand(100, batch);tmp[`vol1] = rand(100, batch);tmp[`vol2] = rand(100, batch);tmp[`vol3] = rand(100, batch);tmp[`vol4] = rand(100, batch);tmp[`vol5] = rand(100, batch);pub.append!(tmp); ";
            conn.run(script);
            List<IMessage> messages = pool.poll(1000, 1000);
            Handler_msgAsTable(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_true_all_dateType()
        {
            PrepareStreamTable3(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `intv)");
            PrepareStreamTable3(conn, "sub1");
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", true);
            writeStreamTable_all_dateType(1000, "pub");
            List<IMessage> messages = pool.poll(1000, 1000);
            Handler_msgAsTable(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_true_all_dateType_10000()
        {
            PrepareStreamTable3(conn, "pub");
            conn.run("setStreamTableFilterColumn(pub, `intv)");
            PrepareStreamTable3(conn, "sub1");
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", true);
            writeStreamTable_all_dateType(10000, "pub");
            List<IMessage> messages = pool.poll(1000, 1000);
            Handler_msgAsTable(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_PollingClient_subscribe_msgAsTable_true_all_dateType_array_10000()
        {
            PrepareStreamTable_array_1();
            TopicPoller pool = client.subscribe(SERVER, PORT, "pub", "sub1", 0, false, null, null, "admin", "123456", true);
            List<IMessage> messages = pool.poll(1000, 1000);
            Handler_msgAsTable(messages);
            checkResult1(conn, "pub", "sub1");
            client.unsubscribe(SERVER, PORT, "pub", "sub1");
            conn.run("undef(`pub, SHARED)");
            conn.run("undef(`sub1, SHARED)");
        }

        [TestMethod]
        public void Test_ThreaedClient_subscribe_StreamDeserializer_msgAsTable_true()
        {
            PrepareStreamTable_StreamDeserializer_array_allDataType();
            Dictionary<string, Tuple<string, string>> tables = new Dictionary<string, Tuple<string, string>>();
            tables["msg1"] = new Tuple<string, string>("", "pub_t1");
            tables["msg2"] = new Tuple<string, string>("", "pub_t2");
            StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
            String re = null;
            try
            {
                TopicPoller poller = client.subscribe(SERVER, PORT, "outTables", "mutiSchema", 0, true, null, streamFilter, "admin", "123456", true);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Cannot set deserializer when msgAsTable is true. ", re);
        }
    }
}

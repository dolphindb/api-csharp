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
using dolphindb.streaming.cep;
using dolphindb.io;

namespace dolphindb_csharp_api_test.cep_test
{
    [TestClass]
    public class EventClientTest
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
        static EventSender sender;
        static EventSchema scheme;
        static EventClient client;

        public void clear_env()
        {
            try
            {
                DBConnection conn = new DBConnection();
                conn.connect(SERVER, PORT, "admin", "123456");
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
            clear_env();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            conn.close();
            try { client.unsubscribe(SERVER, PORT, "inputTable", "test1"); } catch (Exception ex) { }
            try { client.unsubscribe(SERVER, PORT, "intput", "test1"); } catch (Exception ex) { }
            try { client.unsubscribe(SERVER, PORT, "inputTable", "StreamingApi"); } catch (Exception ex) { }
            try { client.unsubscribe(SERVER, PORT, "intput", "StreamingApi"); } catch (Exception ex) { }
            clear_env();

        }

        public static void Preparedata(long count)
        {
            String script = "login(`admin, `123456); \n" +
                "n=" + count + ";\n" +
                "boolv = bool(rand([true, false, NULL], n));\n" +
                "charv = char(rand(rand(-100..100, 1000) join take(char(), 4), n));\n" +
                "shortv = short(rand(rand(-100..100, 1000) join take(short(), 4), n));\n" +
                "intv = int(rand(rand(-100..100, 1000) join take(int(), 4), n));\n" +
                "longv = long(rand(rand(-100..100, 1000) join take(long(), 4), n));\n" +
                "doublev = double(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n));\n" +
                "floatv = float(rand(rand(-100..100, 1000)*0.23 join take(float(), 4), n));\n" +
                "datev = date(rand(rand(-100..100, 1000) join take(date(), 4), n));\n" +
                "monthv = month(rand(1967.12M+rand(-100..100, 1000) join take(month(), 4), n));\n" +
                "timev = time(rand(rand(0..100, 1000) join take(time(), 4), n));\n" +
                "minutev = minute(rand(12:13m+rand(-100..100, 1000) join take(minute(), 4), n));\n" +
                "secondv = second(rand(12:13:12+rand(-100..100, 1000) join take(second(), 4), n));\n" +
                "datetimev = datetime(rand(1969.12.23+rand(-100..100, 1000) join take(datetime(), 4), n));\n" +
                "timestampv = timestamp(rand(1970.01.01T00:00:00.023+rand(-100..100, 1000) join take(timestamp(), 4), n));\n" +
                "nanotimev = nanotime(rand(12:23:45.452623154+rand(-100..100, 1000) join take(nanotime(), 4), n));\n" +
                "nanotimestampv = nanotimestamp(rand(rand(-100..100, 1000) join take(nanotimestamp(), 4), n));\n" +
                "symbolv = rand((\"syms\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "stringv = rand((\"stringv\"+string(rand(100, 1000))) join take(string(), 4), n);\n" +
                "uuidv = rand(rand(uuid(), 1000) join take(uuid(), 4), n);\n" +
                "datehourv = datehour(rand(datehour(1969.12.31T12:45:12)+rand(-100..100, 1000) join take(datehour(), 4), n));\n" +
                "ippaddrv = rand(rand(ipaddr(), 1000) join take(ipaddr(), 4), n);\n" +
                "int128v = rand(rand(int128(), 1000) join take(int128(), 4), n);\n" +
                "blobv = blob(string(rand((\"blob\"+string(rand(100, 1000))) join take(\"\", 4), n)));\n" +
                "complexv = rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "pointv = rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n);\n" +
                "decimal32v = decimal32(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 3);\n" +
                "decimal64v = decimal64(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 8);\n" +
                "decimal128v = decimal128(rand(rand(-100..100, 1000)*0.23 join take(double(), 4), n), 10);\n" +
                "share table(boolv, charv, shortv, intv, longv, doublev, floatv,  datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv,  stringv, datehourv, uuidv, ippaddrv, int128v, blobv) as data;\n";
            conn.run(script);
        }

        public static void Preparedata_array(long count1, long count2)
        {
            String script1 = "login(`admin, `123456); \n" +
                "n=" + count1 + ";\n" +
                "m=" + count2 + ";\n" +
                "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], n), m))\n" +
                "cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), n), m))\n" +
                "cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), n), m))\n" +
                "cint = array(INT[]).append!(cut(take(-100..100 join NULL, n), m))\n" +
                "clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), n), m))\n" +
                "cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, n) + 0.254, m))\n" +
                "cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, n) + 0.254f, m))\n" +
                "cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, n), m))\n" +
                "cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, n), m))\n" +
                "ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, n), m))\n" +
                "cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, n), m))\n" +
                "csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, n), m))\n" +
                "cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, n), m))\n" +
                "ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, n), m))\n" +
                "cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, n), m))\n" +
                "cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), n), m))\n" +
                "cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), n), m))\n" +
                "cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), n), m))\n" +
                "cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), n), m))\n" +
                "ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, n), m))\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, n) + 0.254, 3), m))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25467, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25467, 5), m))\n" +
                "share table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128, ccomplex) as data;\n";
            conn.run(script1);
        }

        public static void Preparedata_array_decimal(long count1, long count2)
        {
            String script1 = "login(`admin, `123456); \n" +
                "n=" + count1 + ";\n" +
                "m=" + count2 + ";\n" +
                "cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, n) + 0.254, 3), m))\n" +
                "cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, n) + 0.25467, 4), m))\n" +
                "cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, n) + 0.25467, 5), m))\n" +
                "share table( cdecimal32, cdecimal64,cdecimal128) as data;";
            conn.run(script1);
        }

        public void checkData(BasicTable exception, BasicTable resTable)
        {
            Assert.AreEqual(exception.rows(), resTable.rows());
            for (int i = 0; i < exception.columns(); i++)
            {
                Console.Out.WriteLine("col" + resTable.getColumnName(i));
                Assert.AreEqual(exception.getColumn(i).getString(), resTable.getColumn(i).getString());
            }
        }

        class Handler : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attribute)
            {
                Console.Out.WriteLine("eventType: " + eventType);
                for (int i = 0; i < attribute.Count; i++)
                {
                    Console.Out.WriteLine(attribute[i].ToString());
                }

                try
                {
                    conn.run("tableInsert{outputTable}", attribute);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        class Handler1 : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attribute)
            {
                Console.Out.WriteLine("eventType: " + eventType);
                for (int i = 0; i < attribute.Count; i++)
                {
                    Console.Out.WriteLine(attribute[i].ToString());
                }

                if (eventType.Equals("MarketData"))
                {
                    try
                    {
                        conn.run("tableInsert{outputTable}", attribute);
                    }
                    catch (Exception e)
                    {
                        System.Console.Out.WriteLine(e.ToString());
                    }
                }
                else
                {
                    try
                    {
                        conn.run("tableInsert{outputTable1}", attribute);
                    }
                    catch (Exception e)
                    {
                        System.Console.Out.WriteLine(e.ToString());
                    }
                }
            }
        };

        class Handler_array : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attributes)
            {
                Console.Out.WriteLine("---------eventType------------");
                var cols = new List<IEntity>() { };
                var colNames = new List<String>() {   "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv",  "ippaddrv", "int128v", "complexv" };
                BasicArrayVector boolv = new BasicArrayVector(DATA_TYPE.DT_BOOL_ARRAY);
                boolv.append((IVector)attributes[0]);
                BasicArrayVector charv = new BasicArrayVector(DATA_TYPE.DT_BYTE_ARRAY);
                charv.append((IVector)attributes[1]);
                BasicArrayVector shortv = new BasicArrayVector(DATA_TYPE.DT_SHORT_ARRAY);
                shortv.append((IVector)attributes[2]);
                BasicArrayVector intv = new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY);
                intv.append((IVector)attributes[3]);
                BasicArrayVector longv = new BasicArrayVector(DATA_TYPE.DT_LONG_ARRAY);
                longv.append((IVector)attributes[4]);
                BasicArrayVector doublev = new BasicArrayVector(DATA_TYPE.DT_DOUBLE_ARRAY);
                doublev.append((IVector)attributes[5]);
                BasicArrayVector floatv = new BasicArrayVector(DATA_TYPE.DT_FLOAT_ARRAY);
                floatv.append((IVector)attributes[6]);
                BasicArrayVector datev = new BasicArrayVector(DATA_TYPE.DT_DATE_ARRAY);
                datev.append((IVector)attributes[7]);
                BasicArrayVector monthv = new BasicArrayVector(DATA_TYPE.DT_MONTH_ARRAY);
                monthv.append((IVector)attributes[8]);
                BasicArrayVector timev = new BasicArrayVector(DATA_TYPE.DT_TIME_ARRAY);
                timev.append((IVector)attributes[9]);
                BasicArrayVector minutev = new BasicArrayVector(DATA_TYPE.DT_MINUTE_ARRAY);
                minutev.append((IVector)attributes[10]);
                BasicArrayVector secondv = new BasicArrayVector(DATA_TYPE.DT_SECOND_ARRAY);
                secondv.append((IVector)attributes[11]);
                BasicArrayVector datetimev = new BasicArrayVector(DATA_TYPE.DT_DATETIME_ARRAY);
                datetimev.append((IVector)attributes[12]);
                BasicArrayVector timestampv = new BasicArrayVector(DATA_TYPE.DT_TIMESTAMP_ARRAY);
                timestampv.append((IVector)attributes[13]);
                BasicArrayVector nanotimev = new BasicArrayVector(DATA_TYPE.DT_NANOTIME_ARRAY);
                nanotimev.append((IVector)attributes[14]);
                BasicArrayVector nanotimestampv = new BasicArrayVector(DATA_TYPE.DT_NANOTIMESTAMP_ARRAY);
                nanotimestampv.append((IVector)attributes[15]);
                BasicArrayVector datehourv = new BasicArrayVector(DATA_TYPE.DT_DATEHOUR_ARRAY);
                datehourv.append((IVector)attributes[16]);
                BasicArrayVector uuidv = new BasicArrayVector(DATA_TYPE.DT_UUID_ARRAY);
                uuidv.append((IVector)attributes[17]);              
                BasicArrayVector ippaddrv = new BasicArrayVector(DATA_TYPE.DT_IPADDR_ARRAY);
                ippaddrv.append((IVector)attributes[18]);
                BasicArrayVector int128v = new BasicArrayVector(DATA_TYPE.DT_INT128_ARRAY);
                int128v.append((IVector)attributes[19]);
                BasicArrayVector complexv = new BasicArrayVector(DATA_TYPE.DT_COMPLEX_ARRAY);
                complexv.append((IVector)attributes[20]);
                cols.Add(boolv);
                cols.Add(charv);
                cols.Add(shortv);
                cols.Add(intv);
                cols.Add(longv);
                cols.Add(doublev);
                cols.Add(floatv);
                cols.Add(datev);
                cols.Add(monthv);
                cols.Add(timev);
                cols.Add(minutev);
                cols.Add(secondv);
                cols.Add(datetimev);
                cols.Add(timestampv);
                cols.Add(nanotimev);
                cols.Add(nanotimestampv);
                cols.Add(datehourv);
                cols.Add(uuidv);
                cols.Add(ippaddrv);
                cols.Add(int128v);
                cols.Add(complexv);
                //BasicTable bt = new BasicTable(colNames, cols);

                //var variable = new Dictionary<string, IEntity>();
                //variable.Add("table_tmp", bt);
                //conn.upload(variable);
                //conn.run("outputTable.tableInsert(table_tmp)");
                //Console.Out.WriteLine("eventType: " + eventType);
                //for (int i = 0; i < attributes.Count; i++)
                //{
                //    Console.Out.WriteLine(attributes[i].getString());

                //}
                try
                {
                    conn.run("tableInsert{outputTable}", cols);
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.ToString());
                }
            }
        };

        class Handler_array_decimal : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attributes)
            {
               
                String decimal32v = attributes[0].getString().Replace(",,", ",NULL,").Replace("[,", "[NULL,").Replace(",]", ",NULL]").Replace(",", "\" \"").Replace("[", "[\"").Replace("]", "\"]");
                String decimal64v = attributes[1].getString().Replace(",,", ",NULL,").Replace("[,", "[NULL,").Replace(",]", ",NULL]").Replace(",", "\" \"").Replace("[", "[\"").Replace("]", "\"]");
                String decimal128v = attributes[2].getString().Replace(",,", ",NULL,").Replace("[,", "[NULL,").Replace(",]", ",NULL]").Replace(",", "\" \"").Replace("[", "[\"").Replace("]", "\"]");

                for (int i = 0; i < attributes.Count; i++)
                {
                    //attributes[i].getString();
                    Console.WriteLine(attributes[i].getString());
                }
                String script = null;
                script = String.Format("insert into outputTable values( decimal32({0},2),decimal64({1},7),decimal128({2},19))", decimal32v, decimal64v, decimal128v);
                Console.WriteLine("script:" + script);
                conn.run(script);
            }
        };



        public void PrepareUser(String userName, String password)
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            conn.run("def create_user(){try{deleteUser(`" + userName + ")}catch(ex){};createUser(`" + userName + ", '" + password + "');};" +
                    "rpc(getControllerAlias(),create_user);");
        }


        [TestMethod]
        public void test_EventClient_EventScheme_null()
        {
            List<EventSchema> eventSchemas = new List<EventSchema>();
            List<String> eventTimeFields = new List<String>();
            List<String> commonFields = new List<String>();
            String re = null;
            try
            {
                EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventSchemas must not be empty.", re);
        }

        [TestMethod]
        public void test_EventClient_EventType_null()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventType must be non-empty.", re);
        }

        [TestMethod]
        public void test_EventClient_EventType_repetition()
        {
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time", "time1", "time2", "time3" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            EventSchema scheme1 = new EventSchema("market", new List<string> { "market", "time0", "time1", "time2", "time3" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });

            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            eventSchemas.Add(scheme1);
            List<String> eventTimeFields = new List<string>() { "market" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("EventType must be unique.", re);
        }

        [TestMethod]
        public void test_EventClient_fieldNames_repetition()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "time", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "time" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("fieldNames must be unique.", re);
        }

        [TestMethod]
        public void test_EventClient_fieldNames_one_colume()
        {
            String script = "share streamTable(1:0, [`timestamp], [TIMESTAMP]) as outputTable;\n" +
                    "class MarketData{\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData(t){\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "class MainMonitor{\n" +
                    "def MainMonitor(){}\n" +
                    "def updateMarketData(event)\n" +
                    "def onload(){addEventListener(updateMarketData,'MarketData',,'all')}\n" +
                    "def updateMarketData(event){emitEvent(event)}\n" +
                    "}\n" +
                    "dummy = table(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs);\n" +
                    "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"timestamp\", \"TIMESTAMP\", 12, 0)\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\")\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>();

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput", "test1", handler, -1, true, "admin", "123456");
            conn.run("marketData1 = MarketData(now());\n appendEvent(inputSerializer, [marketData1])");
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
            BasicTable re1 = (BasicTable)conn.run("select timestamp from intput");
            checkData(re1, re);
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }


        [TestMethod]
        public void test_EventClient_FieldForms_null_1()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_VOID, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM>());

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("not support VOID type. ", re);
        }

        [TestMethod]
        public void test_EventClient_FieldForms_not_support()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_PAIR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("FieldForm only can be DF_SCALAR or DF_VECTOR.", re);
        }


        //[TestMethod]
        //public void test_EventClient_attrExtraParams_null()
        //{
        //    EventSchema scheme = new EventSchema();
        //    scheme.setEventType("market");
        //    scheme.setFieldNames(Arrays.asList("market", "code", "decimal32", "decimal64", "decimal128"));
        //    scheme.setFieldTypes(Arrays.asList(DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //    scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR));
        //    List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        //    List<String> eventTimeFields = new List<String>();
        //    List<String> commonFields = new List<String>();
        //    EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
        //}

        //[TestMethod]
        //public void test_EventClient_attrExtraParams_set_not_true()
        //{
        //    EventSchema scheme = new EventSchema();
        //    scheme.setEventType("market");
        //    scheme.setFieldNames(Arrays.asList("decimal32", "decimal64", "decimal128"));
        //    scheme.setFieldTypes(Arrays.asList(DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //    scheme.setFieldForms(Arrays.asList(DF_SCALAR, DF_SCALAR, DF_SCALAR));
        //    scheme.setFieldExtraParams(Arrays.asList(10, 19, 39));
        //    List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        //    List<String> eventTimeFields = new List<String>();
        //    List<String> commonFields = new List<String>();
        //    String re = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL32 scale 10 is out of bounds, it must be in [0,9].", re);

        //    scheme.setFieldExtraParams(Arrays.asList(1, 19, 39));
        //    String re1 = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re1 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL64 scale 19 is out of bounds, it must be in [0,18].", re1);

        //    scheme.setFieldExtraParams(Arrays.asList(1, 18, 39));
        //    String re2 = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re2 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL128 scale 39 is out of bounds, it must be in [0,38].", re2);

        //    scheme.setFieldExtraParams(Arrays.asList(-1, 10, 10));
        //    String re3 = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re3 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL32 scale -1 is out of bounds, it must be in [0,9].", re3);

        //    scheme.setFieldExtraParams(Arrays.asList(1, -1, 0));
        //    String re4 = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re4 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL64 scale -1 is out of bounds, it must be in [0,18].", re4);

        //    scheme.setFieldExtraParams(Arrays.asList(0, 0, -1));
        //    String re5 = null;
        //    try
        //    {
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re5 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL128 scale -1 is out of bounds, it must be in [0,38].", re5);
        //}

        [TestMethod]
        public void test_EventClient_eventTimeFields_not_exist()
        {

            EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Event market doesn't contain eventTimeKey datetimev.", re);
        }

        [TestMethod]
        public void test_EventClient_eventTimeFields_two_column()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            String script = "share streamTable(1:0, `timestamp`time, [TIMESTAMP,TIME]) as outputTable;\n" +
                    "share streamTable(1:0, `string`timestamp, [STRING,TIMESTAMP]) as outputTable1;\n" +
                    "class MarketData{\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "time :: TIME\n" +
                    "def MarketData(t,t1){\n" +
                    "timestamp = t\n" +
                    "time = t1\n" +
                    "}\n" +
                    "}\n" +
                    "class MarketData1{\n" +
                    "string :: STRING\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData1(s,t){\n" +
                    "string = s\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n" +
                    "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\")\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("MarketData1", new List<string> { "string", "timestamp" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);
            List<String> eventTimeFields = new List<string>() { "time", "timestamp" };
            List<String> commonFields = new List<string>();

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
            conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
            BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

            Assert.AreEqual(1, re.rows());
            Assert.AreEqual(1, re1.rows());
            Assert.AreEqual(re2.getColumn(0).get(0).getString(), re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:00:00.001", re.getColumn(1).get(0).getString());
            Assert.AreEqual("tesrtttt", re1.getColumn(0).get(0).getString());
            Assert.AreEqual(re2.getColumn(0).get(1).getString(), re1.getColumn(1).get(0).getString());
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }

        [TestMethod]
        public void test_EventClient_commonFields_one_column()
        {
            String script = "share streamTable(1:0, `timestamp`time`commonKey, [TIMESTAMP,TIME,TIMESTAMP]) as outputTable;\n" +
                    "share streamTable(1:0, `string`timestamp`commonKey, [STRING,TIMESTAMP,TIMESTAMP]) as outputTable1;\n" +
                    "class MarketData{\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "time :: TIME\n" +
                    "def MarketData(t,t1){\n" +
                    "timestamp = t\n" +
                    "time = t1\n" +
                    "}\n" +
                    "}\n" +
                    "class MarketData1{\n" +
                    "string :: STRING\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData1(s,t){\n" +
                    "string = s\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(TIMESTAMP, 0) as commonKey) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n" +
                    "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\", commonField = \"timestamp\")\n";
            conn.run(script);

            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("MarketData1", new List<string> { "string", "timestamp" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);
            List<String> eventTimeFields = new List<string>() { "time", "timestamp" };
            List<String> commonFields = new List<string>() { "timestamp" };

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
            conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
            BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

            Assert.AreEqual(1, re.rows());
            Assert.AreEqual(1, re1.rows());
            Assert.AreEqual(re2.getColumn(0).get(0).getString(), re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:00:00.001", re.getColumn(1).get(0).getString());
            Assert.AreEqual("tesrtttt", re1.getColumn(0).get(0).getString());
            Assert.AreEqual(re2.getColumn(0).get(0).getString(), re1.getColumn(1).get(0).getString());
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }

        [TestMethod]
        public void test_EventClient_commonFields_two_column()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("123456"));
            attributes.Add(new BasicTime(10));
            sender.sendEvent("market", attributes);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(12));
            sender.sendEvent("market1", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(2, re.rows());
            Assert.AreEqual("00:00:00.010", re.getColumn(2).get(0).getString());
            Assert.AreEqual("00:00:00.012", re.getColumn(2).get(1).getString());
            Assert.AreEqual("123456", re.getColumn(3).get(0).getString());
            Assert.AreEqual("tesrtrrr", re.getColumn(3).get(1).getString());
        }

        public void subscribePrepare()
        {
            conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "comment1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>() { "comment1" };

            sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
        }

        [TestMethod]
        public void test_EventClient_subscribe_SERVER_null()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(null, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("host must be non-null.", re);
        }

        [TestMethod]
        public void test_EventClient_subscribe_SERVER_not_true()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe("erer", PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Database connection is not established yet.", re);
        }
        [TestMethod]
        public void test_EventClient_subscribe_port_0()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, 0, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Database connection is not established yet.", re);
        }

        [TestMethod]
        public void test_EventClient_subscribe_port_not_true()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, 18888, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Database connection is not established yet.", re);
        }

        [TestMethod]
        public void test_EventClient_subscribe_tableName_not_exist()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, PORT, "inputTable111", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Console.Out.WriteLine(re);
            Assert.AreEqual(true, re.Contains(" Can't find the object with name inputTable111"));
        }

        [TestMethod]
        public void test_EventClient_subscribe_tableName_null()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, PORT, null, "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("tableName must be non-null.", re);
            String re1 = null;
            try
            {
                client.subscribe(SERVER, PORT, "", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re1 = ex.Message;
            }
            Assert.AreEqual("tableName must be non-empty.", re1);
        }

        [TestMethod]
        public void test_EventClient_subscribe_actionName_exist()
        {
            subscribePrepare();
            conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable1;");
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            client.subscribe(SERVER, PORT, "inputTable1", "test1", handler1, -1, true, "admin", "123456");
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            client.unsubscribe(SERVER, PORT, "inputTable1", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_actionName_null()
        {
            subscribePrepare();
            Handler1 handler1 = new Handler1();
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", null, handler1, -1, true, "admin", "123456");
            }
            catch (Exception e)
            {
                re = e.Message;
            }
            Assert.AreEqual("actionName must be non-null.", re);
        }

        [TestMethod]
        public void test_EventClient_subscribe_handler_null()
        {
            subscribePrepare();
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", null, -1, true, "admin", "123456");
            }
            catch (Exception e){
                re = e.Message;
            }
            Assert.AreEqual("handler must be non-null.", re);

        }
        [TestMethod]
        public void test_EventClient_subscribe_offset_negative_1()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }
        [TestMethod]
        public void test_EventClient_subscribe_offset_negative_2()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -2, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }
        [TestMethod]
        public void test_EventClient_subscribe_offset_0()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, 0, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(2, re.rows());
        }

        [TestMethod]
        public void test_EventClient_subscribe_offset_1()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, 1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
        }
        [TestMethod]
        public void test_EventClient_subscribe_offset_not_match()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            String re = null;
            Handler handler = new Handler();
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler, 2, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("Can't find the message with offset"));
        }
        [TestMethod]
        public void test_EventClient_subscribe_reconnect_true()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, false, "admin", "123456");
            Thread.Sleep(1000);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_reconnect_false()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            Thread.Sleep(1000);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_user_error()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin123", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("The user name or password is incorrect"));
        }


        [TestMethod]
        public void test_EventClient_subscribe_password_error()
        {
            subscribePrepare();
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456WWW");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Console.WriteLine(re);
           // Assert.AreEqual(true, re.Contains("The user name or password is incorrect"));
        }

        [TestMethod]
        public void test_EventClient_subscribe_admin()
        {
            PrepareUser("user1", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "user1", "123456");
            conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
            conn.run("addAccessControl(`inputTable)");
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            subscribePrepare();

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
        }
        [TestMethod]
        public void test_EventClient_subscribe_other_user()
        {
            PrepareUser("user1", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "user1", "123456");
            conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
            conn.run("addAccessControl(`inputTable)");
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            subscribePrepare();

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "user1", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
        }
        [TestMethod]
        public void test_EventClient_other_user_unallow()
        {
            PrepareUser("user1", "123456");
            PrepareUser("user2", "123456");
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "user1", "123456");
            conn.run("share streamTable(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]) as inputTable;");
            conn.run("addAccessControl(`inputTable)");
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "comment1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);

            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>() { "comment1" };
            sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            String re = null;
            Handler1 handler1 = new Handler1();
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "user2", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("No access to shared table [inputTable]"));
        }
        [TestMethod]
        public void test_EventClient_subscribe_unsubscribe_resubscribe()
        {
            subscribePrepare();
            conn.run("share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;");
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            sender.sendEvent("MarketData", attributes);
            Handler handler = new Handler();
            for (int i = 0; i < 10; i++)
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
                sender.sendEvent("MarketData", attributes);
                Thread.Sleep(200);
                client.unsubscribe(SERVER, PORT, "inputTable", "test1");
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
                sender.sendEvent("MarketData", attributes);
                Thread.Sleep(200);
                client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            }
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(20, re.rows());
        }
        [TestMethod]
        public void test_EventClient_subscribe_duplicated()
        {
            subscribePrepare();
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            String re = null;
            try
            {
                client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("already be subscribed"));
        }

        [TestMethod]
        public void test_EventClient_not_subscribe_unsubscribe()
        {
            subscribePrepare();
            String re = null;
            try
            {
                client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Console.WriteLine(re);
            Assert.AreEqual(true, re.Contains("/inputTable/test1 doesn't exist."));

        }

        [TestMethod]
        public void test_EventClient_unsubscribe_duplicated()
        {
            subscribePrepare();
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler1, -1, true, "admin", "123456");
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            String re = null;
            try
            {
                client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Console.WriteLine(re);
            Assert.AreEqual(true, re.Contains("/inputTable/test1 doesn't exist."));
        }

        [TestMethod]
        public void test_EventClient_subscribe_haStreamTable()
        {
            String script = "try{\ndropStreamTable(`inputTable)\n}catch(ex){\n}\n" +
                    "table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]);\n" +
                    "haStreamTable(" + HASTREAM_GROUPID + ", table, `inputTable, 100000);\n" +
                    "share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "comment1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);

            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>() { "comment1" };

            sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));

            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
        }
        //[TestMethod]
        public void test_EventClient_subscribe_haStreamTable_leader()
        {
            BasicString StreamLeaderTmp = (BasicString)conn.run(String.Format("getStreamingLeader(%d)", HASTREAM_GROUPID));
            String StreamLeader = StreamLeaderTmp.getString();
            BasicString StreamLeaderSERVERTmp = (BasicString)conn.run(String.Format("(exec SERVER from rpc(getControllerAlias(), getClusterPerf) where name=\"{0}\")[0]", StreamLeader));
            String StreamLeaderSERVER = StreamLeaderSERVERTmp.getString();
            BasicInt StreamLeaderPortTmp = (BasicInt)conn.run(String.Format("(exec port from rpc(getControllerAlias(), getClusterPerf) where mode = 0 and  name=\"{0}\")[0]", StreamLeader));
            int StreamLeaderPort = StreamLeaderPortTmp.getInt();
            Console.Out.WriteLine(StreamLeaderSERVER);
            Console.Out.WriteLine(StreamLeaderPort);
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamLeaderSERVER, StreamLeaderPort, "admin", "123456");
            String script = "try{\ndropStreamTable(`inputTable_1)\n}catch(ex){\n}\n" +
                "table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]);\n" +
                "haStreamTable(" + HASTREAM_GROUPID + ", table, `inputTable_1, 100000);\n" +
                "share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;\n";
            conn1.run(script);

            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "comment1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>() { "comment1" };

            sender = new EventSender(conn, "inputTable_1", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            Handler handler = new Handler();
            client.subscribe(StreamLeaderSERVER, StreamLeaderPort, "inputTable_1", "test1", handler, -1, true, "admin", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn1.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("2024.03.22T10:45:03.100", re.getColumn(0).get(0).getString());
            Assert.AreEqual("123456", re.getColumn(1).get(0).getString());
            client.unsubscribe(StreamLeaderSERVER, StreamLeaderPort, "inputTable_1", "test1");
        }

        //[TestMethod]//not support
        public void test_EventClient_subscribe_haStreamTable_follower()
        {
            String script0 = "leader = getStreamingLeader(" + HASTREAM_GROUPID + ");\n" +
                    "groupSitesStr = (exec sites from getStreamingRaftGroups() where id ==" + HASTREAM_GROUPID + ")[0];\n" +
                    "groupSites = split(groupSitesStr, \",\");\n" +
                    "followerInfo = exec top 1 *  from rpc(getControllerAlias(), getClusterPerf) where site in groupSites and name!=leader;";
            conn.run(script0);
            BasicString StreamFollowerSERVERTmp = (BasicString)conn.run("(exec SERVER from followerInfo)[0]");
            String StreamFollowerSERVER = StreamFollowerSERVERTmp.getString();
            BasicInt StreamFollowerPortTmp = (BasicInt)conn.run("(exec port from followerInfo)[0]");
            int StreamFollowerPort = StreamFollowerPortTmp.getInt();
            Console.Out.WriteLine(StreamFollowerSERVER);
            Console.Out.WriteLine(StreamFollowerPort);
            DBConnection conn1 = new DBConnection();
            conn1.connect(StreamFollowerSERVER, StreamFollowerPort, "admin", "123456");
            String script = "try{\ndropStreamTable(`inputTable_1)\n}catch(ex){\n}\n" +
                    "table = table(1000000:0, `timestamp`eventType`event`comment1, [TIMESTAMP,STRING,BLOB,STRING]);\n" +
                    "haStreamTable(" + HASTREAM_GROUPID + ", table, `inputTable_1, 100000);\n" +
                    "share table(100:0, `timestamp`comment1, [TIMESTAMP,STRING]) as outputTable;\n";
            conn1.run(script);
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "comment1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "timestamp" };
            List<String> commonFields = new List<string>() { "comment1" };
            sender = new EventSender(conn, "inputTable_1", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTimestamp(10));
            attributes.Add(new BasicString("123456"));
            Handler handler = new Handler();
            client.subscribe(StreamFollowerSERVER, StreamFollowerPort, "inputTable_1", "test1", handler, -1, true, "user1", "123456");
            sender.sendEvent("MarketData", attributes);
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn1.run("select * from outputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("2024.03.22T10:45:03.100", re.getColumn(0).get(0).getString());
            Assert.AreEqual("123456", re.getColumn(1).get(0).getString());
            client.unsubscribe(StreamFollowerSERVER, StreamFollowerPort, "inputTable_1", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_special_char()
        {
            String script = "share streamTable(1:0, `timestamp`time, [STRING,TIMESTAMP]) as outputTable;\n" +
                    "class MarketData{\n" +
                    "string :: STRING\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData(s,t){\n" +
                    "string = s\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(STRING, 0) as comment1,array(TIMESTAMP, 0) as comment2 ) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, commonField = [\"string\",\"timestamp\"])\n";
            conn.run(script);
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,STRING,TIMESTAMP]) as inputTable;");
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "_market中文", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "_market中文", "time" };

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    "));
            attributes.Add(new BasicTimestamp(1));
            sender.sendEvent("MarketData", attributes);

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput", "test1", handler, -1, true, "admin", "123456");
            conn.run(" marketData1 = MarketData(\"!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    \",timestamp(1));\n appendEvent(inputSerializer, marketData1)");
            Thread.Sleep(1000);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            BasicTable re1 = (BasicTable)conn.run("select * from intput");
            BasicTable re2 = (BasicTable)conn.run("select * from outputTable");

            checkData(re, re1);
            Console.Out.WriteLine(re2.getString());
            Assert.AreEqual("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    ", re2.getColumn(0).get(0).getString());
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_scaler_complex()
        {
            String script = "share streamTable(1:0, `timestamp`time, [STRING,TIMESTAMP]) as outputTable;\n" +
                    "class MarketData{\n" +
                    "string :: STRING\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData(s,t){\n" +
                    "string = s\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(STRING, 0) as comment1,array(TIMESTAMP, 0) as comment2 ) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, commonField = [\"string\",\"timestamp\"])\n";
            conn.run(script);
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,STRING,TIMESTAMP]) as inputTable;");
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "_market中文", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "_market中文", "time" };

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    "));
            attributes.Add(new BasicTimestamp(1));
            sender.sendEvent("MarketData", attributes);

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput", "test1", handler, -1, true, "admin", "123456");
            conn.run(" marketData1 = MarketData(\"!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    \",timestamp(1));\n appendEvent(inputSerializer, marketData1)");
            Thread.Sleep(1000);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            BasicTable re1 = (BasicTable)conn.run("select * from intput");
            BasicTable re2 = (BasicTable)conn.run("select * from outputTable");

            checkData(re, re1);
            Console.Out.WriteLine(re2.getString());
            Assert.AreEqual("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文    ", re2.getColumn(0).get(0).getString());
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_all_dateType_1()
        {
            String script = "share streamTable(1:0, `eventTime`eventType`blobs, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippAddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_dateType{\n" +
                    "\tboolv :: BOOL\n" +
                    "\tcharv :: CHAR\n" +
                    "\tshortv :: SHORT\n" +
                    "\tintv :: INT\n" +
                    "\tlongv :: LONG\n" +
                    "\tdoublev :: DOUBLE \n" +
                    "\tfloatv :: FLOAT\n" +
                    "\tdatev :: DATE\n" +
                    "\tmonthv :: MONTH\n" +
                    "\ttimev :: TIME\n" +
                    "\tminutev :: MINUTE\n" +
                    "\tsecondv :: SECOND\n" +
                    "\tdatetimev :: DATETIME \n" +
                    "\ttimestampv :: TIMESTAMP\n" +
                    "\tnanotimev :: NANOTIME\n" +
                    "\tnanotimestampv :: NANOTIMESTAMP\n" +
                    // "\tsymbolv :: SYMBOL\n" +
                    "\tstringv :: STRING\n" +
                    "\tdatehourv :: DATEHOUR\n" +
                    "\tuuidv :: UUID\n" +
                    "\tippAddrv :: IPAddR \n" +
                    "\tint128v :: INT128\n" +
                    "\tblobv :: BLOB\n" +
                    "  def event_all_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp,  string, datehour, uuid, ippAddr, int128, blob){\n" +
                    "\tboolv = bool\n" +
                    "\tcharv = char\n" +
                    "\tshortv = short\n" +
                    "\tintv = int\n" +
                    "\tlongv = long\n" +
                    "\tdoublev = double\n" +
                    "\tfloatv = float\n" +
                    "\tdatev = date\n" +
                    "\tmonthv = month\n" +
                    "\ttimev = time\n" +
                    "\tminutev = minute\n" +
                    "\tsecondv = second\n" +
                    "\tdatetimev = datetime\n" +
                    "\ttimestampv = timestamp\n" +
                    "\tnanotimev = nanotime\n" +
                    "\tnanotimestampv = nanotimestamp\n" +
                    // "\tsymbolv = symbol\n" +
                    "\tstringv = string\n" +
                    "\tdatehourv = datehour\n" +
                    "\tuuidv = uuid\n" +
                    "\tippAddrv = ippAddr\n" +
                    "\tint128v = int128\n" +
                    "\tblobv = blob\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_dateType'\n" +
                    "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,stringv,datehourv,uuidv,ippAddrv,int128v,blobv';\n" +
                    "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB];\n" +
                    "formV = [SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput", "test1", handler, -1, true, "admin", "123456");

            Preparedata(1);
            String script2 = "data1=select * from data;\n" +
                    "i = 0\n" +
                    "\tall_data_type1=event_all_dateType(data1.row(i)[`boolv], data1.row(i)[`charv], data1.row(i)[`shortv], data1.row(i)[`intv],data1.row(i)[`longv], data1.row(i)[`doublev], data1.row(i)[`floatv], data1.row(i)[`datev],data1.row(i)[`monthv], data1.row(i)[`timev], data1.row(i)[`minutev], data1.row(i)[`secondv],data1.row(i)[`datetimev], data1.row(i)[`timestampv], data1.row(i)[`nanotimev], data1.row(i)[`nanotimestampv], data1.row(i)[`stringv], data1.row(i)[`datehourv], data1.row(i)[`uuidv], data1.row(i)[`ippaddrv],data1.row(i)[`int128v], blob(data1.row(i)[`blobv]))\n" +
                    "\tappendEvent(inputSerializer, all_data_type1)\n";
            conn.run(script2);
            Thread.Sleep(10000);
            BasicTable bt1 = (BasicTable)conn.run("select * from data;");
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(20000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventClient_subscribe_all_dateType_100()
        {
            String script = "share streamTable(1:0, `eventTime`eventType`blobs, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippAddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_dateType{\n" +
                    "\tboolv :: BOOL\n" +
                    "\tcharv :: CHAR\n" +
                    "\tshortv :: SHORT\n" +
                    "\tintv :: INT\n" +
                    "\tlongv :: LONG\n" +
                    "\tdoublev :: DOUBLE \n" +
                    "\tfloatv :: FLOAT\n" +
                    "\tdatev :: DATE\n" +
                    "\tmonthv :: MONTH\n" +
                    "\ttimev :: TIME\n" +
                    "\tminutev :: MINUTE\n" +
                    "\tsecondv :: SECOND\n" +
                    "\tdatetimev :: DATETIME \n" +
                    "\ttimestampv :: TIMESTAMP\n" +
                    "\tnanotimev :: NANOTIME\n" +
                    "\tnanotimestampv :: NANOTIMESTAMP\n" +
                    // "\tsymbolv :: SYMBOL\n" +
                    "\tstringv :: STRING\n" +
                    "\tdatehourv :: DATEHOUR\n" +
                    "\tuuidv :: UUID\n" +
                    "\tippAddrv :: IPAddR \n" +
                    "\tint128v :: INT128\n" +
                    "\tblobv :: BLOB\n" +
                    "  def event_all_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp,  string, datehour, uuid, ippAddr, int128, blob){\n" +
                    "\tboolv = bool\n" +
                    "\tcharv = char\n" +
                    "\tshortv = short\n" +
                    "\tintv = int\n" +
                    "\tlongv = long\n" +
                    "\tdoublev = double\n" +
                    "\tfloatv = float\n" +
                    "\tdatev = date\n" +
                    "\tmonthv = month\n" +
                    "\ttimev = time\n" +
                    "\tminutev = minute\n" +
                    "\tsecondv = second\n" +
                    "\tdatetimev = datetime\n" +
                    "\ttimestampv = timestamp\n" +
                    "\tnanotimev = nanotime\n" +
                    "\tnanotimestampv = nanotimestamp\n" +
                    // "\tsymbolv = symbol\n" +
                    "\tstringv = string\n" +
                    "\tdatehourv = datehour\n" +
                    "\tuuidv = uuid\n" +
                    "\tippAddrv = ippAddr\n" +
                    "\tint128v = int128\n" +
                    "\tblobv = blob\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_dateType'\n" +
                    "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,stringv,datehourv,uuidv,ippAddrv,int128v,blobv';\n" +
                    "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB];\n" +
                    "formV = [SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR, SCALAR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput", "test1", handler, -1, true, "admin", "123456");

            Preparedata(100);
            String script2 = "data1=select * from data;\n" +
                    "for(i in 0..99){\n" +
                    "\tall_data_type1=event_all_dateType(data1.row(i)[`boolv], data1.row(i)[`charv], data1.row(i)[`shortv], data1.row(i)[`intv],data1.row(i)[`longv], data1.row(i)[`doublev], data1.row(i)[`floatv], data1.row(i)[`datev],data1.row(i)[`monthv], data1.row(i)[`timev], data1.row(i)[`minutev], data1.row(i)[`secondv],data1.row(i)[`datetimev], data1.row(i)[`timestampv], data1.row(i)[`nanotimev], data1.row(i)[`nanotimestampv],data1.row(i)[`stringv], data1.row(i)[`datehourv], data1.row(i)[`uuidv], data1.row(i)[`ippaddrv],data1.row(i)[`int128v], blob(data1.row(i)[`blobv]))\n" +
                    "\tappendEvent(inputSerializer, all_data_type1)\n" +
                    "\t}";
            conn.run(script2);
            Thread.Sleep(10000);
            BasicTable bt1 = (BasicTable)conn.run("select * from data;");
            Assert.AreEqual(100, bt1.rows());
            Thread.Sleep(20000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(100, bt2.rows());
            checkData(bt1, bt2);
        }

        //    [TestMethod]
        //    public  void test_EventClient_all_dateType_vector()  {
        //        Preparedata_array(100,10);
        //        String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"+
        //                "colNames=\"col\"+string(1..25);\n" +
        //                "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
        //                "share table(1:0,colNames,colTypes) as outputTable;\n" ;
        //        conn.run(script);
        //        String script1 ="class event_all_array_dateType{\n" +
        //                "\tboolv :: BOOL VECTOR\n" +
        //                "\tcharv :: CHAR VECTOR\n" +
        //                "\tshortv :: SHORT VECTOR\n" +
        //                "\tintv :: INT VECTOR\n" +
        //                "\tlongv :: LONG VECTOR\n" +
        //                "\tdoublev :: DOUBLE  VECTOR\n" +
        //                "\tfloatv :: FLOAT VECTOR\n" +
        //                "\tdatev :: DATE VECTOR\n" +
        //                "\tmonthv :: MONTH VECTOR\n" +
        //                "\ttimev :: TIME VECTOR\n" +
        //                "\tminutev :: MINUTE VECTOR\n" +
        //                "\tsecondv :: SECOND VECTOR\n" +
        //                "\tdatetimev :: DATETIME VECTOR \n" +
        //                "\ttimestampv :: TIMESTAMP VECTOR\n" +
        //                "\tnanotimev :: NANOTIME VECTOR\n" +
        //                "\tnanotimestampv :: NANOTIMESTAMP VECTOR\n" +
        //                "\t//stringv :: STRING VECTOR\n" +
        //                "\tdatehourv :: DATEHOUR VECTOR\n" +
        //                "\tuuidv :: UUID VECTOR\n" +
        //                "\tippaddrv :: IPADDR  VECTOR\n" +
        //                "\tint128v :: INT128 VECTOR\n" +
        //                "\t//blobv :: BLOB VECTOR\n" +
        //                "\tpointv :: POINT VECTOR\n" +
        //                "\tcomplexv :: COMPLEX VECTOR\n" +
        //                "\tdecimal32v :: DECIMAL32(3)  VECTOR\n" +
        //                "\tdecimal64v :: DECIMAL64(8) VECTOR\n" +
        //                "\tdecimal128v :: DECIMAL128(10) VECTOR \n" +
        //                "  def event_all_array_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, datehour, uuid, ippaddr, int128,point, complex, decimal32, decimal64, decimal128){\n" +
        //                "\tboolv = bool\n" +
        //                "\tcharv = char\n" +
        //                "\tshortv = short\n" +
        //                "\tintv = int\n" +
        //                "\tlongv = long\n" +
        //                "\tdoublev = double\n" +
        //                "\tfloatv = float\n" +
        //                "\tdatev = date\n" +
        //                "\tmonthv = month\n" +
        //                "\ttimev = time\n" +
        //                "\tminutev = minute\n" +
        //                "\tsecondv = second\n" +
        //                "\tdatetimev = datetime\n" +
        //                "\ttimestampv = timestamp\n" +
        //                "\tnanotimev = nanotime\n" +
        //                "\tnanotimestampv = nanotimestamp\n" +
        //                "\t//stringv = string\n" +
        //                "\tdatehourv = datehour\n" +
        //                "\tuuidv = uuid\n" +
        //                "\tippaddrv = ippaddr\n" +
        //                "\tint128v = int128\n" +
        //                "\t//blobv = blob\n" +
        //                "\tpointv = point\n" +
        //                "\tcomplexv = complex\n" +
        //                "\tdecimal32v = decimal32\n" +
        //                "\tdecimal64v = decimal64\n" +
        //                "\tdecimal128v = decimal128\n" +
        //                "  \t}\n" +
        //                "}   \n" +
        //                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
        //                "eventType = 'event_all_dateType'\n" +
        //                "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,datehourv,uuidv,ippaddrv,int128v,pointv,complexv,decimal32v,decimal64v,decimal128v';\n" +
        //                "typeV = [BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[],MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], UUID[], IPADDR[], INT128[], POINT[], COMPLEX[], DECIMAL32(3)[], DECIMAL64(8)[], DECIMAL128(10)[]];\n" +
        //                "formV = [VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR];\n" +
        //                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
        //                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
        //                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
        //                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
        //        conn.run(script1);
        //        EventSchema scheme = new EventSchema();
        //        scheme.setEventType("event_all_array_dateType");
        //        scheme.setFieldNames(Arrays.asList("boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv",  "datehourv", "uuidv", "ippaddrv", "int128v", "pointv", "complexv", "decimal32v", "decimal64v", "decimal128v"));
        //        scheme.setFieldTypes(Arrays.asList(DT_BOOL, DT_BYTE, DT_SHORT, DT_INT, DT_LONG, DT_DOUBLE, DT_FLOAT, DT_DATE,DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_UUID, DT_IPADDR, DT_INT128, DT_POINT, DT_COMPLEX, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128));
        //        scheme.setFieldForms(Arrays.asList( DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR, DF_VECTOR));
        //        List<EventSchema> eventSchemas = Collections.singletonList(scheme);
        //        List<String> eventTimeFields = new List<String>();
        //        List<String> commonFields = new List<String>();
        //        EventSender sender = EventSender.createEventSender(eventSchemas, eventTimeFields, commonFields);
        //        sender.connect(conn,"inputTable");
        //
        //        EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
        //        client.subscribe(SERVER, PORT, "intput1", "test1", handler, -1, true, "admin", "123456");
        //
        //        Preparedata_array(100,10);
        //        BasicTable bt = (BasicTable)conn.run("select * from data");
        //        String script2 = "data1=select * from data;\n" +
        //                "for(i in 0..9){\n" +
        //                "\tevent_all_array_dateType1=event_all_array_dateType(data1.row(i)[`cbool], data1.row(i)[`cchar], data1.row(i)[`cshort], data1.row(i)[`cint],data1.row(i)[`clong], data1.row(i)[`cdouble], data1.row(i)[`cfloat], data1.row(i)[`cdate], data1.row(i)[`cmonth], data1.row(i)[`ctime], data1.row(i)[`cminute], data1.row(i)[`csecond], data1.row(i)[`cdatetime], data1.row(i)[`ctimestamp], data1.row(i)[`cnanotime], data1.row(i)[`cnanotimestamp], data1.row(i)[`cdatehour], data1.row(i)[`cuuid], data1.row(i)[`cipaddr], data1.row(i)[`cint128],data1.row(i)[`cpoint], data1.row(i)[`ccomplex], data1.row(i)[`cdecimal32], data1.row(i)[`cdecimal64], data1.row(i)[`cdecimal128])\n" +
        //                "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
        //                "\t}" ;
        //        conn.run(script2);
        //        Thread.Sleep(10000);
        //        for(int i=0;i<bt.rows();i++){
        //            List<IEntity> attributes = new List<IEntity>();
        //            for(int j=0;j<bt.columns();j++){
        //                Entity pt = bt.getColumn(j).get(i);
        //                Console.Out.WriteLine(pt.getDataType());
        //                Console.Out.WriteLine(i + "行， " + j + "列：" + pt.getString());
        //                attributes.Add(pt);
        //            }
        //            sender.sendEvent("event_all_array_dateType",attributes);
        //        }
        //        Thread.Sleep(1000);
        //        BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        //        Assert.AreEqual(10,bt1.rows());
        //        BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
        //        Assert.AreEqual(10,bt2.rows());
        //        checkData(bt,bt2);
        //        Thread.Sleep(10000);
        //        BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
        //        Assert.AreEqual(10,bt3.rows());
        //        checkData(bt,bt3);
        //    }

        [TestMethod]
        public void test_EventClient_all_dateType_vector_no_decimal()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..21);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tboolv :: BOOL VECTOR\n" +
                    "\tcharv :: CHAR VECTOR\n" +
                    "\tshortv :: SHORT VECTOR\n" +
                    "\tintv :: INT VECTOR\n" +
                    "\tlongv :: LONG VECTOR\n" +
                    "\tdoublev :: DOUBLE  VECTOR\n" +
                    "\tfloatv :: FLOAT VECTOR\n" +
                    "\tdatev :: DATE VECTOR\n" +
                    "\tmonthv :: MONTH VECTOR\n" +
                    "\ttimev :: TIME VECTOR\n" +
                    "\tminutev :: MINUTE VECTOR\n" +
                    "\tsecondv :: SECOND VECTOR\n" +
                    "\tdatetimev :: DATETIME VECTOR \n" +
                    "\ttimestampv :: TIMESTAMP VECTOR\n" +
                    "\tnanotimev :: NANOTIME VECTOR\n" +
                    "\tnanotimestampv :: NANOTIMESTAMP VECTOR\n" +
                    "\t//stringv :: STRING VECTOR\n" +
                    "\tdatehourv :: DATEHOUR VECTOR\n" +
                    "\tuuidv :: UUID VECTOR\n" +
                    "\tippaddrv :: IPADDR  VECTOR\n" +
                    "\tint128v :: INT128 VECTOR\n" +
                    //"\t//blobv :: BLOB VECTOR\n" +
                    //"\tpointv :: POINT VECTOR\n" +
                    "\tcomplexv :: COMPLEX VECTOR\n" +
                    "  def event_all_array_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, datehour, uuid, ippaddr, int128, complex){\n" +
                    "\tboolv = bool\n" +
                    "\tcharv = char\n" +
                    "\tshortv = short\n" +
                    "\tintv = int\n" +
                    "\tlongv = long\n" +
                    "\tdoublev = double\n" +
                    "\tfloatv = float\n" +
                    "\tdatev = date\n" +
                    "\tmonthv = month\n" +
                    "\ttimev = time\n" +
                    "\tminutev = minute\n" +
                    "\tsecondv = second\n" +
                    "\tdatetimev = datetime\n" +
                    "\ttimestampv = timestamp\n" +
                    "\tnanotimev = nanotime\n" +
                    "\tnanotimestampv = nanotimestamp\n" +
                    "\t//stringv = string\n" +
                    "\tdatehourv = datehour\n" +
                    "\tuuidv = uuid\n" +
                    "\tippaddrv = ippaddr\n" +
                    "\tint128v = int128\n" +
                    //"\t//blobv = blob\n" +
                    //"\tpointv = point\n" +
                    "\tcomplexv = complex\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,datehourv,uuidv,ippaddrv,int128v,complexv';\n" +
                    //"typeV = [BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[],MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], UUID[], IPADDR[], INT128[]];\n" +
                    "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, UUID, IPADDR, INT128, COMPLEX];\n" +

                    "formV = [VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR ];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v", "complexv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_COMPLEX }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR});
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            Handler_array handler_Array = new Handler_array();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler_Array, -1, true, "admin", "123456");

            Preparedata_array(100, 10);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            Console.WriteLine(bt.getColumn("ccomplex").getString());
            String script2 = "data1=select * from data;\n" +
                    "for(i in 0..9){\n" +
                    "\tevent_all_array_dateType1=event_all_array_dateType(data1.row(i)[`cbool], data1.row(i)[`cchar], data1.row(i)[`cshort], data1.row(i)[`cint],data1.row(i)[`clong], data1.row(i)[`cdouble], data1.row(i)[`cfloat], data1.row(i)[`cdate], data1.row(i)[`cmonth], data1.row(i)[`ctime], data1.row(i)[`cminute], data1.row(i)[`csecond], data1.row(i)[`cdatetime], data1.row(i)[`ctimestamp], data1.row(i)[`cnanotime], data1.row(i)[`cnanotimestamp], data1.row(i)[`cdatehour], data1.row(i)[`cuuid], data1.row(i)[`cipaddr], data1.row(i)[`cint128], data1.row(i)[`ccomplex])\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
                    "\t}";
            conn.run(script2);
            Thread.Sleep(5000);
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    Console.Out.WriteLine(pt.getDataType());
                    //Console.Out.WriteLine(i + "行， " + j + "列：" + pt.getString());
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_array_dateType", attributes);
            }
            Thread.Sleep(1000);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(10, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(10, bt2.rows());
            checkData(bt1, bt2);
            Thread.Sleep(10000);
            BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(10, bt3.rows());
            checkData(bt, bt3);
        }

        [TestMethod]
        public void test_EventClient_all_dateType_vector_no_decimal_1()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..21);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tboolv :: BOOL VECTOR\n" +
                    "\tcharv :: CHAR VECTOR\n" +
                    "\tshortv :: SHORT VECTOR\n" +
                    "\tintv :: INT VECTOR\n" +
                    "\tlongv :: LONG VECTOR\n" +
                    "\tdoublev :: DOUBLE  VECTOR\n" +
                    "\tfloatv :: FLOAT VECTOR\n" +
                    "\tdatev :: DATE VECTOR\n" +
                    "\tmonthv :: MONTH VECTOR\n" +
                    "\ttimev :: TIME VECTOR\n" +
                    "\tminutev :: MINUTE VECTOR\n" +
                    "\tsecondv :: SECOND VECTOR\n" +
                    "\tdatetimev :: DATETIME VECTOR \n" +
                    "\ttimestampv :: TIMESTAMP VECTOR\n" +
                    "\tnanotimev :: NANOTIME VECTOR\n" +
                    "\tnanotimestampv :: NANOTIMESTAMP VECTOR\n" +
                    "\t//stringv :: STRING VECTOR\n" +
                    "\tdatehourv :: DATEHOUR VECTOR\n" +
                    "\tuuidv :: UUID VECTOR\n" +
                    "\tippaddrv :: IPADDR  VECTOR\n" +
                    "\tint128v :: INT128 VECTOR\n" +
                    //"\t//blobv :: BLOB VECTOR\n" +
                    //"\tpointv :: POINT VECTOR\n" +
                    "\tcomplexv :: COMPLEX VECTOR\n" +
                    "  def event_all_array_dateType(bool, char, short, int, long, double, float, date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, datehour, uuid, ippaddr, int128, complex){\n" +
                    "\tboolv = bool\n" +
                    "\tcharv = char\n" +
                    "\tshortv = short\n" +
                    "\tintv = int\n" +
                    "\tlongv = long\n" +
                    "\tdoublev = double\n" +
                    "\tfloatv = float\n" +
                    "\tdatev = date\n" +
                    "\tmonthv = month\n" +
                    "\ttimev = time\n" +
                    "\tminutev = minute\n" +
                    "\tsecondv = second\n" +
                    "\tdatetimev = datetime\n" +
                    "\ttimestampv = timestamp\n" +
                    "\tnanotimev = nanotime\n" +
                    "\tnanotimestampv = nanotimestamp\n" +
                    "\t//stringv = string\n" +
                    "\tdatehourv = datehour\n" +
                    "\tuuidv = uuid\n" +
                    "\tippaddrv = ippaddr\n" +
                    "\tint128v = int128\n" +
                    //"\t//blobv = blob\n" +
                    //"\tpointv = point\n" +
                    "\tcomplexv = complex\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'boolv,charv,shortv,intv,longv,doublev,floatv,datev,monthv,timev,minutev,secondv,datetimev,timestampv,nanotimev,nanotimestampv,datehourv,uuidv,ippaddrv,int128v,complexv';\n" +
                    //"typeV = [BOOL[], CHAR[], SHORT[], INT[], LONG[], DOUBLE[], FLOAT[], DATE[],MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], UUID[], IPADDR[], INT128[]];\n" +
                    "typeV = [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE,MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, UUID, IPADDR, INT128,COMPLEX];\n" +

                    "formV = [VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR, VECTOR , VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v", "complexv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_COMPLEX }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);

            Handler_array handler_Array = new Handler_array();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler_Array, 0, true, "admin", "123456");

            Preparedata_array(5000, 500);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            String script2 = "data1=select * from data;\n" +
                    "for(i in 0..9){\n" +
                    "\tevent_all_array_dateType1=event_all_array_dateType(data1.row(i)[`cbool], data1.row(i)[`cchar], data1.row(i)[`cshort], data1.row(i)[`cint],data1.row(i)[`clong], data1.row(i)[`cdouble], data1.row(i)[`cfloat], data1.row(i)[`cdate], data1.row(i)[`cmonth], data1.row(i)[`ctime], data1.row(i)[`cminute], data1.row(i)[`csecond], data1.row(i)[`cdatetime], data1.row(i)[`ctimestamp], data1.row(i)[`cnanotime], data1.row(i)[`cnanotimestamp], data1.row(i)[`cdatehour], data1.row(i)[`cuuid], data1.row(i)[`cipaddr], data1.row(i)[`cint128], data1.row(i)[`ccomplex])\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
                    "\t}";
            conn.run(script2);
           // Thread.Sleep(5000);
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                   // Console.Out.WriteLine(pt.getDataType());
                    //Console.Out.WriteLine(i + "行， " + j + "列：" + pt.getString());
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_array_dateType", attributes);
            }
            //Thread.Sleep(1000);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(10, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(10, bt2.rows());
            checkData(bt1, bt2);
            Thread.Sleep(10000);
            BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(10, bt3.rows());
            checkData(bt, bt3);
        }


        [TestMethod]
        public void test_EventClient_all_dateType_vector_decimal()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..3);\n" +
                    "colTypes=[DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tdecimal32v :: DECIMAL32(2)  VECTOR\n" +
                    "\tdecimal64v :: DECIMAL64(7) VECTOR\n" +
                    "\tdecimal128v :: DECIMAL128(19) VECTOR \n" +
                    "  def event_all_array_dateType(decimal32, decimal64, decimal128){\n" +
                    "\tdecimal32v = decimal32\n" +
                    "\tdecimal64v = decimal64\n" +
                    "\tdecimal128v = decimal128\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'decimal32v,decimal64v,decimal128v';\n" +
                    "typeV = [ DECIMAL32(2)[], DECIMAL64(7)[], DECIMAL128(19)[]];\n" +
                    "formV = [ VECTOR, VECTOR, VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "decimal32v", "decimal64v", "decimal128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR }, new List<int> { 2, 7, 19 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();

            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler_array_decimal handler_array_decimal = new Handler_array_decimal();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler_array_decimal, -1, true, "admin", "123456");

            Preparedata_array_decimal(100, 10);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            String script2 = "data1=select * from data;\n" +
                    "for(i in 0..9){\n" +
                    "\tevent_all_array_dateType1=event_all_array_dateType( data1.row(i)[`cdecimal32], data1.row(i)[`cdecimal64], data1.row(i)[`cdecimal128])\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n" +
                    "\t}";
            conn.run(script2);
            Thread.Sleep(5000);
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    Console.Out.WriteLine(pt.getDataType());
                    Console.Out.WriteLine(i + "行， " + j + "列：" + pt.getString());
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_array_dateType", attributes);
            }
            Thread.Sleep(1000);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(10, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(10, bt2.rows());
            checkData(bt1, bt2);
            Thread.Sleep(10000);
            BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(10, bt3.rows());
            checkData(bt, bt3);
        }

        [TestMethod]
        public void test_EventClient_vector_string()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[STRING]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_string{\n" +
                    "\tstringv :: STRING  VECTOR\n" +
                    "  def event_string(string){\n" +
                    "\tstringv = string\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_string'\n" +
                    "eventKeys = 'stringv';\n" +
                    "typeV = [ STRING];\n" +
                    "formV = [ VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_string", new List<string> { "stringv" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR }, new List<int> { 2 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler, -1, true, "admin", "123456");

            String script2 = "\tevent_string1=event_string( [\"111\",\"222\",\"\",NULL])\n" +
                    "\tappendEvent(inputSerializer, event_string1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicStringVector(new String[] { "111", "222", "", "" }));
            sender.sendEvent("event_string", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
            client.unsubscribe(SERVER, PORT, "intput1", "test1");
        }

        [TestMethod]
        public void test_EventClient_vector_symbol()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                "share table(1:0,[\"col1\"],[STRING]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_symbol{\n" +
                "\tsymbolv :: SYMBOL  VECTOR\n" +
                "  def event_symbol(symbol){\n" +
                "\tsymbolv = symbol\n" +
                "  \t}\n" +
                "}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_symbol'\n" +
                "eventKeys = 'symbolv';\n" +
                "typeV = [ SYMBOL];\n" +
                "formV = [ VECTOR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_symbol", new List<string> { "symbolv" }, new List<DATA_TYPE> { DATA_TYPE.DT_SYMBOL }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler, -1, true, "admin", "123456");

            String script2 = "\tevent_symbol1=event_symbol( symbol([\"111\",\"111\",\"111\",\"111\",\"222\",\"\",NULL]))\n" +
                "\tappendEvent(inputSerializer, event_symbol1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicSymbolVector(new String[] { "111", "111", "111", "111", "222", "", "" }));
            sender.sendEvent("event_symbol", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
            client.unsubscribe(SERVER, PORT, "intput1", "test1");
        }

        [TestMethod]
        public void test_EventClient_vector_complex()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[COMPLEX]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_complex{\n" +
                    "\tcomplexv :: COMPLEX  VECTOR\n" +
                    "  def event_complex(complex){\n" +
                    "\tcomplexv = complex\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_complex'\n" +
                    "eventKeys = 'complexv';\n" +
                    "typeV = [ COMPLEX];\n" +
                    "formV = [ VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);
            EventSchema scheme = new EventSchema("event_complex", new List<string> { "complexv" }, new List<DATA_TYPE> { DATA_TYPE.DT_COMPLEX }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR }, new List<int> { 2 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "intput1", "test1", handler, -1, true, "admin", "123456");

            String script2 = "\tevent_complex1=event_complex( [complex(-1.44,0.33),complex(1.44,-0.33),complex(0,0),NULL])\n" +
                    "\tappendEvent(inputSerializer, event_complex1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicComplexVector(new Double2[] { new Double2(-1.44, 0.33), new Double2(1.44, -0.33), new Double2(0, 0), new Double2(double.MinValue, double.MinValue) }));
            sender.sendEvent("event_complex", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
            client.unsubscribe(SERVER, PORT, "intput1", "test1");
        }

        [TestMethod]
        public void test_EventClient_all_dateType_array()
        {
            String script0 = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..21);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[], DATEHOUR[],UUID[],IPADDR[],INT128[],COMPLEX[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script0);


            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v", "complexv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL_ARRAY, DATA_TYPE.DT_BYTE_ARRAY, DATA_TYPE.DT_SHORT_ARRAY, DATA_TYPE.DT_INT_ARRAY, DATA_TYPE.DT_LONG_ARRAY, DATA_TYPE.DT_DOUBLE_ARRAY, DATA_TYPE.DT_FLOAT_ARRAY, DATA_TYPE.DT_DATE_ARRAY, DATA_TYPE.DT_MONTH_ARRAY, DATA_TYPE.DT_TIME_ARRAY, DATA_TYPE.DT_MINUTE_ARRAY, DATA_TYPE.DT_SECOND_ARRAY, DATA_TYPE.DT_DATETIME_ARRAY, DATA_TYPE.DT_TIMESTAMP_ARRAY, DATA_TYPE.DT_NANOTIME_ARRAY, DATA_TYPE.DT_NANOTIMESTAMP_ARRAY, DATA_TYPE.DT_DATEHOUR_ARRAY, DATA_TYPE.DT_UUID_ARRAY, DATA_TYPE.DT_IPADDR_ARRAY, DATA_TYPE.DT_INT128_ARRAY, DATA_TYPE.DT_COMPLEX_ARRAY }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
            conn.run(script);
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler_array handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler_array, -1, true, "admin", "123456");
            Preparedata_array(100, 10);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            List<IEntity> attributes = new List<IEntity>();
            for (int j = 0; j < bt.columns(); j++)
            {
                IEntity pt = (bt.getColumn(j));
                Console.Out.WriteLine(pt.getDataType());
                attributes.Add(pt);
            }
            sender.sendEvent("event_all_array_dateType", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(2000);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        //[TestMethod]
        public void test_EventClient_reconnect()
        {
            String script = "share streamTable(1:0, `timestamp`time`commonKey, [TIMESTAMP,TIME,TIMESTAMP]) as outputTable;\n" +
                    "share streamTable(1:0, `string`timestamp`commonKey, [STRING,TIMESTAMP,TIMESTAMP]) as outputTable1;\n" +
                    "class MarketData{\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "time :: TIME\n" +
                    "def MarketData(t,t1){\n" +
                    "timestamp = t\n" +
                    "time = t1\n" +
                    "}\n" +
                    "}\n" +
                    "class MarketData1{\n" +
                    "string :: STRING\n" +
                    "timestamp :: TIMESTAMP\n" +
                    "def MarketData1(s,t){\n" +
                    "string = s\n" +
                    "timestamp = t\n" +
                    "}\n" +
                    "}\n" +
                    "share streamTable(array(TIMESTAMP, 0) as timestamp, array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(TIMESTAMP, 0) as commonKey) as intput\n" +
                    "schema = table(1:0, `eventType`eventKeys`eventValuesTypeString`eventValueTypeID`eventValuesFormID, [STRING, STRING, STRING, INT[], INT[]])\n" +
                    "insert into schema values(\"MarketData\", \"timestamp,time\", \"TIMESTAMP,TIME\", [12 8], [0 0])\n" +
                    "insert into schema values(\"MarketData1\", \"string,timestamp\", \"STRING,TIMESTAMP\", [18 12], [0 0])\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schema, outputTable=intput, eventTimeField = \"timestamp\", commonField = \"timestamp\")\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("MarketData", new List<string> { "timestamp", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("MarketData1", new List<string> { "string", "timestamp" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);

            List<String> eventTimeFields = new List<string>() { "time", "timestamp" };
            List<String> commonFields = new List<string>() { "timestamp" };

            EventClient client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler1 handler1 = new Handler1();
            client.subscribe(SERVER, PORT, "intput", "test1", handler1, -1, true, "admin", "123456");
            conn.run("marketData1 = MarketData(now(),time(1));\n marketData2 = MarketData1(\"tesrtttt\",now());\n appendEvent(inputSerializer, [marketData1,marketData2])");
            Thread.Sleep(1000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable");
            BasicTable re1 = (BasicTable)conn.run("select * from outputTable1");
            BasicTable re2 = (BasicTable)conn.run("select timestamp from intput");

            Assert.AreEqual(1, re.rows());
            Assert.AreEqual(1, re1.rows());
            Assert.AreEqual(re2.getColumn(0).get(0).getString(), re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:00:00.001", re.getColumn(1).get(0).getString());
            Assert.AreEqual("tesrtttt", re1.getColumn(0).get(0).getString());
            Assert.AreEqual(re2.getColumn(0).get(0).getString(), re1.getColumn(1).get(0).getString());
            client.unsubscribe(SERVER, PORT, "intput", "test1");
        }




    }
}

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using dolphindb_config;
using dolphindb.streaming.cep;
using System.Threading;
using System.Text;

namespace dolphindb_csharp_api_test.cep_test
{
    [TestClass]
    public class EventSenderTest
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
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
        }

        [TestCleanup]
        public void TestCleanup()
        {
            conn.close();
            try { client.unsubscribe(SERVER, PORT, "inputTable", "test1"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "intput", "test1"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "inputTable", "javaStreamingApi"); } catch (Exception ) { }
            try { client.unsubscribe(SERVER, PORT, "intput", "javaStreamingApi"); } catch (Exception ) { }
            clear_env();

        }
        public void PrepareInputSerializer(String type, DATA_TYPE data_type)
        {
            String script = "login(`admin, `123456); \n" +
                "class event_dateType{\n" +
                "\tt_type :: " + type + "\n" +
                "  def event_dateType(type){\n" +
                "\tt_type = type\n" +
                "                }\n" +
                "\t}   \n" +
                "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                "eventType = 'event_dateType'\n" +
                "eventKeys = 't_type';\n" +
                "typeV = [" + type + "];\n" +
                "formV = [SCALAR];\n" +
                "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                "share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(" + type + ", 0) as commonField) as intput;\n" +
                "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, commonField=\"t_type\");\n" +
                "share streamTable(1000000:0, `eventType`blobs`commonField, [STRING,BLOB," + type + "]) as inputTable;";
            conn.run(script);
            scheme = new EventSchema("event_dateType", new List<string> { "t_type" }, new List<DATA_TYPE> { data_type }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR });

            List<EventSchema> eventSchemas = new List<EventSchema> { scheme };
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string> { "t_type" };
            sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
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
                "share table(cbool, cchar, cshort, cint, clong, cdouble, cfloat, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cdatehour, cuuid, cipaddr, cint128) as data;\n";
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
                //Console.Out.WriteLine("eventType: " + eventType);
                //for (int i = 0; i < attribute.Count; i++)
                //{
                //    Console.Out.WriteLine(attribute[i].getString());
                //}

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

        class Handler_array : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attributes)
            {
                Console.WriteLine("eventType: " + eventType);
                //for (int i = 0; i < attributes.Count; i++)
                //{
                //    //attributes[i].getString();
                //    Console.WriteLine(attributes[i].getString());
                //}
                var cols = new List<IVector>() { };
                var colNames = new List<String>() { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v" };
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

                BasicTable bt = new BasicTable(colNames, cols);
                //Console.WriteLine("-------------" + bt.getString());
                var variable = new Dictionary<string, IEntity>();
                variable.Add("table_tmp", bt);
                conn.upload(variable);
                conn.run("outputTable.tableInsert(table_tmp)");
            }
        };

        class Handler_array_null : EventMessageHandler
        {

            public void doEvent(String eventType, List<IEntity> attributes)
            {
                Console.WriteLine("eventType: " + eventType);
                String boolv = attributes[0].getString().Replace("[]", " ");
                String charv = attributes[1].getString().Replace("[]", " "); 
                String shortv = attributes[2].getString().Replace("[]", " "); 
                String intv = attributes[3].getString().Replace("[]", " "); 
                String longv = attributes[4].getString().Replace("[]", " "); 
                String doublev = attributes[5].getString().Replace("[]", " "); 
                String floatv = attributes[6].getString().Replace("[]", " "); 
                String datev = attributes[7].getString().Replace("[]", " ");
                String monthv = attributes[8].getString().Replace("[]", " ");
                String timev = attributes[9].getString().Replace("[]", " ");
                String minutev = attributes[10].getString().Replace("[]", " ");
                String secondv = attributes[11].getString().Replace("[]", " ");
                String datetimev = attributes[12].getString().Replace("[]", " ");
                String timestampv = attributes[13].getString().Replace("[]", " ");
                String nanotimev = attributes[14].getString().Replace("[]", " ");
                String nanotimestampv = attributes[15].getString().Replace("[]", " ");
                String datehourv = attributes[16].getString().Replace("[]", " ");
                String uuidv = attributes[17].getString().Replace("[]", " ");
                String ippaddrv = attributes[18].getString().Replace("[]", " ");
                String int128v = attributes[19].getString().Replace("[]", " ");
                String decimal32v = attributes[20].getString().Replace("[]", " ");
                String decimal64v = attributes[21].getString().Replace("[]", " ");
                String decimal128v = attributes[22].getString().Replace("[]", " ");
                for (int i = 0; i < attributes.Count; i++)
                {
                    Console.WriteLine(attributes[i].getString());
                }
                String script = null;
                //script = String.Format("insert into outputTable values( {0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},[datehour({0})],[uuid({0})],[ipaddr({0})],[int128({0})],[point({0})],[complex({0})],{0},{0},{0})", boolv, charv, shortv, intv, longv, doublev, floatv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, datehourv, uuidv, ippaddrv, int128v, pointv, complexv, decimal32v, decimal64v, decimal128v);
                script = String.Format("insert into outputTable values( {0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22})", boolv, charv, shortv, intv, longv, doublev, floatv, datev, monthv, timev, minutev, secondv, datetimev, timestampv, nanotimev, nanotimestampv, datehourv, uuidv, ippaddrv, int128v, decimal32v, decimal64v, decimal128v);
                Console.WriteLine(script);

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
        public void test_EventSender_EventScheme_null()
        {
            List<EventSchema> eventSchemas = new List<EventSchema>();
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventSchemas must not be empty.", re);
        }

        [TestMethod]
        public void test_EventSender_EventScheme_null_1()
        {
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", null, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventSchema must be non-null.", re);
        }

        [TestMethod]
        public void test_EventSender_EventType_null()
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
        public void test_EventSender_EventType_null_1()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema(null, new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });

            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventType must be non-null.", re);
        }

        [TestMethod]
        public void test_EventSender_EventType_special_character()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 ", new List<string> { "market" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema> { scheme };
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 "));
            List<IEntity> attributes2 = new List<IEntity>();
            BasicString bb = new BasicString("");
            bb.setNull();
            attributes2.Add(bb);
            sender.sendEvent("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 ", attributes1);
            sender.sendEvent("!@#$%&*()_+QWERTYUIOP{}[]-=';./,~`1^;中文 ", attributes2);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(2, bt1.rows());
        }

        [TestMethod]
        public void test_EventSender_EventType_repetition()
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
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("EventType must be unique.", re);
        }

        [TestMethod]
        public void test_EventSender_fieldNames_null()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", null, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("fieldNames must be non-null.", re);
        }

        [TestMethod]
        public void test_EventSender_fieldNames_null_1()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", new List<string> { "", "market", "time3", "time2", "time4" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("the element of fieldNames must be non-empty.", re);
        }

        [TestMethod]
        public void test_EventSender_fieldName_repetition()
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
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("fieldNames must be unique.", re);
        }

        [TestMethod]
        public void test_EventSender_fieldName_one_colume()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "time" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicTime(1));
            sender.sendEvent("market", attributes);
            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("00:00:00.001", re.getColumn(0).get(0).getString());
        }

        [TestMethod]
        public void test_EventSender_FieldTypes_null()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, null, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("fieldTypes must be non-null.", re);
        }

        [TestMethod]
        public void test_EventSender_FieldForms_null()
        {
            String re = null;
            try
            {
                EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_VOID, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, null);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("fieldForms must be non-null.", re);
        }

        //[TestMethod]
        //public void test_EventSender_fieldExtraParams_null()
        //{
        //    conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        //    EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_PAIR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 0, 0, 0, 1, 2 });
        //    List<EventSchema> eventSchemas = new List<EventSchema>();
        //    eventSchemas.Add(scheme);
        //    List<String> eventTimeFields = new List<string>();
        //    List<String> commonFields = new List<string>() { "market", "code", "decimal32", "decimal64", "decimal128" };

        //    EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
        //    List<IEntity> attributes = new List<IEntity>();
        //    attributes.Add(new BasicString("1"));
        //    attributes.Add(new BasicString("2"));
        //    attributes.Add(new BasicDecimal32("2", 0));
        //    attributes.Add(new BasicDecimal64("2.88", 1));
        //    attributes.Add(new BasicDecimal128("-2.1", 2));
        //    String re = null;
        //    try
        //    {
        //        sender.sendEvent("market", attributes);

        //    }
        //    catch (Exception ex)
        //    {
        //        re = ex.Message;
        //    }
        //    Assert.AreEqual("The decimal attribute' scale doesn't match to schema fieldExtraParams scale.", re);
        //}

        //[TestMethod]
        //public void test_EventSender_fieldExtraParams_null_1()
        //{
        //    conn.run("share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;");
        //    EventSchema scheme = new EventSchema("market", new List<string> { "market", "code" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_PAIR, DATA_FORM.DF_SCALAR });
        //    List<EventSchema> eventSchemas = new List<EventSchema>();
        //    eventSchemas.Add(scheme);
        //    List<String> eventTimeFields = new List<string>();
        //    List<String> commonFields = new List<string>();

        //    EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
        //    List<IEntity> attributes = new List<IEntity>();
        //    attributes.Add(new BasicString("1"));
        //    attributes.Add(new BasicString("2"));
        //    sender.sendEvent("market", attributes);
        //    BasicTable re = (BasicTable)conn.run("select * from inputTable");
        //    Assert.AreEqual(1, re.rows());
        //}
        //[TestMethod]
        //public void test_EventSender_fieldExtraParams_not_match()
        //{
        //    conn.run("share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;");
        //    EventSchema scheme = new EventSchema("market", new List<string> { "market", "code" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_PAIR, DATA_FORM.DF_SCALAR }, new List<int> { 1 });
        //    List<EventSchema> eventSchemas = new List<EventSchema>();
        //    eventSchemas.Add(scheme);
        //    List<String> eventTimeFields = new List<string>();
        //    List<String> commonFields = new List<string>();

        //    String re = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
        //    }
        //    catch (Exception ex)
        //    {
        //        re = ex.Message;
        //    }
        //    Assert.AreEqual("the number of eventKey, eventTypes, eventForms and eventExtraParams (if set) must have the same length.", re);
        //}
        //[TestMethod]
        //public void test_EventSender_fieldExtraParams_set_not_true()
        //{
        //    EventSchema scheme = new EventSchema("market", new List<string> {  "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 10, 19, 39 });
        //    List<EventSchema> eventSchemas = new List<EventSchema>();
        //    eventSchemas.Add(scheme);
        //    List<String> eventTimeFields = new List<string>();
        //    List<String> commonFields = new List<string>() { "market", "code", "decimal32", "decimal64", "decimal128" };

        //    String re = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL32 scale 10 is out of bounds, it must be in [0,9].", re);

        //    EventSchema scheme1 = new EventSchema("market", new List<string> { "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 1, 19, 39 });
        //    List<EventSchema> eventSchemas1 = new List<EventSchema>();
        //    eventSchemas1.Add(scheme1);
        //    String re1 = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas1, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re1 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL64 scale 19 is out of bounds, it must be in [0,18].", re1);


        //    EventSchema scheme2 = new EventSchema("market", new List<string> { "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 1, 18, 39 });
        //    List<EventSchema> eventSchemas2 = new List<EventSchema>();
        //    eventSchemas2.Add(scheme2);
        //    String re2 = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas2, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re2 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL128 scale 39 is out of bounds, it must be in [0,38].", re2);

        //    EventSchema scheme3 = new EventSchema("market", new List<string> { "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { -1, 10, 10 });
        //    List<EventSchema> eventSchemas3 = new List<EventSchema>();
        //    eventSchemas3.Add(scheme3);
        //    String re3 = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas3, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re3 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL32 scale -1 is out of bounds, it must be in [0,9].", re3);

        //    EventSchema scheme4 = new EventSchema("market", new List<string> { "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 1, -1, 0 });
        //    List<EventSchema> eventSchemas4 = new List<EventSchema>();
        //    eventSchemas4.Add(scheme4);
        //    String re4 = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas4, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re4 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL64 scale -1 is out of bounds, it must be in [0,18].", re4);

        //    EventSchema scheme5 = new EventSchema("market", new List<string> { "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 0, 0, -1 });
        //    List<EventSchema> eventSchemas5 = new List<EventSchema>();
        //    eventSchemas5.Add(scheme5);
        //    String re5 = null;
        //    try
        //    {
        //        EventSender sender = new EventSender(conn, "inputTable", eventSchemas5, eventTimeFields, commonFields);

        //    }
        //    catch (Exception ex)
        //    {
        //        re5 = ex.Message;
        //    }
        //    Assert.AreEqual("DT_DECIMAL128 scale -1 is out of bounds, it must be in [0,38].", re5);
        //}
        //[TestMethod]
        //public void test_EventSender_fieldExtraParams_set_true()
        //{
        //    conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        //    conn.run("share streamTable(1000000:0, `eventType`event`market`code`decimal32`decimal64`decimal128, [STRING,BLOB,STRING,STRING,DECIMAL32(0),DECIMAL64(1),DECIMAL128(2)]) as inputTable;");
        //    EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "decimal32", "decimal64", "decimal128" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_PAIR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 0, 0, 0, 1, 2 });
        //    List<EventSchema> eventSchemas = new List<EventSchema>();
        //    eventSchemas.Add(scheme);
        //    List<String> eventTimeFields = new List<string>();
        //    List<String> commonFields = new List<string>() { "market", "code", "decimal32", "decimal64", "decimal128" };
        //    EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

        //    List<IEntity> attributes = new List<IEntity>();
        //    attributes.Add(new BasicString("1"));
        //    attributes.Add(new BasicString("2"));
        //    attributes.Add(new BasicDecimal32("2", 0));
        //    attributes.Add(new BasicDecimal64("2.88", 1));
        //    attributes.Add(new BasicDecimal128("-2.1", 2));
        //    sender.sendEvent("market", attributes);
        //    BasicTable re = (BasicTable)conn.run("select * from inputTable");
        //    Assert.AreEqual(1, re.rows());
        //    Assert.AreEqual("1", re.getColumn(2).get(0).getString());
        //    Assert.AreEqual("2", re.getColumn(3).get(0).getString());
        //    Assert.AreEqual("2", re.getColumn(4).get(0).getString());
        //    Assert.AreEqual("2.9", re.getColumn(5).get(0).getString());
        //    Assert.AreEqual("-2.10", re.getColumn(6).get(0).getString());
        //}

        [TestMethod]
        public void test_EventSender_eventTimeFields_not_exist()
        {
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "code", "price", "qty", "eventTime" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_INT, DATA_TYPE.DT_TIMESTAMP }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Event market doesn't contain eventTimeKey datetimev.", re);
        }
        [TestMethod]
        public void test_EventSender_eventTimeFields_not_time_column()
        {
            conn.run("share streamTable(1000000:0, `string`eventType`event, [STRING,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "code" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "market" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The first column of the output table must be temporal if eventTimeKey is specified.", re);
        }

        [TestMethod]
        public void test_EventSender_eventTimeFields_one_column()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);


            List<String> eventTimeFields = new List<string>() { "time" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("123456"));
            attributes.Add(new BasicTime(12));
            sender.sendEvent("market", attributes);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicTime(10000));
            attributes1.Add(new BasicTime(123456));
            sender.sendEvent("market1", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(2, re.rows());
            Assert.AreEqual("00:00:00.012", re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:00:10.000", re.getColumn(0).get(1).getString());
        }

        [TestMethod]
        public void test_EventSender_eventTimeFields_one_column_1()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);
            List<String> eventTimeFields = new List<string>() { "time1" };
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Event market doesn't contain eventTimeKey time1.", re);
        }

        [TestMethod]
        public void test_EventSender_eventTimeFields_two_column()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);

            List<String> eventTimeFields = new List<string>() { "time", "time1" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("123456"));
            attributes.Add(new BasicTime(12));
            sender.sendEvent("market", attributes);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicTime(10000));
            attributes1.Add(new BasicTime(123456));
            sender.sendEvent("market1", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(2, re.rows());
            Assert.AreEqual("00:00:00.012", re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:02:03.456", re.getColumn(0).get(1).getString());
        }

        [TestMethod]
        public void test_EventSender_commonFields_not_exist()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);

            List<String> eventTimeFields = new List<string>() { "time", "time1" };
            List<String> commonFields = new List<string>() { "time123" };
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Event market doesn't contain commonField time123.", re);
        }

        [TestMethod]
        public void test_EventSender_commonFields_one_column()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);

            List<String> eventTimeFields = new List<string>() { "time", "time1" };
            List<String> commonFields = new List<string>() { "time" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicString("123456"));
            attributes.Add(new BasicTime(12));
            sender.sendEvent("market", attributes);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicTime(10000));
            attributes1.Add(new BasicTime(123456));
            sender.sendEvent("market1", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(2, re.rows());
            Assert.AreEqual("00:00:00.012", re.getColumn(0).get(0).getString());
            Assert.AreEqual("00:02:03.456", re.getColumn(0).get(1).getString());
            Assert.AreEqual("00:00:00.012", re.getColumn(3).get(0).getString());
            Assert.AreEqual("00:00:10.000", re.getColumn(3).get(1).getString());
        }

        [TestMethod]
        public void test_EventSender_commonFields_one_column_1()
        {
            conn.run("share streamTable(1000000:0, `time`eventType`event`comment, [TIME,STRING,BLOB,TIME]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            EventSchema scheme1 = new EventSchema("market1", new List<string> { "time", "time1" }, new List<DATA_TYPE> { DATA_TYPE.DT_TIME, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            eventSchemas.Add(scheme1);

            List<String> eventTimeFields = new List<string>() { "time", "time1" };
            List<String> commonFields = new List<string>() { "time1" };
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Event market doesn't contain commonField time1.", re);
        }

        [TestMethod]
        public void test_EventSender_commonFields_two_column()
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
            attributes.Add(new BasicString("1234567"));
            attributes.Add(new BasicTime(12));
            sender.sendEvent("market", attributes);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(123456));
            sender.sendEvent("market1", attributes1); ;

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(2, re.rows());
            Assert.AreEqual("00:00:00.012", re.getColumn(2).get(0).getString());
            Assert.AreEqual("00:02:03.456", re.getColumn(2).get(1).getString());
            Assert.AreEqual("1234567", re.getColumn(3).get(0).getString());
            Assert.AreEqual("tesrtrrr", re.getColumn(3).get(1).getString());
        }

        [TestMethod]
        public void test_EventSender_connect_not_connect()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            DBConnection conn1 = new DBConnection();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn1, "inputTable", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Database connection is not established yet.", re);
        }

        [TestMethod]
        public void test_EventSender_connect_duplicated()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            EventSender sender1 = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
        }

        [TestMethod]
        public void test_EventSender_conn_ssl_true()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            DBConnection conn1 = new DBConnection(false, true);
            conn1.connect(SERVER, PORT);
            EventSender sender = new EventSender(conn1, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(1));
            sender.sendEvent("market", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("00:00:00.001", re.getColumn(2).get(0).getString());
            Assert.AreEqual("tesrtrrr", re.getColumn(3).get(0).getString());
        }
        [TestMethod]
        public void test_EventSender_conn_compress_true()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            DBConnection conn1 = new DBConnection(false, false, true);
            conn1.connect(SERVER, PORT);
            EventSender sender = new EventSender(conn1, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(1));
            sender.sendEvent("market", attributes1);

            BasicTable re = (BasicTable)conn.run("select * from inputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("00:00:00.001", re.getColumn(2).get(0).getString());
            Assert.AreEqual("tesrtrrr", re.getColumn(3).get(0).getString());
        }
        [TestMethod]
        public void test_EventSender_conn_not_admin()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            PrepareUser("user1", "123456");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            DBConnection conn1 = new DBConnection(false, false, true);
            conn1.connect(SERVER, PORT, "user1", "123456");
            EventSender sender = new EventSender(conn1, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(1));
            sender.sendEvent("market", attributes1);

            BasicTable re = (BasicTable)conn1.run("select * from inputTable");
            Assert.AreEqual(1, re.rows());
            Assert.AreEqual("00:00:00.001", re.getColumn(2).get(0).getString());
            Assert.AreEqual("tesrtrrr", re.getColumn(3).get(0).getString());
        }

        [TestMethod]
        public void test_EventSender_connect_tableName_not_exist()
        {
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable11", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("Can't find the object with name inputTable11"));
        }

        [TestMethod]
        public void test_EventSender_connect_tableName_null()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, null, eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual(true, re.Contains("tableName must be non-null."));
            String re1 = null;
            try
            {
                EventSender sender1 = new EventSender(conn, "", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re1 = ex.Message;
            }
            Assert.AreEqual(true, re1.Contains("tableName must not be empty."));
        }

        [TestMethod]
        public void test_EventSender_connect_table_cloumn_not_match()
        {
            String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable1;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippaddrv", "int128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            String re = null;
            try
            {
                EventSender sender = new EventSender(conn, "inputTable1", eventSchemas, eventTimeFields, commonFields);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Incompatible table columnns for table inputTable1, expected: 2, got: 3.", re);
        }

        [TestMethod]
        public void test_EventSender_sendEvent_eventType_not_exist()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(1));
            String re = null;
            try
            {
                sender.sendEvent("market111", attributes1);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("unknown eventType market111.", re);
        }
        [TestMethod]
        public void test_EventSender_sendEvent_eventType_null()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicTime(1));
            String re = null;
            try
            {
                sender.sendEvent(null, attributes1);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("eventType must be non-null.", re);
            String re1 = null;
            try
            {
                sender.sendEvent("", attributes1);
            }
            catch (Exception ex)
            {
                re1 = ex.Message;
            }
            Assert.AreEqual("unknown eventType .", re1);
        }

        [TestMethod]
        public void test_EventSender_sendEvent_attributes_column_not_match()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            // attributes1.Add(new BasicTime(1));
            String re = null;
            try
            {
                sender.sendEvent("market", attributes1);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("the number of event values does not match.", re);
        }
        [TestMethod]
        public void test_EventSender_sendEvent_attributes_type_not_match()
        {
            conn.run("share streamTable(1000000:0, `eventType`event`comment1`comment2, [STRING,BLOB,TIME,STRING]) as inputTable;");
            EventSchema scheme = new EventSchema("market", new List<string> { "market", "time" }, new List<DATA_TYPE> { DATA_TYPE.DT_STRING, DATA_TYPE.DT_TIME }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>() { "time", "market" };
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicString("tesrtrrr"));
            attributes1.Add(new BasicInt(1));
            String re = null;
            try
            {
                sender.sendEvent("market", attributes1);
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("Expected type for the field time of market : TIME, but now it is INT.", re);
        }

        [TestMethod]
        public void test_EventSender_sendEvent_attributes_null()
        {
            String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`symbolv`stringv`datehourv`uuidv`ippAddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, SYMBOL,STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("event_all_dateType_null", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "symbolv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_SYMBOL, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");
            List<IEntity> attributes = new List<IEntity>();
            BasicBoolean boolv = new BasicBoolean(true);
            //boolv.setNull();
            BasicByte charv = new BasicByte((byte)1);
            charv.setNull();
            BasicShort shortv = new BasicShort((short)1);
            shortv.setNull();
            BasicInt intv = new BasicInt(0);
            intv.setNull();
            BasicLong longv = new BasicLong(0);
            longv.setNull();
            BasicDouble doublev = new BasicDouble(0);
            doublev.setNull();
            BasicFloat floatv = new BasicFloat(0);
            floatv.setNull();
            BasicDate datev = new BasicDate(0);
            datev.setNull();
            BasicMonth monthv = new BasicMonth(0);
            monthv.setNull();
            BasicTime timev = new BasicTime(0);
            timev.setNull();
            BasicMinute minutev = new BasicMinute(0);
            minutev.setNull();
            BasicSecond secondv = new BasicSecond(0);
            secondv.setNull();
            BasicDateTime datetimev = new BasicDateTime(0);
            datetimev.setNull();
            BasicTimestamp timestampv = new BasicTimestamp(0);
            timestampv.setNull();
            BasicNanoTime nanotimev = new BasicNanoTime(0);
            nanotimev.setNull();
            BasicNanoTimestamp nanotimestampv = new BasicNanoTimestamp(0);
            nanotimestampv.setNull();
            BasicString symbolv = new BasicString("0");
            symbolv.setNull();
            BasicString stringv = new BasicString("0");
            stringv.setNull();
            BasicUuid uuidv = new BasicUuid(1, 1);
            uuidv.setNull();
            BasicDateHour datehourv = new BasicDateHour(0);
            datehourv.setNull();
            BasicIPAddr ippAddrv = new BasicIPAddr(1, 1);
            ippAddrv.setNull();
            BasicInt128 int128v = new BasicInt128(1, 1);
            int128v.setNull();
            BasicString blobv = new BasicString("= new String[0],true", true);
            blobv.setNull();
            attributes.Add(boolv);
            attributes.Add(charv);
            attributes.Add(shortv);
            attributes.Add(intv);
            attributes.Add(longv);
            attributes.Add(doublev);
            attributes.Add(floatv);
            attributes.Add(datev);
            attributes.Add(monthv);
            attributes.Add(timev);
            attributes.Add(minutev);
            attributes.Add(secondv);
            attributes.Add(datetimev);
            attributes.Add(timestampv);
            attributes.Add(nanotimev);
            attributes.Add(nanotimestampv);
            attributes.Add(symbolv);
            attributes.Add(stringv);
            attributes.Add(datehourv);
            attributes.Add(uuidv);
            attributes.Add(ippAddrv);
            attributes.Add(int128v);
            attributes.Add(blobv);
            sender.sendEvent("event_all_dateType_null", attributes);
            //conn.run("tableInsert{outputTable}", attributes);
            Thread.Sleep(2000);
            BasicTable re = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(1, re.rows());
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventClient_subscribe_attributes_vector_null()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..23);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(10)[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);
            BasicTable re1111 = (BasicTable)conn.run("select * from outputTable;");

            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v", "decimal32v", "decimal64v", "decimal128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR },new List<int> { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 7, 10});
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler_array_null handler_array_null = new Handler_array_null();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler_array_null, -1, true, "admin", "123456");

            List<IEntity> attributes = new List<IEntity>(); ;
            attributes.Add(new BasicBooleanVector(0));
            attributes.Add(new BasicByteVector(0));
            attributes.Add(new BasicShortVector(0));
            attributes.Add(new BasicIntVector(0));
            attributes.Add(new BasicLongVector(0));
            attributes.Add(new BasicDoubleVector(0));
            attributes.Add(new BasicFloatVector(0));
            attributes.Add(new BasicDateVector(0));
            attributes.Add(new BasicMonthVector(0));
            attributes.Add(new BasicTimeVector(0));
            attributes.Add(new BasicMinuteVector(0));
            attributes.Add(new BasicSecondVector(0));
            attributes.Add(new BasicDateTimeVector(0));
            attributes.Add(new BasicTimestampVector(0));
            attributes.Add(new BasicNanoTimeVector(0));
            attributes.Add(new BasicNanoTimestampVector(0));
            attributes.Add(new BasicDateHourVector(0));
            attributes.Add(new BasicUuidVector(0));
            attributes.Add(new BasicIPAddrVector(0));
            attributes.Add(new BasicInt128Vector(0));
            attributes.Add(new BasicDecimal32Vector(0,2));
            attributes.Add(new BasicDecimal64Vector(0,7));
            attributes.Add(new BasicDecimal128Vector(0,10));
            sender.sendEvent("event_all_array_dateType", attributes);
            //conn.run("tableInsert{outputTable}", attributes);
            Thread.Sleep(2000);
            BasicTable re = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, re.rows());
            BasicTable re1 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(1, re1.rows());
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }
        [TestMethod]
        public void test_EventClient_subscribe_attributes_array_null()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..24);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[],POINT[],COMPLEX[],DECIMAL32(2)[],DECIMAL64(7)[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);

            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL_ARRAY, DATA_TYPE.DT_BYTE_ARRAY, DATA_TYPE.DT_SHORT_ARRAY, DATA_TYPE.DT_INT_ARRAY, DATA_TYPE.DT_LONG_ARRAY, DATA_TYPE.DT_DOUBLE_ARRAY, DATA_TYPE.DT_FLOAT_ARRAY, DATA_TYPE.DT_DATE_ARRAY, DATA_TYPE.DT_MONTH_ARRAY, DATA_TYPE.DT_TIME_ARRAY, DATA_TYPE.DT_MINUTE_ARRAY, DATA_TYPE.DT_SECOND_ARRAY, DATA_TYPE.DT_DATETIME_ARRAY, DATA_TYPE.DT_TIMESTAMP_ARRAY, DATA_TYPE.DT_NANOTIME_ARRAY, DATA_TYPE.DT_NANOTIMESTAMP_ARRAY, DATA_TYPE.DT_DATEHOUR_ARRAY, DATA_TYPE.DT_UUID_ARRAY, DATA_TYPE.DT_IPADDR_ARRAY, DATA_TYPE.DT_INT128_ARRAY }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_BOOL_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_BYTE_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_SHORT_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_INT_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_LONG_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_DOUBLE_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_FLOAT_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_DATE_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_MONTH_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_TIME_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_MINUTE_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_SECOND_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_DATETIME_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_TIMESTAMP_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIME_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_NANOTIMESTAMP_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_DATEHOUR_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_UUID_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_IPADDR_ARRAY));
            attributes.Add(new BasicArrayVector(DATA_TYPE.DT_INT128_ARRAY));

            sender.sendEvent("event_all_array_dateType", attributes);
            //conn.run("tableInsert{outputTable}", attributes);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");

        }
        [TestMethod]
        public void test_EventSender_all_dateType_scalar()
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
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput, eventTimeField = \"timestampv\");" +
                    "all_data_type1=event_all_dateType(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,   \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\"))\n" +
                    "appendEvent(inputSerializer, all_data_type1)";
            conn.run(script1);
            String script2 = "colNames=\"col\"+string(1..22)\n" +
                    "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DOUBLE,FLOAT,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,STRING,DATEHOUR,UUID,IPADDR,INT128,BLOB]\n" +
                    "t=table(1:0,colNames,colTypes)\n" +
                    "insert into t values(true, 'a', 2h, 2, 22l, 2.1, 2.1f, 2012.12.06, 2012.06M, 12:30:00.008, 12:30m, 12:30:00, 2012.06.12 12:30:00, 2012.06.12 12:30:00.008, 13:30:10.008007006, 2012.06.13 13:30:10.008007006,  \"world\", datehour(2012.06.13 13:30:10), uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"), ipaddr(\"192.168.1.253\"), int128(\"e1671797c52e15f763380b45e841ec32\"), blob(\"123\")) ;";
            conn.run(script2);

            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "timestampv" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            BasicTable bt = (BasicTable)conn.run("select * from t");
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).get(i);
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_dateType", attributes);

                BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
                Assert.AreEqual(1, bt1.rows());
                Thread.Sleep(2000);
                BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
                Assert.AreEqual(1, bt2.rows());
                checkData(bt1, bt2);
            }
        }


        [TestMethod]
        public void test_EventSender_scaler_BOOL()
        {
            PrepareInputSerializer("BOOL", DATA_TYPE.DT_BOOL);
            String script = "event_dateType1 = event_dateType(true);\n" +
                    "event_dateType2 = event_dateType(false);\n" +
                    "event_dateType3 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicBoolean(true));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicBoolean(false));
            List<IEntity> attributes3 = new List<IEntity>();
            BasicBoolean bb = new BasicBoolean(true);
            bb.setNull();
            attributes3.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(3, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(3, bt2.rows());
            checkData(bt1, bt2);

        }

        [TestMethod]
        public void test_EventSender_scaler_CHAR()
        {
            PrepareInputSerializer("CHAR", DATA_TYPE.DT_BYTE);
            String script = "event_dateType1 = event_dateType('1');\n" +
                    "event_dateType2 = event_dateType('2');\n" +
                    "event_dateType3 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicByte((byte)49));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicByte((byte)50));
            List<IEntity> attributes3 = new List<IEntity>();
            BasicByte bb = new BasicByte((byte)1);
            bb.setNull();
            attributes3.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(3, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(3, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_scaler_INT()
        {
            PrepareInputSerializer("INT", DATA_TYPE.DT_INT);
            String script = "event_dateType1 = event_dateType(-2147483648);\n" +
                    "event_dateType2 = event_dateType(2147483647);\n" +
                    "event_dateType3 = event_dateType(0);\n" +
                    "event_dateType4 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicInt(-2147483648));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicInt(2147483647));
            List<IEntity> attributes3 = new List<IEntity>();
            attributes3.Add(new BasicInt(0));
            List<IEntity> attributes4 = new List<IEntity>();
            BasicInt bb = new BasicInt(1);
            bb.setNull();
            attributes4.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            sender.sendEvent("event_dateType", attributes4);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(4, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(4, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_scaler_LONG()
        {
            PrepareInputSerializer("LONG", DATA_TYPE.DT_LONG);
            String script = "event_dateType1 = event_dateType(-9223372036854775808);\n" +
                    "event_dateType2 = event_dateType(9223372036854775807);\n" +
                    "event_dateType3 = event_dateType(0);\n" +
                    "event_dateType4 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicLong(-9223372036854775808L));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicLong(9223372036854775807L));
            List<IEntity> attributes3 = new List<IEntity>();
            attributes3.Add(new BasicLong(0));
            List<IEntity> attributes4 = new List<IEntity>();
            BasicLong bb = new BasicLong(1);
            bb.setNull();
            attributes4.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            sender.sendEvent("event_dateType", attributes4);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(4, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(4, bt2.rows());
            checkData(bt1, bt2);
        }
        [TestMethod]
        public void test_EventSender_scaler_DOUBLE()
        {
            PrepareInputSerializer("DOUBLE", DATA_TYPE.DT_DOUBLE);
            String script = "event_dateType1 = event_dateType(-922337.2036854775808);\n" +
                    "event_dateType2 = event_dateType(92233.72036854775807);\n" +
                    "event_dateType3 = event_dateType(0);\n" +
                    "event_dateType4 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicDouble(-922337.2036854775808));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicDouble(92233.72036854775807));
            List<IEntity> attributes3 = new List<IEntity>();
            attributes3.Add(new BasicDouble(0));
            List<IEntity> attributes4 = new List<IEntity>();
            BasicDouble bb = new BasicDouble(1);
            bb.setNull();
            attributes4.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            sender.sendEvent("event_dateType", attributes4);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(4, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(4, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_scaler_FLOAT()
        {
            PrepareInputSerializer("FLOAT", DATA_TYPE.DT_FLOAT);
            String script = "event_dateType1 = event_dateType(-922337.2036854775808f);\n" +
                    "event_dateType2 = event_dateType(92233.72036854775807f);\n" +
                    "event_dateType3 = event_dateType(0);\n" +
                    "event_dateType4 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicFloat(-922337.2036854775808f));
            List<IEntity> attributes2 = new List<IEntity>();
            attributes2.Add(new BasicFloat(92233.72036854775807f));
            List<IEntity> attributes3 = new List<IEntity>();
            attributes3.Add(new BasicFloat(0));
            List<IEntity> attributes4 = new List<IEntity>();
            BasicFloat bb = new BasicFloat(1);
            bb.setNull();
            attributes4.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            sender.sendEvent("event_dateType", attributes3);
            sender.sendEvent("event_dateType", attributes4);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(4, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(4, bt2.rows());
            checkData(bt1, bt2);
        }
        [TestMethod]
        public void test_EventSender_scaler_UUID()
        {
            PrepareInputSerializer("UUID", DATA_TYPE.DT_UUID);
            String script = "event_dateType1 = event_dateType(uuid(\"00000000-0000-006f-0000-000000000001\"));\n" +
                    "event_dateType2 = event_dateType(NULL);\n" +
                    "appendEvent(inputSerializer, [event_dateType1, event_dateType2]);";
            conn.run(script);
            List<IEntity> attributes1 = new List<IEntity>();
            attributes1.Add(new BasicUuid(111, 1));
            List<IEntity> attributes2 = new List<IEntity>();
            BasicUuid bb = new BasicUuid(0, 0);
            bb.setNull();
            attributes2.Add(bb);
            sender.sendEvent("event_dateType", attributes1);
            sender.sendEvent("event_dateType", attributes2);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(2, bt1.rows());
            Thread.Sleep(2000);
            BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
            Assert.AreEqual(2, bt2.rows());
            checkData(bt1, bt2);
        }

        //[TestMethod]
        //public void test_EventSender_scaler_COMPLEX()
        //{
        //    PrepareInputSerializer("COMPLEX", DATA_TYPE.DT_COMPLEX);
        //    String script = "event_dateType1 = event_dateType(complex(111, 1));\n" +
        //            "event_dateType2 = event_dateType( complex(0, 0));\n" +
        //            "event_dateType3 = event_dateType(complex(-0.99, -0.11));\n" +
        //            "event_dateType4 = event_dateType(NULL);\n" +
        //            "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        //    conn.run(script);
        //    List<IEntity> attributes1 = new List<IEntity>();
        //    attributes1.Add(new BasicComplex(111, 1));
        //    List<IEntity> attributes2 = new List<IEntity>();
        //    attributes2.Add(new BasicComplex(0, 0));
        //    List<IEntity> attributes3 = new List<IEntity>();
        //    attributes3.Add(new BasicComplex(-0.99, -0.11));
        //    List<IEntity> attributes4 = new List<IEntity>();
        //    BasicComplex bb = new BasicComplex(0, 0);
        //    bb.setNull();
        //    System.out.println(bb.getString());
        //    attributes4.Add(bb);
        //    sender.sendEvent("event_dateType", attributes1);
        //    sender.sendEvent("event_dateType", attributes2);
        //    sender.sendEvent("event_dateType", attributes3);
        //    sender.sendEvent("event_dateType", attributes4);
        //    BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        //    Assert.AreEqual(4, bt1.rows());
        //    Thread.Sleep(2000);
        //    BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
        //    Assert.AreEqual(4, bt2.rows());
        //    checkData(bt1, bt2);
        //}
        //[TestMethod]
        //public void test_EventSender_scaler_POINT()
        //{
        //    PrepareInputSerializer("POINT", DATA_TYPE.DT_POINT);
        //    String script = "event_dateType1 = event_dateType(point(111, 1));\n" +
        //            "event_dateType2 = event_dateType( point(0, 0));\n" +
        //            "event_dateType3 = event_dateType(point(-0.99, -0.11));\n" +
        //            "event_dateType4 = event_dateType(NULL);\n" +
        //            "appendEvent(inputSerializer, [event_dateType1, event_dateType2, event_dateType3, event_dateType4]);";
        //    conn.run(script);
        //    List<IEntity> attributes1 = new List<IEntity>();
        //    attributes1.Add(new BasicPoint(111, 1));
        //    List<IEntity> attributes2 = new List<IEntity>();
        //    attributes2.Add(new BasicPoint(0, 0));
        //    List<IEntity> attributes3 = new List<IEntity>();
        //    attributes3.Add(new BasicPoint(-0.99, -0.11));
        //    List<IEntity> attributes4 = new List<IEntity>();
        //    BasicPoint bb = new BasicPoint(0, 0);
        //    bb.setNull();
        //    attributes4.Add(bb);
        //    sender.sendEvent("event_dateType", attributes1);
        //    sender.sendEvent("event_dateType", attributes2);
        //    sender.sendEvent("event_dateType", attributes3);
        //    sender.sendEvent("event_dateType", attributes4);
        //    BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
        //    Assert.AreEqual(4, bt1.rows());
        //    Thread.Sleep(2000);
        //    BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
        //    Assert.AreEqual(4, bt2.rows());
        //    checkData(bt1, bt2);
        //}



        [TestMethod]
        public void test_EventSender_all_dateType_scalar_DECIMAL()
        {
            String script = "share streamTable(1:0, `eventType`blobs, [STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `decimal32v`decimal64v`decimal128v, [DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_dateType{\n" +
                    "\tdecimal32v :: DECIMAL32(3)\n" +
                    "\tdecimal64v :: DECIMAL64(8)\n" +
                    "\tdecimal128v :: DECIMAL128(10) \n" +
                    "  def event_all_dateType(decimal32, decimal64, decimal128){\n" +
                    "\tdecimal32v = decimal32\n" +
                    "\tdecimal64v = decimal64\n" +
                    "\tdecimal128v = decimal128\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_dateType'\n" +
                    "eventKeys = 'decimal32v,decimal64v,decimal128v';\n" +
                    "typeV = [ DECIMAL32(3), DECIMAL64(8), DECIMAL128(10)];\n" +
                    "formV = [ SCALAR, SCALAR, SCALAR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput);" +
                    "all_data_type1=event_all_dateType(decimal32(1.1, 3),decimal64(1.1, 8),decimal128(1.1, 10))\n" +
                    "appendEvent(inputSerializer, all_data_type1)";
            conn.run(script1);
            String script2 = "colNames=\"col\"+string(1..3)\n" +
                    "colTypes=[DECIMAL32(3),DECIMAL64(8),DECIMAL128(10)]\n" +
                    "t=table(1:0,colNames,colTypes)\n" +
                    "insert into t values(decimal32(1.1, 3), decimal64(1.1, 8), decimal128(1.1, 10)) ;";
            conn.run(script2);

            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "decimal32v", "decimal64v", "decimal128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32, DATA_TYPE.DT_DECIMAL64, DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR }, new List<int> { 3, 8, 10 });

            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<String>();
            List<String> commonFields = new List<String>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            BasicTable bt = (BasicTable)conn.run("select * from t");
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).get(i);
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_dateType", attributes);

                BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
                Assert.AreEqual(1, bt1.rows());
                Thread.Sleep(2000);
                BasicTable bt2 = (BasicTable)conn.run("select * from intput;");
                Assert.AreEqual(1, bt2.rows());
                checkData(bt1, bt2);
                BasicTable bt3 = (BasicTable)conn.run("select * from outputTable;");
                Assert.AreEqual(1, bt3.rows());
                Console.WriteLine(bt3.getString());
                client.unsubscribe(SERVER, PORT, "inputTable", "test1");
            }
        }

        [TestMethod]
        public void test_EventSender_subscribe_all_dateType_scalar_1()
        {
            String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);

            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            Preparedata(1);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_dateType", attributes);
            }
            Console.WriteLine(bt.columns());
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            Thread.Sleep(20000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt, bt2);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventSender_subscribe_all_dateType_scalar_100()
        {
            String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);

            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            Preparedata(100);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_dateType", attributes);
            }
            Console.WriteLine(bt.columns());
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(100, bt1.rows());
            Thread.Sleep(20000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(100, bt2.rows());
            checkData(bt, bt2);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventSender_subscribe_all_dateType_scalar_100000()
        {
            String script = "share streamTable(1000000:0, `time`eventType`event, [TIMESTAMP,STRING,BLOB]) as inputTable;\n" +
                    "share table(100:0, `boolv`charv`shortv`intv`longv`doublev`floatv`datev`monthv`timev`minutev`secondv`datetimev`timestampv`nanotimev`nanotimestampv`stringv`datehourv`uuidv`ippaddrv`int128v`blobv, [BOOL, CHAR, SHORT, INT, LONG, DOUBLE, FLOAT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP,  STRING, DATEHOUR, UUID, IPADDR, INT128, BLOB]) as outputTable;\n";
            conn.run(script);

            EventSchema scheme = new EventSchema("event_all_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "stringv", "datehourv", "uuidv", "ippaddrv", "int128v", "blobv" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_STRING, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128, DATA_TYPE.DT_BLOB }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR, DATA_FORM.DF_SCALAR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>() { "datetimev" };
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            Preparedata(100000);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_dateType", attributes);
            }
            Console.WriteLine("bt.columns()" + bt.columns());
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(100000, bt1.rows());
            Thread.Sleep(30000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(100000, bt2.rows());
            checkData(bt, bt2);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventSender_all_dateType_vector()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "colNames=\"col\"+string(1..20);\n" +
                    "colTypes=[BOOL[],CHAR[],SHORT[],INT[],LONG[],DOUBLE[],FLOAT[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],DATEHOUR[],UUID[],IPADDR[],INT128[]];\n" +
                    "share table(1:0,colNames,colTypes) as outputTable;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BYTE, DATA_TYPE.DT_SHORT, DATA_TYPE.DT_INT, DATA_TYPE.DT_LONG, DATA_TYPE.DT_DOUBLE, DATA_TYPE.DT_FLOAT, DATA_TYPE.DT_DATE, DATA_TYPE.DT_MONTH, DATA_TYPE.DT_TIME, DATA_TYPE.DT_MINUTE, DATA_TYPE.DT_SECOND, DATA_TYPE.DT_DATETIME, DATA_TYPE.DT_TIMESTAMP, DATA_TYPE.DT_NANOTIME, DATA_TYPE.DT_NANOTIMESTAMP, DATA_TYPE.DT_DATEHOUR, DATA_TYPE.DT_UUID, DATA_TYPE.DT_IPADDR, DATA_TYPE.DT_INT128 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            Preparedata_array(100, 10);
            BasicTable bt = (BasicTable)conn.run("select * from data");

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler_array handler_array = new Handler_array();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler_array, -1, true, "admin", "123456");

            for (int i = 0; i < bt.rows(); i++)
            {
                List<IEntity> attributes = new List<IEntity>();
                for (int j = 0; j < bt.columns(); j++)
                {
                    IEntity pt = bt.getColumn(j).getEntity(i);
                    //Console.WriteLine(pt.getDataType());
                    //Console.WriteLine(i + "行， " + j + "列：" + pt.getString());
                    attributes.Add(pt);
                }
                sender.sendEvent("event_all_array_dateType", attributes);
            }
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(10, bt1.rows());
            Thread.Sleep(5000);
            BasicTable bt2 = (BasicTable)conn.run("select * from outputTable;");
            Assert.AreEqual(10, bt2.rows());
            checkData(bt, bt2);
            client.unsubscribe(SERVER, PORT, "inputTable", "test1");
        }

        [TestMethod]
        public void test_EventSender_decimal_1()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[DECIMAL32(3)]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_decimal{\n" +
                    "\tdecimal32v :: DECIMAL32(3) \n" +
                    "  def event_decimal(decimal32){\n" +
                    "\tdecimal32v = decimal32\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_decimal'\n" +
                    "eventKeys = 'decimal32v';\n" +
                    "typeV = [ DECIMAL32(3)];\n" +
                    "formV = [ SCALAR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);

            EventSchema scheme = new EventSchema("event_decimal", new List<string> { "decimal32v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32 }, new List<DATA_FORM> { DATA_FORM.DF_SCALAR }, new List<int> { 3 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            String script2 = "\t event_decimal1=event_decimal( decimal32(2.001,2))\n" +
                    "\tappendEvent(inputSerializer, event_decimal1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicDecimal32("2.001", 2));
            sender.sendEvent("event_decimal", attributes);

            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_vector_decimal32()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[DECIMAL32(3)[]]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tdecimal32v :: DECIMAL32(3)  VECTOR\n" +
                    "  def event_all_array_dateType(decimal32){\n" +
                    "\tdecimal32v = decimal32\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'decimal32v';\n" +
                    "typeV = [ DECIMAL32(3)[]];\n" +
                    "formV = [ VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);

            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "decimal32v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL32 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR }, new List<int> { 3 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            String script2 = "\tevent_all_array_dateType1=event_all_array_dateType( decimal32(1 2.001,2))\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicDecimal32Vector(new String[] { "1", "2.001" }, 2));
            sender.sendEvent("event_all_array_dateType", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_vector_decimal64()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[DECIMAL64(3)[]]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tdecimal64v :: DECIMAL64(3)  VECTOR\n" +
                    "  def event_all_array_dateType(decimal64){\n" +
                    "\tdecimal64v = decimal64\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'decimal64v';\n" +
                    "typeV = [ DECIMAL64(3)[]];\n" +
                    "formV = [ VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);

            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "decimal64v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL64 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR }, new List<int> { 3 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            String script2 = "\tevent_all_array_dateType1=event_all_array_dateType( decimal64(1 2.001,2))\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicDecimal64Vector(new String[] { "1", "2.001" }, 2));
            sender.sendEvent("event_all_array_dateType", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_vector_decimal128()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n" +
                    "share table(1:0,[\"col1\"],[DECIMAL128(3)[]]) as outputTable;\n";
            conn.run(script);
            String script1 = "class event_all_array_dateType{\n" +
                    "\tdecimal128v :: DECIMAL128(3)  VECTOR\n" +
                    "  def event_all_array_dateType(decimal128){\n" +
                    "\tdecimal128v = decimal128\n" +
                    "  \t}\n" +
                    "}   \n" +
                    "schemaTable = table(array(STRING, 0) as eventType, array(STRING, 0) as eventKeys, array(INT[], ) as type, array(INT[], 0) as form)\n" +
                    "eventType = 'event_all_array_dateType'\n" +
                    "eventKeys = 'decimal128v';\n" +
                    "typeV = [ DECIMAL128(3)[]];\n" +
                    "formV = [ VECTOR];\n" +
                    "insert into schemaTable values([eventType], [eventKeys], [typeV],[formV]);\n" +
                    "share streamTable( array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as intput1;\n" +
                    "try{\ndropStreamEngine(`serInput)\n}catch(ex){\n}\n" +
                    "inputSerializer = streamEventSerializer(name=`serInput, eventSchema=schemaTable, outputTable=intput1);";
            conn.run(script1);

            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "decimal128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_DECIMAL128 }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR }, new List<int> { 3 });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);
            String script2 = "\tevent_all_array_dateType1=event_all_array_dateType( decimal128(1 2.001,2))\n" +
                    "\tappendEvent(inputSerializer, event_all_array_dateType1)\n";
            conn.run(script2);
            List<IEntity> attributes = new List<IEntity>();
            attributes.Add(new BasicDecimal128Vector(new String[] { "1", "2.001" }, 2));
            sender.sendEvent("event_all_array_dateType", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
            BasicTable bt2 = (BasicTable)conn.run("select * from intput1;");
            Assert.AreEqual(1, bt2.rows());
            checkData(bt1, bt2);
        }

        [TestMethod]
        public void test_EventSender_all_dateType_array()
        {
            String script = "share streamTable(1000000:0, `eventType`event, [STRING,BLOB]) as inputTable;\n";
            conn.run(script);
            EventSchema scheme = new EventSchema("event_all_array_dateType", new List<string> { "boolv", "charv", "shortv", "intv", "longv", "doublev", "floatv", "datev", "monthv", "timev", "minutev", "secondv", "datetimev", "timestampv", "nanotimev", "nanotimestampv", "datehourv", "uuidv", "ippAddrv", "int128v" }, new List<DATA_TYPE> { DATA_TYPE.DT_BOOL_ARRAY, DATA_TYPE.DT_BYTE_ARRAY, DATA_TYPE.DT_SHORT_ARRAY, DATA_TYPE.DT_INT_ARRAY, DATA_TYPE.DT_LONG_ARRAY, DATA_TYPE.DT_DOUBLE_ARRAY, DATA_TYPE.DT_FLOAT_ARRAY, DATA_TYPE.DT_DATE_ARRAY, DATA_TYPE.DT_MONTH_ARRAY, DATA_TYPE.DT_TIME_ARRAY, DATA_TYPE.DT_MINUTE_ARRAY, DATA_TYPE.DT_SECOND_ARRAY, DATA_TYPE.DT_DATETIME_ARRAY, DATA_TYPE.DT_TIMESTAMP_ARRAY, DATA_TYPE.DT_NANOTIME_ARRAY, DATA_TYPE.DT_NANOTIMESTAMP_ARRAY, DATA_TYPE.DT_DATEHOUR_ARRAY, DATA_TYPE.DT_UUID_ARRAY, DATA_TYPE.DT_IPADDR_ARRAY, DATA_TYPE.DT_INT128_ARRAY }, new List<DATA_FORM> { DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR, DATA_FORM.DF_VECTOR });
            List<EventSchema> eventSchemas = new List<EventSchema>();
            eventSchemas.Add(scheme);
            List<String> eventTimeFields = new List<string>();
            List<String> commonFields = new List<string>();
            EventSender sender = new EventSender(conn, "inputTable", eventSchemas, eventTimeFields, commonFields);

            client = new EventClient(eventSchemas, eventTimeFields, commonFields);
            Handler handler = new Handler();
            client.subscribe(SERVER, PORT, "inputTable", "test1", handler, -1, true, "admin", "123456");

            Preparedata_array(100, 10);
            BasicTable bt = (BasicTable)conn.run("select * from data");
            List<IEntity> attributes = new List<IEntity>();
            for (int j = 0; j < bt.columns(); j++)
            {
                IEntity pt = (bt.getColumn(j));
                Console.WriteLine(pt.getDataType());
                //                Console.WriteLine(j + "列：" + pt.getObject().ToString());
                attributes.Add(pt);
            }
            sender.sendEvent("event_all_array_dateType", attributes);
            BasicTable bt1 = (BasicTable)conn.run("select * from inputTable;");
            Assert.AreEqual(1, bt1.rows());
        }


    }
}

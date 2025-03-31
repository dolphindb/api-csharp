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
using System.Configuration;
using dolphindb_config;
using dolphindb_csharpapi_net_core.src;
using System.Numerics;
using System.Linq;

namespace dolphindb_csharp_api_test.data_test
{
    [TestClass]
    public class BasicIotAnyVectorTest
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private ExtendedDataOutput @out = null;
        private ExtendedDataInput @in = null;
        private SymbolBase @base = null;

        [TestMethod]
        public void Test_BasicIotAnyVector_Scalar()
        {
            BasicByte bbyte = new BasicByte((byte)127);
            BasicShort bshort = new BasicShort((short)0);
            BasicInt bint = new BasicInt(-4);
            BasicLong blong = new BasicLong(-4);
            BasicBoolean bbool = new BasicBoolean(false);
            BasicFloat bfloat = new BasicFloat((float)1.99);
            BasicDouble bdouble = new BasicDouble(1.99);
            BasicString bsting = new BasicString("最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa");
            List<IScalar> scalar = new List<IScalar>() { bbyte, bshort, bint, blong, bbool, bfloat, bdouble, bsting };
            BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);

            Assert.AreEqual(DATA_CATEGORY.MIXED, BIV.getDataCategory());
            Assert.AreEqual(DATA_TYPE.DT_IOTANY, BIV.getDataType());
            Assert.AreEqual(8, BIV.rows());
            Assert.AreEqual(1, BIV.columns());
            Console.Out.WriteLine(BIV.getString());
            Assert.AreEqual("[127, 0, -4, -4, false, 1.99000000, 1.99000000, 最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa]", BIV.getString());
            List<IScalar> scalar1 = new List<IScalar>() { new BasicString("qqa123"), new BasicString("最新字符qqa") };
            BasicIotAnyVector BIV1 = new BasicIotAnyVector(scalar1);
            Assert.AreEqual("[qqa123, 最新字符qqa]", BIV1.getString());
            Console.Out.WriteLine(BIV1.getString());
            List<IScalar> scalar2 = new List<IScalar>(0);
            for (int i = 0; i < 100000; i++)
            {
                scalar2.Add(scalar[i % scalar.Count]);

            }
            BasicIotAnyVector BIV2 = new BasicIotAnyVector(scalar2);
            //Console.Out.WriteLine(BIV2.getString());
            Assert.AreEqual(100000, BIV2.rows());
            Assert.AreEqual(1, BIV2.columns());

            String re = null;
            try
            {
                Console.WriteLine(BIV.getSubVector(new int[] { 0, 1 }).getString());
            }
            catch (Exception E)
            {
                re = E.Message;
            }
            Assert.AreEqual("The method or operation is not implemented.", re);

        }

        [TestMethod]
        public void Test_BasicIotAnyVector_Scalar_null()
        {
            List<IScalar> scalar = new List<IScalar>() { };
            String re = null;
            try
            {
                BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);
            }
            catch (Exception E)
            {
                re = E.Message;
            }
            Assert.AreEqual("The param 'scalars' cannot be null or empty.", re);
            List<IScalar> scalar1 = null;
            String re1 = null;
            try
            {
                BasicIotAnyVector BIV1 = new BasicIotAnyVector(scalar1);
            }
            catch (Exception E)
            {
                re1 = E.Message;
            }
            Assert.AreEqual("The param 'scalars' cannot be null or empty.", re1);

            BasicByte bbyte = new BasicByte((byte)127);
            bbyte.setNull();
            BasicShort bshort = new BasicShort((short)0);
            bshort.setNull();
            BasicInt bint = new BasicInt(-4);
            bint.setNull();
            BasicLong blong = new BasicLong(-4);
            blong.setNull();
            BasicBoolean bbool = new BasicBoolean(false);
            bbool.setNull();
            BasicFloat bfloat = new BasicFloat((float)1.99);
            bfloat.setNull();
            BasicDouble bdouble = new BasicDouble(1.99);
            bdouble.setNull();
            BasicString bsting = new BasicString("BasicString");
            bsting.setNull();
            List<IScalar> scalar2 = new List<IScalar>() { bbyte, bshort, bint, blong, bbool, bfloat, bdouble, bsting };
            BasicIotAnyVector BIV2 = new BasicIotAnyVector(scalar2);
            Assert.AreEqual("[, , , , , , , ]", BIV2.getString());
        }

        //   [TestMethod]  该种构建方式不支持，可能后面会支持，故case仅注释掉
        //    public void Test_BasicIotAnyVector_1() throws IOException {
        //        byte[] vchar = new byte[]{(byte)'c',(byte)'a'};
        //        BasicByteVector bcv = new BasicByteVector(vchar);
        //        //cshort
        //        short[] vshort = new short[]{32767,-29};
        //        BasicShortVector bshv = new BasicShortVector(vshort);
        //        //cint
        //        int[] vint = new int[]{2147483647,483647};
        //        BasicIntVector bintv = new BasicIntVector(vint);
        //        //clong
        //        long[] vlong = new long[]{2147483647,483647};
        //        BasicLongVector blongv = new BasicLongVector(vlong);
        //
        //        boolean[] vbool = new boolean[]{true,false};
        //        BasicBooleanVector bboolv = new BasicBooleanVector(vbool);
        //
        //        float[] vfloat = new float[]{2147.483647f,483.647f};
        //        BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
        //        //cdouble
        //        double[] vdouble = new double[]{214.7483647,48.3647};
        //        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
        //        //csymbol
        //        List<String> list = new ArrayList<>();
        //        list.Add("KingBase");
        //        list.Add("vastBase");
        //        list.Add(null);
        //        list.Add("OceanBase");
        //        BasicSymbolVector bsymbolv = new BasicSymbolVector(list);
        //
        //        //cstring
        //        String[] vstring1 = new String[]{"GOOG","MS"};
        //        BasicStringVector bstringv1 = new BasicStringVector(vstring1);
        //        //cstring
        //        String[] vstring = new String[]{"string","test string1"};
        //        BasicStringVector bstringv = new BasicStringVector(vstring);
        //
        //        Entity[] entity = new Entity[]{bcv,bshv,bintv, blongv,bboolv,bfloatv,bdoublev,bsymbolv,bstringv1,bstringv};
        //BasicIotAnyVector BIV = new BasicIotAnyVector(entity);
        //Console.Out.WriteLine(BIV.getString());
        //Assert.AreEqual("['c','a',32767,-29,2147483647,483647,2147483647,483647,true,false,2147.48364258,483.64700317,214.7483647,48.3647,KingBase,vastBase,,OceanBase,GOOG,MS,string,test string1]",BIV.getString());
        //Console.Out.WriteLine(BIV.getString(23));
        //}

        [TestMethod]
        public void Test_BasicIotAnyVector_2()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT222\")) dropDatabase(\"dfs://testIOT222\")\n" +
                    "     create database \"dfs://testIOT222\" partitioned by  HASH([INT, 40]),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT222\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT222\",\"pt\");\n" +
                    "t = table([1,2] as deviceId,\n" +
                    "  [now(),now()] as timestamp,\n" +
                    "  [`loc1`loc2] as location,\n" +
                    "  [long(233),string(234)] as value)\n" +
                    "pt.append!(t)";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT222\", `pt) ;");
            Console.Out.WriteLine(entity1.getString());
            BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
            Assert.AreEqual(DATA_CATEGORY.MIXED, BIV.getDataCategory());
            Assert.AreEqual(DATA_TYPE.DT_IOTANY, BIV.getDataType());
            Assert.AreEqual(2, BIV.rows());
            Assert.AreEqual(1, BIV.columns());
            Assert.AreEqual("[233, 234]", BIV.getString());
            Assert.AreEqual("233", BIV.get(0).getString());
            Assert.AreEqual(0, BIV.getExtraParamForType()); 
            Assert.AreEqual(false, BIV.isNull(0));
            String re = null;
            try
            {
                BIV.getUnitLength();
            }
            catch (Exception ex)
            {
                re = ex.Message;
            }
            Assert.AreEqual("The method or operation is not implemented.", re);
            String re1 = null;
            try
            {
                BIV.asof(new BasicString("1"));
            }
            catch (Exception ex)
            {
                re1 = ex.Message;
            }
            Assert.AreEqual("The method or operation is not implemented.", re1);


            String re3 = null;
            try
            {
                BIV.set(0, new BasicInt(1));
            }
            catch (Exception ex)
            {
                re3 = ex.Message;
            }
            Assert.AreEqual("The method or operation is not implemented.", re3);

            String re7 = null;
            try
            {
                BIV.setNull(0);
            }
            catch (Exception ex)
            {
                re7 = ex.Message;
            }
            Assert.AreEqual("The method or operation is not implemented.", re7);
        }

        [TestMethod]
        public void Test_iotAnyVector_select_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            String script = "if(existsDatabase(\"dfs://testIOT_allDateType\")) dropDatabase(\"dfs://testIOT_allDateType\")\n" +
                    "     create database \"dfs://testIOT_allDateType\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                    "     create table \"dfs://testIOT_allDateType\".\"pt\"(\n" +
                    "     deviceId INT,\n" +
                    "     timestamp TIMESTAMP,\n" +
                    "     location SYMBOL,\n" +
                    "     value IOTANY,\n" +
                    " )\n" +
                    "partitioned by deviceId, timestamp,\n" +
                    "sortColumns=[`deviceId, `location, `timestamp],\n" +
                    "latestKeyCache=true;\n" +
                    "pt = loadTable(\"dfs://testIOT_allDateType\",\"pt\");\n";
            conn.run(script);
            BasicTable table1 = (BasicTable)conn.run("select * from loadTable(\"dfs://testIOT_allDateType\", `pt) ;\n");
            Console.WriteLine("table1:" + table1.getString());
            Assert.AreEqual(0, table1.rows());
        }

        [TestMethod]
        public void Test_iotAnyVector_bigData()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT123\")) dropDatabase(\"dfs://testIOT123\")\n" +
            "     create database \"dfs://testIOT123\" partitioned by  HASH([INT, 40]),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT123\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT123\",\"pt\");\n" +
            "t=table(take(1..100000,100000) as deviceId, take(now()+(0..100), 100000) as timestamp,  take(\"bb\"+string(0..100), 100000) as location, take(int(1..100000),100000) as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table(take(100001..200000,100000) as deviceId, take(now()+(0..100), 100000) as timestamp,take(lpad(string(1), 8, \"0\"), 100000) as location, rand(200.0, 100000) as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT123\", `pt) order by timestamp;");
            //Console.Out.WriteLine(entity1.getColumn(3).getString());
            Assert.AreEqual(200000, entity1.rows());
            BasicTable entity2 = (BasicTable)conn.run("select * from loadTable( \"dfs://testIOT123\", `pt)  where deviceId in 1..100000 order by timestamp");
            Assert.AreEqual(entity2.getColumn(0).getString(), entity2.getColumn(0).getString());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
            "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
            "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity1.getColumn("value").getString());
            BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("  exee=exec value from loadTable( \"dfs://testIOT\", `pt) order by deviceId;exee");
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity2.getString());
            BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", BIV.getString());
            Assert.AreEqual("'Q'", BIV.get(0).getString());
            Assert.AreEqual("233", BIV.get(1).getString());
            Assert.AreEqual("-233", BIV.get(2).getString());
            Assert.AreEqual("233121", BIV.get(3).getString());
            Assert.AreEqual("true", BIV.get(4).getString());
            Assert.AreEqual("233.34000000", BIV.get(5).getString());
            Assert.AreEqual("233.34000000", BIV.get(6).getString());
            Assert.AreEqual("loc1", BIV.get(7).getString());
            Assert.AreEqual("AAA", BIV.get(8).getString());
            Assert.AreEqual("bbb", BIV.get(9).getString());
            Assert.AreEqual("xxx", BIV.get(10).getString());
            Assert.AreEqual("'Q'", BIV.get(0).getString());
            Assert.AreEqual("233", BIV.get(1).getString());
            Assert.AreEqual("-233", BIV.get(2).getString());
            Assert.AreEqual("233121", BIV.get(3).getString());
            Assert.AreEqual("true", BIV.get(4).getString());
            Assert.AreEqual("233.34000000", BIV.get(5).getString());
            Assert.AreEqual("233.34000000", BIV.get(6).getString());
            Assert.AreEqual("loc1", BIV.get(7).getString());
            Assert.AreEqual("AAA", BIV.get(8).getString());
            Assert.AreEqual("bbb", BIV.get(9).getString());
            Assert.AreEqual("xxx", BIV.get(10).getString());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
            "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
            "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol([`AAA,`AAA,NULL])] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("[, , , , , , , , AAA, AAA,...]", entity1.getColumn("value").getString());
            BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("  exec value from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("[, , , , , , , , AAA, AAA,...]", entity2.getString());
            BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
            Assert.AreEqual("[, , , , , , , , AAA, AAA,...]", BIV.getString());
            Assert.AreEqual("", BIV.get(0).getString());
            Assert.AreEqual("", BIV.get(1).getString());
            Assert.AreEqual("", BIV.get(2).getString());
            Assert.AreEqual("", BIV.get(3).getString());
            Assert.AreEqual("", BIV.get(4).getString());
            Assert.AreEqual("", BIV.get(5).getString());
            Assert.AreEqual("", BIV.get(6).getString());
            Assert.AreEqual("", BIV.get(7).getString());
            Assert.AreEqual("AAA", BIV.get(8).getString());
            Assert.AreEqual("AAA", BIV.get(9).getString());
            Assert.AreEqual("", BIV.get(10).getString());
            Assert.AreEqual("", BIV.get(0).getString());
            Assert.AreEqual("", BIV.get(1).getString());
            Assert.AreEqual("", BIV.get(2).getString());
            Assert.AreEqual("", BIV.get(3).getString());
            Assert.AreEqual("", BIV.get(4).getString());
            Assert.AreEqual("", BIV.get(5).getString());
            Assert.AreEqual("", BIV.get(6).getString());
            Assert.AreEqual("", BIV.get(7).getString());
            Assert.AreEqual("AAA", BIV.get(8).getString());
            Assert.AreEqual("AAA", BIV.get(9).getString());
            Assert.AreEqual("", BIV.get(10).getString());

            Assert.AreEqual(true, BIV.isNull(0));
            Assert.AreEqual(true, BIV.isNull(1));
            Assert.AreEqual(true, BIV.isNull(2));
            Assert.AreEqual(true, BIV.isNull(3));
            Assert.AreEqual(true, BIV.isNull(4));
            Assert.AreEqual(true, BIV.isNull(5));
            Assert.AreEqual(true, BIV.isNull(6));
            Assert.AreEqual(true, BIV.isNull(7));
            Assert.AreEqual(false, BIV.isNull(8));
            Assert.AreEqual(false, BIV.isNull(9));
            Assert.AreEqual(true, BIV.isNull(10));
        }
        [TestMethod]
        public void Test_iotAnyVector_allDateType_upload()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
                "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
                "     create table \"dfs://testIOT\".\"pt\"(\n" +
                "     deviceId INT,\n" +
                "     timestamp TIMESTAMP,\n" +
                "     location SYMBOL,\n" +
                "     value IOTANY,\n" +
                " )\n" +
                "partitioned by deviceId, timestamp,\n" +
                "sortColumns=[`deviceId, `location, `timestamp],\n" +
                "latestKeyCache=true;\n" +
                "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
                "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n" +
                "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
                "pt.append!(t)\n" +
                "flushTSDBCache()\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity1.getColumn("value").getString());
            BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
            Console.Out.WriteLine(BIV.getString());
            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("iotAny1", BIV);
            conn.upload(upObj);

            IEntity entity2 = conn.run("iotAny1");
            Console.Out.WriteLine(entity2.getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity2.getString());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType_upload_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
            "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
            "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [bool(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [float(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [double(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [string(NULL)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol([NULL,`AAA,`AAA])] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("[, , , , , , , , , AAA,...]", entity1.getColumn("value").getString());
            BasicIotAnyVector BIV = (BasicIotAnyVector)entity1.getColumn("value");
            Console.Out.WriteLine(BIV.getString());
            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("iotAny1", BIV);
            conn.upload(upObj);

            IEntity entity2 = conn.run("iotAny1");
            Console.Out.WriteLine(entity2.getString());
            Assert.AreEqual("[, , , , , , , , , AAA,...]", entity2.getString());

            IEntity entity22 = conn.run("typestr(iotAny1)");
            Console.Out.WriteLine(entity22.getString());
            Assert.AreEqual("IOTANY VECTOR", entity22.getString());

            Dictionary<string, IEntity> upObj1 = new Dictionary<string, IEntity>();
            upObj1.Add("iotAny2", entity1);
            conn.upload(upObj1);
            BasicTable entity3 = (BasicTable)conn.run("iotAny2");
            Console.Out.WriteLine(entity3.getString());
            Assert.AreEqual("[, , , , , , , , , AAA,...]", entity3.getColumn(3).getString());

            BasicTable entity33 = (BasicTable)conn.run("select typeString from schema(pt).colDefs where name = \"value\";");
            Console.Out.WriteLine(entity33.getString());
            Assert.AreEqual("IOTANY", entity33.getColumn(0).get(0).getString());

            BasicTable entity4 = (BasicTable)conn.run("select *  from loadTable( \"dfs://testIOT\", `pt) order by deviceId limit 9 ;");
            Console.Out.WriteLine(entity4.getColumn("value"));
            BasicIotAnyVector entity44 = (BasicIotAnyVector)entity4.getColumn("value");
            Dictionary<string, IEntity> upObj2 = new Dictionary<string, IEntity>();
            upObj2.Add("iotAny3", entity44);
            conn.upload(upObj2);
            BasicIotAnyVector entity5 = (BasicIotAnyVector)conn.run("iotAny3");
            Console.Out.WriteLine(entity5.getString());
            Assert.AreEqual("[, , , , , , , , ]", entity5.getString());

            List<String> colNames = new List<String>() { };
            colNames.Add("deviceId");
            colNames.Add("timestamp");
            colNames.Add("location");
            colNames.Add("value");
            List<IVector> cols = new List<IVector>() { };
            cols.Add(new BasicIntVector(0));
            cols.Add(new BasicTimestampVector(0));
            cols.Add(new BasicStringVector(0));
            cols.Add(new BasicIntVector(0));
            BasicTable bt = new BasicTable(colNames, cols);

            Dictionary<string, IEntity> upObj3 = new Dictionary<string, IEntity>();
            upObj3.Add("iotAnyTable", bt);
            conn.upload(upObj3);
            BasicTable iotAnyTable = (BasicTable)conn.run("iotAnyTable");
            Console.Out.WriteLine(iotAnyTable.getString());
            Assert.AreEqual(0, iotAnyTable.rows());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType_void()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
            "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT\",\"pt\");\n" +
            "t=table([1] as deviceId, [now()]  as timestamp,  [`loc1] as location, [char('Q')] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([2] as deviceId, [now()]  as timestamp,  [`loc1] as location, [short(233)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([3] as deviceId, [now()]  as timestamp,  [`loc1] as location, [int(-233)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([4] as deviceId, [now()]  as timestamp,  [`loc1] as location, [long(233121)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([5] as deviceId, [now()]  as timestamp,  [`loc1] as location, [true] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([6] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34f] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([7] as deviceId, [now()]  as timestamp,  [`loc1] as location, [233.34] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table([8] as deviceId, [now()]  as timestamp,  [`loc1] as location, [`loc1] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n" +
            "t=table(12..14 as deviceId, [now(),2022.06.13 13:30:10.008,2020.06.13 13:30:10.008]  as timestamp,  [`loc1`loc2`loc3] as location, [symbol(`AAA`bbb`xxx)] as value)\n" +
            "pt.append!(t)\n" +
            "flushTSDBCache()\n";
            conn.run(script);
            BasicIotAnyVector entity1 = (BasicIotAnyVector)conn.run("exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId;");
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity1.getString());
            BasicIotAnyVector entity2 = (BasicIotAnyVector)conn.run("pt = exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId; subarray(pt,0:20)");
            Console.Out.WriteLine(entity2.getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity2.getString());
            Assert.AreEqual("", entity2.get(11).getString());
            Assert.AreEqual(DATA_TYPE.DT_VOID, entity2.get(11).getDataType());
            Assert.AreEqual("", entity2.get(11).getString());
            Assert.AreEqual("", entity2.get(19).getString());

            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("iotAny1", entity2);
            conn.upload(upObj);


            IEntity entity3 = conn.run("iotAny1");
            Console.Out.WriteLine(entity3.getString());
            Assert.AreEqual("['Q', 233, -233, 233121, true, 233.34000000, 233.34000000, loc1, AAA, bbb,...]", entity3.getString());
            BasicIotAnyVector entity4 = (BasicIotAnyVector)conn.run("pt = exec value  from loadTable( \"dfs://testIOT\", `pt) order by deviceId; subarray(pt,11:20)");
            Console.Out.WriteLine(entity4.getString());
            Assert.AreEqual("[, , , , , , , , ]", entity4.getString());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType_void_1()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://IOTDB\")) dropDatabase(\"dfs://IOTDB\")\n" +
            "create database \"dfs://IOTDB\" partitioned by HASH([SYMBOL,10]),VALUE([today()]),engine = \"IOTDB\";\n" +
            "create table \"dfs://IOTDB\".\"data\" (\n" +
            "        Time TIMESTAMP,\n" +
            "        systemID SYMBOL,\n" +
            "        TagName SYMBOL,\n" +
            "        Value IOTANY,\n" +
            ")\n" +
            "partitioned by systemID, Time,\n" +
            "sortColumns = [\"systemID\",\"TagName\",\"Time\"],\n" +
            "latestKeyCache = true\n" +
            "pt = loadTable(\"dfs://IOTDB\",\"data\")\n" +
            "Time = take(2024.01.01T00:00:00,2)\n" +
            "systemID = `0x001`0x002\n" +
            "TagName = `value1`value1\n" +
            "Value = [4,6]\n" +
            "t = table(Time,systemID,TagName,Value)\n" +
            "pt.append!(t)\n" +
            "Time = take(2024.01.01T00:00:00,1)\n" +
            "systemID = [`0x001]\n" +
            "TagName = [`value2]\n" +
            "Value = [true]\n" +
            "t = table(Time,systemID,TagName,Value)\n" +
            "pt.append!(t)\n";
            conn.run(script);
            BasicTable entity1 = (BasicTable)conn.run("select Value from pt pivot by Time,sysTemID,TagName;");
            Assert.AreEqual("[true, ]", entity1.getColumn(3).getString());
            Assert.AreEqual("", entity1.getColumn(3).get(1).getString());
            Assert.AreEqual(DATA_TYPE.DT_IOTANY, entity1.getColumn(3).getDataType());
            Assert.AreEqual(DATA_TYPE.DT_VOID, entity1.getColumn(3).get(1).getDataType());

            Dictionary<string, IEntity> upObj = new Dictionary<string, IEntity>();
            upObj.Add("iotAny1", entity1);
            conn.upload(upObj);

            IEntity entity3 = conn.run("iotAny1");
            Console.Out.WriteLine(entity3.getString());
            Assert.AreEqual(entity1.getString(), entity3.getString());
        }

        [TestMethod]
        public void Test_iotAnyVector_allDateType_tableInsert()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, USER, PASSWORD);
            String script = "if(existsDatabase(\"dfs://testIOT\")) dropDatabase(\"dfs://testIOT\")\n" +
            "     create database \"dfs://testIOT\" partitioned by   VALUE(1..20),RANGE(2020.01.01 2022.01.01 2038.01.01), engine='IOTDB'\n" +
            "     create table \"dfs://testIOT\".\"pt\"(\n" +
            "     deviceId INT,\n" +
            "     timestamp TIMESTAMP,\n" +
            "     location SYMBOL,\n" +
            "     value IOTANY,\n" +
            " )\n" +
            "partitioned by deviceId, timestamp,\n" +
            "sortColumns=[`deviceId, `location, `timestamp],\n" +
            "latestKeyCache=true;\n" +
            "pt = loadTable(\"dfs://testIOT\",\"pt\");\n";
            conn.run(script);
            BasicByte bbyte = new BasicByte((byte)127);
            BasicShort bshort = new BasicShort((short)0);
            BasicInt bint = new BasicInt(-4);
            BasicLong blong = new BasicLong(-4);
            BasicBoolean bbool = new BasicBoolean(false);
            BasicFloat bfloat = new BasicFloat((float)1.99);
            BasicDouble bdouble = new BasicDouble(1.99);
            BasicString bsting = new BasicString("最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa");
            List<IScalar> scalar = new List<IScalar>() { bbyte, bshort, bint, blong, bbool, bfloat, bdouble, bsting, bdouble, bsting };
            BasicIotAnyVector BIV = new BasicIotAnyVector(scalar);
            Console.WriteLine(BIV.getString());
            BasicIntVector deviceId = new BasicIntVector(new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });

            BasicAnyVector BIV1 = new BasicAnyVector(10);
            for (int i = 0; i < 10; i++)
            {
                BIV1.set(i, BIV.get(i));
            }
            Console.Out.WriteLine(BIV1.getString());

            BasicTimestampVector timestamp = new BasicTimestampVector(new long[] { 1577836800001, 1577836800002, 1577836800003, 1577836800004, 1577836800005, 1577836800006, 1577836800007, 1577836800008, 1577836800009, 1577836800010 });
            BasicSymbolVector location = new BasicSymbolVector(new String[] { "d1d", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10" });

            List<IEntity> args = new List<IEntity>() { deviceId, timestamp, location, BIV };
            List<String> colNames = new List<string>() { "deviceId", "timestamp", "location", "value" };
            List<IVector> cols = new List<IVector>() { deviceId, timestamp, location, BIV1 };
            BasicTable table1 = new BasicTable(colNames, cols);
            Console.WriteLine(table1.getString());

            conn.run(String.Format("testTable = loadTable('{0}','{1}')", "dfs://testIOT", "pt"));
            List<IEntity> argList = new List<IEntity>(1);
            argList.Add(table1);
            conn.run("tableInsert{testTable}", argList);
            BasicTable entity4 = (BasicTable)conn.run(" select *  from testTable ;");
            Assert.AreEqual("[127, 0, -4, -4, false, 1.99000000, 1.99000000, 最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa, 1.99000000, 最新特殊字符：!@#$%^&*()_++_}{|{\":>?</.,';\\][=-0987654321`~asdQWHH这个点做工&&，。、te长qqa]", entity4.getColumn("value").getString());

            Console.Out.WriteLine(entity4.getString());
        }
    }
}

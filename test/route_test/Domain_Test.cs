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
using dolphindb_csharpapi_net_core.src;

namespace dolphindb_csharpapi_net_core_test.route_test
{
    [TestClass]
    public class Domain_Test
    {
        private string SERVER = MyConfigReader.SERVER;
        static private int PORT = MyConfigReader.PORT;
        static private int PORTCON = MyConfigReader.PORTCON;
        static private int PORTDATE2 = MyConfigReader.PORTDATE2;
        static private int PORTDATE3 = MyConfigReader.PORTDATE3;
        static private int PORTDATE4 = MyConfigReader.PORTDATE4;
        private readonly string USER = MyConfigReader.USER;
        private readonly string PASSWORD = MyConfigReader.PASSWORD;
        private string NODE1_HOST = MyConfigReader.NODE1_HOST;
        private readonly int NODE1_PORT = MyConfigReader.NODE1_PORT;
        public static string[] HASTREAM_GROUP = MyConfigReader.HASTREAM_GROUP;
        private readonly int HASTREAM_GROUPID = MyConfigReader.HASTREAM_GROUPID;
        static private string[] HASTREAM_GROUP1 = MyConfigReader.HASTREAM_GROUP1;

        [TestMethod]
        public void Test_ListDomain_Create()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT, "admin", "123456");
            BasicIntVector biv1 = new BasicIntVector(new int[] { 1, 2, 3, 4 });
            try
            {
                ListDomain Ld1 = new ListDomain(biv1, DATA_TYPE.DT_INT, DATA_CATEGORY.INTEGRAL);
            }
            catch(Exception e)
            {
                Assert.AreEqual("The input list must be a tuple.", e.Message);
                Console.WriteLine(e.Message);
            }

        }
    }
}

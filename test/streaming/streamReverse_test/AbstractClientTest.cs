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
    public class AbstractClientTest
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
        //private readonly int TIMEOUT = 10000;

        [TestMethod]
        public void Test_activeClosePublishConnection_host_port()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, PORT);
            //string localIP = "localhost";
            List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(SERVER),
                    new BasicInt(PORT)
                };
            conn.run("activeClosePublishConnection", @params);  
            
        }
        [TestMethod]
        public void Test_activeClosePublishConnection_host_null()
        {
            DBConnection conn = new DBConnection();
            conn.connect(SERVER, 18921);
            try {
                List<IEntity> @params = new List<IEntity>
                {
                    //new BasicString(SERVER),
                    new BasicInt(8878)
                };
                conn.run("activeClosePublishConnection", @params);
            }
            catch(Exception ex){ 
                Console.WriteLine(ex.ToString());   
            }            
        }
    }
    
}

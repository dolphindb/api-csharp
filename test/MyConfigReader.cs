using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace dolphindb_config
{
    public static class MyConfigReader
    {
        public static String SERVER = "192.168.1.167";
        public static int PORT = 18944;
        public static string USER = "admin";
        public static string PASSWORD = "123456";
        public static String NODE1_HOST = "192.168.1.167";
        public static int NODE1_PORT = 18944;
        public static String LOCALHOST = "192.168.0.44";
        public static int LOCALPORT = 18817;
        public static int HASTREAM_GROUPID = 3;
        public static string[] HASTREAM_GROUP = new string[] { "192.168.1.167:18944", "192.168.1.167:18945", "192.168.1.167:18946" };
        public static int SUB_FLAG = 0;


    }
}




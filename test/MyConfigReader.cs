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
        public static String SERVER = "192.168.100.4";
        public static int PORT = 11954;
        public static int PORTCON = 11951;
        public static int PORTDATE2 = 11954;
        public static int PORTDATE3 = 11955;
        public static int PORTDATE4 = 11956;
        public static string USER = "admin";
        public static string PASSWORD = "123456";
        public static String NODE1_HOST = "192.168.100.4";
        public static int NODE1_PORT = 11954;
        public static String LOCALHOST = "192.168.100.4";
        public static int LOCALPORT = 18817;
        public static int HASTREAM_GROUPID = 3;
        public static string[] HASTREAM_GROUP = new string[] { "192.168.100.4:11954", "192.168.100.4:11955", "192.168.100.4:11956" };
        public static string[] HASTREAM_GROUP1 = new string[] { "192.168.100.4:11954" };
        public static int SUB_FLAG = 0;


    }
}




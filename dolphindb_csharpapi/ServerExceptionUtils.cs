using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb
{
    class ServerExceptionUtils
    {
        public static bool isNotLogin(string exMsg)
        {
            return exMsg.Contains(string.Format("<{0}>",ServerException.NotAuthenticated.ToString()));
        }
    }

    enum ServerException
    {
        NotAuthenticated
    }
}

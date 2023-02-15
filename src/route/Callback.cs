using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb_csharpapi_net_core.src.route
{
    public interface Callback
    {
        void writeCompletion(ITable callbackTable);
    }
}

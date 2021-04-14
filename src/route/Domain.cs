using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;

namespace dolphindb.route
{
    public interface Domain
    {
        List<int> getPartitionKeys(IVector partitionCol);
    }
}

﻿//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using System.Collections.Generic;
namespace dolphindb
{
    public interface IDBConnectionPool
    {
        void execute(List<IDBTask> tasks);
        void execute(IDBTask task);
        int getConnectionCount();
        void shutdown();
    }
}

//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using dolphindb.data;
using System.Collections.Generic;
namespace dolphindb
{
    public interface IDBConnectionPool
    {
        void execute(List<IDBTask> tasks);
        void execute(IDBTask task);
        int getConnectionCount();
        void shutdown();
        void waitForThreadCompletion();

        IEntity run(string function, IList<IEntity> arguments, int priority = 4, int parallelism = 64, bool clearMemory = false);

        IEntity run(string script, int priority = 4, int parallelism = 64, bool clearMemory = false);
    }
}

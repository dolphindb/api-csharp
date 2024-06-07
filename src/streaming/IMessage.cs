using dolphindb.data;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.streaming
{
    public interface IMessage
    {
        string getTopic();
        long getOffset();
        IEntity getEntity(int colIndex);
        T getValue<T>(int colIndex);
        int size();
        string getSym();
    }
}

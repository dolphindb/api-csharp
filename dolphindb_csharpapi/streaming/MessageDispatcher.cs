using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.streaming
{
    public interface MessageDispatcher
    {
        bool isRemoteLittleEndian(string host);
        void dispatch(IMessage message);
        void batchDispatch(List<IMessage> message);
    }
}

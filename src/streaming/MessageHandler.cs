using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.streaming
{
    public interface MessageHandler
    {
        void doEvent(IMessage msg);
        void batchHandler(List<IMessage> msgs);
    }
}

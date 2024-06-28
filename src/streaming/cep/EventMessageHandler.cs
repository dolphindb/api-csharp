using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.streaming.cep
{
    public interface EventMessageHandler { 
    
        void doEvent(string eventType, List<IEntity> attribute);
    }
}

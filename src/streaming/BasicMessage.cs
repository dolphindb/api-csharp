using dolphindb.data;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.streaming
{
    public class BasicMessage : IMessage
    {
        private long offset = 0;
        private string topic = "";
        private BasicAnyVector msg = null;
        private string sym_;

        public BasicMessage(long offset, string topic, BasicAnyVector msg, string sym = "")
        {
            this.offset = offset;
            this.topic = topic;
            this.msg = msg;
            this.sym_ = sym;
        }
        
        public T getValue<T>(int colIndex)
        {
            return (T)msg.getEntity(colIndex);
        }

        public string getTopic()
        {
            return topic;
        }

        public long getOffset()
        {
            return offset;
        }

        public IEntity getEntity(int colIndex)
        {
            return msg.getEntity(colIndex);
        }

        public int size()
        {
            return msg.rows();
        }

        public string getSym()
        {
            return sym_;
        }
    }
}

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
        private List<string> colsName_;

        public BasicMessage(long offset, string topic, BasicAnyVector msg, string sym = "", List<string> colNames = null)
        {
            this.offset = offset;
            this.topic = topic;
            this.msg = msg;
            this.sym_ = sym;
            this.colsName_ = colNames;
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

        public BasicTable getTable()
        {
            if (colsName_ == null) {
                throw new Exception("The getTable method must be used when msgAsTable is true. ");
            }
            List<IVector> cols = new List<IVector> ();
            int rows = msg.rows();
            for(int i = 0; i < rows; ++i)
            {
                cols.Add((IVector)msg.getEntity(i));
            }
            return new BasicTable(colsName_, cols);
        }
    }
}

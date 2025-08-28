using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dolphindb.streaming.cep
{
    public class EventSender
    {
        public EventSender(DBConnection conn, string tableName, List<EventSchema> eventSchema, List<string> eventTimeFields = null, List<string> commonFields = null)
        {
            Utils.checkParamIsNull(conn, "conn");
            Utils.checkParamIsNull(tableName, "tableName");
            Utils.checkParamIsNull(eventSchema, "eventSchema");
            conn_ = conn;
            if (eventTimeFields == null) eventTimeFields = new List<string>();
            if (commonFields == null) commonFields = new List<string>();
            eventHandler_ = new EventHandler(eventSchema, eventTimeFields, commonFields);
            if (tableName == "")
            {
                throw new Exception("tableName must not be empty.");
            }
            string sql = "select top 0 * from " + tableName;
            BasicTable outputTable = (BasicTable)conn_.run(sql);
            eventHandler_.checkOutputTable(outputTable, tableName);
            insertScript_ = "tableInsert{" + tableName + "}";
        }
        public void sendEvent(string eventType, List<IEntity> attributes)
        {
            Utils.checkParamIsNull(eventType, "eventType");
            Utils.checkParamIsNull(attributes, "attributes");
            List<IEntity> args = new List<IEntity>();
            eventHandler_.serializeEvent(eventType, attributes, args);
            conn_.run(insertScript_, args);
        }

        private string insertScript_;
        private EventHandler eventHandler_;
        private DBConnection conn_;
    };

}

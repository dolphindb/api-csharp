using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using dolphindb.data;
using dolphindb.io;
using System.IO;

namespace dolphindb.streaming
{
    public class StreamDeserializer
    {
        Dictionary<string, MsgDeserializer> msgDeserializers_;
        Dictionary<string, Tuple<string, string>> tableNames_;

        public BasicMessage parse(IMessage message)
        {
            if (msgDeserializers_ == null)
                throw new Exception("The StreamDeserializer is not inited. ");
            if (message.size() < 3)
                throw new Exception("The data must contain 3 columns. ");
            if (message.getEntity(1).getDataType() != DATA_TYPE.DT_SYMBOL && message.getEntity(1).getDataType() != DATA_TYPE.DT_STRING)
                throw new Exception("The 2rd column must be a vector type with symbol or string. ");
            if (message.getEntity(2).getDataType() != DATA_TYPE.DT_BLOB)
                throw new Exception("The 3rd column must be a vector type with blob. ");

            string sym = message.getEntity(1).getString();
            byte[] blob = ((BasicString)message.getEntity(2)).getBytes();
            MsgDeserializer deserializer = null;
            if (!msgDeserializers_.TryGetValue(sym, out deserializer))
            {
                throw new Exception("The filter " + sym + " does not exist. ");
            }
            BasicMessage mixedMessage = new BasicMessage(message.getOffset(), message.getTopic(), deserializer.parse(blob), sym);
            return mixedMessage;
        }

        public StreamDeserializer(Dictionary<string, List<DATA_TYPE>> filters)
        {
            msgDeserializers_ = new Dictionary<string, MsgDeserializer>();
            foreach (KeyValuePair<string, List<DATA_TYPE>> keyValue in filters)
            {
                List<DATA_TYPE> colTypes = keyValue.Value;
                msgDeserializers_[keyValue.Key] =new MsgDeserializer(colTypes);
            }
        }

        public StreamDeserializer(Dictionary<string, BasicDictionary> filters)
        {
            init(filters);
        }

        public StreamDeserializer(Dictionary<string, Tuple<string, string>> tableNames, DBConnection conn = null)
        {
            tableNames_ = tableNames;
            if (conn != null)
                init(conn);
        }

        void init(Dictionary<string, BasicDictionary> filters)
        {
            msgDeserializers_ = new Dictionary<string, MsgDeserializer>();
            foreach (KeyValuePair<string, BasicDictionary> keyValue in filters)
            {
                List<DATA_TYPE> colTypes = new List<DATA_TYPE>();
                List<string> colNames = new List<string>();

                BasicTable colDefs = (BasicTable)keyValue.Value.get("colDefs");
                BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
                BasicDictionary data = keyValue.Value;
                for (int i = 0; i < colDefsTypeInt.rows(); ++i)
                {
                    colTypes.Add((DATA_TYPE)colDefsTypeInt.getInt(i));
                }
                msgDeserializers_[keyValue.Key] = new MsgDeserializer(colTypes);
            }
        }

        public void init(DBConnection conn)
        {
            if(msgDeserializers_ != null)
                throw new Exception("The StreamDeserializer is inited. ");
            if (tableNames_ == null)
                throw new Exception("The tableNames_ is null. ");
            msgDeserializers_ = new Dictionary<string, MsgDeserializer>();
            Dictionary<string, BasicDictionary> filters = new Dictionary<string, BasicDictionary>();
            foreach (KeyValuePair<string, Tuple<string, string>> value in tableNames_)
            {
                string dbName = value.Value.Item1;
                string tableName = value.Value.Item2;
                BasicDictionary schema;
                if (dbName == "")
                {
                    schema = (BasicDictionary)conn.run("schema(" + tableName + ")");
                }
                else
                {
                    schema = (BasicDictionary)conn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
                }
                filters[value.Key] = schema;
            }
            init(filters);
        }

        public bool isInited()
        {
            return msgDeserializers_ != null;
        }

        public void checkSchema(BasicDictionary schema)
        {
            BasicTable colDefs = (BasicTable)schema.get("colDefs");
            BasicStringVector types = (BasicStringVector)colDefs.getColumn(1);
            if (colDefs.rows() < 3)
                throw new Exception("The data must contain 3 columns. ");
            if (types.getString(1) != "SYMBOL" && types.getString(1) != "STRING")
                throw new Exception("The 2rd column must be a vector type with symbol or string. ");
            if (types.getString(2) != "BLOB")
                throw new Exception("The 3rd column must be a vector type with blob. ");
        }

        class MsgDeserializer
        {
            List<DATA_TYPE> colTypes_;
            public MsgDeserializer(List<DATA_TYPE> colTypes)
            {
                colTypes_ = new List<DATA_TYPE>();
                colTypes_.AddRange(colTypes);
            }

            public BasicAnyVector parse(byte[] data)
            {
                MemoryStream memoryStream = new MemoryStream();
                ExtendedDataOutput writeStream = new BigEndianDataOutputStream(memoryStream);
                writeStream.writeBlob(data);
                LittleEndianDataInputStream dataStream = new LittleEndianDataInputStream(new MemoryStream(memoryStream.GetBuffer(), 0, (int)memoryStream.Position));
                BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();

                int columns = colTypes_.Count;
                dataStream.readInt();
                BasicAnyVector ret = new BasicAnyVector(columns);
                for (int i = 0; i < columns; ++i)
                    ret.setEntity(i, basicEntityFactory.createEntity(DATA_FORM.DF_SCALAR, colTypes_[i], dataStream, false));
                return ret;
            }
        }
    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
using dolphindb;
using System.Collections;
using System.Data;
namespace dolphindb.route
{
    public class autoFitTableAppender
    {
        private DBConnection conn;
        private bool asynTask;
        private string dbUrl;
        private string tableName;
        private string tableStr;
        Dictionary<string, DATA_TYPE> dataTypeMap;
        Dictionary<string, int> extrasMap;
        bool supportDecimal;



        public autoFitTableAppender(string dbUrl, string tableName, bool asynTask,DBConnection conn)
        {
            this.dbUrl = dbUrl;
            this.tableName = tableName;
            this.asynTask = asynTask;
            this.conn = conn;
            conn.setasynTask(false);
            tableStr = dbUrl == "" ? tableName : "loadTable(\"" + dbUrl + "\", \"" + tableName + "\")";
            BasicDictionary tableInfo = (BasicDictionary)conn.run("schema(" + tableStr + ")");
            BasicTable colDefs;
            colDefs = ((BasicTable)tableInfo.get(new BasicString("colDefs")));
            BasicStringVector names = (BasicStringVector)colDefs.getColumn("name");
            BasicIntVector types = (BasicIntVector)colDefs.getColumn("typeInt");
            supportDecimal = colDefs.getColumnNames().Contains("extra");
            BasicIntVector extras = supportDecimal ? (BasicIntVector)colDefs.getColumn("extra") : null;
            int rows = names.rows();

            this.dataTypeMap = new Dictionary<string, DATA_TYPE>();
            this.extrasMap = new Dictionary<string, int>();
            for (int i = 0; i < rows; ++i)
            {
                dataTypeMap.Add(names.getString(i), (DATA_TYPE)types.getInt(i));
                if(supportDecimal) extrasMap.Add(names.getString(i), extras.getInt(i));
            }
            conn.setasynTask(asynTask);

        }

        public IEntity append(DataTable table)
        {
            BasicTable bt = Utils.fillSchema(table, dataTypeMap, extrasMap);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(bt);
            string appendScript = "tableInsert{" + tableStr + "}";
            return conn.run(appendScript, args);
           
        }

        public IEntity append(BasicTable table)
        {
            BasicTable bt = Utils.fillSchema(table, dataTypeMap, extrasMap);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(bt);
            string appendScript = "tableInsert{" + tableStr + "}";
            return conn.run(appendScript, args);           
        }



    }
}

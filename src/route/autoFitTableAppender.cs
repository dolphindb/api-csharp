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
        Dictionary<string, DATA_TYPE> schema;
        

        public autoFitTableAppender(string dbUrl, string tableName, bool asynTask,DBConnection conn)
        {
            this.dbUrl = dbUrl;
            this.tableName = tableName;
            this.asynTask = asynTask;
            this.conn = conn;
            conn.setasynTask(false);
            BasicDictionary tableInfo = (BasicDictionary)conn.run("schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))");
            BasicTable colDefs;
            colDefs = ((BasicTable)tableInfo.get(new BasicString("colDefs")));
            BasicStringVector names = (BasicStringVector)colDefs.getColumn("name");
            BasicIntVector types = (BasicIntVector)colDefs.getColumn("typeInt");
            int rows = names.rows();


            this.schema = new Dictionary<string, DATA_TYPE>();
            for (int i = 0; i < rows; ++i)
            {
               
                schema.Add(names.getString(i), (DATA_TYPE)types.getInt(i));
            }
            conn.setasynTask(asynTask);

        }

        public IEntity append(DataTable table)
        {
            BasicTable bt = Utils.fillSchema(table, schema);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(bt);
            string appendScript = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
            return conn.run(appendScript, args);
           
        }

        public IEntity append(BasicTable table)
        {
            BasicTable bt = Utils.fillSchema(table, schema);
            List<IEntity> args = new List<IEntity>(1);
            args.Add(bt);
            string appendScript = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
            return conn.run(appendScript, args);           
        }



    }
}

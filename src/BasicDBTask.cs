//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using dolphindb.data;
using System.Collections.Generic;
using System;
namespace dolphindb
{
    public class BasicDBTask:IDBTask
    {
        private IEntity result = null;
        private string errMsg = null;
        private bool successful = false;
        private string script;
        private List<IEntity> args;
        private DBConnection conn;
        public BasicDBTask(string script, List<IEntity> args)
        {
            this.script = script;
            this.args = args;
        }
        public BasicDBTask(string script)
        {
            this.script = script;
        }
        public IEntity call()
        {
            try
            {
                if (args != null)
                    result = conn.run(script, args);
                else
                    result = conn.run(script);
                errMsg = null;
                successful = true;
                return result;
            }
            catch(Exception t)
            {
                successful = false;
                result = null;
                errMsg = t.Message;
                return null;
            }
        }
        public void setDBConnection(DBConnection conn)
        {
            this.conn = conn;
        }
        public IEntity getResults()
        {
            return result;
        }
        public string getErrorMsg()
        {
            return errMsg;
        }
        public bool isSuccessful()
        {
            return successful;
        }
    }
}

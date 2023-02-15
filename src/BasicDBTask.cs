//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using dolphindb.data;
using System.Collections.Generic;
using System;
using System.Threading;
using dolphindb.io;

namespace dolphindb
{
    public class BasicDBTask:IDBTask
    {
        private IEntity result = null;
        private string errMsg = null;
        private bool successful = false;
        private string script;
        private List<IEntity> args;
        private bool clearMemory_;
        private int priority_;
        private int parallelism_;
        private DBConnection conn;
        private WaitHandle waitHandle = new AutoResetEvent(false);
        private Semaphore semaphore = new Semaphore(1,1);

        public void waitFor()
        {
            semaphore.WaitOne();
        }
        public void finish()
        {
            semaphore.Release();
        }
        public BasicDBTask(string script, List<IEntity> args, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            semaphore.WaitOne();
            this.script = script;
            this.args = args;
            this.clearMemory_ = clearMemory;
            this.priority_ = priority;
            this.parallelism_ = parallelism;
        }
        public BasicDBTask(string script, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            semaphore.WaitOne();
            this.script = script;
            this.clearMemory_ = clearMemory;
            this.priority_ = priority;
            this.parallelism_ = parallelism;
        }
        public IEntity call()
        {
            try
            {
                if (args != null)
                    result = conn.run(script, args, priority_, parallelism_, 0, clearMemory_);
                else
                    result = conn.run(script, priority_, parallelism_, 0, clearMemory_);
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

        public bool isFinished()
        {
            if(successful || errMsg != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}

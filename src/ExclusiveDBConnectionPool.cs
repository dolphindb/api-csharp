//-------------------------------------------------------------------------------------------
//	Copyright © 2021 DolphinDB Inc.
//	Date   : 2021.01.21
//  Author : zhikun.luo
//-------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using dolphindb.data;
using System.Threading;

namespace dolphindb
{
    public class ExclusiveDBConnectionPool:IDBConnectionPool
    {
        private List<DBConnection> conns;
        public ExclusiveDBConnectionPool(string host, int port, string uid,string pwd, int count, bool loadBalance,bool highAvaliability) {
            conns = new List<DBConnection>(count);
            if (!loadBalance)
            {
                for(int i = 0; i < count; i++)
                {
                    DBConnection conn = new DBConnection();
                    if (!conn.connect(host, port, uid, pwd, "", highAvaliability))
                        throw new Exception("Cant't connect to the specified host.");
                    conns.Add(conn);
                }
            }
            else
            {
                DBConnection entryPoint = new DBConnection();
                if (!entryPoint.connect(host, port, uid, pwd))
                    throw new Exception("Can't connect to the specified host.");
                BasicStringVector nodes = (BasicStringVector)entryPoint.run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
                int nodeCount = nodes.rows();
                string[] hosts = new string[nodeCount];
                int[] ports = new int[nodeCount];
                for(int i = 0; i < nodeCount; i++)
                {
                    string[] fields = nodes.getString(i).Split(':');
                    if (fields.Length < 2)
                        throw new Exception("Invalid datanode adress: " + nodes.getString(i));
                    hosts[i] = fields[0];
                    ports[i] = int.Parse(fields[1]);
                }
                for(int i = 0; i < count; i++)
                {
                    DBConnection conn = new DBConnection();
                    if (!(conn.connect(hosts[i % nodeCount], ports[i % nodeCount], uid, pwd, "", highAvaliability)))
                        throw new Exception("Can't connect to the host: " + nodes.getString(i%nodeCount));
                    conns.Add(conn);
                }
            }
            ThreadPool.SetMaxThreads(count, count);
        }
        public void execute(List<IDBTask> tasks) {
            int taskSize = tasks.Count;
            if (taskSize > conns.Count)
                throw new Exception("The number of tasks can't exceed the number of connections in the pool.");
            for (int i = 0; i < taskSize; i++)
                tasks[i].setDBConnection(conns[i]);
            WaitHandle[] waits = new WaitHandle[taskSize];
            try
            {
                for (int i = 0; i < taskSize; i++)
                {
                    ThreadPool.QueueUserWorkItem(new WaitCallback(threadexec), tasks[i]);
                    waits[i] = ((BasicDBTask)tasks[i]).WaitFor();
                }    
            }
            catch (NotSupportedException ie)
            {
                throw new Exception(ie.Message);
            }

            WaitHandle.WaitAll(waits);
        }
        private void threadexec(object obj)
        {
            IDBTask task = (IDBTask)obj;
            task.call();
            ((BasicDBTask)task).Finish();
        }
        public void execute(IDBTask task) {
            if (conns.Count == 0)
                throw new Exception("Empty DBConnection pool, task execute failed.");
            task.setDBConnection(conns[0]);
            task.call();
        }
        public int getConnectionCount() {
            return conns.Count;
        }
        public void shutdown() {
            for(int i = 0; i < conns.Count; i++)
                conns[i].close();
        }
    }
}

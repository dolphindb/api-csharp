﻿//-------------------------------------------------------------------------------------------
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
        private List<AsynWorker> workers_ = new List<AsynWorker>();
        private int tasksCount_ = 0;
        private WorkItem workItem_ = new WorkItem();

        class WorkItem
        {
            public Mutex finishedTasklock_ = new Mutex();
            public Queue<IDBTask> taskLists_ = new Queue<IDBTask>();
            public int finishedTaskCount_ = 0;

            public WorkItem()
            {
                this.finishedTaskCount_ = 0;
            }
        }
        class AsynWorker
        {
            private DBConnection connection_;
            public Thread workThread_;
            public WorkItem threadWorkItem;

            public AsynWorker(DBConnection connection, WorkItem finishedCount1)
            {
                this.connection_ = connection;
                this.threadWorkItem = finishedCount1;
            }

            public void run()
            {
                while (true)
                {
                    IDBTask task = null;
                    lock (threadWorkItem.taskLists_)
                    {
                        if (threadWorkItem.taskLists_.Count == 0)
                        {
                            try
                            {
                                Monitor.Wait(threadWorkItem.taskLists_);
                            }
                            catch (Exception e)
                            {
                                break;
                            }
                        }
                    }

                    while (true)
                    {
                        lock (threadWorkItem.taskLists_)
                        {
                            if (threadWorkItem.taskLists_.Count == 0)
                                break;
                            task = threadWorkItem.taskLists_.Dequeue();
                        }
                        if (task == null)
                        {
                            break;
                        }
                        try
                        {
                            task.setDBConnection(connection_);
                            task.call();
                        }
                        catch (Exception e)
                        {
                            Console.Out.WriteLine(e.StackTrace);
                            Console.WriteLine(e.StackTrace);
                        }
                        ((BasicDBTask)task).finish();
                        lock (threadWorkItem.finishedTasklock_)
                        {
                            threadWorkItem.finishedTaskCount_++;
                        }
                    }
                    lock (threadWorkItem.finishedTasklock_)
                    {
                        Monitor.Pulse(threadWorkItem.finishedTasklock_);
                    }
                }
                connection_.close();
            }
        }

        public ExclusiveDBConnectionPool(string host, int port, string uid,string pwd, int count, bool loadBalance,bool highAvailability, string[] highAvailabilitySites = null, string startup = "", bool compress = false, bool useSSL = false, bool usePython = false) {
            if (count <= 0)
                throw new Exception("The thread count can not be less than 1");
            if (!loadBalance)
            {
                for(int i = 0; i < count; i++)
                {
                    DBConnection conn = new DBConnection(false, useSSL, compress, usePython);
                    conn.setLoadBalance(false);
                    if (!conn.connect(host, port, uid, pwd, startup, highAvailability))
                        throw new Exception("Cant't connect to the specified host.");
                    AsynWorker asyn = new AsynWorker(conn, workItem_);
                    asyn.workThread_ = new Thread(new ThreadStart(asyn.run));
                    asyn.workThread_.Start();
                    workers_.Add(asyn);
                }
            }
            else
            {
                BasicStringVector nodes = null;
                if (highAvailabilitySites != null)
                {
                    nodes = new BasicStringVector(highAvailabilitySites);
                }
                else
                {
                    DBConnection entryPoint = new DBConnection();
                    if (!entryPoint.connect(host, port, uid, pwd))
                        throw new Exception("Can't connect to the specified host.");
                    nodes = (BasicStringVector)entryPoint.run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
                    entryPoint.close();
                }
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
                    DBConnection conn = new DBConnection(false, useSSL, compress, usePython);
                    conn.setLoadBalance(false);
                    if (!(conn.connect(hosts[i % nodeCount], ports[i % nodeCount], uid, pwd, startup, highAvailability)))
                        throw new Exception("Can't connect to the host: " + nodes.getString(i%nodeCount));
                    AsynWorker asyn = new AsynWorker(conn, workItem_);
                    asyn.workThread_ = new Thread(new ThreadStart(asyn.run));
                    asyn.workThread_.Start();
                    workers_.Add(asyn);
                }
            }
        }


        public void execute(List<IDBTask> tasks)
        {
            tasksCount_ += tasks.Count;
            lock (workItem_.taskLists_)
            {
                foreach (IDBTask task in tasks)
                {
                    workItem_.taskLists_.Enqueue(task);
                    Monitor.PulseAll(workItem_.taskLists_);
                }
            }
            for (int i = 0; i < tasks.Count; i++)
            {
                ((BasicDBTask)tasks[i]).waitFor();
                ((BasicDBTask)tasks[i]).finish();
            }
        }

        public void execute(IDBTask task) {
            tasksCount_++;
            lock (workItem_.taskLists_)
            {
                workItem_.taskLists_.Enqueue(task);
                Monitor.Pulse(workItem_.taskLists_);
            }
            ((BasicDBTask)task).waitFor();
            ((BasicDBTask)task).finish();
        }

        public IEntity run(string function, IList<IEntity> arguments)
        {
            IDBTask task = new BasicDBTask(function, (List<IEntity>)arguments);
            execute(task);
            if (!task.isSuccessful())
            {
                throw new Exception(task.getErrorMsg());
            }
            return task.getResults();
        }

        public IEntity run(string script)
        {
            IDBTask task = new BasicDBTask(script);
            execute(task);
            if (!task.isSuccessful())
            {
                throw new Exception(task.getErrorMsg());
            }
            return task.getResults();
        }

        public void waitForThreadCompletion()
        {
            try
            {
                lock (workItem_.finishedTasklock_)
                {
                    Console.Out.WriteLine("Waiting for tasks to complete, remain Task: " + (tasksCount_ - workItem_.finishedTaskCount_));
                    while (workItem_.finishedTaskCount_ >= 0)
                    {
                        if (workItem_.finishedTaskCount_ < tasksCount_)
                        {
                            Monitor.Wait(workItem_.finishedTasklock_);
                        }
                        else if (workItem_.finishedTaskCount_ == tasksCount_)
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        public int getConnectionCount() {
            return workers_.Count;
        }
        public void shutdown() {
            waitForThreadCompletion();
;           foreach(AsynWorker one in workers_)
            {
                lock (one.workThread_)
                {
                    one.workThread_.Interrupt();
                }
            }
        }
    }
}

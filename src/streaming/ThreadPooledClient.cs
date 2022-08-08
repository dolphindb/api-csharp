using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using dolphindb.data;

namespace dolphindb.streaming
{
    using QueueHandlerBinder = Tuple<BlockingCollection<List<IMessage>>, MessageHandler>;

    public class ThreadPooledClient : AbstractClient
    {
        private Dictionary<string, QueueHandlerBinder> queueHandlers = new Dictionary<string, QueueHandlerBinder>();
        private Thread thread_;

        private class HandlerRunner
        {
            private MessageHandler handler;
            private IMessage message;

            public HandlerRunner(MessageHandler handler, IMessage message)
            {
                this.handler = handler;
                this.message = message;
            }

            public void run(Object threadContext)
            {
                handler.doEvent(message);
            }
        }

        class TaskPool
        {
            private int threadsMaxCount;

            public TaskPool(int threadsMaxCount)
            {
                this.threadsMaxCount = threadsMaxCount;
            }
        }

        public ThreadPooledClient() : this(DEFAULT_PORT) { } 

        public ThreadPooledClient(string subscribeHost, int subscribePort) : base(subscribeHost,subscribePort) {
            Dictionary<String, Queue<IMessage>> backlog = new Dictionary<string, Queue<IMessage>>();

            bool fillBacklog()
            {
                backlog.Clear();
                bool filled = false;
                lock (queueHandlers)
                {
                    foreach (KeyValuePair<string, QueueHandlerBinder> keyValue in queueHandlers)
                    {
                        if (keyValue.Value.Item1.TryTake(out List<IMessage> messages))
                        {
                            Queue<IMessage> queue = new Queue<IMessage>();
                            messages.ForEach(queue.Enqueue);
                            backlog[keyValue.Key] = queue;
                            filled = true;
                        }
                    }
                }
                return filled;
            }

            void run()
            {
                try {
                    while (!this.isClose())
                    {
                        foreach (KeyValuePair<String, Queue<IMessage>> keyValue in backlog)
                        {
                            Queue<IMessage> value = keyValue.Value;
                            string topic = keyValue.Key;
                            while (value.Count > 0)
                            {
                                IMessage msg = value.Dequeue();
                                QueueHandlerBinder binder = null;
                                lock (queueHandlers)
                                {
                                    if (queueHandlers.ContainsKey(topic))
                                    {
                                        binder = queueHandlers[topic];
                                        HandlerRunner handlerRunner = new HandlerRunner(binder.Item2, msg);
                                        ThreadPool.QueueUserWorkItem(new WaitCallback(handlerRunner.run));
                                    }
                                }
                            }
                        }
                        fillBacklog();
                    }
                }
                catch (ThreadInterruptedException)
                {
                }
                Console.WriteLine("ThreadPooledClient thread stopped.");
            }

            thread_ = new Thread(new ThreadStart(run));
            thread_.Start();
        }

        public ThreadPooledClient(int subscribePort) : this(DEFAULT_HOST, subscribePort) { }
        protected override bool doReconnect(SubscribeInfo subscribeInfo, Site site)
        {
            try
            {
                subscribe(site.host, site.port, subscribeInfo.getTableName(), subscribeInfo.getActionName(), subscribeInfo.getMessageHandler(), subscribeInfo.getMsgId() + 1, true, subscribeInfo.getFilter(), subscribeInfo.getDeseriaLizer(), subscribeInfo.getUser(), subscribeInfo.getPassword(), false);
                Console.WriteLine("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + subscribeInfo.getTableName());
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.WriteLine(ex.StackTrace);
            }
            return false;
        }


        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = ""){
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, user, password, true);
        }

        private void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer, string user, string password, bool createSubInfo)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, user, password, createSubInfo);
            lock (queueHandlers)
            {
                DBConnection dbConn = new DBConnection();
                dbConn.connect(host, port);
                List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(tableName),
                    new BasicString(actionName)
                };
                IEntity re = dbConn.run("getSubscriptionTopic", @params);
                string topic = ((BasicAnyVector)re).getEntity(0).getString();
                if (createSubInfo)
                {
                    lock (queueHandlers)
                    {
                        queueHandlers.Add(topic, new QueueHandlerBinder(queue, handler));
                    }
                }
            }
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, filter);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect)
        {
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, null);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, null);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, false, filter);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect, null);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, false, null);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler)
        {
            subscribe(host, port, tableName, handler, -1);
        }

        public void unsubscribe(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName);
        }

        public void unsubscribe(string host, int port, string tableName, string actionName)
        {
            unsubscribeInternal(host, port, tableName, actionName);
        }

        public void close()
        {
            base.close();
            thread_.Interrupt();
        }

        protected override void unsubscribeInternal(string host, int port, string tableName, string actionName)
        {
            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            try
            {
                lock (subscribeInfos_)
                {
                    string localIP = listeningHost_;
                    if (localIP == null || localIP.Equals(String.Empty))
                        localIP = dbConn.LocalAddress;
                    List<IEntity> @params = new List<IEntity>
                    {
                    new BasicString(tableName),
                    new BasicString(actionName)
                    };
                    IEntity re = dbConn.run("getSubscriptionTopic", @params);
                    string topic = ((BasicAnyVector)re).getEntity(0).getString();
                    SubscribeInfo subscribeInfo = null;
                    if (!subscribeInfos_.TryRemove(topic, out subscribeInfo))
                    {
                        throw new Exception("The subscription " + topic + " doesn't exist. ");
                    }
                    lock (queueHandlers)
                    {
                        queueHandlers.Remove(topic);
                    }
                    @params = new List<IEntity>
                    {
                    new BasicString(localIP),
                    new BasicInt(listeningPort_),
                    new BasicString(tableName),
                    new BasicString(actionName)
                    };
                    lock (subscribeInfo)
                    {
                        subscribeInfo.close();
                        dbConn.run("stopPublishTable", @params);
                    }
                    Console.WriteLine("Successfully unsubscribed table " + topic);
                }
            }
            finally
            {
                dbConn.close();
            }
            return;
        }
    }
}

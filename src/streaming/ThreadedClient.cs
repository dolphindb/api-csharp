using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using dolphindb.data;

namespace dolphindb.streaming
{
    public class ThreadedClient : AbstractClient
    {
        private Dictionary<string, HandlerLooper> handlerLoopers = new Dictionary<string, HandlerLooper>();
        public ThreadedClient() : this(DEFAULT_PORT) { }

        public ThreadedClient(int subscribePort) : base(DEFAULT_HOST, subscribePort) { }

        public ThreadedClient(string subscribeHost, int subscribePort) : base(subscribeHost, subscribePort) { }

        class HandlerLooper
        {
            public Thread thread_;
            BlockingCollection<List<IMessage>> queue_;
            MessageHandler handler_;
            int batchSize_;
            float throttle_;
            MessageDispatcher dispatcher_;
            internal HandlerLooper(BlockingCollection<List<IMessage>> queue, MessageHandler handler,int batchSize, float throttle, MessageDispatcher dispatcher)
            {
                this.queue_ = queue;
                this.handler_ = handler;
                this.batchSize_ = batchSize;
                this.throttle_ = throttle;
                this.dispatcher_ = dispatcher;
            }

            public void run()
            {
                try
                {
                    while (!dispatcher_.isClose())
                    {
                        if (batchSize_ != -1)
                        {
                            List<IMessage> messages = new List<IMessage>();
                            List<IMessage> tmp = null;
                            if(!this.queue_.TryTake(out tmp, 1000))
                                continue;
                            messages.AddRange(tmp);
                            DateTime end = DateTime.Now.AddSeconds(throttle_);
                            while (messages.Count < batchSize_ && DateTime.Now < end)
                            {
                                List<IMessage> item = null;
                                int timeout = (int)(DateTime.Now - end).TotalMilliseconds;
                                if (timeout <= 0)
                                    break;
                                if (this.queue_.TryTake(out item, timeout))
                                    messages.AddRange(item);
                            }
                            try
                            {
                                handler_.batchHandler(messages);
                            }catch(Exception e)
                            {
                                Console.WriteLine("failed to batchHandler: " + e.Message);
                                Console.Write(e.StackTrace);
                            }
                        }
                        else
                        {
                            List<IMessage> messages = null;
                            if(!this.queue_.TryTake(out messages, 1000))
                                continue;
                            foreach (IMessage message in messages)
                            {
                                try
                                {
                                    handler_.doEvent(message);
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("failed to doEvent: " + e.Message);
                                    Console.Write(e.StackTrace);
                                }
                            }
                        }
                    }

                }
                catch (ThreadInterruptedException)
                {
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.WriteLine(ex.StackTrace);
                }
                Console.WriteLine("Handler thread stopped.");
            }
        }

        protected override bool doReconnect(SubscribeInfo subscribeInfo, Site site)
        {
            try
            {
                subscribe(site.host, site.port, subscribeInfo.getTableName(), subscribeInfo.getActionName(), subscribeInfo.getMessageHandler(), 
                    subscribeInfo.getMsgId() + 1, true, subscribeInfo.getFilter(), -1, 0, subscribeInfo.getDeseriaLizer(), subscribeInfo.getUser(), subscribeInfo.getPassword(), false);
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

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, 
            float throttle = 0.01f, StreamDeserializer deserializer = null, string user = "", string password = ""){
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, filter, batchSize, throttle, deserializer, user, password, true);
        }


        private void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter,int batchSize, float throttle, StreamDeserializer deserializer, string user, string password, bool createSubInfo)
        {
            if (!(batchSize == -1 || batchSize > 0))
                throw new Exception("Invalid batchSize value, should be -1 or positive integer.");
            if (throttle < 0)
                throw new Exception("Throttle can't be less than 0.");
            lock (this)
            {
                DBConnection dbConn = new DBConnection();
                try
                {
                    dbConn.connect(host, port);
                    List<IEntity> @params = new List<IEntity>
                    {
                        new BasicString(tableName),
                        new BasicString(actionName)
                    };
                    IEntity re = dbConn.run("getSubscriptionTopic", @params);
                    string topic = ((BasicAnyVector)re).getEntity(0).getString();
                    BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, deserializer, user, password, createSubInfo);
                    if (createSubInfo)
                    {
                        lock (handlerLoopers){
                            HandlerLooper handlerLooper = new HandlerLooper(queue, handler, batchSize, throttle, this);
                            handlerLooper.thread_ = new Thread(new ThreadStart(handlerLooper.run));
                            handlerLooper.thread_.Start();
                            handlerLoopers.Add(topic, handlerLooper);
                        }
                    }
                }
                finally
                {
                    dbConn.close();
                }
            }
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, filter,batchSize);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, null,batchSize);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, null,batchSize);
        }


        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, filter,batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1, float throttle = 0.01f)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect, batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset,batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler,int batchSize=-1)
        {
            subscribe(host, port, tableName, handler, -1,batchSize);
        }

        public void unsubscribe(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName);
        }

        public void unsubscribe(string host, int port, string tableName, string actionName)
        {
            unsubscribeInternal(host, port, tableName, actionName);
        }
        
        public override void close()
        {
            base.close();
        }

        protected override void unsubscribeInternal(string host, int port, string tableName, string actionName)
        {
            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            try
            {
                lock (this)
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
                    lock (handlerLoopers){
                        handlerLoopers.Remove(topic);
                    }
                    @params = new List<IEntity>
                    {
                        new BasicString(localIP),
                        new BasicInt(listeningPort_),
                        new BasicString(tableName),
                        new BasicString(actionName)
                    };
                    subscribeInfo.close();
                    dbConn.run("stopPublishTable", @params);
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

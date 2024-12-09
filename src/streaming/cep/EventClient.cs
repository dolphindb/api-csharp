using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using dolphindb.data;
using dolphindb.streaming.cep;

namespace dolphindb.streaming.cep
{
    public class EventClient : AbstractClient
    {
        private Dictionary<string, HandlerLooper> handlerLoopers = new Dictionary<string, HandlerLooper>();
        public EventClient(List<EventSchema> eventSchema, List<string> eventTimeFields = null, List<string> commonFields = null) : base("", -1)
        {
            if(eventTimeFields == null) eventTimeFields = new List<string>();
            if(commonFields == null) commonFields = new List<string>();
            eventHandler_ = new EventHandler(eventSchema, eventTimeFields, commonFields);
        }

        EventHandler eventHandler_;

        class HandlerLooper
        {
            public Thread thread_;
            BlockingCollection<List<IMessage>> queue_;
            EventMessageHandler handler_;
            int batchSize_;
            float throttle_;
            MessageDispatcher dispatcher_;
            EventHandler eventHandler_;
            internal HandlerLooper(BlockingCollection<List<IMessage>> queue, EventMessageHandler handler, int batchSize, float throttle, MessageDispatcher dispatcher, EventHandler eventHandler)
            {
                this.queue_ = queue;
                this.handler_ = handler;
                this.batchSize_ = batchSize;
                this.throttle_ = throttle;
                this.dispatcher_ = dispatcher;
                this.eventHandler_ = eventHandler;
            }

            public void run()
            {
                try
                {
                    while (!dispatcher_.isClose())
                    {
                        List<IMessage> messages = null;
                        if (!this.queue_.TryTake(out messages, 1000))
                            continue;
                        List<string> eventTypes;
                        List<List<IEntity>> attributes;
                        try
                        {
                            eventTypes = new List<string>();
                            attributes = new List<List<IEntity>>();
                            eventHandler_.deserializeEvent(messages, eventTypes, attributes);
                            for (int i = 0; i < eventTypes.Count; ++i)
                            {
                                handler_.doEvent(eventTypes[i], attributes[i]);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("failed to doEvent: " + e.Message);
                            Console.Write(e.StackTrace);
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
                subscribe(site.host, site.port, subscribeInfo.getTableName(), subscribeInfo.getActionName(), null,
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

        public void subscribe(string host, int port, string tableName, string actionName, EventMessageHandler handler, long offset = -1, bool reconnect = true, string user = "", string password = "")
        {
            Utils.checkStringNotNullAndEmpty(host, "host");
            Utils.checkStringNotNullAndEmpty(tableName, "tableName");
            Utils.checkStringNotNullAndEmpty(actionName, "actionName");
            Utils.checkParamIsNull(handler, "handler");
            Utils.checkParamIsNull(user, "user");
            Utils.checkParamIsNull(password, "password");
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, null, -1, 1, null, user, password, true);
        }


        private void subscribe(string host, int port, string tableName, string actionName, EventMessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, float throttle, StreamDeserializer deserializer, string user, string password, bool createSubInfo)
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
                    string sql = "select top 0 * from " + tableName;
                    BasicTable table = (BasicTable)dbConn.run(sql);
                    eventHandler_.checkOutputTable(table, tableName);
                    List<IEntity> @params = new List<IEntity>
                    {
                        new BasicString(tableName),
                        new BasicString(actionName)
                    };
                    IEntity re = dbConn.run("getSubscriptionTopic", @params);
                    string topic = ((BasicAnyVector)re).getEntity(0).getString();
                    BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, filter, deserializer, user, password, createSubInfo, false);
                    if (createSubInfo)
                    {
                        lock (handlerLoopers)
                        {
                            HandlerLooper handlerLooper = new HandlerLooper(queue, handler, batchSize, throttle, this, eventHandler_);
                            handlerLooper.thread_ = new Thread(new ThreadStart(handlerLooper.run));
                            handlerLooper.thread_.Start();
                            handlerLoopers.Add(topic, handlerLooper);
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    dbConn.close();
                }
            }
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
                    lock (handlerLoopers)
                    {
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

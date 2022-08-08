using dolphindb.data;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Collections.Concurrent;

namespace dolphindb.streaming
{
    public abstract class AbstractClient : MessageDispatcher
    {
        protected static readonly int DEFAULT_PORT = 8849;
        protected static readonly string DEFAULT_HOST = "127.0.0.1";
        protected static readonly string DEFAULT_ACTION_NAME = "csharpStreamingApi";
        protected string listeningHost_;
        protected int listeningPort_;
        protected Thread pThread_;
        protected ConcurrentDictionary<string, string> HATopicToTrueTopic_ = new ConcurrentDictionary<string, string>();

        protected ConcurrentDictionary<string, SubscribeInfo> subscribeInfos_ = new ConcurrentDictionary<string, SubscribeInfo>();
        Deamon deamon_;
        bool isClose_ = false;

        public ConcurrentDictionary<string, SubscribeInfo> getSubscribeInfos()
        {
            return subscribeInfos_;
        }

        abstract protected bool doReconnect(SubscribeInfo subscribeInfo, Site site);
        
        public string getTopicForHATopic(string HATopic)
        {
            string topic = null;
            if (HATopicToTrueTopic_.TryGetValue(HATopic, out topic))
                return topic;
            else
            {
                System.Console.Out.WriteLine("Subscription with topic " + HATopic + " doesn't exist. ");
                return null;
            }
        }

        public bool tryReconnect(SubscribeInfo subscribeInfo)
        {
            Site[] sites = subscribeInfo.getSites();
            List<Site> activateSites = getActiveSites(sites);
            foreach (Site site in activateSites)
            {
                if (doReconnect(subscribeInfo, site))
                {
                    return true;
                }
            }
            return false;
            
        }

        public List<Site> getActiveSites(Site[] sites)
        {
            int siteId = 0;
            int siteNum = sites.Length;

            List<Site> activateSites = new List<Site>();
            while (siteId < siteNum)
            {
                siteId = siteId % siteNum;
                Site site = sites[siteId];
                siteId = (siteId + 1);
                try
                {
                    DBConnection conn = new DBConnection();
                    conn.connect(site.host, site.port);
                    try
                    {
                        conn.run("1");
                        activateSites.Add(site);
                    }
                    catch (Exception ioex)
                    {
                        throw ioex;
                    }
                    finally
                    {
                        conn.close();
                    }
                }
                catch (Exception ex)
                {
                    System.Console.Out.WriteLine(ex.Message);
                    System.Console.Out.WriteLine(ex.StackTrace);
                }
            }
            return activateSites;
        }

        public void activeCloseConnection(Site site)
        {
            try
            {
                DBConnection conn = new DBConnection();
                conn.connect(site.host, site.port);
                try
                {
                    string localIP = listeningHost_;
                    if (localIP == null || localIP.Equals(String.Empty))
                        localIP = conn.LocalAddress;
                    List<IEntity> @params = new List<IEntity>
                        {
                            new BasicString(localIP),
                            new BasicInt(listeningPort_)
                        };
                    conn.run("activeClosePublishConnection", @params);
                }
                catch (Exception ioex)
                {
                    throw ioex;
                }
                finally
                {
                    conn.close();
                }
                return;
            }
            catch (Exception)
            {
                Console.WriteLine("Unable to actively close the publish connection from site " + site.host + ":" + site.port);
            }
        }

        public AbstractClient(string subscribeHost, int subscribePort)
        {
            listeningHost_ = subscribeHost;
            listeningPort_ = subscribePort;
            deamon_ = new Deamon(subscribePort, this);
            pThread_ = new Thread(new ThreadStart(deamon_.run));
            deamon_.setThread(pThread_);
            pThread_.Start();
        }

        public BlockingCollection<List<IMessage>> getQueue(string HATopic)
        {
            string topic = getTopicForHATopic(HATopic);
            SubscribeInfo subscribeInfo = null;
            if (!subscribeInfos_.TryGetValue(topic, out subscribeInfo))
            {
                System.Console.Out.WriteLine("Subscription with topic " + topic + " doesn't exist. ");
                return null;
            }
            return subscribeInfo.getQueue();
        }

        public void dispatch(IMessage msg)
        {
            foreach (string topic in msg.getTopic().Split(','))
            {
                try
                {
                    BlockingCollection<List<IMessage>> queue = getQueue(topic);
                    if(queue != null)
                        queue.Add(new List<IMessage> { msg });
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
        }

        public void batchDispatch(List<IMessage> messages)
        {
            Dictionary<string, List<IMessage>> queMap = new Dictionary<string, List<IMessage>>();
            foreach (IMessage message in messages)
            {
                foreach (string topic in message.getTopic().Split(','))
                {
                    if (!queMap.ContainsKey(topic))
                    {
                        queMap[topic] = new List<IMessage>();
                    }
                    queMap[topic].Add(message);
                }
            }
            foreach(KeyValuePair<string, List<IMessage>> value in queMap)
            {
                try
                {
                    BlockingCollection<List<IMessage>> queue = getQueue(value.Key); 
                    if (queue != null)
                        queue.Add(value.Value);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
        }

        private String getTopic(String host, int port, String alias, String tableName, String actionName)
        {
            return String.Format("{0:G}:{1:G}:{2:G}/{3:G}/{4:G}", host, port, alias, tableName, actionName);
        }

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter)
        {
            return subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, null, "", "", true);
        }


        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, 
        StreamDeserializer deserializer, string user, string password, bool createSubInfo)
        {
            string topic = "";
            IEntity re;
            BlockingCollection<List<IMessage>> queue;

            DBConnection dbConn = new DBConnection();
            if(user != "" && password != "")
                dbConn.connect(host, port, user, password);
            else
                dbConn.connect(host, port);
            if (deserializer != null && !deserializer.isInited())
                deserializer.init(dbConn);
            if (deserializer != null)
            {
                BasicDictionary schema = (BasicDictionary)dbConn.run(tableName + ".schema()");
                deserializer.checkSchema(schema);
            }
            try
            {
                string localIP = listeningHost_;
                if (localIP == null || localIP.Equals(String.Empty))
                    localIP = dbConn.LocalAddress;

                List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(tableName),
                    new BasicString(actionName)
                };
                re = dbConn.run("getSubscriptionTopic", @params);
                topic = ((BasicAnyVector)re).getEntity(0).getString();
                @params.Clear();

                @params.Add(new BasicString(localIP));
                @params.Add(new BasicInt(listeningPort_));
                @params.Add(new BasicString(tableName));
                @params.Add(new BasicString(actionName));
                @params.Add(new BasicLong(offset));
                if (filter != null)
                    @params.Add(filter);
                re = dbConn.run("publishTable", @params);
                lock (subscribeInfos_)
                {
                    if (createSubInfo)
                    {
                        Site[] sites;
                        if (re is BasicAnyVector)
                        {
                            BasicStringVector HASiteStrings = (BasicStringVector)((BasicAnyVector)re).getEntity(1);
                            int HASiteNum = HASiteStrings.rows();
                            sites = new Site[HASiteNum];
                            for (int i = 0; i < HASiteNum; ++i)
                            {
                                String HASite = HASiteStrings.get(i).getString();
                                String[] HASiteHostAndPort = HASite.Split(':');
                                String HASiteHost = HASiteHostAndPort[0];
                                int HASitePort = int.Parse(HASiteHostAndPort[1]);
                                String HASiteAlias = HASiteHostAndPort[2];
                                sites[i] = new Site(HASiteHost, HASitePort);
                                String HATopic = getTopic(HASiteHost, HASitePort, HASiteAlias, tableName, actionName);
                                HATopicToTrueTopic_[HATopic] = topic;
                            }
                        }
                        else
                        {
                            sites = new Site[] { new Site(host, port) };
                            lock (HATopicToTrueTopic_)
                            {
                                HATopicToTrueTopic_[topic] = topic;
                            }
                        }
                        SubscribeInfo subscribeInfo = new SubscribeInfo(DateTime.Now, new BlockingCollection<List<IMessage>>(), sites, topic, offset, reconnect, filter, handler, tableName, actionName, deserializer, user, password);
                        subscribeInfo.setConnectState(ConnectState.REQUEST);
                        queue = subscribeInfo.getQueue();
                        if (subscribeInfos_.ContainsKey(topic))
                        {
                            throw new Exception("Subscription with topic " + topic + " exist. ");
                        }
                        else
                        {
                            subscribeInfos_.TryAdd(topic, subscribeInfo);
                        }
                        Console.WriteLine("Successfully subscribed table " + topic);
                    }
                    else
                    {
                        SubscribeInfo subscribeInfo = null;
                        if(!subscribeInfos_.TryGetValue(topic, out subscribeInfo))
                        {
                            throw new Exception("Subscription with topic " + topic + " doesn't exist. ");
                        }
                        lock (subscribeInfo)
                        {
                            if(subscribeInfo.getConnectState() == ConnectState.RECEIVED_SCHEMA)
                            {
                                throw new Exception("Subscription with topic " + topic + " the connection has been created. ");
                            }
                            subscribeInfo.setConnectState(ConnectState.REQUEST);
                        }
                        queue = subscribeInfo.getQueue();
                    }
                }

            }
            finally
            {
                dbConn.close();
            }

            return queue;
        }

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, long offset, bool reconnect)
        {
            return subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, null);
        }

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, long offset)
        {
            return subscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME, offset, false);
        }

        protected virtual void unsubscribeInternal(string host, int port, string tableName, string actionName)
        {
            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            try
            {
                string localIP = listeningHost_;
                if (localIP == null || localIP.Equals(String.Empty))
                    localIP = dbConn.LocalAddress; 
                lock (subscribeInfos_)
                {
                    List<IEntity> @params = new List<IEntity>
                    {
                    new BasicString(tableName),
                    new BasicString(actionName)
                    };
                    IEntity re = dbConn.run("getSubscriptionTopic", @params);
                    string topic = ((BasicAnyVector)re).getEntity(0).getString();
                    SubscribeInfo subscribeInfo;
                    if (!subscribeInfos_.TryRemove(topic, out subscribeInfo))
                        throw new Exception("Subscription with topic " + topic + " doesn't exist. ");
                    lock (subscribeInfo)
                    {
                        subscribeInfo.close();
                        @params = new List<IEntity>
                        {
                            new BasicString(localIP),
                            new BasicInt(listeningPort_),
                            new BasicString(tableName),
                            new BasicString(actionName)
                        };
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

        protected void unsubscribeInternal(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
        }

        public void close()
        {
            isClose_ = true;
            deamon_.close();
        }

        public bool isClose()
        {
            return isClose_;
        }
    }
}

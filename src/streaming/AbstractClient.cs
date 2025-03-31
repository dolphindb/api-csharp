using dolphindb.data;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Runtime.ConstrainedExecution;
using dolphindb.route;

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


        private Deamon deamon_ = null;
        bool isClose_ = false;
        private BlockingCollection<DBConnection> connList = new BlockingCollection<DBConnection>();

        DBVersion dbVersion = null;

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
                        
                        List<IEntity> args = new List<IEntity>();
                        conn.run("version", args);
                        activateSites.Add(site);
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

        private void activeCloseConnection(Site site, string actionName, string tableName, DBConnection conn)
        {
            try
            {
                List<IEntity> @params;
                if (dbVersion.getSubV(0) >= 2 && dbVersion.getSubV(1) >= 0 && dbVersion.getSubV(2) >= 9)
                {
                    @params = new List<IEntity>
                    {
                        new BasicString(actionName),
                        new BasicString(tableName)
                    };
                }
                else
                {
                    @params = new List<IEntity>
                    {
                        new BasicString(site.host),
                        new BasicInt(site.port)
                    };
                }
                if (dbVersion.getVerNum() >= 995)
                {
                    @params.Add(new BasicBoolean(true));
                }
                conn.run("activeClosePublishConnection", @params);
            }
            catch (Exception)
            {
                Console.WriteLine("Unable to actively close the publish connection from site " + site.host + ":" + site.port);
                throw;
            }
        }

        public AbstractClient(string subscribeHost, int subscribePort)
        {
            listeningHost_ = subscribeHost;
            listeningPort_ = subscribePort;
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
            return subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter, null, "", "", true, false);
        }


        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter,
        StreamDeserializer deserializer, string user, string password, bool createSubInfo, bool msgAsTable)
        {
            if (deserializer != null && msgAsTable)
            {
                throw new Exception("Cannot set deserializer when msgAsTable is true. ");
            }
            checkServerVersion(host, port);
            string topic = "";
            IEntity re;
            BlockingCollection<List<IMessage>> queue;

            DBConnection dbConn;
            if( listeningPort_ > 0)
            {
                dbConn = new DBConnection();
            }
            else
            {
                dbConn = new DBConnection(false, false, false,false, true);
            }

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

                if (!createSubInfo)
                    // close active connection before reconnect
                    activeCloseConnection(new Site(localIP, listeningPort_), actionName, tableName, dbConn);

                List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(tableName),
                    new BasicString(actionName)
                };

                re = dbConn.run("getSubscriptionTopic", @params);
                topic = ((BasicAnyVector)re).getEntity(0).getString();
                List<string> colsName = new List<string>();
                if(!(((BasicAnyVector)re).getEntity(1) is data.Void))
                {
                    AbstractVector paramCols = (AbstractVector)((BasicAnyVector)re).getEntity(1);
                    for (int i = 0; i < paramCols.rows(); ++i)
                    {
                        colsName.Add(paramCols.getEntity(i).getString());
                    }
                }
                @params.Clear();

                @params.Add(new BasicString(localIP));
                @params.Add(new BasicInt(listeningPort_));
                @params.Add(new BasicString(tableName));
                @params.Add(new BasicString(actionName));
                @params.Add(new BasicLong(offset));
                if (filter != null)
                    @params.Add(filter);
                re = dbConn.run("publishTable", @params);
                connList.Add(dbConn);
                lock (this)
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
                        SubscribeInfo subscribeInfo = new SubscribeInfo(DateTime.Now, new BlockingCollection<List<IMessage>>(4096), sites, topic, offset - 1, reconnect, filter, handler, tableName, actionName, deserializer, user, password, msgAsTable, colsName);
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
                        if(subscribeInfo.getConnectState() == ConnectState.RECEIVED_SCHEMA)
                        {
                            throw new Exception("Subscription with topic " + topic + " the connection has been created. ");
                        }
                        subscribeInfo.setConnectState(ConnectState.REQUEST);
                        queue = subscribeInfo.getQueue();
                    }
                }

            }
            finally
            {
                if(listeningPort_ > 0 )
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
                lock (this)
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
                    subscribeInfo.close();
                    @params = new List<IEntity>
                    {
                        new BasicString(localIP),
                        new BasicInt(listeningPort_),
                        new BasicString(tableName),
                        new BasicString(actionName)
                    };
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

        protected void unsubscribeInternal(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
        }

        private void checkServerVersion(string host, int port)
        {
            DBConnection conn = null;
            try
            {
                conn = new DBConnection();
                conn.connect(host, port);
                List<IEntity> args = new List<IEntity>();
                string version = conn.run("version", args).getString();
                dbVersion = new DBVersion(version);
                int v0 = dbVersion.getSubV(0);
                int v1 = dbVersion.getSubV(1);
                int v2 = dbVersion.getSubV(2);
                if ((v0 == 2 && v1 == 0 && v2 >= 9) || (v0 == 2 && v1 == 10) || (v0 == 3 && v1 == 0 && v2 >= 0))
                {
                    // server only support reverse connection
                    listeningPort_ = 0;
                }
                else
                {
                    // server Not support reverse connection
                    if (listeningPort_ == 0)
                    {
                        throw new IOException("The server does not support subscription through reverse connection (connection initiated by the subscriber). Specify a valid port parameter.");
                    }
                }
                if (deamon_ == null)
                {
                    lock (this)
                    {
                        if (deamon_ == null)
                        {
                            deamon_ = new Deamon(this.listeningPort_, this, connList);
                            pThread_ = new Thread(new ThreadStart(deamon_.run));
                            deamon_.setThread(pThread_);
                            pThread_.Start();
                        }
                    }
                }
            }
            finally
            {
                conn.close();
            }
        }
        public virtual void close()
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

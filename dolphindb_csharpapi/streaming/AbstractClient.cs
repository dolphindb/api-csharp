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
        protected static readonly string DEFAULT_HOST = "localhost";
        protected static readonly string DEFAULT_ACTION_NAME = "csharpStreamingApi";
        protected string listeningHost;
        protected int listeningPort;
        protected QueueManager queueManager = new QueueManager();
        protected Dictionary<string, List<IMessage>> messageCache = new Dictionary<string, List<IMessage>>();
        protected Dictionary<string, string> tableNameToTopic = new Dictionary<string, string>();
        protected Dictionary<string, bool> hostEndian = new Dictionary<string, bool>();
        protected Thread pThread;
        protected Dictionary<string, Site> topicToSite = new Dictionary<string, Site>();

        protected class Site
        {
            public string host;
            public int port;
            public string tableName;
            public string actionName;
            public MessageHandler handler;
            public long msgId;
            public bool reconnect;
            public IVector filter = null;
            public bool closed = false;

            public Site(string host, int port, string tableName, string actionName, MessageHandler handler, long msgId, bool reconnect, IVector filter)
            {
                this.host = host;
                this.port = port;
                this.tableName = tableName;
                this.actionName = actionName;
                this.handler = handler;
                this.msgId = msgId;
                this.reconnect = reconnect;
                this.filter = filter;
            }
        }

        abstract protected void doReconnect(Site site);

        public void setMsgId(string topic, long msgId)
        {
            lock (topicToSite)
            {
                Site site = topicToSite[topic];
                if (site != null)
                    site.msgId = msgId;
            }
        }

        public void tryReconnect(string topic)
        {
            Console.WriteLine("Trigger reconnect");
            queueManager.removeQueue(topic);
            Site site = null;
            lock (topicToSite)
            {
                site = topicToSite[topic];
            }
            if (site == null || !site.reconnect)
                return;
            tableNameToTopic.Remove(site.host + ":" + site.port + ":" + site.tableName);
            topicToSite.Remove(topic);
            activeCloseConnection(site);
            doReconnect(site);
        }

        private void activeCloseConnection(Site site)
        {
            while (true)
            {
                try
                {
                    DBConnection conn = new DBConnection();
                    conn.connect(site.host, site.port);
                    try
                    {
                        string localIP = listeningHost;
                        if (localIP == null || localIP.Equals(String.Empty))
                            localIP = conn.LocalAddress;
                        List<IEntity> @params = new List<IEntity>
                        {
                            new BasicString(localIP),
                            new BasicInt(listeningPort)
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
                Thread.Sleep(1000);
            }
        }

        public AbstractClient() : this(DEFAULT_PORT) { }

        public AbstractClient(int subscribePort)
        {
            listeningPort = subscribePort;
            Deamon deamon = new Deamon(subscribePort, this);
            pThread = new Thread(new ThreadStart(deamon.run));
            pThread.Start();
        }

        public AbstractClient(string subscribeHost, int subscribePort)
        {
            listeningHost = subscribeHost;
            listeningPort = subscribePort;
            Deamon deamon = new Deamon(subscribePort, this);
            pThread = new Thread(new ThreadStart(deamon.run));
            pThread.Start();
        }
        private void addMessageToCache(IMessage msg)
        {
            string topic = msg.getTopic();
            if (!messageCache.TryGetValue(topic, out List<IMessage> cache))
            {
                cache = new List<IMessage>();
                messageCache.Add(topic, cache);
            }
            cache.Add(msg);
        }

        private void flushToQueue()
        {
            foreach (string topic in messageCache.Keys)
            {
                try
                {
                    queueManager.getQueue(topic).Add(messageCache[topic]);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
            messageCache.Clear();
        }

        public void dispatch(IMessage msg)
        {
            BlockingCollection<List<IMessage>> queue = queueManager.getQueue(msg.getTopic());
            try
            {
                queue.Add(new List<IMessage> { msg });
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
            }
        }

        public void batchDispatch(List<IMessage> messages)
        {
            foreach (IMessage message in messages)
            {
                addMessageToCache(message);
            }
            flushToQueue();
        }

        public bool isRemoteLittleEndian(string host)
        {
            if (hostEndian.ContainsKey(host))
                return hostEndian[host];
            else
                return false;
        }

        public bool isClosed(string topic)
        {
            lock (topicToSite)
            {
                Site site = topicToSite[topic];
                if (site != null)
                    return site.closed;
                else
                    return true;
            }
        }

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter)
        {
            string topic = "";
            IEntity re;

            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            try
            {
                string localIP = listeningHost;
                if (localIP == null || localIP.Equals(String.Empty))
                    localIP = dbConn.LocalAddress;

                if (!hostEndian.ContainsKey(host))
                    hostEndian.Add(host, dbConn.RemoteLittleEndian);

                List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(tableName),
                    new BasicString(actionName)
                };
                re = dbConn.run("getSubscriptionTopic", @params);
                topic = ((BasicAnyVector)re).getEntity(0).getString();
                @params.Clear();

                lock (tableNameToTopic)
                {
                    tableNameToTopic.Add(host + ":" + port + ":" + tableName, topic);
                }
                lock (topicToSite)
                {
                    topicToSite.Add(topic, new Site(host, port, tableName, actionName, handler, offset - 1, reconnect, filter));
                }

                @params.Add(new BasicString(localIP));
                @params.Add(new BasicInt(listeningPort));
                @params.Add(new BasicString(tableName));
                @params.Add(new BasicString(actionName));
                @params.Add(new BasicLong(offset));
                if (filter != null)
                    @params.Add(filter);
                re = dbConn.run("publishTable", @params);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                dbConn.close();
            }
            
            BlockingCollection<List<IMessage>> queue = queueManager.addQueue(topic);
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

        protected void unsubscribeInternal(string host, int port, string tableName, string actionName)
        {
            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            try
            {
                string localIP = dbConn.LocalAddress;
                List<IEntity> @params = new List<IEntity>
                {
                    new BasicString(localIP),
                    new BasicInt(listeningPort),
                    new BasicString(tableName),
                    new BasicString(actionName)
                };
                dbConn.run("stopPublishTable", @params);
                string topic = null;
                string fullTableName = host + ":" + port + ":" + tableName;
                if (tableNameToTopic.Count > 0) { 
                    lock (tableNameToTopic)
                    {
                        topic = tableNameToTopic[fullTableName];
                    }
                }
                if ( topicToSite.Count>0 ) { 
                    lock (topicToSite)
                    {
                        Site site = topicToSite[topic];
                        if (site != null)
                            site.closed = true;
                    }
                }
                Console.WriteLine("Successfully unsubscribed table " + fullTableName);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw ex;
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
    }
}

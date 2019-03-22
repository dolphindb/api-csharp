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
        protected string localIP;
        protected int listeningPort;
        protected QueueManager queueManager = new QueueManager();
        protected Dictionary<string, List<IMessage>> messageCache = new Dictionary<string, List<IMessage>>();
        protected Dictionary<string, string> tableName2Topic = new Dictionary<string, string>();
        protected Dictionary<string, bool> hostEndian = new Dictionary<string, bool>();
        protected Thread pThread;

        public AbstractClient() : this(DEFAULT_PORT) { }

        public AbstractClient(int subscribePort)
        {
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

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, string actionName, long offset)
        {
            IEntity re;
            string topic = "";

            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
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
            BlockingCollection<List<IMessage>> queue = queueManager.addQueue(topic);
            @params.Clear();

            tableName2Topic.Add(host + ":" + port + ":" + tableName, topic);

            @params.Add(new BasicString(localIP));
            @params.Add(new BasicInt(listeningPort));
            @params.Add(new BasicString(tableName));
            @params.Add(new BasicString(actionName));
            if (offset != -1)
                @params.Add(new BasicLong(offset));
            dbConn.run("publishTable", @params);

            dbConn.close();
            return queue;
        }

        protected BlockingCollection<List<IMessage>> subscribeInternal(string host, int port, string tableName, long offset)
        {
            return subscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME, offset);
        }

        protected void unsubscribeInternal(string host, int port, string tableName, string actionName)
        {
            DBConnection dbConn = new DBConnection();
            dbConn.connect(host, port);
            localIP = dbConn.LocalAddress;
            List<IEntity> @params = new List<IEntity>
            {
                new BasicString(localIP),
                new BasicInt(port),
                new BasicString(tableName),
                new BasicString(actionName)
            };
            dbConn.run("stopPublishTable", @params);
            dbConn.close();
        }

        protected void unsubscribeInternal(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
        }
    }
}

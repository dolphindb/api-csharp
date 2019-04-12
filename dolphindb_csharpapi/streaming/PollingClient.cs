using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using dolphindb.data;
using System.Threading;

namespace dolphindb.streaming
{
    public class PollingClient : AbstractClient
    {
        private TopicPoller topicPoller = null;

        public PollingClient(int subscribePort) : base(subscribePort) { }

        override protected void doReconnect(Site site)
        {
            while (true)
            {
                try
                {
                    Thread.Sleep(5000);
                    BlockingCollection<List<IMessage>> queue = subscribeInternal(site.host, site.port, site.tableName, site.actionName, null, site.msgId + 1, true, site.filter);
                    Console.WriteLine("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
                    topicPoller.setQueue(queue);
                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Unable to subscribe table. Will try again after 5 seconds.");
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, filter);
            return new TopicPoller(queue);
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect)
        {
            return subscribe(host, port, tableName, actionName, offset, reconnect, null);
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset, IVector filter)
        {
            return subscribe(host, port, tableName, actionName, offset, false, filter);
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset)
        {
            return subscribe(host, port, tableName, actionName, offset, false, null);
        }

        public TopicPoller subscribe(string host, int port, string tableName, long offset, bool reconnect)
        {
            return subscribe(host, port, tableName, DEFAULT_ACTION_NAME, offset, reconnect);
        }

        public TopicPoller subscribe(string host, int port, string tableName, long offset)
        {
            return subscribe(host, port, tableName, DEFAULT_ACTION_NAME, offset);
        }

        public TopicPoller subscribe(string host, int port, string tableName)
        {
            return subscribe(host, port, tableName, -1);
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName)
        {
            return subscribe(host, port, tableName, actionName, -1);
        }

        public void unsubscribe(string host, int port, string tableName, string actionName)
        {
            unsubscribeInternal(host, port, tableName, actionName);
        }

        public void unsubscribe(string host, int port, string tableName)
        {
            unsubscribeInternal(host, port, tableName, DEFAULT_ACTION_NAME);
        }
    }
}

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

        public PollingClient(int subscribePort) : base(DEFAULT_HOST, subscribePort) { }

        public PollingClient(string subscribeHost, int subscribePort) : base(subscribeHost, subscribePort) { }

        override protected bool doReconnect(SubscribeInfo subscribeInfo, Site site)
        {
            try
            {
                BlockingCollection<List<IMessage>> queue = subscribeInternal(site.host, site.port, subscribeInfo.getTableName(), subscribeInfo.getActionName(), null, subscribeInfo.getMsgId() + 1, true, subscribeInfo.getFilter(), subscribeInfo.getDeseriaLizer(), subscribeInfo.getUser(), subscribeInfo.getPassword(), false);
                Console.WriteLine("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + subscribeInfo.getTableName());
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            return false;
        }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, null, offset, reconnect, filter, deserializer, user, password, true);
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

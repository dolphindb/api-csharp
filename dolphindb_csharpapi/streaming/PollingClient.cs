using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;

namespace dolphindb.streaming
{
    public class PollingClient : AbstractClient
    {
        public PollingClient(int subscribePort) : base(subscribePort) { }

        public TopicPoller subscribe(string host, int port, string tableName, string actionName, long offset)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, offset);
            return new TopicPoller(queue);
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

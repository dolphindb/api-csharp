using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;

namespace dolphindb.streaming
{
    using QueueHandlerBinder = Tuple<BlockingCollection<List<IMessage>>, MessageHandler>;
    public class ThreadPooledClient : AbstractClient
    {
        private Dictionary<string, QueueHandlerBinder> queueHandlers = new Dictionary<string, QueueHandlerBinder>();

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

        public ThreadPooledClient() : this(DEFAULT_PORT) { }

        public ThreadPooledClient(int subscribePort) : base(subscribePort)
        {
            Queue<IMessage> backlog = new Queue<IMessage>();

            bool fillBacklog()
            {
                bool filled = false;
                lock (queueHandlers)
                {
                    foreach (QueueHandlerBinder binder in queueHandlers.Values)
                    {
                        if (binder.Item1.TryTake(out List<IMessage> messages))
                        {
                            messages.ForEach(backlog.Enqueue);
                            filled = true;
                        }
                    }
                }
                return filled;
            }

            void run()
            {
                while (true)
                {
                    while (backlog.Count > 0)
                    {
                        IMessage msg = backlog.Dequeue();
                        QueueHandlerBinder binder;
                        lock (queueHandlers)
                        {
                            binder = queueHandlers[msg.getTopic()];
                        }
                        HandlerRunner handlerRunner = new HandlerRunner(binder.Item2, msg);
                        ThreadPool.QueueUserWorkItem(new WaitCallback(handlerRunner.run));
                    }
                    fillBacklog();
                }
            }

            new Thread(new ThreadStart(run)).Start();
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, offset);
            lock (queueHandlers)
            {
                queueHandlers.Add(tableName2Topic[host + ":" + port + ":" + tableName], new QueueHandlerBinder(queue, handler));
            }
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
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using dolphindb.data;

namespace dolphindb.streaming
{
    using QueueHandlerBinder = Tuple<BlockingCollection<List<IMessage>>, MessageHandler>;

    public class ThreadPooledClient : AbstractClient
    {
        private Dictionary<string, QueueHandlerBinder> queueHandlers = new Dictionary<string, QueueHandlerBinder>();
        private Thread thread;

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

        class TaskPool
        {
            private int threadsMaxCount;

            public TaskPool(int threadsMaxCount)
            {
                this.threadsMaxCount = threadsMaxCount;
            }
        }

        public ThreadPooledClient() : this(DEFAULT_PORT) { }

        public ThreadPooledClient(string subscribeHost, int subscribePort) : base(subscribeHost,subscribePort) {
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

            thread = new Thread(new ThreadStart(run));
            thread.Start();
        }

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

            thread = new Thread(new ThreadStart(run));
            thread.Start();
        }

        protected override void doReconnect(Site site)
        {
            if (thread.IsAlive)
                thread.Interrupt();
            while (true)
            {
                try
                {
                    Thread.Sleep(5000);
                    subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter);
                    Console.WriteLine("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Unable to subscribe table. Will try again after 5 seconds.");
                    Console.WriteLine(ex.ToString());
                }
            }
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter);
            lock (queueHandlers)
            {
                queueHandlers.Add(tableNameToTopic[host + ":" + port + ":" + tableName], new QueueHandlerBinder(queue, handler));
            }
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, filter);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect)
        {
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, null);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, null);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, false, filter);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect, null);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, false, null);
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

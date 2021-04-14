using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using dolphindb.data;

namespace dolphindb.streaming
{
    public class ThreadedClient : AbstractClient
    {
        private HandlerLooper handlerLooper = null;
        private Thread thread = null;
        public ThreadedClient() : this(DEFAULT_PORT) { }

        public ThreadedClient(int subscribePort) : base(subscribePort) { }

        public ThreadedClient(string subscribeHost, int subscribePort) : base(subscribeHost, subscribePort) { }

        class HandlerLooper
        {
            BlockingCollection<List<IMessage>> queue;
            MessageHandler handler;
            int batchSize;
            internal HandlerLooper(BlockingCollection<List<IMessage>> queue, MessageHandler handler,int batchSize)
            {
                this.queue = queue;
                this.handler = handler;
                this.batchSize = batchSize;
            }

            public void run()
            {
                while (true)
                {
                    try
                    {
                        if (batchSize == -1)
                        {
                            List<IMessage> messages = queue.Take();
                            foreach (IMessage message in messages)
                                handler.doEvent(message);
                        }
                        else
                        {
                            if (queue.Count < batchSize)
                                continue;
                            for (int i = 0; i < batchSize; i++)
                            {
                                List<IMessage> messages = queue.Take();
                                handler.batchHandler(messages);
                            }
                        }

                    }
                    catch (ThreadInterruptedException)
                    {
                        Console.WriteLine("Handler thread stopped.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Console.Write(ex.StackTrace);
                    }
                }
            }
        }

        protected override void doReconnect(Site site)
        {
            if (handlerLooper == null || thread == null)
                throw new Exception("Subscribe thread is not started");
            thread.Interrupt();
            while (true)
            {
                try
                {
                    Thread.Sleep(5000);
                    subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true, site.filter,-1);
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

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter,int batchSize)
        {
            if (!(batchSize == -1 || batchSize > 0))
                throw new Exception("Invalid batchSize value, should be -1 or positive integer.");
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, filter);
            handlerLooper = new HandlerLooper(queue, handler,batchSize);
            thread = new Thread(new ThreadStart(handlerLooper.run));
            thread.Start();
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, filter,batchSize);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, reconnect, null,batchSize);
        }

        public void subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset,int batchSize=-1)
        {
            subscribe(host, port, tableName, actionName, handler, offset, false, null,batchSize);
        }


        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, IVector filter,int batchSize=-1)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, filter,batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset, bool reconnect,int batchSize=-1)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset, reconnect,batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset,int batchSize=-1)
        {
            subscribe(host, port, tableName, DEFAULT_ACTION_NAME, handler, offset,batchSize);
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler,int batchSize=-1)
        {
            subscribe(host, port, tableName, handler, -1,batchSize);
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

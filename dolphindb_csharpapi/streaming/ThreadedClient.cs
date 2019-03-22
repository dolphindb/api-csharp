using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;

namespace dolphindb.streaming
{
    public class ThreadedClient : AbstractClient
    {
        public ThreadedClient() : this(DEFAULT_PORT) { }

        public ThreadedClient(int subscribePort) : base(subscribePort) { }

        class HandlerLooper
        {
            BlockingCollection<List<IMessage>> queue;
            MessageHandler handler;

            internal HandlerLooper(BlockingCollection<List<IMessage>> queue, MessageHandler handler)
            {
                this.queue = queue;
                this.handler = handler;
            }

            public void run()
            {
                while (true)
                {
                    try
                    {
                        List<IMessage> messages = queue.Take();
                        foreach (IMessage message in messages)
                            handler.doEvent(message);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Console.Write(ex.StackTrace);
                    }
                }
            }
        }

        public void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
        {
            BlockingCollection<List<IMessage>> queue = subscribeInternal(host, port, tableName, offset);
            HandlerLooper handlerLooper = new HandlerLooper(queue, handler);
            Thread thread = new Thread(new ThreadStart(handlerLooper.run));
            thread.Start();
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

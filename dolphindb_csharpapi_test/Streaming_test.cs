using dolphindb.data;
using dolphindb.streaming;
using dolphindb;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace dolphindb_csharp_api_test
{
    [TestClass]
    public class Streaming_test
    {
        private readonly int SUB_PORT = 8802;
        //private static readonly string REMOTE_HOST = "192.168.1.104";
        //private static readonly int REMOTE_PORT = 8848;
        private readonly string REMOTE_HOST = "192.168.1.106";
        private readonly int REMOTE_PORT = 8701;
        private readonly string REMOTE_TABLE_NAME = "trades";
        private readonly int TIMEOUT = 1000;

        public Streaming_test() { }
        [TestMethod]
        public void testPolling()
        {
            PollingClient client = new PollingClient(SUB_PORT);
            try
            {
                TopicPoller poller = client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, 0);
                int count = 0;
                bool started = false;
                long start = DateTime.Now.Ticks;
                long last = DateTime.Now.Ticks;
                while (true)
                {
                    List<IMessage> messages = poller.poll(TIMEOUT);
                    if (messages == null || messages.Count == 0)
                    {
                        start = DateTime.Now.Ticks;
                        continue;
                    }
                    int messageCount = messages.Count;
                    if (messageCount > 0 && !started)
                    {
                        started = true;
                        start = DateTime.Now.Ticks;
                    }
                    count += messageCount;
                    foreach (IMessage message in messages)
                    {
                        string symbol = message.getEntity(0).getString();
                        string price = message.getEntity(1).getString();
                        string size = message.getEntity(2).getString();
                        string ex = message.getEntity(3).getString();
                    }

                    if (messageCount > 0)
                    {
                        if (((BasicInt)messages.Last().getEntity(4)).getValue() == -1)
                            break;
                    }
                    long now = DateTime.Now.Ticks;
                    if (now - last >= 1000)
                    {
                        long batchEnd = DateTime.Now.Ticks;
                        Console.WriteLine(count + " messages took " + ((batchEnd - start) / 1000.0) + " ms, throghput: " + count / ((batchEnd - start) / 1000000.0) + " messages/s");
                        last = now;
                    }
                }
                long end = DateTime.Now.Ticks;
                Console.WriteLine(count + " messages took " + ((end - start) / 1000.0) + " ms, throghput: " + count / ((end - start) / 1000000.0) + " messages/s");
                System.Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
        }
        [TestMethod]
        public void testThreaded()
        {
            ThreadedClient client = new ThreadedClient(SUB_PORT);
            try
            {
                client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, new SampleMessageHandler(), 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
        }
        [TestMethod]
        public void testPooledThread()
        {
            ThreadPooledClient client = new ThreadPooledClient(SUB_PORT);
            try
            {
                client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, new SampleMessageHandler(), 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
        }
        [TestMethod]
        public void testThreadUnSubscribe()
        {
            ThreadedClient client = new ThreadedClient(SUB_PORT);
            try
            {
                client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, new SampleMessageHandler(), 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
            ThreadedClient client1 = new ThreadedClient(SUB_PORT);
            try { 
                client1.unsubscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME);
            }
            catch (Exception e){
                Assert.Fail(e.Message);
            }

        }

        [TestMethod]
        public void testPollingUnSubscribe()
        {
            PollingClient client = new PollingClient(SUB_PORT);
            try
            {
                client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
            PollingClient client1 = new PollingClient(SUB_PORT);
            try
            {
                client1.unsubscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME);
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
        }
        [TestMethod]
        public void testPollingThreadUnSubscribe()
        {
            ThreadPooledClient client = new ThreadPooledClient(SUB_PORT);
            try
            {
                client.subscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME, new SampleMessageHandler(), 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
                Assert.Fail(ex.Message);
            }
            ThreadPooledClient client1 = new ThreadPooledClient(SUB_PORT);
            try
            {
                client1.unsubscribe(REMOTE_HOST, REMOTE_PORT, REMOTE_TABLE_NAME);
            }
            catch (Exception e)
            {
                Assert.Fail(e.Message);
            }
        }
        internal class SampleMessageHandler : MessageHandler
        {
            private bool started = false;
            private long start = 0;
            private long count = 0;

            public SampleMessageHandler() { }

            public void doEvent(IMessage msg)
            {
                if (!started)
                {
                    started = true;
                    start = DateTime.Now.Ticks;
                }
                
                Interlocked.Increment(ref count);

                long countVal = Interlocked.Read(ref count);
                if (countVal % 100000 == 0)
                {
                    long end = DateTime.Now.Ticks;
                    Console.WriteLine(countVal + " messages took " + (end - start) + "ms, throughput: " + countVal / ((end - start) / 1000000.0) + " messages/s");
                }
                if (countVal == 2000000)
                    Console.WriteLine("Done");
            }
        }
    }
}

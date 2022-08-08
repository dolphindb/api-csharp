
using dolphindb;
using dolphindb.data;
using dolphindb.streaming;
using System;
using System.Collections.Generic;
using System.IO;

public class StreamingData
{
    private static DBConnection conn;
    public static string HOST = "localhost";
    public static int PORT = 8848;
    public static ThreadedClient client;
    public static char METHOD = 'P';
    public static int subscribePORT = 8892;

    public virtual void createStreamTable()
    {
        conn.login("admin", "123456", false);
        conn.run("share streamTable(30000:0,`id`time`sym`qty`price,[INT,TIME,SYMBOL,INT,DOUBLE]) as Trades\n");
        conn.run("def saveData(data){ Trades.tableInsert(data)}");
    }

    public virtual void PollingClient()
    {
        PollingClient client = new PollingClient(subscribePORT);
        try
        {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades");
            int count = 0;
            bool started = false;
            long start = DateTime.Now.Ticks;
            long last = DateTime.Now.Ticks;
            while (true)
            {
                List<IMessage> msgs = poller1.poll(1000);
                if (msgs == null)
                {
                    count = 0;
                    start = DateTime.Now.Ticks;
                    continue;
                }
                if (msgs.Count > 0 && started == false)
                {
                    started = true;
                    start = DateTime.Now.Ticks;
                }

                count += msgs.Count;
                for (int i = 0; i < msgs.Count; ++i)
                {
                    BasicTime time = (BasicTime)msgs[i].getEntity(1);
                    Console.Write("time:" + time + " ");
                    string symbol = msgs[i].getEntity(2).getString();
                    Console.Write("sym:" + symbol + " ");
                    int qty = ((BasicInt)msgs[i].getEntity(3)).getValue();
                    Console.Write("qty:" + qty + " ");
                    double? price = ((BasicDouble)msgs[i].getEntity(4)).getValue();
                    Console.Write("price:" + price + " \n");
                }
                if (msgs.Count > 0)
                {
                    if (((BasicInt)msgs[msgs.Count - 1].getEntity(0)).getValue() == -1)
                    {
                        break;
                    }
                }
                long now = DateTime.Now.Ticks;
                if (now - last >= 1000)
                {
                    long endtime = DateTime.Now.Ticks;
                    Console.WriteLine(count + " messages took " + (endtime - start) + "ms, throughput: " + count / ((endtime - start) / 1000.0) + " messages/s");
                    last = now;
                }

            }
            long end = DateTime.Now.Ticks;
            Console.WriteLine(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
            client.unsubscribe(HOST, PORT, "Trades");
        }
        catch (IOException e)
        {
            Console.WriteLine(e.ToString());
            Console.Write(e.StackTrace);
        }
        Environment.Exit(0);
    }


    internal class SampleMessageHandler : MessageHandler
    {
        private long start = 0;

        private bool started = false;

        public SampleMessageHandler() { }

        public void doEvent(IMessage msg)
        {
            if (started == false)
            {
                started = true;
                start = DateTime.Now.Ticks;
            }
            BasicTime time = (BasicTime)msg.getEntity(1);
            Console.WriteLine("time:" + time + " ");
            string symbol = msg.getEntity(2).getString();
            Console.WriteLine("sym:" + symbol + " ");
            int qty = ((BasicInt)msg.getEntity(3)).getValue();
            Console.WriteLine("qty:" + qty + " ");
            double price = ((BasicDouble)msg.getEntity(4)).getValue();
            Console.WriteLine("price:" + price + " \n");
        }

        void MessageHandler.batchHandler(List<IMessage> msgs)
        {
            throw new NotImplementedException();
        }
    }

    public void ThreadedClient()
    {
        ThreadedClient client = new ThreadedClient(subscribePORT);
        try {
            client.subscribe(HOST, PORT, "Trades", new SampleMessageHandler());
        } catch (IOException e) {
            Console.WriteLine(e.ToString());
            Console.Write(e.StackTrace);
        }
    }

    public static void Main(string[] args)
    {
        if (args.Length == 4)
        {
            try
            {
                HOST = args[0];
                PORT = int.Parse(args[1]);
                subscribePORT = int.Parse(args[2]);
                METHOD = args[3][0];
                if (METHOD != 'p' && METHOD != 'P' && METHOD != 'T' && METHOD != 't')
                {
                    throw new Exception("the 4th parameter 'subscribeMethod' must be 'P' or 'T'");
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Wrong arguments");
            }
        }
        else if (args.Length != 4 && args.Length != 0)
        {
            Console.WriteLine("wrong arguments");
            return;
        }
        conn = new DBConnection();
        try
        {
            conn.connect(HOST, PORT);
        }
        catch (IOException e)
        {
            Console.WriteLine("Connection error");
            Console.WriteLine(e.ToString());
            Console.Write(e.StackTrace);
        }
        try
        {
            (new StreamingData()).createStreamTable();
        }
        catch (IOException)
        {
            Console.WriteLine("Writing error");
        }
        conn.close();
        try
        {
            switch (METHOD)
            {
                case 'p':
                case 'P':
                    (new StreamingData()).PollingClient();
                    break;
                case 't':
                case 'T':
                    (new StreamingData()).ThreadedClient();
                    break;
            }
        }
        catch (IOException)
        {
            Console.WriteLine("Subscription error");
        }
    }

}


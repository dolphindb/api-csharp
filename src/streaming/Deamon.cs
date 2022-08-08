using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;

namespace dolphindb.streaming
{
    class Deamon
    {
        // Convert tcp_keepalive C struct To C# struct
        [
               System.Runtime.InteropServices.StructLayout
               (
                   System.Runtime.InteropServices.LayoutKind.Explicit
               )
        ]
        private unsafe struct TcpKeepAlive
        {
            [System.Runtime.InteropServices.FieldOffset(0)]
            [
                  System.Runtime.InteropServices.MarshalAs
                   (
                       System.Runtime.InteropServices.UnmanagedType.ByValArray,
                       SizeConst = 12
                   )
            ]
            public fixed byte Bytes[12];

            [System.Runtime.InteropServices.FieldOffset(0)]
            public uint On_Off;

            [System.Runtime.InteropServices.FieldOffset(4)]
            public uint KeepaLiveTime;

            [System.Runtime.InteropServices.FieldOffset(8)]
            public uint KeepaLiveInterval;
        }

        public int SetKeepAliveValues
              (
                   System.Net.Sockets.Socket Socket,
                   bool On_Off,
                   uint KeepaLiveTime,
                   uint KeepaLiveInterval
               )
        {
            int Result = -1;

            unsafe
            {
                TcpKeepAlive KeepAliveValues = new TcpKeepAlive();

                KeepAliveValues.On_Off = Convert.ToUInt32(On_Off);
                KeepAliveValues.KeepaLiveTime = KeepaLiveTime;
                KeepAliveValues.KeepaLiveInterval = KeepaLiveInterval;

                byte[] InValue = new byte[12];

                for (int I = 0; I < 12; I++)
                    InValue[I] = KeepAliveValues.Bytes[I];

                Result = Socket.IOControl(IOControlCode.KeepAliveValues, InValue, null);
            }

            return Result;
        }

        private int listeningPort_ = 0;
        private MessageDispatcher dispatcher_;
        private TcpListener ssocket_;
        Thread thread_;
        Thread rcThread;
        private ConcurrentBag<Tuple<Thread, Socket>> parserThreads = new ConcurrentBag<Tuple<Thread, Socket>>();
        

        public Deamon(int port, MessageDispatcher dispatcher) {
            listeningPort_ = port;
            this.dispatcher_ = dispatcher;
            IPAddress localAddr = IPAddress.Parse("0.0.0.0");
            ssocket_ = new TcpListener(localAddr, listeningPort_);
            ssocket_.Start();
            ReconnectDetector reDetector = new ReconnectDetector(dispatcher);
            rcThread = new Thread(reDetector.run);
            reDetector.setRunningThread(rcThread);
            rcThread.Start();
        }

        public void setThread(Thread thread)
        {
            thread_ = thread;
        }

        public void run()
        {
            try
            {
                while (!dispatcher_.isClose())
                {
                    Socket socket = ssocket_.AcceptSocket();
                    SetKeepAliveValues(socket, true, 30000, 5000);
                    MessageParser listener = new MessageParser(socket, dispatcher_);
                    Thread listeningThread = new Thread(new ThreadStart(listener.run));
                    parserThreads.Add(new Tuple<Thread, Socket>(listeningThread, socket));
                    listeningThread.Start();
                }
            }
            catch (ThreadInterruptedException)
            {
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                if (ssocket_ != null)
                {
                    try
                    {
                        ssocket_.Stop();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            Console.WriteLine("Deamon thread stopped.");
        }

        public void close()
        {
            rcThread.Interrupt();
            try
            {
                ssocket_.Stop();
            }
            catch (SocketException)
            {

            }
            thread_.Interrupt();
            foreach(Tuple<Thread, Socket> value in parserThreads)
            {
                value.Item2.Close();
                value.Item1.Interrupt();
            }
        }

        class ReconnectDetector
        {
            MessageDispatcher dispatcher_ = null;
            public ReconnectDetector(MessageDispatcher d)
            {
                this.dispatcher_ = d;
            }
            private Thread thread = null;
            public void setRunningThread(Thread runningThread)
            {
                thread = runningThread;
            }

            public void run()
            {
                ConcurrentDictionary<string, SubscribeInfo> subscribeInfos = dispatcher_.getSubscribeInfos();
                try
                {
                    while (!dispatcher_.isClose())
                    {
                        foreach (SubscribeInfo subscribeInfo in subscribeInfos.Values)
                        {
                            lock (subscribeInfo)
                            {
                                if (!subscribeInfo.isClose())
                                {
                                    if (subscribeInfo.getConnectState() == ConnectState.NO_CONNECT)
                                    {
                                        System.Console.Out.WriteLine("try to reconnect topic " + subscribeInfo.getTopic());
                                        dispatcher_.tryReconnect(subscribeInfo);
                                    }
                                    // try reconnect after 3 second when reconnecting stat
                                    //if messageParser can't get the schema timeout 3 second, will reconnect
                                    else if (subscribeInfo.getConnectState() == ConnectState.REQUEST && (DateTime.Now - subscribeInfo.getLastActivateTime()).Seconds > 3)
                                    {
                                        System.Console.Out.WriteLine("try to reconnect topic " + subscribeInfo.getTopic());
                                        dispatcher_.tryReconnect(subscribeInfo);
                                    }
                                }
                            }
                        }
                        Thread.Sleep(1000);
                    }
                }
                catch (ThreadInterruptedException)
                {
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    Console.WriteLine(e.StackTrace);
                }
                Console.WriteLine("Deamon thread stopped.");
            }
        }
    }
}

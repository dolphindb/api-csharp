using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;
using dolphindb.io;

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

#if NETFRAMEWORK
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
#endif

        private int listeningPort_ = 0;
        private MessageDispatcher dispatcher_;
        private TcpListener ssocket_;
        Thread thread_;
        Thread rcThread;
        private ConcurrentBag<Tuple<Thread, Socket>> parserThreads = new ConcurrentBag<Tuple<Thread, Socket>>();
        private BlockingCollection<DBConnection> connList = new BlockingCollection<DBConnection>();

        public Deamon(int port, MessageDispatcher dispatcher, BlockingCollection<DBConnection> connections) {
            listeningPort_ = port;
            this.dispatcher_ = dispatcher;
            this.connList = connections;
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
                    Socket socket;
                    DBConnectionAndSocket dBConnectionAndSocket = new DBConnectionAndSocket();
                    if ( listeningPort_ > 0)
                    {
                        socket = ssocket_.AcceptSocket();
                        dBConnectionAndSocket.socket = socket;
                        dBConnectionAndSocket.conn = null;
                    }
                    else{
                        DBConnection conn = null;
                        if(!connList.TryTake(out conn, 1000))
                            continue;
                        socket = conn.getSocket();
                        dBConnectionAndSocket.socket = null;
                        dBConnectionAndSocket.conn =conn;
                    }

#if NETFRAMEWORK
                    SetKeepAliveValues(socket, true, 30000, 5000);
#else
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 5000);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 30000);
#endif

                    MessageParser listener = new MessageParser(dBConnectionAndSocket, dispatcher_, listeningPort_);
                    Thread listeningThread = new Thread(new ThreadStart(listener.run));
                    parserThreads.Add(new Tuple<Thread, Socket>(listeningThread, socket));
                    listeningThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
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
            try
            {
                ssocket_.Stop();
            }
            catch (SocketException)
            {

            }
            foreach(Tuple<Thread, Socket> value in parserThreads)
            {
                value.Item2.Close();
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
                        lock(dispatcher_){
                            foreach (SubscribeInfo subscribeInfo in subscribeInfos.Values)
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
                        Thread.Sleep(3000);
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

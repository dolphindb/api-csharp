using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;

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

        private int listeningPort = 0;
        private MessageDispatcher dispatcher;

        public Deamon(int port, MessageDispatcher dispatcher) {
            listeningPort = port;
            this.dispatcher = dispatcher;
        }

        public void run()
        {
            TcpListener ssocket = null;
            try
            {
                IPAddress localAddr = IPAddress.Parse("0.0.0.0");
                ssocket = new TcpListener(localAddr, listeningPort);
                ssocket.Start();
                while (true)
                {
                    Socket socket = ssocket.AcceptSocket();
                    SetKeepAliveValues(socket, true, 30000, 5000);
                    MessageParser listener = new MessageParser(socket, dispatcher);
                    Thread listeningThread = new Thread(new ThreadStart(listener.run));
                    listeningThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.Write(ex.StackTrace);
            }
            finally
            {
                if (ssocket != null)
                {
                    try
                    {
                        ssocket.Stop();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        Console.Write(ex.StackTrace);
                    }
                }
            }
        }
    }
}

using System;
using System.Threading;
using System.Net;
using System.Net.Sockets;

namespace dolphindb.streaming
{
    class Deamon
    {
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

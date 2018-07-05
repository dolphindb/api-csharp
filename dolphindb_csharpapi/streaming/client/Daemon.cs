using System.Threading;

namespace com.xxdb.streaming.client
{


	internal class Daemon : Runnable
	{
		private int listeningPort = 0;
		private MessageDispatcher dispatcher;
		public Daemon(int port, MessageDispatcher dispatcher)
		{
			this.listeningPort = port;
			this.dispatcher = dispatcher;
		}

		public override void run()
		{
			ServerSocket ssocket = null;
			try
			{
				ssocket = new ServerSocket(this.listeningPort);
				while (true)
				{
					Socket socket = ssocket.accept();
					MessageParser listener = new MessageParser(socket, dispatcher);
					Thread listeningThread = new Thread(listener);
					listeningThread.Start();
				}
			}
			catch (IOException e)
			{
				Console.WriteLine(e.ToString());
				Console.Write(e.StackTrace);
			}
			finally
			{
				if (ssocket != null)
				{
					try
					{
						ssocket.close();
					}
					catch (IOException e)
					{
						Console.WriteLine(e.ToString());
						Console.Write(e.StackTrace);
					}
				}
			}

		}



	}


}
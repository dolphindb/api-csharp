using System.Collections.Generic;

namespace com.xxdb.streaming.client
{



	public class ThreadedClient : AbstractClient
	{

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public ThreadedClient() throws java.net.SocketException
		public ThreadedClient() : this(DEFAULT_PORT)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public ThreadedClient(int subscribePort) throws java.net.SocketException
		public ThreadedClient(int subscribePort) : base(subscribePort)
		{
		}

		internal class HandlerLopper : System.Threading.Thread
		{
			private readonly ThreadedClient outerInstance;

			internal BlockingQueue<IList<IMessage>> queue;
			internal MessageHandler handler;
			internal HandlerLopper(ThreadedClient outerInstance, BlockingQueue<IList<IMessage>> queue, MessageHandler handler)
			{
				this.outerInstance = outerInstance;
				this.queue = queue;
				this.handler = handler;
			}
			public virtual void run()
			{
				while (true)
				{
					try
					{
						IList<IMessage> msgs = queue.take();
						foreach (IMessage msg in msgs)
						{
							handler.doEvent(msg);
						}
					}
					catch (InterruptedException e)
					{
						Console.WriteLine(e.ToString());
						Console.Write(e.StackTrace);
					}
				}
			}
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset) throws java.io.IOException
		public virtual void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
		{
			BlockingQueue<IList<IMessage>> queue = subscribeInternal(host,port,tableName, offset);
			(new HandlerLopper(this, queue, handler)).Start();
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void subscribe(String host,int port,String tableName, MessageHandler handler) throws java.io.IOException
		public virtual void subscribe(string host, int port, string tableName, MessageHandler handler)
		{
			subscribe(host, port, tableName, handler, -1);
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: void unsubscribe(String host,int port,String tableName) throws java.io.IOException
		internal virtual void unsubscribe(string host, int port, string tableName)
		{
			unsubscribeInternal(host, port, tableName);
		}
	}

}
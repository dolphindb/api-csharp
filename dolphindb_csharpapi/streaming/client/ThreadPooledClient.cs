using System.Collections.Generic;
using System.Threading;

namespace com.xxdb.streaming.client
{



	public class ThreadPooledClient : AbstractClient
	{
		private static int CORES = Runtime.Runtime.availableProcessors();
		private ExecutorService threadPool;

		private class QueueHandlerBinder
		{
			private readonly ThreadPooledClient outerInstance;

			public QueueHandlerBinder(ThreadPooledClient outerInstance, BlockingQueue<IList<IMessage>> queue, MessageHandler handler)
			{
				this.outerInstance = outerInstance;
				this.queue = queue;
				this.handler = handler;
			}

			internal BlockingQueue<IList<IMessage>> queue;
			internal MessageHandler handler;
		}
		private Dictionary<string, QueueHandlerBinder> queueHandlers = new Dictionary<string, QueueHandlerBinder>();
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public ThreadPooledClient() throws java.net.SocketException
		public ThreadPooledClient() : this(DEFAULT_PORT, CORES)
		{
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public ThreadPooledClient(int subscribePort, int threadCount) throws java.net.SocketException
		public ThreadPooledClient(int subscribePort, int threadCount) : base(subscribePort)
		{
			threadPool = Executors.newFixedThreadPool(threadCount);
			new ThreadAnonymousInnerClass(this)
			.start();
		}

		private class ThreadAnonymousInnerClass : System.Threading.Thread
		{
			private readonly ThreadPooledClient outerInstance;

			public ThreadAnonymousInnerClass(ThreadPooledClient outerInstance)
			{
				this.outerInstance = outerInstance;
				backlog = new LinkedList<>();
			}

			private LinkedList<IMessage> backlog;

			private bool fillBacklog()
			{
				bool filled = true;
				lock (outerInstance.queueHandlers)
				{
					ISet<string> keySet = outerInstance.queueHandlers.Keys;
					foreach (string topic in keySet)
					{
						IList<IMessage> messages = outerInstance.queueHandlers[topic].queue.poll();
						if (messages != null)
						{
							backlog.addAll(messages);
							filled = true;
						}
					}
				}
				return filled;
			}

			private void refill()
			{
				int count = 200;
				while (fillBacklog() == false)
				{
					if (count > 100)
					{
						;
					}
					else if (count > 0)
					{
						Thread.yield();
					}
					else
					{
						LockSupport.park();
					}
					count = count - 1;
				}
			}

			public virtual void run()
			{
				while (true)
				{
					IMessage msg;
					while ((msg = backlog.poll()) != null)
					{
						QueueHandlerBinder binder;
						lock (outerInstance.queueHandlers)
						{
							binder = outerInstance.queueHandlers[msg.Topic];
						}
						outerInstance.threadPool.execute(new HandlerRunner(outerInstance, binder.handler, msg));
					}
					refill();
				}
			}
		}
		internal class HandlerRunner : Runnable
		{
			private readonly ThreadPooledClient outerInstance;

			internal MessageHandler handler;
			internal IMessage message;
			internal HandlerRunner(ThreadPooledClient outerInstance, MessageHandler handler, IMessage message)
			{
				this.outerInstance = outerInstance;
				this.handler = handler;
				this.message = message;
			}
			public virtual void run()
			{
				this.handler.doEvent(message);
			}
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void subscribe(String host, int port, String tableName, MessageHandler handler, long offset) throws java.io.IOException
		public virtual void subscribe(string host, int port, string tableName, MessageHandler handler, long offset)
		{
			BlockingQueue<IList<IMessage>> queue = subscribeInternal(host, port,tableName, offset);
			lock (queueHandlers)
			{
				queueHandlers[tableName2Topic[host + ":" + port + ":" + tableName]] = new QueueHandlerBinder(this, queue, handler);
			}
		}
		// subscribe to host:port on tableName with offset set to position past the last element
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
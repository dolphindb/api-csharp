using System;
using System.Collections.Generic;
using System.Threading;

namespace com.xxdb.streaming.client
{


	using BasicInt = com.xxdb.data.BasicInt;
	using BasicLong = com.xxdb.data.BasicLong;
	using BasicString = com.xxdb.data.BasicString;
	using Entity = com.xxdb.data.Entity;

	internal abstract class AbstractClient : MessageDispatcher
	{
		public abstract void batchDispatch(IList<IMessage> message);
		protected internal const int DEFAULT_PORT = 8849;
		protected internal const string DEFAULT_HOST = "localhost";
		protected internal int listeningPort;
		protected internal string localIP;
		protected internal QueueManager queueManager = new QueueManager();
		protected internal Dictionary<string, IList<IMessage>> messageCache = new Dictionary<string, IList<IMessage>>();
		protected internal Dictionary<string, string> tableName2Topic = new Dictionary<string, string>();
		protected internal Dictionary<string, bool?> hostEndian = new Dictionary<string, bool?>();

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public AbstractClient() throws java.net.SocketException
		public AbstractClient() : this(DEFAULT_PORT)
		{
		}
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public AbstractClient(int subscribePort) throws java.net.SocketException
		public AbstractClient(int subscribePort)
		{
			this.listeningPort = subscribePort;
			this.localIP = this.GetLocalIP();
			Daemon daemon = new Daemon(subscribePort, this);
			Thread pThread = new Thread(daemon);
			pThread.Start();
		}

		private void addMessageToCache(IMessage msg)
		{
			string topic = msg.Topic;
			IList<IMessage> cache = messageCache[topic];
			if (cache == null)
			{
				cache = new List<>();
				messageCache[msg.Topic] = cache;
			}
			cache.Add(msg);
		}
		private void flushToQueue()
		{

			ISet<string> keySet = messageCache.Keys;
			foreach (string topic in keySet)
			{
				try
				{
					queueManager.getQueue(topic).put(messageCache[topic]);
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
					Console.Write(e.StackTrace);
				}
			}
			messageCache.Clear();
		}

		public virtual void dispatch(IMessage msg)
		{
			BlockingQueue<IList<IMessage>> queue = queueManager.getQueue(msg.Topic);
			try
			{
				queue.put(msg);
			}
			catch (InterruptedException e)
			{
				Console.WriteLine(e.ToString());
				Console.Write(e.StackTrace);
			}
		}
		public virtual void batchDispatch(IList<IMessage> messags)
		{
			for (int i = 0; i < messags.Count; ++i)
			{
				addMessageToCache(messags[i]);
			}
			flushToQueue();
		}

		public virtual bool isRemoteLittleEndian(string host)
		{
			if (hostEndian.ContainsKey(host))
			{
				return hostEndian[host].Value;
			}
			else
			{
				return false; //default bigEndian
			}
		}


//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected java.util.concurrent.BlockingQueue<List<com.xxdb.streaming.client.IMessage>> subscribeInternal(String host, int port, String tableName, long offset) throws java.io.IOException,RuntimeException
		protected internal virtual BlockingQueue<IList<IMessage>> subscribeInternal(string host, int port, string tableName, long offset)
		{

			Entity re;
			string topic = "";

			DBConnection dbConn = new DBConnection();
			dbConn.connect(host, port);

			if (!hostEndian.ContainsKey(host))
			{
				hostEndian[host] = dbConn.RemoteLittleEndian;
			}

			IList<Entity> @params = new List<Entity>();
			@params.Add(new BasicString(tableName));

			re = dbConn.run("getSubscriptionTopic", @params);
			topic = re.String;
			BlockingQueue<IList<IMessage>> queue = queueManager.addQueue(topic);
			@params.Clear();

			tableName2Topic[host + ":" + port + ":" + tableName] = topic;

			@params.Add(new BasicString(this.localIP));
			@params.Add(new BasicInt(this.listeningPort));
			@params.Add(new BasicString(tableName));
			if (offset != -1)
			{
			@params.Add(new BasicLong(offset));
			}
			re = dbConn.run("publishTable", @params);

			dbConn.close();
			return queue;
		}


//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: protected void unsubscribeInternal(String host,int port,String tableName) throws java.io.IOException
		protected internal virtual void unsubscribeInternal(string host, int port, string tableName)
		{

			DBConnection dbConn = new DBConnection();
			dbConn.connect(host, port);
			IList<Entity> @params = new List<Entity>();
			@params.Add(new BasicString(this.localIP));
			@params.Add(new BasicInt(this.listeningPort));
			@params.Add(new BasicString(tableName));
			dbConn.run("stopPublishTable", @params);
			dbConn.close();
			return;

		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: private String GetLocalIP() throws java.net.SocketException
		private string GetLocalIP()
		{
				try
				{
					for (IEnumerator<NetworkInterface> interfaces = NetworkInterface.NetworkInterfaces; interfaces.MoveNext();)
					{
						NetworkInterface networkInterface = interfaces.Current;
						if (networkInterface.Loopback || networkInterface.Virtual || !networkInterface.Up)
						{
							continue;
						}
						IEnumerator<InetAddress> addresses = networkInterface.InetAddresses;

						while (addresses.MoveNext())
						{
							try
							{
								Inet4Address ip = (Inet4Address) addresses.Current;
								if (ip != null)
								{
									return ip.HostAddress;
								}
							}
							catch (System.InvalidCastException)
							{

							}
						}
					}
				}
				catch (SocketException e)
				{
					throw e;
				}
				return null;
		}
	}

}
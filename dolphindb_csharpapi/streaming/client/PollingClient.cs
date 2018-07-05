using System.Collections.Generic;

namespace com.xxdb.streaming.client
{



	public class PollingClient : AbstractClient
	{
//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public PollingClient() throws java.net.SocketException
		public PollingClient() : this(DEFAULT_PORT)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public PollingClient(int subscribePort) throws java.net.SocketException
		public PollingClient(int subscribePort) : base(subscribePort)
		{
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public TopicPoller subscribe(String host,int port,String tableName, long offset) throws java.io.IOException
		public virtual TopicPoller subscribe(string host, int port, string tableName, long offset)
		{
			BlockingQueue<IList<IMessage>> queue = subscribeInternal(host,port,tableName, offset);
			return new TopicPoller(queue);
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public TopicPoller subscribe(String host,int port,String tableName) throws java.io.IOException
		public virtual TopicPoller subscribe(string host, int port, string tableName)
		{
			return subscribe(host, port, tableName, -1);
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: public void unsubscribe(String host,int port,String tableName) throws java.io.IOException
		public virtual void unsubscribe(string host, int port, string tableName)
		{
			unsubscribeInternal(host, port, tableName);
		}
	}

}
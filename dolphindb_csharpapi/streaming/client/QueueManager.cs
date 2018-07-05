using System;
using System.Collections.Generic;

namespace com.xxdb.streaming.client
{


	internal class QueueManager
	{
		private Dictionary<string, BlockingQueue<IList<IMessage>>> queueMap = new Dictionary<string, BlockingQueue<IList<IMessage>>>();

		public virtual BlockingQueue<IList<IMessage>> addQueue(string topic)
		{
			lock (this)
			{
				if (!queueMap.ContainsKey(topic))
				{
					BlockingQueue<IList<IMessage>> q = new ArrayBlockingQueue<IList<IMessage>>(4096);
					queueMap[topic] = q;
					return q;
				}
				throw new Exception("Topic " + topic + " already subscribed");
			}
		}

		public virtual BlockingQueue<IList<IMessage>> getQueue(string topic)
		{
			lock (this)
			{
				BlockingQueue<IList<IMessage>> q = queueMap[topic];
				return q;
			}
		}
	}
}
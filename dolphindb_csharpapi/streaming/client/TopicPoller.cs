using System.Collections.Generic;

namespace com.xxdb.streaming.client
{


	public class TopicPoller
	{
		internal BlockingQueue<IList<IMessage>> queue;
		internal List<IMessage> cache = null;

		public TopicPoller(BlockingQueue<IList<IMessage>> queue)
		{
			this.queue = queue;
		}
		private void fillCache(long timeout)
		{
			assert(cache == null);
			IList<IMessage> list = queue.poll();
			if (cache == null)
			{
				try
				{
					if (timeout >= 0)
					{
						list = queue.poll(timeout, TimeUnit.MILLISECONDS);
					}
					else
					{
						list = queue.take();
					}
				}
				catch (InterruptedException e)
				{
					Console.WriteLine(e.ToString());
					Console.Write(e.StackTrace);
				}
			}
			if (list != null)
			{
				cache = new List<>(list.Count);
				cache.AddRange(list);
			}
		}


		public virtual List<IMessage> poll(long timeout)
		{
			if (cache == null)
			{
				fillCache(timeout);
			}
			List<IMessage> cachedMessages = cache;
			cache = null;
			return cachedMessages;
		}

		// take one message from the topic, block if necessary
		public virtual IMessage take()
		{
			if (cache == null)
			{
				fillCache(-1);
			}
			return cache.Remove(0);
		}
	}

}
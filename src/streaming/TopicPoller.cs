using System.Collections.Generic;
using System.Collections.Concurrent;
using System;
using System.Linq;
using System.Diagnostics;

namespace dolphindb.streaming
{
    public class TopicPoller
    {
        private BlockingCollection<List<IMessage>> queue;
        private Queue<IMessage> cache = null;

        public TopicPoller(BlockingCollection<List<IMessage>> queue)
        {
            this.queue = queue;
        }

        public void setQueue(BlockingCollection<List<IMessage>> queue)
        {
            this.queue = queue;
        }

        private void fillCache(long timeout)
        {
            Debug.Assert(cache == null);
            List<IMessage> list = null;
            if (cache == null)
            {
                if (timeout > 0)
                    queue.TryTake(out list, (int)timeout);
                else
                    list = queue.Take();
            }
            if (list != null)
            {
                cache = new Queue<IMessage>(list);
            }
        }

        public List<IMessage> poll(long timeout)
        {
            if (cache == null)
            {
                fillCache(timeout);
            }
            //List<IMessage> cachedMessages = cache;
            Queue<IMessage> cachedMessages = cache;
            cache = null;
            return cachedMessages == null ? new List<IMessage>() : cachedMessages.ToList();
        }
        
        public IMessage take()
        {
            if (cache == null)
                fillCache(-1);
            //IMessage message = cache.First();
            //cache.RemoveAt(0);
            //return message;
            return cache.Dequeue();
        }
    }
}

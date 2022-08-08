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
        private List<IMessage> cache = new List<IMessage>();

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
            cache = new List<IMessage>();
            List<IMessage> list = new List<IMessage>();
            int count = queue.Count;
            for (int i = 0; i < count; i++)
            {
                if (timeout > 0)
                    queue.TryTake(out list, (int)timeout);
                else
                    list = queue.Take();
                cache.AddRange(list);
            }
        }

        public List<IMessage> poll(long timeout)
        {
            if (cache.Count == 0)
            {
                fillCache(timeout);
            }
            //List<IMessage> cachedMessages = cache;
            List<IMessage> cachedMessages = cache;
            cache = new List<IMessage>();
            return cachedMessages == null ? new List<IMessage>() : cachedMessages;
        }
        
        public List<IMessage> poll(long timeout, int size)
        {
            if (size <= 0)
                throw new Exception("Size must be greater than zero");
            List<IMessage> messages = new List<IMessage>(cache);
            cache.Clear();
            DateTime now = System.DateTime.Now;
            DateTime end = now.AddTicks(timeout * 10000);
            while(messages.Count < size && System.DateTime.Now < end)
            {
                try
                {
                    long mileSeconds = (end.Ticks - System.DateTime.Now.Ticks) / 10000;
                    List<IMessage> tmp = new List<IMessage>();
                    queue.TryTake(out tmp, (int)mileSeconds);
                    if(tmp != null)
                    {
                        messages.AddRange(tmp);
                    }
                }catch(Exception e)
                {
                    return messages;
                }
            }
            return messages;
        }

        public IMessage take()
        {
            while (true)
            {
                if (cache.Count != 0)
                {
                    IMessage message = cache[0];
                    cache.Remove(message);
                    return message;
                }
                try
                {
                    List<IMessage> tmp = queue.Take();
                    if (tmp != null)
                    {
                        cache.AddRange(tmp);
                    }
                }
                catch (Exception e)
                {
                    return null;
                }
            }
        }
    }
}

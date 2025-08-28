using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;

namespace dolphindb.streaming
{
    // public class QueueManager
    // {
    //     private Dictionary<string, BlockingCollection<List<IMessage>>> queueMap = new Dictionary<string, BlockingCollection<List<IMessage>>>();

    //     private object Lock = new object();

    //     public BlockingCollection<List<IMessage>> addQueue(string topic)
    //     {
    //         lock (Lock)
    //         {
    //             if (!queueMap.ContainsKey(topic))
    //             {
    //                 BlockingCollection<List<IMessage>> q = new BlockingCollection<List<IMessage>>(4096);
    //                 queueMap.Add(topic, q);
    //                 return q;
    //             }
    //             throw new Exception("Topic " + topic + " already subscribed");
    //         }
    //     }

    //     public BlockingCollection<List<IMessage>> getQueue(string topic)
    //     {
    //         lock (Lock)
    //         {
    //             BlockingCollection<List<IMessage>> q = queueMap[topic];
    //             return q;
    //         }
    //     }

    //     public void removeQueue(string topic)
    //     {
    //         lock (Lock)
    //         {
    //             queueMap.Remove(topic);
    //         }
    //     }
    // }
}

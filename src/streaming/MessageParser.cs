﻿using dolphindb.data;
using dolphindb.io;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Collections.Concurrent;
using System.Threading;

namespace dolphindb.streaming
{
    public class MessageParser
    {
        private readonly int MAX_FORM_VALUE = Enum.GetNames(typeof(DATA_FORM)).Length;
        private readonly int MAX_TYPE_VALUE = Enum.GetNames(typeof(DATA_TYPE)).Length;

        Socket socket_ = null;
        MessageDispatcher dispatcher_;
        BufferedStream bis_ = null;
        string topics;
        HashSet<string> successTopics = new HashSet<string>();

        public MessageParser(Socket socket, MessageDispatcher dispatcher)
        {
            this.socket_ = socket;
            this.dispatcher_ = dispatcher;
        }

        private static readonly char[] hexArray = "0123456789ABCDEF".ToCharArray();
        public static string bytesToHex(byte[] bytes)
        {
            char[] hexChars = new char[bytes.Length * 2];
            for (int j = 0; j < bytes.Length; j++)
            {
                uint v = (uint)bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0f];
            }
            return new string(hexChars);
        }

        public void run()
        {
            ConcurrentDictionary<string, SubscribeInfo> subscribeInfos = dispatcher_.getSubscribeInfos();
            Socket socket = this.socket_;
            try
            {
                if (bis_ == null)
                    bis_ = new BufferedStream(new NetworkStream(socket));
                ExtendedDataInput @in = null;

                while (!dispatcher_.isClose())
                {
                    if (@in == null)
                    {
                        bool isLittle = bis_.ReadByte() != 0;
                        if (isLittle)
                            @in = new LittleEndianDataInputStream(bis_);
                        else
                            @in = new BigEndianDataInputStream(bis_);
                    }
                    else
                    {
                        @in.readBoolean();
                    }

                    @in.readLong();
                    long msgid = @in.readLong();
                    
                    topics = @in.readString();
                    short flag = @in.readShort();
                    IEntityFactory factory = new BasicEntityFactory();
                    int form = flag >> 8;
                    int type = flag & 0xff;
                    bool extended = type >= 128;
                    if (type >= 128)
                        type -= 128;

                    if (form < 0 || form > MAX_FORM_VALUE)
                        throw new IOException("Invalid form value: " + form);
                    if (type < 0 || type > MAX_TYPE_VALUE)
                        throw new IOException("Invalid type value: " + type);

                    DATA_FORM df = (DATA_FORM)form;
                    DATA_TYPE dt = (DATA_TYPE)type;

                    IEntity body;
                    try
                    {
                        body = factory.createEntity(df, dt, @in, extended);
                    }
                    catch
                    {
                        throw;
                    }
                    if (body.isTable())
                    {
                        foreach (string HATopic in topics.Split(','))
                        {
                            string topic = dispatcher_.getTopicForHATopic(HATopic);
                            if (topic == null)
                                throw new Exception("Subscription with topic " + HATopic + " does not exist. ");
                            if (!successTopics.Contains(topic))
                            {
                                SubscribeInfo subscribeInfo = null;
                                //Prevents a situation where streaming data arrives earlier than the subscription succeeds.
                                lock (subscribeInfos)
                                {
                                    if (!subscribeInfos.TryGetValue(topic, out subscribeInfo))
                                    {
                                        throw new Exception("Subscription with topic " + topic + " does not exist. ");
                                    }
                                }
                                lock (subscribeInfo)
                                {
                                    if (subscribeInfo.getConnectState() != ConnectState.RECEIVED_SCHEMA)
                                    {
                                        subscribeInfo.setConnectState(ConnectState.RECEIVED_SCHEMA);
                                    }
                                    else
                                    {
                                        throw new Exception("Subscription with topic " + topic + " already has a thread parsing the stream data. ");
                                    }
                                }
                                successTopics.Add(topic);
                            }
                        }
                    }
                    else if (body.isVector())
                    {
                        foreach (string HATopic in topics.Split(','))
                        {
                            string topic = dispatcher_.getTopicForHATopic(HATopic);
                            if (topic == null)
                                throw new Exception("Subscription with topic " + HATopic + " does not exist. ");
                            SubscribeInfo subscribeInfo = null;
                            if (!subscribeInfos.TryGetValue(topic, out subscribeInfo))
                            {
                                throw new Exception("Subscription with topic " + topic + " does not exist. ");
                            }
                            BasicAnyVector dTable = (BasicAnyVector)body;

                            int colSize = dTable.rows();
                            int rowSize = dTable.getEntity(0).rows();
                            if (rowSize == 1)
                            {
                                BasicMessage rec = new BasicMessage(msgid, topic, dTable);
                                if (subscribeInfo.getDeseriaLizer() != null)
                                    rec = subscribeInfo.getDeseriaLizer().parse(rec);
                                dispatcher_.dispatch(rec);
                            }
                            else if (rowSize > 1)
                            {
                                List<IMessage> messages = new List<IMessage>(rowSize);
                                for (int i = 0; i < rowSize; i++)
                                {
                                    BasicAnyVector row = new BasicAnyVector(colSize);

                                    for (int j = 0; j < colSize; j++)
                                    {
                                        AbstractVector vector = (AbstractVector)dTable.getEntity(j);
                                        IEntity entity = vector.get(i);
                                        row.setEntity(j, entity);
                                    }
                                    BasicMessage rec = new BasicMessage(msgid, topic, row);
                                    if (subscribeInfo.getDeseriaLizer() != null)
                                        rec = subscribeInfo.getDeseriaLizer().parse(rec);
                                    messages.Add(rec);
                                }
                                dispatcher_.batchDispatch(messages);
                            }
                            lock (subscribeInfo)
                            {
                                subscribeInfo.setMsgId(msgid);
                            }
                        }
                    }
                    else
                    {
                        throw new Exception("message body has an invalid format. Vector or table is expected");
                    }
                }
            }
            catch (Exception e)
            {
                System.Console.Out.WriteLine(e.StackTrace);
                System.Console.Out.WriteLine(e.Message);
                foreach (string topic in successTopics)
                {
                    SubscribeInfo subscribeInfo = null;
                    if (!subscribeInfos.TryGetValue(topic, out subscribeInfo))
                    {
                        System.Console.Out.WriteLine("Subscription with topic " + topic + " doesn't exist. ");
                    }
                    else {
                        lock (subscribeInfo)
                        {
                            subscribeInfo.setConnectState(ConnectState.NO_CONNECT);
                        }
                    }
                }
            }
            finally
            {
                try
                {
                    socket.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
            Console.WriteLine("MessageParser thread stopped.");
        }
    }
}

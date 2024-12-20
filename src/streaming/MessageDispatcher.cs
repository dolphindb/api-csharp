﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using dolphindb.data;

namespace dolphindb.streaming
{
    public enum ConnectState { NO_CONNECT, REQUEST, RECEIVED_SCHEMA };
    public interface MessageDispatcher
    {
        void dispatch(IMessage message);
        void batchDispatch(List<IMessage> message);
        bool tryReconnect(SubscribeInfo subscribeInfo);
        ConcurrentDictionary<string, SubscribeInfo> getSubscribeInfos();
        List<Site> getActiveSites(Site[] sites);
        string getTopicForHATopic(string HATopic);
        bool isClose();
    }

    public class SubscribeInfo
    {
        private DateTime lastActivateTime_;
        private BlockingCollection<List<IMessage>> queue_;
        private ConnectState connectState_ = ConnectState.NO_CONNECT;
        private Site[] sites_;
        private string topic_;
        private long msgId_;
        private bool reconnect_;
        private IVector filter_;
        private bool closed_ = false;
        private MessageHandler messageHandler_;
        private string tableName_;
        private string actionName_;
        private StreamDeserializer deserializer_;
        private string user_;
        private string password_;
        private bool msgAsTable_;
        private List<string> colsName_;

        public SubscribeInfo(DateTime activate, BlockingCollection<List<IMessage>> queue, Site[] sites, string topic, long msgId, bool reconnect, IVector filter, 
            MessageHandler messageHandler, string tableName, string actionName, StreamDeserializer deserializer, string user, string password, bool msgAsTable_, List<string> colsName)
        {
            lastActivateTime_ = activate;
            this.queue_ = queue;
            connectState_ = ConnectState.NO_CONNECT;
            this.sites_ = sites;
            this.topic_ = topic;
            this.msgId_ = msgId;
            this.reconnect_ = reconnect;
            this.filter_ = filter;
            this.messageHandler_ = messageHandler;
            this.tableName_ = tableName;
            this.actionName_ = actionName;
            deserializer_ = deserializer;
            user_ = user;
            password_ = password;
            this.msgAsTable_ = msgAsTable_;
            this.colsName_ = colsName;
        }

        public string getTopic()
        {
            return topic_;
        }

        public void setConnectState(ConnectState connectState)
        {
            connectState_ = connectState;
        }

        public ConnectState getConnectState()
        {
            return connectState_;
        }

        public void setActivateTime(DateTime ActivateTime)
        {
            lastActivateTime_ = ActivateTime;
        }

        public DateTime getLastActivateTime()
        {
            return lastActivateTime_;
        }

        public Site[] getSites()
        {
            return sites_;
        }

        public BlockingCollection<List<IMessage>> getQueue()
        {
            return queue_;
        }
        
        public string getTableName()
        {
            return tableName_;
        }

        public string getActionName()
        {
            return actionName_;
        }

        public MessageHandler getMessageHandler()
        {
            return messageHandler_;
        }

        public long getMsgId()
        {
            return msgId_;
        }

        public IVector getFilter()
        {
            return filter_;
        }

        public void setMsgId(long msgId)
        {
            msgId_ = msgId;
        }

        public void close()
        {
            closed_ = true;
        }

        public bool isClose()
        {
            return closed_;
        }

        public StreamDeserializer getDeseriaLizer()
        {
            return deserializer_;
        }
        public string getUser()
        {
            return user_;
        }
        
        public string getPassword()
        {
            return password_;
        }

        public bool getMsgAsTable()
        {
            return msgAsTable_;
        }

        public List<string> getColsName()
        {
            return colsName_;
        }
    }

    public struct Site
    {
        public string host;
        public int port;

        public Site(string host, int port)
        {
            this.host = host;
            this.port = port;
        }
    }
}

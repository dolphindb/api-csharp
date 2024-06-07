﻿//-------------------------------------------------------------------------------------------
//	Copyright © 2018 DolphinDB Inc.
//	Date   : 2018.03.09
//  Author : liang.lin
//-------------------------------------------------------------------------------------------
using dolphindb.data;
using dolphindb.io;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Security.Cryptography.X509Certificates;
using System.Collections;
using System.Net.Security;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;

namespace dolphindb
{

    /// <summary>
    /// Sets up a connection to DolphinDB server through TCP/IP protocol
    /// Executes DolphinDB scripts
    /// 
    /// Example:
    /// 
    /// import dolphindb;
    /// DBConnection conn = new DBConnection();
    /// boolean success = conn.connect("localhost", 8080);
    /// conn.run("sum(1..100)");
    /// 
    /// </summary>

    public class DBConnection
    {
        private enum NotLeaderStatus
        {
            NEW_LEADER, WAIT, CONN_FAIL, OTHER_EXCEPTION
        }

        enum ExceptionType
        {
            ET_IGNORE = 0,
            ET_UNKNOW = 1,
            ET_NEWLEADER = 2,
            ET_NODENOTAVAIL = 3,
            ET_NOINITIALIZED = 4,
            ET_NOTSUPPORT = 5
        };

        private static readonly int MAX_FORM_VALUE = Enum.GetValues(typeof(DATA_FORM)).Length - 1;
        private static readonly int MAX_TYPE_VALUE = Enum.GetValues(typeof(DATA_TYPE)).Length - 1;

        private readonly object threadLock_ = new object();

        //private string hostName_;

        //private int port_;
        private string userId_;
        private string password_;

        private string startup_;
        //=============HighAvailability 2019.07.17 Linl======================
        //private bool ha_;
        //2021.01.14 cwj
        //private string[] highAvailabilitySites_ = null;

        //=============Asyntask 2021.01.14 cwj======================
        private bool asynTask_ = false;
        private bool isUseSSL_ = false;
        //=============Asyntask 2021.02.04 cwj======================
        //===================================================================
        private bool compress_ = false;
        private bool loadBalance_ = true;
        private bool isReverseStreaming_ = false;

        List<Node> nodes_ = new List<Node>();
        DBConnectionImpl conn_;

        public class DBConnectionImpl
        {
            private Socket socket_ = null;
            private SslStream sslStream_;
            private ExtendedDataOutput @out;
            private ExtendedDataInput @in;
            private string sessionID_ = "";
            private string hostName_ = "";
            private int port_ = 0;
            private string userId_ = "";
            private string password_ = "";
            bool encrypted_ = false;
            bool isConnected_ = false;
            bool littleEndian_ = false;
            bool sslEnable_ = false;
            bool asynTask_ = false;
            bool compress_ = false;
            bool isReverseStreaming_ = false;
            private string startup_;
            private bool usePython_ = false;
            private static readonly int MAX_FORM_VALUE = Enum.GetValues(typeof(DATA_FORM)).Length - 1;
            private static readonly int MAX_TYPE_VALUE = Enum.GetValues(typeof(DATA_TYPE)).Length - 1;
            BasicEntityFactory factory = (BasicEntityFactory)BasicEntityFactory.instance();

            public string getSessionID()
            {
                return sessionID_;
            }

            public Socket getSocket()
            {
                return socket_;
            }

            public ExtendedDataInput getDataInputStream()
            {
                return @in;
            }

            private int generateRequestFlag(bool clearSessionMemory)
            {
                int flag = 0;
                if (asynTask_)
                    flag += 4;
                if (clearSessionMemory)
                    flag += 16;
                if (compress_)
                    flag += 64;
                if (usePython_)
                    flag += 2048;
                if (isReverseStreaming_)
                    flag += 131072;
                return flag;
            }

            public DBConnectionImpl(bool sslEnable = false, bool asynTask = false, bool compress = false, bool usePython = false,bool isReverseStreaming = false)
            {
                sslEnable_ = sslEnable;
                asynTask_ = asynTask;
                compress_ = compress;
                usePython_ = usePython;
                isReverseStreaming_ = isReverseStreaming;
            }

            public bool connect(string hostName, int port, string userId, string password, bool sslEnable,
                bool asynTask, bool compress, string startup)
            {
                hostName_ = hostName;
                port_ = port;
                userId_ = userId;
                password_ = password;
                encrypted_ = false;
                sslEnable_ = sslEnable;
                asynTask_ = asynTask;
                compress_ = compress;
                startup_ = startup;
                return connect();
            }

            bool connect()
            {
                try
                {
                    isConnected_ = false;
                    if (sslEnable_)
                    {
                        TcpClient client = new TcpClient(hostName_, port_);

                        sslStream_ = new SslStream(client.GetStream(), false, new RemoteCertificateValidationCallback(ValidateServerCertificate), null);
                        try
                        {
                            sslStream_.AuthenticateAsClient("www.example.com", null, (SslProtocols)(0xc0 | 0x300 | 0xc00), false);
                        }
                        catch (AuthenticationException e)
                        {
                            Console.WriteLine("Exception: {0}", e.Message);
                            if (e.InnerException != null)
                            {
                                Console.WriteLine("Inner exception: {0}", e.InnerException.Message);
                            }
                            Console.WriteLine("Authentication failed - closing the connection.");
                            client.Close();
                            return false;
                        }

                        socket_ = client.Client;
                        socket_.NoDelay = true;
                        @in = new LittleEndianDataInputStream(new BufferedStream(sslStream_));
                        @out = new LittleEndianDataOutputStream(new BufferedStream(sslStream_));

                    }
                    else
                    {
                        socket_ = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                        LingerOption lingerOption = new LingerOption(true, 0);
                        socket_.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, lingerOption);
                        socket_.Connect(hostName_, port_);
                        socket_.NoDelay = true;
                        @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket_)));
                        @in = new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket_)));
                    }
                }
                catch (System.Net.Sockets.SocketException ex)
                {
                    Console.Out.WriteLine(ex.Message);
                    throw new IOException(ex.Message);
                }

                string body = "connect\n";
                @out.writeBytes("API 0 ");
                @out.writeBytes(body.Length.ToString());
                int flag = generateRequestFlag(false);
                @out.writeBytes(" / " + flag + "_1_" + 4 + "_" + 2);
                @out.writeByte('\n');
                @out.writeBytes(body);
                @out.flush();


                string line = @in.readLine();
                int endPos = line.IndexOf(' ');
                if (endPos <= 0)
                {
                    throw new IOException("Invalid ack msg : " + line);
                }
                sessionID_ = line.Substring(0, endPos);

                int startPos = endPos + 1;
                endPos = line.IndexOf(' ', startPos);
                if (endPos != line.Length - 2)
                {
                    throw new IOException("Invalid ack msg : " + line);
                    //return false;
                }

                if (line[endPos + 1] == '0')
                {
                    littleEndian_ = false;
                    @out = new BigEndianDataOutputStream(new BufferedStream(new NetworkStream(socket_)));
                }
                else
                {
                    littleEndian_ = true;
                }

                if (sslEnable_)
                {
                    @in = littleEndian_ ? new LittleEndianDataInputStream(new BufferedStream(sslStream_)) :
                        (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(sslStream_));
                }
                else
                {
                    @in = littleEndian_ ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket_))) :
                        (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket_)));
                }

                isConnected_ = true;


                if (userId_ != null && userId_.Length > 0 && password_.Length > 0)
                {
                    if (asynTask_)
                    {
                        login(userId_, password_, false);
                    }
                    else
                    {
                        login();
                    }
                }

                if (startup_ != null && startup_ != "")
                    run(startup_);

                return true;
            }

            public void login(string userId, string password, bool enableEncryption)
            {
                userId_ = userId;
                password_ = password;
                encrypted_ = enableEncryption;
                login();
            }
            public void login()
            {
                List<IEntity> args = new List<IEntity>();
                if (encrypted_)
                {
                    BasicString keyCode = (BasicString)run("getDynamicPublicKey()");

                    string key = RSAUtils.GetKey(keyCode.getString());
                    string usr = RSAUtils.RSA(userId_, key);
                    string pass = RSAUtils.RSA(password_, key);


                    args.Add(new BasicString(usr));
                    args.Add(new BasicString(pass));
                    args.Add(new BasicBoolean(true));
                    run("login", args);
                }
                else
                {
                    args.Add(new BasicString(userId_));
                    args.Add(new BasicString(password_));
                    run("login('" + userId_ + "','" + password_ + "')"); //no encrypted temporary

                }
            }

            public IEntity run(string script, ProgressListener listener = null, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
            {
                return run(script, "script", null, null, priority, parallelism, fetchSize, clearMemory);
            }

            public IEntity run(string function, IList<IEntity> arguments, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
            {
                return run(function, "function", null, arguments, priority, parallelism, fetchSize, clearMemory);
            }

            private IEntity run(string script, string scriptType, IList<IEntity> arguments, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
            {
                return run(script, scriptType, null, arguments, priority, parallelism, fetchSize, clearMemory);
            }

            private IEntity run(string script, string scriptType, ProgressListener listener = null, IList<IEntity> arguments = null, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
            {
                try
                {
                    if (!isConnected_)
                        throw new InvalidOperationException("Database connection is not established yet.");

                    if (fetchSize > 0 && fetchSize < 8192)
                        throw new InvalidOperationException("fetchSize must be greater than 8192");

                    if (parallelism <= 0 || parallelism > 64)
                        throw new InvalidOperationException("parallelism must be greater than 0 and less than 65");

                    script = script.Replace(Environment.NewLine, "\n");

                    string body = scriptType + "\n" + script;
                    if (scriptType != "script")
                    {
                        body += ("\n" + arguments.Count.ToString() + "\n");
                        body += littleEndian_ ? "1" : "0";
                    }

                    string[] headers = null;

                    @out.writeBytes((listener != null ? "API2 " : "API ") + sessionID_ + " ");
                    @out.writeBytes(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                    int flag = generateRequestFlag(clearMemory);
                    @out.writeBytes(" / " + flag.ToString() + "_1_" + priority.ToString() + "_" + parallelism.ToString());
                    if (fetchSize > 0)
                        @out.writeBytes("__" + fetchSize.ToString());
                    @out.writeByte('\n');
                    @out.writeBytes(body);
                    if (arguments != null)
                    {
                        for (int i = 0; i < arguments.Count; ++i)
                        {
                            if (compress_ && arguments[i].isTable())//TODO: which compress method to use
                                arguments[i].writeCompressed((ExtendedDataOutput)@out);
                            else
                                arguments[i].write(@out);
                        }
                    }
                    @out.flush();

                    if (asynTask_) return null;

                    headers = @in.readLine().Split(' ');

                    if (headers.Length != 3)
                        throw new IOException("Received invalid header.");

                    int numObject = int.Parse(headers[1]);

                    string msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        throw new InvalidOperationException(msg);
                    }

                    if (numObject == 0)
                    {
                        return new data.Void();
                    }

                    flag = @in.readShort();
                    int form = flag >> 8;
                    int type = flag & 0xff;
                    bool extended = type >= 128;
                    if (type >= 128)
                        type -= 128;

                    if (form < 0 || form > MAX_FORM_VALUE)
                    {
                        throw new IOException("Invalid form value: " + form);
                    }
                    if (type < 0 || type > (int)DATA_TYPE.DT_DECIMAL128_ARRAY || type > (int)DATA_TYPE.DT_DECIMAL128 && type < (int)(int)DATA_TYPE.DT_BOOL_ARRAY)
                    {
                        throw new IOException("Invalid type value: " + type);
                    }

                    DATA_FORM df = (DATA_FORM)Enum.GetValues(typeof(DATA_FORM)).GetValue(form);
                    DATA_TYPE dt = (DATA_TYPE)type;

                    if(fetchSize == 0)
                    {
                        return factory.createEntity(df, dt, @in, extended);
                    }
                    else
                    {
                        if (df == DATA_FORM.DF_SCALAR)
                        {
                            factory.createEntity(df, dt, @in, extended);
                            throw new InvalidOperationException("fetchsize cannot be used with a return value of type scalar. ");
                        }
                        return new EntityBlockReader(@in);
                    }
                }
                catch (IOException)
                {
                    socket_.Close();
                    isConnected_ = false;
                    throw;
                }
                catch (InvalidOperationException e)
                {
                    throw new IOException(e.Message);
                }
            }

            public void upload(IDictionary<string, IEntity> variableObjectMap)
            {
                StringBuilder varNames = new StringBuilder();
                IList<IEntity> objects = new List<IEntity>();
                foreach (string key in variableObjectMap.Keys)
                {
                    if (!isVariableCandidate(key))
                    {
                        throw new System.ArgumentException("'" + key + "' is not a good variable name.");
                    }
                    varNames.Append(key + ",");
                    objects.Add(variableObjectMap[key]);
                }
                run(varNames.ToString(), "variable", objects);
            }

            private bool isVariableCandidate(string word)
            {
                char cur = word[0];
                if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z'))
                {
                    return false;
                }
                for (int i = 1; i < word.Length; i++)
                {
                    cur = word[i];
                    if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') && (cur < '0' || cur > '9') && cur != '_')
                    {
                        return false;
                    }
                }
                return true;
            }

            public static bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
            {
                if (sslPolicyErrors == SslPolicyErrors.None)
                    return true;
                else return true;
                //Console.WriteLine("Certificate error: {0}", sslPolicyErrors);

                // Do not allow this client to communicate with unauthenticated servers.
                //return false;
            }

            ~DBConnectionImpl()
            {
                if (socket_ != null)
                    socket_.Close();
            }

            public void close()
            {
                if (socket_ != null)
                    socket_.Close();
                sessionID_ = "";
                socket_ = null;
                isConnected_ = false;
            }

            public bool isConnected()
            {
                return isConnected_;
            }

            public virtual string HostName
            {
                get
                {
                    return hostName_;
                }
            }

            public virtual int Port
            {
                get
                {
                    return port_;
                }
            }

            public string LocalAddress
            {
                get
                {
                    return ((IPEndPoint)socket_.LocalEndPoint).Address.ToString();
                }
            }

            public void setasynTask(bool asynTask)
            {
                asynTask_ = asynTask;
            }

            public void getHostPort(out string host, out int port) { host = hostName_; port = port_; }

            public virtual bool RemoteLittleEndian()
            {
                return littleEndian_;
            }


        }

        public bool isConnected
        {
            get
            {
                return conn_.isConnected();
            }
        }


        public string getSessionID()
        {
            return conn_.getSessionID();
        }

        public Socket getSocket()
        {
            return conn_.getSocket();
        }

        public ExtendedDataInput getDataInputStream()
        {
            return conn_.getDataInputStream();
        }

        private int lastConnNodeIndex_ = 0;

        public DBConnection(bool asynchronousTask = false, bool useSSL = false, bool compress = false, bool usePython = false,bool isReverseStreaming = false)
        {
            asynTask_ = asynchronousTask;
            isUseSSL_ = useSSL;

            this.compress_ = compress;
            this.isReverseStreaming_ = isReverseStreaming;
            conn_ = new DBConnectionImpl(useSSL, asynchronousTask, compress, usePython, isReverseStreaming);
        }

        public bool isBusy()
        {
            if (Monitor.TryEnter(threadLock_))
            {
                Monitor.Exit(threadLock_);
                return false;
            }
            else
            {
                return true;
            }
        }
        public void setasynTask(bool asynTask)
        {
            conn_.setasynTask(asynTask_);
            asynTask_ = asynTask;
        }



        public bool connect(string hostName, int port, string[] highAvailabilitySites)
        {
            return connect(hostName, port, "", "", "", true, highAvailabilitySites);
        }

#if NETCOREAPP
        public async Task<bool> connectAsync(string hostName, int port, string userId = "", string password = "", string startup = "", bool highAvailability = false, string[] highAvailabilitySites = null, bool reconnect = false)
        {
            bool result = await Task.Run(() => connect(hostName, port, userId, password, startup, highAvailability, highAvailabilitySites, reconnect));
            return result;
        }
#endif

        public bool connect(string hostName, int port, string userId = "", string password = "", string startup = "", bool highAvailability = false, string[] highAvailabilitySites = null, bool reconnect = false)
        {
            lock (threadLock_)
            {
                userId_ = userId;
                password_ = password;
                startup_ = startup;
                nodes_ = new List<Node>();
                if (highAvailability)
                {
                    nodes_ = new List<Node>();
                    bool firstFound = false;
                    foreach(Node it in nodes_)
                    {
                        if(it.hostName_ == hostName && it.port_ == port)
                        {
                            firstFound = true;
                            break;
                        }
                    }
                    if(!firstFound)
                        nodes_.Add(new Node(hostName, port));
                    if (highAvailabilitySites != null)
                    {
                        foreach (string site in highAvailabilitySites)
                        {
                            string[] HAsite = site.Split(':');
                            if (HAsite.Length != 2)
                            {
                                throw new ArgumentException("The site '" + site + "' is invalid.");
                            }
                            string parseHost = HAsite[0];
                            int parsePort = int.Parse(HAsite[1]);
                            if (parseHost != hostName || parsePort != port)
                            {
                                nodes_.Add(new Node(parseHost, parsePort));
                            }
                        }
                    }
                    connectMinNode();
                    return true;

                }
                else
                {
                    if (reconnect)
                    {
                        nodes_ = new List<Node> { new Node(hostName, port) };
                        switchDataNode();
                    }
                    else
                    {
                        if (!connectNode(hostName, port))
                            return false;
                    }
                }
            }
            return true;
        }

        void switchDataNode(string host = "", int port = -1)
        {
            bool connected = false;
            do
            {
                if (host != "")
                {
                    if (connectNode(host, port))
                    {
                        connected = true;
                        break;
                    }
                }
                if (nodes_.Count == 0)
                {
                    throw new Exception("Failed to connect to " + host + ":" + port.ToString());
                }
                for (int i = nodes_.Count - 1; i >= 0; i--)
                {
                    lastConnNodeIndex_ = (lastConnNodeIndex_ + 1) % nodes_.Count;
                    if (connectNode(nodes_[lastConnNodeIndex_].hostName_, nodes_[lastConnNodeIndex_].port_))
                    {
                        connected = true;
                        break;
                    }
                }
                Thread.Sleep(1000);
            } while (connected == false);
        }

        public void login(string userId, string password, bool enableEncryption)
        {
            lock (threadLock_)
            {
                conn_.login(userId, password, enableEncryption);
                this.userId_ = userId;
                this.password_ = password;
                //this.encrypted = false; //no encrypted temporary
            }
        }

        public virtual bool RemoteLittleEndian
        {
            get
            {
                return conn_.RemoteLittleEndian();
            }
        }

        public virtual IEntity tryRun(string script)
        {

            if (isBusy())
            {
                return null;
            }
            try
            {
                return run(script);
            }
            catch
            {
                throw;
            }
        }

#if NETCOREAPP
        public async Task<IEntity> runAsync(string script, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
        {
            IEntity result = await Task.Run(() => run(script, priority, parallelism, fetchSize, clearMemory));
            return result;
        }

        public async Task<IEntity> runAsync(string function, IList<IEntity> arguments, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
        {
            IEntity result = await Task.Run(() => run(function, arguments, priority, parallelism, fetchSize, clearMemory));
            return result;
        }
#endif

        public IEntity run(string script)
        {
            return run(script, (ProgressListener)null);
        }

        public IEntity run(string script, ProgressListener listener)
        {
            return run(script, listener, false);
        }

        public IEntity run(string script, bool clearSessionMemory)
        {
            return run(script, (ProgressListener)null, clearSessionMemory);
        }

        public IEntity run(string script, ProgressListener listener, bool clearSessionMemory)
        {
            return run(script, listener, 4, 2, 0, clearSessionMemory);
        }

        public IEntity tryRun(string function, IList<IEntity> arguments)
        {
            if (isBusy())
            {
                return null;
            }
            try
            {
                return run(function, arguments);
            }
            finally
            {
            }
        }

        public IEntity run(string script, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
        {
            return run(script, (ProgressListener)null, priority, parallelism, fetchSize, clearMemory);
        }

        public IEntity run(string script, ProgressListener listener = null, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
        {
            lock (threadLock_)
            {
                if (nodes_.Count > 0)
                {
                    while (true)
                    {
                        try
                        {
                            return conn_.run(script, listener, priority, parallelism, fetchSize, clearMemory);
                        }
                        catch (IOException e)
                        {
                            string host = "";
                            int port = 0;
                            if (connected())
                            {
                                ExceptionType type = parseException(e.Message, out host, out port);
                                if (type == ExceptionType.ET_IGNORE)
                                    return new BasicBoolean(true);
                                else if (type == ExceptionType.ET_UNKNOW)
                                    throw new IOException(e.Message);
                            }
                            else
                            {
                                ExceptionType type = parseException(e.Message, out host, out port);
                                if (type == ExceptionType.ET_NOTSUPPORT) throw;
                            }
                            switchDataNode(host, port);
                        }
                    }
                }
                else
                {
                    return conn_.run(script, listener, priority, parallelism, fetchSize, clearMemory);
                }
            }
        }
        public IEntity run(string function, IList<IEntity> arguments, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false)
        {
            lock (threadLock_)
            {
                if (nodes_.Count > 0)
                {
                    while (true)
                    {
                        try
                        {
                            return conn_.run(function, arguments, priority, parallelism, fetchSize, clearMemory);
                        }
                        catch (IOException e)
                        {
                            string host = "";
                            int port = 0;
                            if (connected())
                            {
                                ExceptionType type = parseException(e.Message, out host, out port);
                                if (type == ExceptionType.ET_IGNORE)
                                    return new BasicBoolean(true);
                                else if (type == ExceptionType.ET_UNKNOW)
                                    throw new IOException(e.Message);
                            }
                            else
                            {
                                ExceptionType type = parseException(e.Message, out host, out port);
                                if (type == ExceptionType.ET_NOTSUPPORT) throw;
                            }
                            switchDataNode(host, port);
                        }
                    }
                }
                else
                {
                    return conn_.run(function, arguments);
                }
            }
        }

        public IEntity run(string script, ProgressListener listener, int priority, int parallelism, int fetchSize)
        {
            return run(script, listener, priority, parallelism, fetchSize, false);
        }

        public void tryUpload(IDictionary<string, IEntity> variableObjectMap)
        {
            if (isBusy())
            {
                throw new IOException("The connection is in use.");
            }
            upload(variableObjectMap);
        }
        public void upload(IDictionary<string, IEntity> variableObjectMap)
        {
            lock (threadLock_)
            {
                if (nodes_.Count > 0)
                {
                    while (true)
                    {
                        try
                        {
                            conn_.upload(variableObjectMap);
                            break;
                        }
                        catch (IOException e)
                        {
                            string host = "";
                            int port = 0;
                            if (connected())
                            {
                                ExceptionType type = parseException(e.Message, out host, out port);
                                if (type == ExceptionType.ET_IGNORE)
                                    return;
                                else if (type == ExceptionType.ET_UNKNOW)
                                    throw new IOException(e.Message);
                            }
                            switchDataNode(host, port);
                        }
                    }
                }
                else
                {
                    conn_.upload(variableObjectMap);
                }
            }
        }

        public List<IEntity> run(IList<string> sqlList, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            List<IEntity> results = new List<IEntity>();
            foreach (string sql in sqlList)
            {
                IEntity entity = run(sql, priority, parallelism, 0, clearMemory);
                results.Add(entity);
            }
            return results;
        }

#if NETCOREAPP
        public async Task<List<IEntity>> runAsync(IList<string> sqlList, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            List<IEntity> result = await Task.Run(() => run(sqlList, priority, parallelism, clearMemory));
            return result;
        }
#endif

        public List<IEntity> run(IList<string> sqlList, IList<IList<IEntity>> argumentsList, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            if(sqlList.Count != argumentsList.Count)
            {
                throw new Exception("The length of sqlList and argumentsList are not equal");
            }
            List<IEntity> results = new List<IEntity>();
            for(int i = 0; i < sqlList.Count; i++)
            {
                IEntity entity = run(sqlList[i], argumentsList[i], priority, parallelism, 0, clearMemory);
                results.Add(entity);
            }
            return results;
        }

#if NETCOREAPP
        public async Task<List<IEntity>> runAsync(IList<string> sqlList, IList<IList<IEntity>> argumentsList, int priority = 4, int parallelism = 2, bool clearMemory = false)
        {
            List<IEntity> result = await Task.Run(() => run(sqlList, argumentsList, priority, parallelism, clearMemory));
            return result;
        }
#endif

        public virtual void close()
        {
            lock (threadLock_)
            {
                conn_.close();
            }
        }

        public void setLoadBalance(bool loadBalance)
        {
            this.loadBalance_ = loadBalance;
        }

        private bool isVariableCandidate(string word)
        {
            char cur = word[0];
            if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z'))
            {
                return false;
            }
            for (int i = 1; i < word.Length; i++)
            {
                cur = word[i];
                if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') && (cur < '0' || cur > '9') && cur != '_')
                {
                    return false;
                }
            }
            return true;
        }

        public virtual string HostName
        {
            get
            {
                return conn_.HostName;
            }
        }

        public virtual int Port
        {
            get
            {
                return conn_.Port;
            }
        }

        public string LocalAddress
        {
            get
            {
                return conn_.LocalAddress;
            }
        }
        public static bool ValidateServerCertificate(
                        object sender,
                        X509Certificate certificate,
                        X509Chain chain,
                        SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;
            else return true;
            //Console.WriteLine("Certificate error: {0}", sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            //return false;
        }

        bool connected()
        {
            try
            {
                BasicInt ret = (BasicInt)conn_.run("1+1");
                return ret.getInt() == 2;
            }
            catch (IOException)
            {
                return false;
            }
        }




        bool connectMinNode()
        {
            while (true)
            {
                string hostName;
                int port;
                Node connectedNode = null;
                BasicTable table = null;

                //Ensure getClusterPerf success
                while (true)
                {
                    while (connectedNode == null)
                    {
                        foreach (Node one in nodes_)
                        {
                            if (connectNode(one.hostName_, one.port_))
                            {
                                connectedNode = one;
                                break;
                            }
                            Thread.Sleep(100);
                        }
                    }
                    try
                    {
                        table = (BasicTable)conn_.run("rpc(getControllerAlias(), getClusterPerf)");
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.Out.WriteLine("ERROR getting other data nodes, exception: " + e.Message);
                        ExceptionType type = parseException(e.Message, out hostName, out port);
                        if (connected())
                        {
                            if (type == ExceptionType.ET_IGNORE)
                                break;
                            else if (type == ExceptionType.ET_NEWLEADER || type == ExceptionType.ET_NODENOTAVAIL)
                            {
                                switchDataNode(hostName, port);
                            }
                        }
                        else
                        {
                            switchDataNode(hostName, port);
                        }
                    }
                }
                if (loadBalance_)
                {
                    BasicStringVector colHost = (BasicStringVector)table.getColumn("host");
                    BasicIntVector colPort = (BasicIntVector)table.getColumn("port");
                    BasicIntVector colMode = (BasicIntVector)table.getColumn("mode");
                    BasicIntVector colmaxConnections = (BasicIntVector)table.getColumn("maxConnections");
                    BasicIntVector colconnectionNum = (BasicIntVector)table.getColumn("connectionNum");
                    BasicIntVector colworkerNum = (BasicIntVector)table.getColumn("workerNum");
                    BasicIntVector colexecutorNum = (BasicIntVector)table.getColumn("executorNum");
                    double load;
                    for (int i = 0; i < colMode.rows(); ++i)
                    {
                        if (colMode.getInt(i) == 0)
                        {
                            string nodeHost = colHost.getString(i);
                            int nodePort = colPort.getInt(i);
                            Node existNode = null;
                            foreach (Node node in nodes_)
                            {
                                if (node.hostName_ == nodeHost && node.port_ == nodePort)
                                {
                                    existNode = node;
                                    break;
                                }
                            }

                            //node is out of highAvailabilitySites
                            if (existNode == null)
                                continue;

                            if (colexecutorNum.getInt(i) < colmaxConnections.getInt(i))
                            {
                                load = (colconnectionNum.getInt(i) + colworkerNum.getInt(i) + colexecutorNum.getInt(i)) / 3.0;
                            }
                            else
                            {
                                load = double.MaxValue;
                            }

                            existNode.load_ = load;
                        }
                    }

                    Node minNode = nodes_[0];
                    foreach (Node one in nodes_)
                    {
                        if (one.load_ < minNode.load_)
                            minNode = one;
                    }
                    if (!(minNode.hostName_ == connectedNode.hostName_ && minNode.port_ == connectedNode.port_))
                    {
                        Console.Out.WriteLine(string.Format("Connect to min load node: host:{0} port:{1}", minNode.hostName_, minNode.port_));
                        conn_.close();
                        switchDataNode(minNode.hostName_, minNode.port_);
                    }
                }

                return true;
            }
        }

        bool connectNode(string hostName, int port)
        {
            Console.Out.WriteLine("Connect to " + hostName + ":" + port + ".");
            while (true)
            {
                try
                {
                    return conn_.connect(hostName, port, userId_, password_, isUseSSL_, asynTask_, compress_, startup_);
                }
                catch (IOException e)
                {
                    if (connected())
                    {
                        ExceptionType type = parseException(e.Message, out hostName, out port);
                        if (type != ExceptionType.ET_NEWLEADER)
                        {
                            if (type == ExceptionType.ET_IGNORE)
                                return true;
                            else if (type == ExceptionType.ET_NODENOTAVAIL)
                                return false;
                            else if (type == ExceptionType.ET_NOINITIALIZED)
                                return false;
                            else //UNKNOW
                                throw new IOException(e.Message);
                        }
                    }
                    else
                    {
                        Console.Out.WriteLine(e.Message);
                        return false;
                    }
                }
                Thread.Sleep(100);
            }
        }

        class Node
        {
            public string hostName_;
            public int port_;
            public double load_;//-1 : unknow

            bool isEqual(Node node)
            {
                return hostName_ == node.hostName_ && this.port_ == node.port_;
            }
            public Node(string hostName, int port, double load = -1.0)
            {
                this.hostName_ = hostName;
                this.port_ = port;
                this.load_ = load;
            }
        };

        ExceptionType parseException(string msg, out string host, out int port)
        {
            int index = msg.IndexOf("<NotLeader>");
            if (index != -1)
            {
                index = msg.IndexOf(">");
                string ipport = msg.Substring(index + 1);
                parseIpPort(ipport, out host, out port);
                Console.Out.WriteLine(string.Format("New leader is {0}:{1}.", host, port));
                return ExceptionType.ET_NEWLEADER;
            }
            else if((index = msg.IndexOf("<DataNodeNotAvail>")) != -1)
            {
                index = msg.IndexOf(">");
                string ipPort = msg.Substring(index + 1);
                string newIp;
                int newPort;
                parseIpPort(ipPort, out newIp, out newPort);
                string lastHost;
                int lastPort;
                conn_.getHostPort(out lastHost, out lastPort);
                //if (lastHost == newIp && lastPort == newPort)
                //{
                host = "";
                port = 0;
                Console.Out.WriteLine(msg);
                return ExceptionType.ET_NODENOTAVAIL;
                //}
                //else
                //{//other node not avail, ignore it
                //    host = "";
                //    port = 0;
                //    Console.Out.WriteLine(string.Format("This node {0}:{1} is not avail.", newIp, newPort));
                //    return ExceptionType.ET_IGNORE;
                //}
            }
            else if((index = msg.IndexOf("The datanode isn't initialized yet. Please try again later")) != -1 || (index = msg.IndexOf("DFS is not enabled or the script was not executed on a data node.")) != -1)
            {
                host = "";
                port = 0;
                return ExceptionType.ET_NOINITIALIZED;
            }
            else if ((index = msg.IndexOf("Data type")) != -1 && (index = msg.IndexOf("is not supported")) != -1)
            {
                host = "";
                port = 0;
                return ExceptionType.ET_NOTSUPPORT;
            }
            else
            {
                host = "";
                port = 0;
                return ExceptionType.ET_UNKNOW;
            }
        }

        void parseIpPort(string ipPort, out string ip, out int port)
        {
            string[] v = ipPort.Split(':');
            if (v.Length < 2)
            {
                throw new InvalidOperationException("The format of highAvailabilitySite " + ipPort +
                    " is incorrect, should be host:port, e.g. 192.168.1.1:8848");
            }
            ip = v[0];
            port = int.Parse(v[1]);
            if (port <= 0 || port > 65535)
            {
                throw new InvalidOperationException("The format of highAvailabilitySite " + ipPort +
                    " is incorrect, port should be a positive integer less or equal to 65535");
            }
        }
    }


}
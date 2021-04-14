//-------------------------------------------------------------------------------------------
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

        private static readonly int MAX_FORM_VALUE = Enum.GetValues(typeof(DATA_FORM)).Length - 1;
        private static readonly int MAX_TYPE_VALUE = Enum.GetValues(typeof(DATA_TYPE)).Length - 1;

        private static readonly object threadLock = new object();
        private string sessionID;
        private Socket socket;
        private bool remoteLittleEndian;
        private ExtendedDataOutput @out;
        private ExtendedDataInput @in;
        private IEntityFactory factory;
        private string hostName;
        private string mainHostName;
        private int port;
        private int mainPort;
        private string userId;
        private string password;
        private bool encrypted;
        private string startup;
        //=============HighAvailability 2019.07.17 Linl======================
        private bool highAvailability;
        //2021.01.14 cwj
        private string[] highAvailabilitySites = null;
        private bool HAreconnect = false;
        //
        private string controllerHost = null;
        private int controllerPort;
        //=============Asyntask 2021.01.14 cwj======================
        private bool asynTask = false;
        private bool isUseSSL = false;
        //=============Asyntask 2021.02.04 cwj======================
        private SslStream sslStream;
        //===================================================================

        public bool isConnected
        {
            get
            {
                return sessionID.Length > 0;
            }
        }

        public string getSessionID()
        {
            return sessionID;
        }
        public DBConnection()
        {
            factory = new BasicEntityFactory();
            sessionID = "";
        }

        public DBConnection(bool asynchronousTask)
        {
            factory = new BasicEntityFactory();
            sessionID = "";
            asynTask = asynchronousTask;
        }
        public DBConnection(bool asynchronousTask, bool useSSL)
        {
            factory = new BasicEntityFactory();
            sessionID = "";
            asynTask = asynchronousTask;
            isUseSSL = useSSL;
        }

        public bool isBusy()
        {
            if (Monitor.TryEnter(threadLock))
            {
                Monitor.Exit(threadLock);
                return false;
            }
            else
            {
                return true;
            }
        }
        public void setasynTask(bool asynTask)
        {
            this.asynTask = asynTask;
        }
        
        public bool connect(string hostName, int port)
        {
            return connect(hostName, port, "","","",false);
        }

        public bool connect(string hostName, int port, string userId, string password)
        {
            return connect(hostName, port, userId, password, "", false);
        }
        public bool connect(string hostName, int port, string userId, string password,string startup)
        {
            return connect(hostName, port, userId, password, startup, false);
        }

        public bool connect(string hostName, int port, string userId, string password, string startup, bool highAvailability)
        {
            return connect(hostName, port, userId, password, startup, highAvailability, null);
        }
        public bool connect(string hostName, int port, string[] highAvailabilitySites  )
        {
            return connect(hostName, port, "", "", "", true, highAvailabilitySites);
        }

        public bool connect(string hostName, int port, string userId, string password, string startup , bool highAvailability, string[] highAvailabilitySites)
        {
            lock (threadLock)
            {
                try
                {
                    if (sessionID.Length > 0)
                    {
                        return true;
                    }

                    this.hostName = hostName;
                    this.mainHostName = hostName;
                    this.port = port;
                    this.mainPort = port;
                    this.userId = userId;
                    this.password = password;
                    encrypted = false;
                    this.startup = startup;
                    this.highAvailability = highAvailability;
                    this.highAvailabilitySites = highAvailabilitySites;

                    if(highAvailabilitySites != null)
                    {
                        foreach(string site in  highAvailabilitySites)
                        {
                            string[] HAsite = site.Split(':');
                            if(HAsite.Length != 2)
                            {
                                throw new ArgumentException("The site '" + site + "' is invalid.");
                            }
                        }
                    }
                    System.Diagnostics.Debug.Assert(highAvailabilitySites == null || highAvailability);

                    return connect();
                }
                finally
                {

                }
            }

        }


        public bool connect()
        {

            try
            {
                if (isUseSSL)
                {
                    TcpClient client = new TcpClient(hostName, port);
                    
                    sslStream = new SslStream(client.GetStream(), false, new RemoteCertificateValidationCallback(ValidateServerCertificate), null);
                    try
                    {
                        sslStream.AuthenticateAsClient("www.example.com", null, (SslProtocols)(0xc0 | 0x300 | 0xc00), false);
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

                    socket = client.Client;
                    socket.NoDelay = true;
                    @in = new LittleEndianDataInputStream(new BufferedStream(sslStream));
                    @out = new LittleEndianDataOutputStream(new BufferedStream(sslStream));
                }
                else
                {
                    
                    socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(hostName, port);
                    socket.NoDelay = true;
                    @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                    @in = new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                }
            }
            catch(System.Net.Sockets.SocketException ex)
            {
                if (HAreconnect)
                    return false;
                if (switchToRandomAvailableSite())
                    return true;
                throw ex;
            }
            
            finally { }                                         

            string body = "connect\n";
            @out.writeBytes("API 0 ");
            @out.writeBytes(body.Length.ToString());
            if (asynTask)
            {
                @out.writeBytes(" / 4_1_4_2");
            }
            else
            {
                @out.writeBytes(" / 0_1_4_2");
            }
            @out.writeByte('\n');
            @out.writeBytes(body);
            @out.flush();


            string line = @in.readLine();
            int endPos = line.IndexOf(' ');
            if (endPos <= 0)
            {
                close();
                if (switchToRandomAvailableSite())
                    return true;
                throw new IOException("Invalid ack msg : " + line);
            }
            sessionID = line.Substring(0, endPos);

            int startPos = endPos + 1;
            endPos = line.IndexOf(' ', startPos);
            if (endPos != line.Length - 2)
            {
                close();
                if (switchToRandomAvailableSite()) return true;
                throw new IOException("Invalid ack msg : " + line);
                //return false;
            }

            if (line[endPos + 1] == '0')
            {
                remoteLittleEndian = false;
                @out = new BigEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
            }
            else
            {
                remoteLittleEndian = true;
            }

            if (isUseSSL)
            {
                @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(sslStream)) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(sslStream));
            }
            else
            {
                @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
            }
            
            

            if (userId.Length > 0 && password.Length > 0)
            {
                if (asynTask)
                {
                    login(userId, password, false);
                }
                else
                {
                    login();
                }
            }

            if (startup != "") run(startup);
            if (highAvailability && controllerHost == null)
            {
                try
                {
                    controllerHost = ((BasicString)run("rpc(getControllerAlias(),getNodeHost)")).getString();
                    Console.Out.WriteLine("get controllerHost " + controllerHost);
                    controllerPort = ((BasicInt)run("rpc(getControllerAlias(),getNodePort)")).getValue();
                    Console.Out.WriteLine("get controllerPort " + controllerPort);
                }
                catch (Exception) { }
            }
            return true;
        }

        public void login(string userId, string password, bool enableEncryption)
        {
            lock (threadLock)
            {
                try
                {
                    this.userId = userId;
                    this.password = password;
                    encrypted = enableEncryption;
                    //this.encrypted = false; //no encrypted temporary
                    login();
                }
                finally
                {

                }

            }

        }

        private void login()
        {
            List<IEntity> args = new List<IEntity>();
            if (encrypted)
            {
                BasicString keyCode = (BasicString)run("getDynamicPublicKey()");

                string key = RSAUtils.GetKey(keyCode.getString());
                string usr = RSAUtils.RSA(userId, key);
                string pass = RSAUtils.RSA(password, key);


                args.Add(new BasicString(usr));
                args.Add(new BasicString(pass));
                args.Add(new BasicBoolean(true));
                run("login", args);
            }
            else
            {
                args.Add(new BasicString(userId));
                args.Add(new BasicString(password));
                run("login('" + userId + "','" + password + "')"); //no encrypted temporary

            }
            //login("login", args);


        }

        public virtual bool RemoteLittleEndian
        {
            get
            {
                return remoteLittleEndian;
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

        public IEntity run(string script)
        {
            return run(script, (ProgressListener)null);
        }

        public bool tryReconnect()
        {
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(hostName, port);
            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));

            @in = new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
            string body = "connect\n";
            @out.writeBytes("API 0 ");
            @out.writeBytes(body.Length.ToString());
            if (asynTask)
            {
                @out.writeBytes(" / 4_1_4_2");
            }
            else
            {
                @out.writeBytes(" / 0_1_4_2");
            }
            @out.writeByte('\n');
            @out.writeBytes(body);
            @out.flush();


            string line = @in.readLine();
            int endPos = line.IndexOf(' ');
            if (endPos <= 0)
            {
                close();
                return false;
            }
            sessionID = line.Substring(0, endPos);

            int startPos = endPos + 1;
            endPos = line.IndexOf(' ', startPos);
            if (endPos != line.Length - 2)
            {
                close();
                return false;
            }

            if (line[endPos + 1] == '0')
            {
                remoteLittleEndian = false;
                @out = new BigEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
            }
            else
            {
                remoteLittleEndian = true;
            }
            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));

            return true;
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
            lock (threadLock)
            {
                try
                {
                    bool reconnect = false;
                    if (socket == null || !socket.Connected)
                    {
                        if (sessionID.Length == 0)
                        {
                            throw new IOException("Database connection is not established yet.");
                        }
                        else
                        {
                            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            socket.Connect(hostName, port);

                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                        }
                    }
                    // "\r\n" replace to "\n" for windows
                    script = script.Replace(Environment.NewLine, "\n");

                    string body = "script\n" + script;
                    string header = null;
                    try
                    {
                        @out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                        @out.writeBytes(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                        int flag = 0;
                        if (asynTask)
                            flag += 4;
                        if (clearSessionMemory)
                            flag += 16;
                        @out.writeBytes(" / "+ flag.ToString() +"_1_4_2");
                        @out.writeByte('\n');
                        @out.writeBytes(body);
                        @out.flush();

                        if (asynTask) return null;

                        header = @in.readLine();
                    }
                    catch (IOException ex)
                    {
                        if (reconnect)
                        {
                            socket = null;
                            throw ex;
                        }

                        try
                        {
                            tryReconnect();
                            @out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                            @out.writeBytes(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                            int flag = 0;
                            if (asynTask)
                                flag += 4;
                            if (clearSessionMemory)
                                flag += 16;
                            @out.writeBytes(" / " + flag.ToString() + "_1_4_2");
                            @out.writeByte('\n');
                            @out.writeBytes(body);
                            @out.flush();

                            if (asynTask) return null;

                            header = @in.readLine();
                            reconnect = true;
                        }
                        catch (Exception e)
                        {
                            socket = null;
                            throw e;
                        }
                    }
                    string msg;

                    while (header.Equals("MSG"))
                    {
                        //read intermediate message to indicate the progress
                        msg = @in.readString();
                        if (listener != null)
                        {
                            listener.progress(msg);
                        }
                        header = @in.readLine();
                    }

                    string[] headers = header.Split(' ');
                    if (headers.Length != 3)
                    {
                        socket = null;
                        throw new IOException("Received invalid header: " + header);
                    }

                    if (reconnect)
                    {
                        sessionID = headers[0];
                        if (userId.Length > 0 && password.Length > 0)
                        {
                            login();
                        }
                        if (this.startup != "") run(startup);
                    }
                    int numObject = int.Parse(headers[1]);

                    msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        if (ServerExceptionUtils.isNotLogin(msg))
                        {
                            if (userId.Length > 0 && password.Length > 0)
                                login();
                            else
                                throw new IOException(msg);
                        }

                        else
                        {
                            throw new IOException(msg);
                        }
                        
                    }

                    if (numObject == 0)
                    {
                        return new data.Void();
                    }
                    try
                    {
                        short flag = @in.readShort();
                        int form = flag >> 8;
                        int type = flag & 0xff;

                        if (form < 0 || form > MAX_FORM_VALUE)
                        {
                            throw new IOException("Invalid form value: " + form);
                        }
                        if (type < 0 || type > MAX_TYPE_VALUE)
                        {
                            throw new IOException("Invalid type value: " + type);
                        }

                        DATA_FORM df = (DATA_FORM)Enum.GetValues(typeof(DATA_FORM)).GetValue(form);
                        DATA_TYPE dt = (DATA_TYPE)Enum.GetValues(typeof(DATA_TYPE)).GetValue(type);

                        return factory.createEntity(df, dt, @in);
                    }
                    catch (IOException ex)
                    {
                        socket = null;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    if (socket != null || !highAvailability) { 
                        Console.WriteLine("socket not null or highAvailability is not set");
                        throw ex;
                    }
                    else if (switchToRandomAvailableSite())
                        return run(script, listener);
                    else
                        throw ex;
                }
            }
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
        public IEntity run(string script, ProgressListener listener, int priority, int parallelism, int fetchSize)
        {
            if (fetchSize < 8192)
                throw new IOException("fetchSize should be no less than 8192");

            lock (threadLock)
            {
                try
                {
                    bool reconnect = false;
                    if (socket == null || !socket.Connected)
                    {
                        if (sessionID.Length == 0)
                        {
                            throw new IOException("Database connection is not established yet.");
                        }
                        else
                        {
                            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            socket.Connect(hostName, port);

                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                        }
                    }
                    // "\r\n" replace to "\n" for windows
                    script = script.Replace(Environment.NewLine, "\n");

                    string body = "script\n" + script;
                    string header = null;
                    try
                    {
                        @out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                        @out.writeBytes(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                        @out.writeBytes(" / 0_1_" + priority.ToString() + "_" + parallelism.ToString());
                        @out.writeBytes("__" + fetchSize.ToString());
                        @out.writeByte('\n');
                        @out.writeBytes(body);
                        @out.flush();
                        header = @in.readLine();
                    }
                    catch (IOException ex)
                    {
                        if (reconnect)
                        {
                            socket = null;
                            throw ex;
                        }

                        try
                        {
                            tryReconnect();
                            @out.writeBytes((listener != null ? "API2 " : "API ") + sessionID + " ");
                            @out.writeBytes(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0).ToString());
                            @out.writeBytes(" / 0_1_" + priority.ToString() + "_" + parallelism.ToString());
                            @out.writeBytes("__" + fetchSize.ToString());
                            @out.writeByte('\n');
                            @out.writeBytes(body);
                            @out.flush();
                            header = @in.readLine();
                            reconnect = true;
                        }
                        catch (Exception e)
                        {
                            socket = null;
                            throw e;
                        }
                    }
                    string msg;

                    while (header.Equals("MSG"))
                    {
                        //read intermediate message to indicate the progress
                        msg = @in.readString();
                        if (listener != null)
                        {
                            listener.progress(msg);
                        }
                        header = @in.readLine();
                    }

                    string[] headers = header.Split(' ');
                    if (headers.Length != 3)
                    {
                        socket = null;
                        throw new IOException("Received invalid header: " + header);
                    }

                    if (reconnect)
                    {
                        sessionID = headers[0];
                        if (userId.Length > 0 && password.Length > 0)
                        {
                            login();
                        }
                        if (this.startup != "") run(startup);
                    }
                    int numObject = int.Parse(headers[1]);

                    msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        if (ServerExceptionUtils.isNotLogin(msg))
                        {
                            if (userId.Length > 0 && password.Length > 0)
                                login();
                            else
                                throw new IOException(msg);
                        }

                        else
                        {
                            throw new IOException(msg);
                        }
                    }

                    if (numObject == 0)
                    {
                        return new data.Void();
                    }
                    try
                    {
                        short flag = @in.readShort();
                        int form = flag >> 8;
                        int type = flag & 0xff;

                        if (form < 0 || form > MAX_FORM_VALUE)
                        {
                            throw new IOException("Invalid form value: " + form);
                        }
                        if (type < 0 || type > MAX_TYPE_VALUE)
                        {
                            throw new IOException("Invalid type value: " + type);
                        }

                        DATA_FORM df = (DATA_FORM)Enum.GetValues(typeof(DATA_FORM)).GetValue(form);
                        DATA_TYPE dt = (DATA_TYPE)Enum.GetValues(typeof(DATA_TYPE)).GetValue(type);
                        if (df != DATA_FORM.DF_VECTOR)
                            throw new IOException("Invalid input data form,should be DF_VECTOR");
                        if (dt != DATA_TYPE.DT_ANY)
                            throw new IOException("Invalid input data type,should be DT_ANY");
                        return new EntityBlockReader(@in);
                    }
                    catch (IOException ex)
                    {
                        socket = null;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    if (socket != null || !highAvailability)
                        throw ex;
                    else if (switchServer())
                        return run(script, listener);
                    else
                        throw ex;
                }
            }
        }
        public IEntity run(string function, IList<IEntity> arguments)
        {
            lock (threadLock)
            {
                try
                {
                    bool reconnect = false;
                    if (socket == null || !socket.Connected)
                    {
                        if (sessionID.Length == 0)
                        {
                            throw new IOException("Database connection is not established yet.");
                        }
                        else
                        {
                            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            socket.Connect(hostName, port);
                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                        }
                    }

                    string body = "function\n" + function;
                    body += ("\n" + arguments.Count + "\n");
                    body += remoteLittleEndian ? "1" : "0";
                    
                    string[] headers = null;
                    try
                    {
                        @out.writeBytes("API " + sessionID + " ");
                        @out.writeBytes(body.Length.ToString());
                        if (asynTask)
                        {
                            @out.writeBytes(" / 4_1_4_2");
                        }
                        else
                        {
                            @out.writeBytes(" / 0_1_4_2");
                        }
                        @out.writeByte('\n');
                        @out.writeBytes(body);
                        for (int i = 0; i < arguments.Count; ++i)
                        {
                            arguments[i].write(@out);
                        }
                        @out.flush();

                        if (asynTask) return null;

                        headers = @in.readLine().Split(' ');
                    }
                    catch (IOException ex)
                    {
                        if (reconnect)
                        {
                            socket = null;
                            throw ex;
                        }
                        NotLeaderStatus status = handleNotLeaderException(ex, null);
                        if (status == NotLeaderStatus.NEW_LEADER)
                            return run(function, arguments);
                        else if (status == NotLeaderStatus.WAIT)
                        {
                            if (!HAreconnect)
                            {
                                HAreconnect = true;
                                while (true)
                                {
                                    try
                                    {
                                        IEntity re = run(function, arguments);
                                        HAreconnect = false;
                                        return re;
                                    }
                                    catch (Exception e)
                                    {
                                    }
                                }
                            }
                            throw ex;
                        }
                        try
                        {
                            tryReconnect();
                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @out.writeBytes("API " + sessionID + " ");
                            @out.writeBytes(body.Length.ToString());
                            if (asynTask)
                            {
                                @out.writeBytes(" / 4_1_4_2");
                            }
                            else
                            {
                                @out.writeBytes(" / 0_1_4_2");
                            }
                            @out.writeByte('\n');
                            @out.writeBytes(body);
                            for (int i = 0; i < arguments.Count; ++i)
                            {
                                arguments[i].write(@out);
                            }
                            @out.flush();

                            if (asynTask) return null;

                            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                            headers = @in.readLine().Split(' ');
                            reconnect = true;
                        }
                        catch (Exception e)
                        {
                            socket = null;
                            throw e;
                        }
                    }

                    if (headers.Length != 3)
                    {
                        socket = null;
                        throw new IOException("Received invalid header.");
                    }

                    if (reconnect)
                    {
                        sessionID = headers[0];
                        if (userId.Length > 0 && password.Length > 0)
                        {
                            login();
                        }
                        if (this.startup != "") run(startup);
                    }
                    int numObject = int.Parse(headers[1]);

                    string msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        throw new IOException(msg);
                    }

                    if (numObject == 0)
                    {
                        return new data.Void();
                    }

                    try
                    {
                        short flag = @in.readShort();
                        int form = flag >> 8;
                        int type = flag & 0xff;

                        if (form < 0 || form > MAX_FORM_VALUE)
                        {
                            throw new IOException("Invalid form value: " + form);
                        }
                        if (type < 0 || type > MAX_TYPE_VALUE)
                        {
                            throw new IOException("Invalid type value: " + type);
                        }

                        DATA_FORM df = (DATA_FORM)Enum.GetValues(typeof(DATA_FORM)).GetValue(form);
                        DATA_TYPE dt = (DATA_TYPE)Enum.GetValues(typeof(DATA_TYPE)).GetValue(type);

                        return factory.createEntity(df, dt, @in);
                    }
                    catch (IOException ex)
                    {
                        socket = null;
                        throw ex;
                    }
                }
                catch (Exception ex)
                {
                    NotLeaderStatus status = handleNotLeaderException(ex, null);
                    if (status == NotLeaderStatus.NEW_LEADER)
                        return run(function, arguments);
                    else if (status == NotLeaderStatus.WAIT)
                    {
                        if (!HAreconnect)
                        {
                            HAreconnect = true;
                            while (true)
                            {
                                try
                                {
                                    IEntity re = run(function, arguments);
                                    HAreconnect = false;
                                    return re;
                                }
                                catch (Exception e)
                                {
                                }
                            }
                        }
                        throw ex;
                    }
                    if (socket != null || !highAvailability)
                        throw ex;
                    else if (switchToRandomAvailableSite())
                        return run(function, arguments);
                    else
                        throw ex;
                }
                finally
                {

                }
            }

        }

        public void tryUpload(IDictionary<string, IEntity> variableObjectMap)
        {
            if (isBusy())
            {
                throw new IOException("The connection is in use.");
            }
            try
            {
                upload(variableObjectMap);
            }
            finally
            {
            }
        }
        public void upload(IDictionary<string, IEntity> variableObjectMap)
        {
            if (asynTask) throw new IOException("Asynchronous upload is not allowed");
            if (variableObjectMap == null || variableObjectMap.Count == 0)
            {
                return;
            }

            lock (threadLock)
            {
                try
                {
                    bool reconnect = false;
                    if (socket == null || !socket.Connected)
                    {
                        if (sessionID.Length == 0)
                        {
                            throw new IOException("Database connection is not established yet.");
                        }
                        else
                        {
                            reconnect = true;
                            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            socket.Connect(hostName, port);

                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
                        }
                    }

                    IList<IEntity> objects = new List<IEntity>();

                    string body = "variable\n";
                    foreach (string key in variableObjectMap.Keys)
                    {
                        if (!isVariableCandidate(key))
                        {
                            throw new System.ArgumentException("'" + key + "' is not a good variable name.");
                        }
                        body += key + ",";
                        objects.Add(variableObjectMap[key]);
                    }
                    body = body.Substring(0, body.Length - 1);
                    body += ("\n" + objects.Count + "\n");
                    body += remoteLittleEndian ? "1" : "0";

                    try
                    {
                        @out.writeBytes("API " + sessionID + " ");
                        @out.writeBytes(body.Length.ToString());
                        @out.writeByte('\n');
                        @out.writeBytes(body);
                        for (int i = 0; i < objects.Count; ++i)
                        {
                            objects[i].write(@out);
                        }
                        @out.flush();
                    }
                    catch (IOException ex)
                    {
                        if (reconnect)
                        {
                            socket = null;
                            throw ex;
                        }

                        try
                        {
                            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            socket.Connect(hostName, port);
                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @out.writeBytes("API " + sessionID + " ");
                            @out.writeBytes(body.Length.ToString());
                            @out.writeByte('\n');
                            @out.writeBytes(body);
                            for (int i = 0; i < objects.Count; ++i)
                            {
                                objects[i].write(@out);
                            }
                            @out.flush();
                            reconnect = true;
                        }
                        catch (Exception e)
                        {
                            if (socket != null || !highAvailability)
                            {
                                throw ex;
                            }
                            else
                            {
                                if (switchServer())
                                {
                                    upload(variableObjectMap);
                                }
                                throw ex;
                            }
                            //socket = null;
                            //throw e;
                        }
                    }
                    
                    string[] headers = @in.readLine().Split(' ');
                    if (headers.Length != 3)
                    {
                        socket = null;
                        throw new IOException("Received invalid header.");
                    }

                    if (reconnect)
                    {
                        sessionID = headers[0];
                        if (userId.Length > 0 && password.Length > 0)
                        {
                            login();
                        }
                        if (this.startup != "") run(startup);
                    }

                    int numObject = int.Parse(headers[1]);

                    string msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        throw new IOException(msg);
                    }

                    if (numObject > 0)
                    {
                        try
                        {
                            short flag = @in.readShort();
                            int form = flag >> 8;
                            int type = flag & 0xff;

                            if (form < 0 || form > MAX_FORM_VALUE)
                            {
                                throw new IOException("Invalid form value: " + form);
                            }
                            if (type < 0 || type > MAX_TYPE_VALUE)
                            {
                                throw new IOException("Invalid type value: " + type);
                            }

                            DATA_FORM df = (DATA_FORM)Enum.GetValues(typeof(DATA_FORM)).GetValue(form);
                            DATA_TYPE dt = (DATA_TYPE)Enum.GetValues(typeof(DATA_TYPE)).GetValue(type);

                            IEntity re = factory.createEntity(df, dt, @in);
                        }
                        catch (IOException ex)
                        {
                            socket = null;
                            throw ex;
                        }
                    }
                }
                finally
                {

                }
            }

        }

        public virtual void close()
        {
            lock (threadLock)
            {
                try
                {
                    if (socket != null)
                    {
                        socket.Close();
                        sessionID = string.Empty;
                        socket = null;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.Write(ex.StackTrace);
                }
            }
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
                return hostName;
            }
        }

        public virtual int Port
        {
            get
            {
                return port;
            }
        }

        public string LocalAddress
        {
            get
            {
                return ((IPEndPoint)socket.LocalEndPoint).Address.ToString();
            }
        }

        public bool switchServer()
        {
            if (!highAvailability)
                return false;
            if (controllerHost == null)
                return false;
            // 1 find live datanodes and pick one
            int tryCount = 0;
            if (highAvailabilitySites != null)
            {
                if (tryCount < 3)
                {
                    hostName = mainHostName;
                    port = mainPort;
                }
                else
                {
                    int rnd = new Random().Next(highAvailabilitySites.Length);
                    String[] site = highAvailabilitySites[rnd].Split(':');
                    hostName = site[0];
                    port = int.Parse(site[1]);
                }
                tryCount++;
            }
            else {
                DBConnection tmp = new DBConnection();
                tmp.connect(controllerHost, controllerPort);
                BasicStringVector liveDataNodes = (BasicStringVector)tmp.run("getLiveDataNodes(false)");
                Console.WriteLine("get getLiveDataNodes " + liveDataNodes.getString());
                tmp.close();

                // 2 try get connection to new datanode
                int size = liveDataNodes.rows();
                Console.WriteLine("living node size: " + size);
                for (int i = 0; i < size; i++)
                {

                    string[] liveDataNodeSite = liveDataNodes.get(i).getString().Split(':');
                    hostName = liveDataNodeSite[0];
                    port = int.Parse(liveDataNodeSite[1]);
                    Console.WriteLine("Try node " + i + ": " + hostName + ":" + port);
                    try
                    {
                        connect();
                        return true;
                    }
                    catch (Exception) { }
                }
            }
            return false;
        }

        private bool switchToRandomAvailableSite()
        {
            if (!highAvailability)
                return false;
            int tryCount = 0;
            while (true)
            {
                HAreconnect = true;
                if(highAvailabilitySites != null)
                {
                    if(tryCount < 3)
                    {
                        ;
                    }
                    else
                    {
                        int rnd = new Random().Next(highAvailabilitySites.Length);
                        string[] site = highAvailabilitySites[rnd].Split(':');
                        hostName = site[0];
                        port = Convert.ToInt32(site[1]);
                    }
                    tryCount++;
                }
                else
                {
                    if (controllerHost == null)
                        return false;
                    DBConnection tmp = new DBConnection();
                    tmp.connect(controllerHost, controllerPort);
                    BasicStringVector availableSites = (BasicStringVector)tmp.run("getClusterLiveDataNodes(false)");
                    tmp.close();
                    int size = availableSites.rows();
                    if (size <= 0)
                        return false;

                    string[] site = availableSites.getString(0).Split(':');
                    hostName = site[0];
                    port = port = Convert.ToInt32(site[1]);

                }

                try
                {
                    Console.Write("Trying to reconnect to " + hostName + ":" + port);
                    if (connect())
                    {
                        HAreconnect = false;
                        Console.Write("Successfully reconnected to " + hostName + ":" + port);
                        return true;
                    }
                }
                catch (Exception e) { }
                try
                {
                    Thread.Sleep(1000);
                }
                catch (Exception e) { }

                

            }
        }

        private NotLeaderStatus handleNotLeaderException(Exception ex, String function)
        {
            String errMsg = ex.Message;
            if (ServerExceptionUtils.isNotLeader(errMsg))
            {
                String newLeaderString = ServerExceptionUtils.newLeader(errMsg);
                String[] newLeader = newLeaderString.Split(':');
                String newHostName = newLeader[0];
                int newPort = int.Parse(newLeader[1]);
                if (hostName.Equals(newHostName) && port == newPort)
                {
                    Console.WriteLine("Got NotLeader exception. Waiting for new leader.");
                    try
                    {
                        Thread.Sleep(1000);
                    }
                    catch (Exception e)
                    {
                    }
                    return NotLeaderStatus.WAIT;
                }
                hostName = newHostName;
                port = newPort;
                try
                {
                    Console.WriteLine("Got NotLeader exception. Switching to " + hostName + ":" + port);
                    if (connect())
                        return NotLeaderStatus.NEW_LEADER;
                    else
                        return NotLeaderStatus.CONN_FAIL;
                }
                catch (IOException e)
                {
                    return NotLeaderStatus.CONN_FAIL;
                }
            }
            return NotLeaderStatus.OTHER_EXCEPTION;
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
            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
            
            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }


        /*private SSLSocketFactory getSSLSocketFactory()
        {
            try
            {
                SSLContext context = SSLContext.getInstance("SSL");

                context.init(null,
                        new TrustManager[]{new X509TrustManager() {
                            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                }

                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                }

                public X509Certificate[] getAcceptedIssuers()
                {
                    return null;
                }
            }
                    },
                    new java.security.SecureRandom());
            return context.getSocketFactory();
        }catch (Exception ex){
            ex.printStackTrace();
            return null;
        }
     }*/
    }

}
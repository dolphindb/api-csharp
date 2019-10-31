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
        private int port;
        private string userId;
        private string password;
        private bool encrypted;
        private string startup;
        //=============HighAvailability 2019.07.17 Linl======================
        private bool highAvailability;
        private string controllerHost = null;
        private int controllerPort;
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
        public bool connect(string hostName, int port)
        {
            return connect(hostName, port, "", "");
        }

        public bool connect(string hostName, int port, string userId, string password, string startup="", bool highAvailability=false)
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
                    this.port = port;
                    this.userId = userId;
                    this.password = password;
                    encrypted = false;
                    this.startup = startup;
                    this.highAvailability = highAvailability;
                    return connect();
                }
                finally
                {

                }
            }

        }

        public bool connect()
        {

            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(hostName, port);
            socket.NoDelay = true;
            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));

            @in = new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
            string body = "connect\n";
            @out.writeBytes("API 0 ");
            @out.writeBytes(body.Length.ToString());
            @out.writeByte('\n');
            @out.writeBytes(body);
            @out.flush();


            string line = @in.readLine();
            int endPos = line.IndexOf(' ');
            if (endPos <= 0)
            {
                close();
                throw new IOException("Invalid ack msg : " + line);
            }
            sessionID = line.Substring(0, endPos);

            int startPos = endPos + 1;
            endPos = line.IndexOf(' ', startPos);
            if (endPos != line.Length - 2)
            {
                close();
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
            @in = remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket))) : (ExtendedDataInput)new BigEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));

            if (userId.Length > 0 && password.Length > 0) login();
            if (startup != "") run(startup);
            if (highAvailability && controllerHost == null)
            {
                try
                {
                    controllerHost = ((BasicString)run("rpc(getControllerAlias(),getNodeHost)")).getString();
                    controllerPort = ((BasicInt)run("rpc(getControllerAlias(),getNodePort)")).getValue();
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

        public virtual IEntity run(string script)
        {
            return run(script, (ProgressListener)null);
        }

        public virtual bool tryReconnect()
        {
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(hostName, port);
            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));

            @in = new LittleEndianDataInputStream(new BufferedStream(new NetworkStream(socket)));
            string body = "connect\n";
            @out.writeBytes("API 0 ");
            @out.writeBytes(body.Length.ToString());
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

        public virtual IEntity run(string script, ProgressListener listener)
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
                    if (socket != null || !highAvailability)
                        throw ex;
                    else if (switchServer())
                        return run(script, listener);
                    else
                        throw ex;
                }
            }
        }

        public virtual IEntity tryRun(string function, IList<IEntity> arguments)
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

        public virtual IEntity run(string function, IList<IEntity> arguments)
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
                        @out.writeByte('\n');
                        @out.writeBytes(body);
                        for (int i = 0; i < arguments.Count; ++i)
                        {
                            arguments[i].write(@out);
                        }
                        @out.flush();

                        headers = @in.readLine().Split(' ');
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
                            @out = new LittleEndianDataOutputStream(new BufferedStream(new NetworkStream(socket)));
                            @out.writeBytes("API " + sessionID + " ");
                            @out.writeBytes(body.Length.ToString());
                            @out.writeByte('\n');
                            @out.writeBytes(body);
                            for (int i = 0; i < arguments.Count; ++i)
                            {
                                arguments[i].write(@out);
                            }
                            @out.flush();

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
                    if (socket != null || !highAvailability)
                        throw ex;
                    else if (switchServer())
                        return run(function, arguments);
                    else
                        throw ex;
                }
                finally
                {

                }
            }

        }

        public virtual void tryUpload(IDictionary<string, IEntity> variableObjectMap)
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
        public virtual void upload(IDictionary<string, IEntity> variableObjectMap)
        {
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
                    string msg = @in.readLine();
                    if (!msg.Equals("OK"))
                    {
                        throw new IOException(msg);
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
            DBConnection tmp = new DBConnection();
            tmp.connect(controllerHost, controllerPort);
            BasicStringVector liveDataNodes = (BasicStringVector) tmp.run("getLiveDataNodes(false)");
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
            return false;
        }
    }

}
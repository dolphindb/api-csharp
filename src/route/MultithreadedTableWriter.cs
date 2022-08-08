using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using dolphindb.data;

namespace dolphindb.route
{
    class RecordTime
    {
        public RecordTime(string name)
        {
            name_ = name;
            startTime_ = DateTime.Now;
            try
            {
                mutex_.WaitOne();
                lastrecordorder_++;
                recordOrder_ = lastrecordorder_;
            }
            finally
            {
                mutex_.ReleaseMutex();
            }
        }
        public void end()
        {
            TimeSpan diff = DateTime.Now - startTime_;
            try
            {
                DateTime dateTime = DateTime.Now;
                mutex_.WaitOne();
                Node pnode;
                if (codemap_.ContainsKey(name_))
                {
                    pnode = codemap_[name_];
                }
                else
                {
                    pnode = new Node();
                    pnode.minOrder = recordOrder_;
                    pnode.name = name_;
                    codemap_[name_] = pnode;
                }
                if (pnode.minOrder > recordOrder_)
                {
                    pnode.minOrder = recordOrder_;
                }
                pnode.costTime.Add(diff);
            }
            finally
            {
                mutex_.ReleaseMutex();
            }
        }
        static string printalltime()
        {
            string output = "";
            try
            {
                mutex_.WaitOne();
                SortedList<long, Node> sortList = new SortedList<long, Node>();
                foreach (Node node in codemap_.Values)
                {
                    sortList.Add(node.minOrder, node);
                }
                foreach (Node node in sortList.Values)
                {
                    TimeSpan sum = TimeSpan.MinValue;
                    TimeSpan max = TimeSpan.MinValue, min = TimeSpan.MinValue;
                    foreach (TimeSpan one in node.costTime)
                    {
                        sum += one;
                        if (max < one)
                        {
                            max = one;
                        }
                        if (min == TimeSpan.MinValue || min > one)
                        {
                            min = one;
                        }
                    }
                    output = output + node.name + ": sum=" + sum.ToString()
                        + " count=" + node.costTime.Count() +
                " avg=" + new TimeSpan(sum.Ticks / node.costTime.Count()).ToString() +
                " min=" + min.ToString() + " max=" + max.ToString() + "\n";
                }
            }
            finally
            {
                mutex_.ReleaseMutex();
            }
            return output;
        }

        static RecordTime()
        {
            lastrecordorder_ = 0;
            mutex_ = new Mutex();
            codemap_ = new Dictionary<string, Node>();
        }

        private string name_;
        private long recordOrder_;
        DateTime startTime_;
        struct Node
        {
            public string name;
            public long minOrder;
            public List<TimeSpan> costTime;
        };
        static long lastrecordorder_;
        static Mutex mutex_;
        static Dictionary<string, Node> codemap_;
    };


    class DLogger
    {
        enum Level
        {
            LevelDebug,
            LevelInfo,
            LevelWarn,
            LevelError,
            LevelCount,
        };

        static DLogger()
        {
            minLevel_ = Level.LevelDebug;
            levelText_ = new List<string> { "DEBUG", "INFO", "WARN", "ERROR" };
        }

        public static void Info(string text)
        {
            Write(text, Level.LevelInfo);
        }
        public static void Debug(string text)
        {
            Write(text, Level.LevelInfo);
        }
        public static void Warn(string text)
        {
            Write(text, Level.LevelWarn);
        }
        public static void Error(string text)
        {
            Write(text, Level.LevelError);
        }
        private static Level minLevel_;
        private static List<string> levelText_ = new List<string>();
        static void Write(string text, Level level)
        {
            if (level < minLevel_)
            {
                return;
            }
            DateTime dateTime = DateTime.Now;
            text = dateTime.ToString() + " : Thread[" +
                    Thread.CurrentThread.ManagedThreadId.ToString() + "] : " + levelText_[(int)level] + " : ";
        }
    }

    public class MultithreadedTableWriter
    {

        public class ThreadStatus
        {
            public long threadId;
            public long sentRows, unsentRows, sendFailedRows;
            public override string ToString()
            {
                return string.Format("threadId : {0} sentRows : {1} unsentRows : {2} sendFailedRows : {3}\n", threadId, sentRows, unsentRows, sendFailedRows);
            }
        };

        public class Status : ErrorCodeInfo
        {
            public bool isExiting;
            public long sentRows, unsentRows, sendFailedRows;
            public List<ThreadStatus> threadStatus;
            public override string ToString()
            {
                string tmp = string.Format("{0}\nisExiting: {1}\nsentRows: {2}\nunsentRows: {3}\nsendFailedRows: {4}\n", errorInfo.ToString(), isExiting, sentRows, unsentRows, sendFailedRows);
                for (int i = 0; i < threadStatus.Count; ++i)
                {
                    tmp += threadStatus[i].ToString();
                }
                return tmp;
            }
        };

        List<IVector> createListVector()
        {
            int cols = colTypes_.Count;
            BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();
            List<IVector> tmp = new List<IVector>();
            for (int i = 0; i < cols; ++i)
                tmp.Add(basicEntityFactory.createVectorWithDefaultValue(colTypes_[i], 0));
            return tmp;
        }

        class WriterThread
        {
            public DBConnection conn_;
            public string scriptTableInsert_;
            public string scriptSaveTable_;
            public List<List<IVector>> writeQueue_;
            public Queue<List<IEntity>> failedQueue_;
            public Thread thread_;
            public Mutex writeMutex_;
            public int threadId_;
            public long sentRows_;
            public bool exit_, isFinished_;
            public MultithreadedTableWriter tableWriter_;
            public Mutex writeAllDataLock_;
            public static int vectorSize = 65536;

            public WriterThread(MultithreadedTableWriter tableWriter, DBConnection conn)
            {
                tableWriter_ = tableWriter;
                sentRows_ = 0;
                conn_ = conn;
                exit_ = false;
                isFinished_ = false;
                writeQueue_ = new List<List<IVector>>();
                writeQueue_.Add(tableWriter.createListVector());
                failedQueue_ = new Queue<List<IEntity>>();
                writeAllDataLock_ = new Mutex();
                thread_ = new Thread(run);
                thread_.Start();
                threadId_ = thread_.GetHashCode();
            }

            public void run()
            {
                if (init() == false)
                {
                    return;
                }
                DateTime batchWaitTimeout;
                int diff;
                try
                {
                    while (!isExiting())
                    {
                        lock (writeQueue_)
                        {
                            //if (!isExiting() && writeQueue_.Count == 0)
                            //    Monitor.Wait(writeQueue_);
                            if (!isExiting() && tableWriter_.throttleMilsecond_ > 0 && tableWriter_.batchSize_ > 1)
                            {
                                batchWaitTimeout = DateTime.Now.AddMilliseconds(tableWriter_.throttleMilsecond_);
                                while (isExiting() == false && (writeQueue_.Count - 1) * vectorSize + writeQueue_[writeQueue_.Count - 1][0].rows() < tableWriter_.batchSize_)
                                {
                                    diff = (int)(batchWaitTimeout - DateTime.Now).TotalMilliseconds;
                                    if (diff > 0)
                                        Monitor.Wait(writeQueue_, diff);
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                        }

                        while (!isExiting() && writeAllData()) ;
                    }
                    while (tableWriter_.hasError_ == false && writeAllData()) ;
                }
                catch (Exception e)
                {
                    System.Console.Out.WriteLine(e.Message);
                    tableWriter_.setError(ErrorCodeInfo.Code.EC_None, e.Message);

                }
                conn_.close();
                isFinished_ = true;
            }
            private bool init()
            {
                if (tableWriter_.dbName_ == "")
                {
                    scriptTableInsert_ = "tableInsert{\"" + tableWriter_.tableName_ + "\"}";
                }
                else
                {
                    scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
                }
                //else
                //{
                //    //string tempTableName = "tmp" + tableWriter_.tableName_;
                //    //string colNames = "";
                //    //string colTypes = "";
                //    //for (int i = 0; i < tableWriter_.colNames_.Count; i++)
                //    //{
                //    //    colNames += "`" + tableWriter_.colNames_[i];
                //    //    colTypes += "`" + tableWriter_.colTypeString_[i];
                //    //}
                //    //string scriptCreateTmpTable = "tempTable = table(" + "1000:0," + colNames + "," + colTypes + ")";
                //    //try
                //    //{
                //    //    conn_.run(scriptCreateTmpTable);
                //    //}
                //    //catch (Exception e)
                //    //{
                //    //    //DLogger::Error("threadid=", writeThread_.threadId, " Init table error: ", e.what(), " script:", scriptCreateTmpTable);
                //    //    tableWriter_.setError(ErrorCodeInfo.ErrorCode.EC_Server, "Init table error: " + e.Message + " script: " + scriptCreateTmpTable);
                //    //    //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to init data to server, with exception: " << e.what() << std::endl;
                //    //    return false;
                //    //}
                //    //scriptTableInsert_ = "tableInsert{tempTable}";
                //    //scriptSaveTable_ = "saveTable(database(\"" + tableWriter_.dbName_ + "\")" + ",tempTable,\"" + tableWriter_.tableName_ + "\", 1);tempTable.clear!();";
                //}
                return true;
            }
            bool writeAllData()
            {
                lock (writeAllDataLock_)
                {
                    List<IVector> items;
                    int addRowCount;
                    lock (writeQueue_)
                    {
                        items = writeQueue_[0];
                        addRowCount = items[0].rows();
                        if (addRowCount < 1)
                            return false;
                        writeQueue_.RemoveAt(0);
                        if (writeQueue_.Count == 0)
                        {
                            writeQueue_.Add(tableWriter_.createListVector());
                        }
                    }
                    bool isWriteDone = true;
                    string runscript = "";
                    BasicTable writeTable = null;

                    writeTable = new BasicTable(tableWriter_.colNames_, items);
                    if (writeTable != null && addRowCount > 0)
                    {
                        runscript = "";
                        try
                        {
                            List<IEntity> args = new List<IEntity>();
                            args.Add(writeTable);
                            runscript = scriptTableInsert_;
                            BasicInt result = (BasicInt)conn_.run(runscript, args);
                            sentRows_ += addRowCount;
                        }
                        catch (Exception e)
                        {
                            Console.Out.WriteLine("threadid= {0}, Save Table error: {1}, script: {2}", threadId_, e.Message, runscript);
                            //System.Console.Out.WriteLine(e.Message);
                            //System.Console.Out.WriteLine(e.StackTrace);
                            //tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " sendindex=" + sentRows_ + " Save table error: " + e + " script:" + runscript);
                            tableWriter_.setError(ErrorCodeInfo.Code.EC_Server, "Save table error: " + e + " script: " + runscript);
                            isWriteDone = false;
                            //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
                        }
                    }

                    if (!isWriteDone)
                    {
                        lock (failedQueue_)
                        {
                            int cols = items.Count;
                            int rows = items[0].rows();
                            for (int i = 0; i < rows; ++i)
                            {
                                List<IEntity> tmp = new List<IEntity>();

                                for (int j = 0; j < cols; ++j)
                                {
                                    if ((int)tableWriter_.colTypes_[j] < AbstractVector.ARRAY_VECTOR_BASE)
                                        tmp.Add(items[j].get(i));
                                    else
                                        tmp.Add(((BasicArrayVector)items[j]).getSubVector(i));
                                }
                                failedQueue_.Enqueue(tmp);
                            }
                        }
                    }
                }
                return true;
            }

            public void getStatus(ThreadStatus status)
            {
                status.threadId = threadId_;
                status.sentRows = sentRows_;
                lock (writeQueue_)
                    status.unsentRows = (writeQueue_.Count - 1) * vectorSize + writeQueue_[writeQueue_.Count - 1][0].rows();
                status.sendFailedRows = failedQueue_.Count();
            }
            bool isExiting()
            {
                return exit_ || tableWriter_.hasError_;
            }
            public void exit()
            {
                lock (writeQueue_)
                {
                    exit_ = true;
                    Monitor.Pulse(writeQueue_);
                }
            }
        };


        /**
         * If fail to connect to the specified DolphinDB server, this function throw an exception.
         */
        public MultithreadedTableWriter(string hostName, int port, string userId, string password,
                                string dbName, string tableName, bool useSSL, bool enableHighAvailability = false, string[] pHighAvailabilitySites = null,
                                int batchSize = 1, float throttle = 0.01f, int threadCount = 5, string partitionCol = "", int[] pCompressMethods = null)
        {
            hostName_ = hostName;
            port_ = port;
            userId_ = userId;
            password_ = password;
            useSSL_ = useSSL;
            dbName_ = dbName;
            tableName_ = tableName;
            batchSize_ = batchSize;
            throttleMilsecond_ = (int)throttle * 1000;
            isExiting_ = false;
            if (threadCount < 1)
            {
                throw new Exception("The parameter threadCount must be greater than or equal to 1.");
            }
            if (batchSize < 1)
            {
                throw new Exception("The parameter batchSize must be greater than or equal to 1.");
            }
            if (throttle < 0)
            {
                throw new Exception("The parameter throttle must be positive.");
            }
            if (threadCount > 1 && partitionCol == String.Empty)
            {
                throw new Exception("The parameter partitionCol must be specified when threadCount is greater than 1.");
            }
            DBConnection pConn = new DBConnection(false, useSSL_, pCompressMethods != null);
            bool ret = pConn.connect(hostName_, port_, userId_, password_, "", enableHighAvailability, pHighAvailabilitySites);
            if (!ret)
            {
                throw new Exception(string.Format("Failed to connect to server {0}:{1}. ", hostName, port));
            }
            BasicDictionary schema;
            if (dbName == "")
            {
                schema = (BasicDictionary)pConn.run("schema(" + tableName + ")");
            }
            else
            {
                schema = (BasicDictionary)pConn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
            }
            IEntity partColNames = null;
            if (schema.ContainsKey("partitionColumnName"))
            {
                partColNames = schema.get(new BasicString("partitionColumnName"));
                isPartionedTable_ = true;
            }
            else
            {
                isPartionedTable_ = false;
                if (dbName != "")
                {
                    if (threadCount > 1)
                    {//只有多线程的时候需要
                        throw new Exception("The parameter threadCount must be 1 for a dimension table.");
                    }
                }
            }
            BasicTable colDefs = (BasicTable)schema.get("colDefs");

            BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");

            BasicStringVector colDefsName = (BasicStringVector)colDefs.getColumn("name");
            BasicStringVector colDefsTypeString = (BasicStringVector)colDefs.getColumn("typeString");
            colTypes_ = new List<DATA_TYPE>();
            colNames_ = new List<string>();
            colTypeString_ = new List<string>();
            int columnSize = colDefsName.rows();
            if (pCompressMethods != null)
            {
                if (columnSize != pCompressMethods.Length)
                {
                    throw new Exception(string.Format("The number of elements in parameter compressMethods does not match the column size {0}. ", columnSize));
                }
                this.compressTypes_ = new int[columnSize];
                Array.Copy(pCompressMethods, this.compressTypes_, columnSize);
            }
            for (int i = 0; i < columnSize; i++)
            {
                colNames_.Add(colDefsName.getString(i));
                colTypes_.Add((DATA_TYPE)colDefsTypeInt.getInt(i));
                colTypeString_.Add(colDefsTypeString.getString(i));
                if (compressTypes_ != null)
                    AbstractVector.checkCompressedMethod(colTypes_[i], compressTypes_[i]);
            }

            if (threadCount > 1)
            {
                if (isPartionedTable_)
                {
                    IEntity partitionSchema;
                    int partitionType;
                    if (partColNames.isScalar())
                    {
                        if (partColNames.getString() != partitionCol)
                        {
                            throw new Exception(string.Format("The parameter partionCol must be the partitioning column \"{0}\" in the table. ", partitionCol));
                        }
                        partitionColumnIdx_ = ((BasicInt)schema.get("partitionColumnIndex")).getInt();
                        partitionSchema = schema.get("partitionSchema");
                        partitionType = ((BasicInt)schema.get("partitionType")).getInt();
                    }
                    else
                    {
                        int dims = ((BasicStringVector)partColNames).rows();
                        if (dims > 1 && partitionCol == "")
                        {
                            throw new Exception("The parameter partitionCol must be specified when threadCount is greater than 1.");
                        }
                        int index = -1;
                        for (int i = 0; i < dims; ++i)
                        {
                            if (((BasicStringVector)partColNames).getString(i) == partitionCol)
                            {
                                index = i;
                                break;
                            }
                        }
                        if (index < 0)
                            throw new Exception(string.Format("The parameter partionCol must be the partitioning column \"{0}\" in the table. ", partitionCol));
                        partitionColumnIdx_ = ((BasicIntVector)schema.get("partitionColumnIndex")).getInt(index);
                        partitionSchema = ((BasicAnyVector)schema.get("partitionSchema")).get(index);
                        partitionType = ((BasicIntVector)schema.get("partitionType")).getInt(index);
                    }
                    DATA_TYPE partitionColType = colTypes_[partitionColumnIdx_];
                    partitionDomain_ = DomainFactory.createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
                }
                else
                {//isPartionedTable_==false
                    if (partitionCol != "")
                    {
                        int threadcolindex = -1;
                        for (int i = 0; i < colNames_.Count; i++)
                        {
                            if (colNames_[i] == partitionCol)
                            {
                                threadcolindex = i;
                                break;
                            }
                        }
                        if (threadcolindex < 0)
                        {
                            throw new Exception(string.Format("No match found for {0}. ", partitionCol));
                        }
                        threadByColIndexForNonPartion_ = threadcolindex;
                    }
                }
            }

            // init done, start thread now.
            isExiting_ = false;
            threads_ = new List<WriterThread>(threadCount);
            for (int i = 0; i < threadCount; i++)
            {
                WriterThread writerThread = new WriterThread(this, pConn);
                if (i == 0)
                {
                    writerThread.conn_ = pConn;
                }
                else
                {
                    writerThread.conn_ = new DBConnection(useSSL_, false);
                    if (writerThread.conn_.connect(hostName_, port_, userId_, password_, "", enableHighAvailability, pHighAvailabilitySites) == false)
                    {
                        throw new Exception(string.Format("Failed to connect to server {0}:{1}. ", hostName, port));
                    }
                }
                threads_.Add(writerThread);
            }
        }

        public ErrorCodeInfo insert(params Object[] args)
        {
            if (hasError_)
            {
                throw new Exception("Thread is exiting. ");
            }
            List<IEntity> convert = new List<IEntity>();
            int size = colTypes_.Count;
            for (int i = 0; i < size; ++i)
            {
                try
                {
                    convert.Add(Utils.createObject(colTypes_[i], args[i]));
                }
                catch (Exception e)
                {
                    return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, e.Message);
                }
            }
            return insert(convert);
        }

        public ErrorCodeInfo insert(List<IScalar> args)
        {
            List<IEntity> tmp = new List<IEntity>();
            foreach (IScalar scalar in args)
                tmp.Add(scalar);
            return insert(tmp);
        }

        public ErrorCodeInfo insert(List<IEntity> args)
        {
            if (hasError_)
            {
                throw new Exception("Thread is exiting. ");
            }
            int argsize = args.Count();
            if (argsize == 0)
                return new ErrorCodeInfo();

            return insertRecursive(args);
        }

        bool suitableType(ErrorCodeInfo errorCodeInfo, List<IEntity> args)
        {
            if (args.Count != colTypes_.Count)
            {
                errorCodeInfo.set(ErrorCodeInfo.Code.EC_InvalidObject, string.Format("Column counts don't match {0}.", colTypes_.Count));
                return false;
            }
            int cols = args.Count();
            for (int i = 0; i < cols; ++i)
            {
                DATA_TYPE argsType;
                if (args[i] is IVector)
                {
                    argsType = ((IVector)args[i]).getDataType();
                }
                else
                {
                    argsType = args[i].getDataType();
                }
                DATA_TYPE colType = args[i] is IVector ? colTypes_[i] - 64 : colTypes_[i];
                if ((int)argsType != (int)colType)
                {
                    if (!((argsType == DATA_TYPE.DT_STRING && (int)colType == (int)DATA_TYPE.DT_SYMBOL) || (argsType == DATA_TYPE.DT_STRING && (int)colType == (int)DATA_TYPE.DT_BLOB)))
                    {
                        errorCodeInfo.set(ErrorCodeInfo.Code.EC_InvalidObject, "Failed to insert data, the type of argument does not match the type of column at column: " + i.ToString());
                        return false;
                    }
                }
            }
            return true;
        }

        ErrorCodeInfo insertRecursive(List<IEntity> args)
        {
            ErrorCodeInfo errorCodeInfo = new ErrorCodeInfo();
            if (!suitableType(errorCodeInfo, args))
                return errorCodeInfo;
            int threadindex;
            if (threads_.Count() > 1)
            {
                BasicEntityFactory factory = (BasicEntityFactory)BasicEntityFactory.instance();
                if (isPartionedTable_)
                {
                    {
                        try
                        {
                            threadindex = partitionDomain_.getPartitionKey((IScalar)args[partitionColumnIdx_]);
                        }
                        catch (Exception e)
                        {
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, e.Message);
                        }
                    }
                }
                else
                {
                    threadindex = args[threadByColIndexForNonPartion_].GetHashCode();
                }
            }
            else
            {
                threadindex = 0;
            }
            insertThreadWrite(threadindex, args);
            return errorCodeInfo;
        }


        void insertThreadWrite(int threadhashkey, List<IEntity> row)
        {
            if (threadhashkey < 0)
            {
                threadhashkey = 0;
            }
            int threadIndex = threadhashkey % threads_.Count();
            WriterThread writerThread = threads_[threadIndex];
            lock (writerThread.writeQueue_)
            {
                int rows = writerThread.writeQueue_[writerThread.writeQueue_.Count - 1][0].rows();
                if (rows > WriterThread.vectorSize)
                {
                    writerThread.writeQueue_.Add(createListVector());
                }

                int size = row.Count;
                for (int i = 0; i < size; ++i)
                {
                    if ((int)colTypes_[i] < AbstractVector.ARRAY_VECTOR_BASE)
                    {
                        writerThread.writeQueue_[writerThread.writeQueue_.Count - 1][i].append((IScalar)row[i]);
                    }
                    else
                        writerThread.writeQueue_[writerThread.writeQueue_.Count - 1][i].append((IVector)row[i]);
                }
                if (writerThread.writeQueue_[writerThread.writeQueue_.Count - 1][0].rows() >= batchSize_)
                    Monitor.Pulse(writerThread.writeQueue_);
            }
        }

        public List<List<IEntity>> getUnwrittenData()
        {
            List<List<IEntity>> data = new List<List<IEntity>>();
            foreach (WriterThread thread in threads_)
            {
                lock (thread.writeAllDataLock_)
                {
                    lock (thread.writeQueue_)
                    {
                        int cols = colTypes_.Count;
                        int size = thread.writeQueue_.Count;
                        for (int i = 0; i < size; ++i)
                        {
                            int rows = thread.writeQueue_[i][0].rows();
                            for (int row = 0; row < rows; ++row)
                            {
                                List<IEntity> tmp = new List<IEntity>();
                                for (int j = 0; j < cols; ++j)
                                {
                                    tmp.Add(thread.writeQueue_[i][j].get(row));
                                }
                                data.Add(tmp);
                            }
                        }
                        data.AddRange(thread.failedQueue_);
                        thread.writeQueue_.Clear();
                        thread.writeQueue_.Add(createListVector());
                    }
                }
            }

            return data;
        }

        public ErrorCodeInfo insertUnwrittenData(List<List<IEntity>> data)
        {
            if (hasError_)
                throw new Exception("Thread is exiting. ");
            BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();
            ErrorCodeInfo errorCodeInfo = new ErrorCodeInfo();
            IVector partitionCol = basicEntityFactory.createVectorWithDefaultValue(colTypes_[partitionColumnIdx_], data.Count);
            int rows = data.Count;
            for (int i = 0; i < rows; ++i)
                partitionCol.set(i, (IScalar)data[i][partitionColumnIdx_]);

            if (threads_.Count() > 1)
            {
                List<int> threadindexs = new List<int>();
                BasicEntityFactory factory = (BasicEntityFactory)BasicEntityFactory.instance();
                if (isPartionedTable_)
                {
                    threadindexs = partitionDomain_.getPartitionKeys(partitionCol);
                }
                else
                {
                    for (int i = 0; i < rows; ++i)
                        threadindexs.Add(partitionCol.get(i).GetHashCode());
                }
                for (int i = 0; i < rows; ++i)
                    insertThreadWrite(threadindexs[i], data[i]);
            }
            else
            {
                for (int i = 0; i < rows; ++i)
                    insertThreadWrite(0, data[i]);
            }
            return new ErrorCodeInfo();
        }


        void setError(ErrorCodeInfo.Code code, string info)
        {
            if (hasError_)
                return;
            hasError_ = true;
            errorCodeInfo.set(code, info);
        }



        public Status getStatus()
        {
            Status status = new Status();
            status.set(errorCodeInfo);
            status.sendFailedRows = status.sentRows = status.unsentRows = 0;
            status.isExiting = isExiting();
            status.threadStatus = new List<ThreadStatus>();
            foreach (WriterThread writerThread in threads_)
            {
                ThreadStatus threadStatus = new ThreadStatus();
                writerThread.getStatus(threadStatus);
                status.threadStatus.Add(threadStatus);

                status.sentRows += threadStatus.sentRows;
                status.unsentRows += threadStatus.unsentRows;
                status.sendFailedRows += threadStatus.sendFailedRows;
            }
            return status;
        }

        public void waitForThreadCompletion()
        {
            isExiting_ = true;
            foreach (WriterThread one in threads_)
            {
                one.exit();
                if (!one.thread_.IsAlive == false)
                {

                    while (!one.thread_.Join(1000))
                    {
                        if (one.isFinished_)
                            break;
                    }
                }
                one.conn_ = null;
            }
            //setError(ErrorCodeInfo.Code.EC_UserBreak, "User break.");
        }

        private bool isExiting() { return isExiting_ || hasError_; }

        ErrorCodeInfo errorCodeInfo = new ErrorCodeInfo();
        private string hostName_;
        private int port_;
        private string userId_;
        private string password_;
        private bool useSSL_;
        private string dbName_;
        public string tableName_;
        public int batchSize_;
        public int throttleMilsecond_;
        bool isPartionedTable_, isExiting_ = false;
        public bool hasError_ = false;
        List<string> colNames_, colTypeString_;
        List<DATA_TYPE> colTypes_;
        Domain partitionDomain_;
        int partitionColumnIdx_;
        int threadByColIndexForNonPartion_;
        List<WriterThread> threads_;
        Mutex tableMutex_;
        public int[] compressTypes_;
    }

}

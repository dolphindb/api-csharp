using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using System.Threading;
using System.Collections.Concurrent;

namespace dolphindb.route
{

    class DestTable
    {
        public DBConnection conn;
        public string dbName;
        public string tableName;
        public string tableInsert;
        public string saveTable;
        public BasicTable colDefs;
        public int columnNum;
        public BasicIntVector colDefsTypeInt;
        public List<string> colNames;
        public List<DATA_TYPE> colTypes;
        public string createTmpSharedTable;
        public ConcurrentQueue<List<IScalar>> writeQueue;
        public ConcurrentQueue<List<IScalar>> failQueue;
        public Thread writeThread;
        public BasicTable writeTable;
        public int sendedRows = 0;
        public bool destroy = false;
        public bool finished = false;
    };

    class WriteThread
    {
        public DestTable destTableRawPtr;
        bool partitioned;

        public WriteThread(DestTable destTable, bool partitioned)
        {
            this.destTableRawPtr = destTable;
            this.partitioned = partitioned;
        }

        public void run()
        {
            List<List<IScalar>> items = new List<List<IScalar>>();
            try
            {
                while (!destTableRawPtr.destroy)
                {
                    items = new List<List<IScalar>>();
                    List<IScalar> item = new List<IScalar>();
                    while ((destTableRawPtr.writeQueue.Count() > 0) && !destTableRawPtr.destroy)
                    {
                        if (!destTableRawPtr.writeQueue.TryDequeue(out item))
                        {
                            continue;
                        }
                        items.Add(item);
                        if (destTableRawPtr.destroy)
                        {
                            throw new Exception("The table " + destTableRawPtr.dbName + " " + destTableRawPtr.tableName + " is destroyed.");
                        }
                    };
                    if (items.Count() == 0)
                    {
                        continue;
                    }

                    int rows = items.Count();
                    int cols = destTableRawPtr.colTypes.Count();
                    List<IVector> columns = new List<IVector>();
                    BasicEntityFactory factory = new BasicEntityFactory();
                    for (int i = 0; i < cols; ++i)
                    {
                        columns.Add(factory.createVectorWithDefaultValue(destTableRawPtr.colTypes[i], rows));
                    }
                    for (int i = 0; i < rows; ++i)
                    {
                        for (int j = 0; j < cols; ++j)
                        {
                            columns[j].set(i, items[i][j]);
                        }
                    }
                    destTableRawPtr.writeTable = new BasicTable(destTableRawPtr.colNames, columns);

                    List<IEntity> arg = new List<IEntity>();
                    if (!partitioned)
                        destTableRawPtr.conn.run(destTableRawPtr.createTmpSharedTable);
                    arg.Add(destTableRawPtr.writeTable);
                    destTableRawPtr.conn.run(destTableRawPtr.tableInsert, arg);
                    destTableRawPtr.sendedRows = destTableRawPtr.sendedRows + rows;
                }
            }
            catch (Exception e)
            {

                if (items != null)
                {
                    destTableRawPtr.finished = true;
                    for (int i = 0; i < items.Count(); ++i)
                    {
                        destTableRawPtr.failQueue.Enqueue(items[i]);
                    }
                }
                Console.WriteLine("Failed to insert data in background thread, with exception: " + e.Message);
            }
            finally
            {
                destTableRawPtr.finished = true;
            }
        }
    }

    public class BatchTableWriter
    {

        public BatchTableWriter(string hostName, int port, string userId, string password)
        {
            this.hostName_ = hostName;
            this.port_ = port;
            this.userId_ = userId;
            this.password_ = password;
            this.rwLock_ = new ReaderWriterLockSlim();
            this.destTables_ = new Dictionary<Tuple<string, string>, DestTable>();
        }

        /**
         * Add the name of the database and table that you want to insert data into before actually call insert.
         * The parameter partitioned indicates whether the added table is a partitioned table. If this function
         * is called to add an in-memory table, the parameter dbName indicates the name of the shared in-memory
         * table, and the parameter tableName should be a null string. If error is raised on the server, this
         * function throws an exception.
         */
        public void addTable(string dbName, string tableName = "", bool partitioned = true)
        {
            try
            {
                rwLock_.EnterReadLock();
                if (destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                    throw new Exception("Failed to add table, the specified table has not been removed yet.");
            }
            finally
            {
                rwLock_.ExitReadLock();
            }
            DBConnection conn = new DBConnection(false, false);
            bool ret = conn.connect(hostName_, port_, userId_, password_);
            if (!ret)
                throw new Exception("Failed to connect to server.");
            string tableInsert;
            string saveTable = "";
            BasicDictionary schema;
            string tmpDiskGlobal = "tmpDiskGlobal";
            if (tableName == "")
            {
                tableInsert = "tableInsert{" + dbName + "}";
                schema = (BasicDictionary)conn.run("schema(" + dbName + ")");
            }
            else if (partitioned)
            {
                tableInsert = "tableInsert{loadTable(\"" + dbName + "\",\"" + tableName + "\")}";
                schema = (BasicDictionary)conn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
            }
            else
            {
                throw new Exception("The target table must be an in-memory table or a table in a distributed database.");
            }

            BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));

            DestTable destTable;
            if (destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                throw new Exception("Failed to add table, the specified table has not been removed yet.");
            destTable = new DestTable();

            destTable.dbName = dbName;
            destTable.tableName = tableName;
            destTable.conn = conn;
            destTable.tableInsert = tableInsert;
            destTable.saveTable = saveTable;
            destTable.colDefs = colDefs;
            destTable.columnNum = colDefs.rows();
            destTable.colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
            destTable.destroy = false;
            destTable.writeQueue = new ConcurrentQueue<List<IScalar>>();
            destTable.failQueue = new ConcurrentQueue<List<IScalar>>();

            List<string> colNames = new List<string>();
            List<DATA_TYPE> colTypes = new List<DATA_TYPE>();
            BasicStringVector colDefsName = (BasicStringVector)colDefs.getColumn("name");
            for (int i = 0; i < destTable.columnNum; i++)
            {
                colNames.Add(colDefsName.getString(i));
                colTypes.Add((DATA_TYPE)destTable.colDefsTypeInt.getInt(i));
            }
            destTable.colNames = colNames;
            destTable.colTypes = colTypes;

            if (!partitioned)
            {
                string colName = "";
                string colType = "";
                BasicStringVector colDefsTypeString = (BasicStringVector)colDefs.getColumn("typeString");
                for (int i = 0; i < destTable.columnNum; i++)
                {
                    colName = colName + "`" + colDefsName.getString(i);
                    colType = colType + "`" + colDefsTypeString.getString(i);
                }
                destTable.createTmpSharedTable = "share table(" + "1000:0," + colName + "," + colType + ") as " + tmpDiskGlobal;
            }
            try
            {
                rwLock_.EnterWriteLock();
                if (destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                    throw new Exception("Failed to add table, the specified table has not been removed yet.");
                destTables_[Tuple.Create<string, string>(dbName, tableName)] = destTable;
                WriteThread writeThreadFuc = new WriteThread(destTable, partitioned);
                destTable.writeThread = new Thread(writeThreadFuc.run);
                destTable.writeThread.Start();
            }

            finally
            {
                rwLock_.ExitWriteLock();
            }


        }

        /**
         * Gets the current size of the specified table write queue and whethre the specified table is removed. If
         * the specified table is not added first, this function throw and exception.
         */
        public Tuple<int, bool, bool> getStatus(string dbName, string tableName = "")
        {
            rwLock_.EnterReadLock();
            try
            {
                if (!destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                    throw new Exception("Failed to get queue depth. Please use addTable to add infomation of database and table first.");
                DestTable destTable = destTables_[Tuple.Create<string, string>(dbName, tableName)];
                return Tuple.Create<int, bool, bool>(destTable.writeQueue.Count(), destTable.destroy, destTable.finished);
            }
            finally
            {
                rwLock_.ExitReadLock();
            }
        }

        /**
         * Gets the current size of all table write queue, number of rows already sended to server, whether a
         * table is being removed, and whether the backgroud thread is returned because of error.
         */
        public BasicTable getAllStatus()
        {
            int columnNum = 6;
            List<string> colNames = new List<string>(new String[] { "DatabaseName", "TableName", "WriteQueueDepth", "SendedRows", "Removing", "Finished" });
            List<DATA_TYPE> colTypes = new List<DATA_TYPE>(new DATA_TYPE[] { DATA_TYPE.DT_STRING, DATA_TYPE.DT_STRING, DATA_TYPE.DT_INT, DATA_TYPE.DT_INT, DATA_TYPE.DT_BOOL, DATA_TYPE.DT_BOOL });
            List<IVector> columnVecs = new List<IVector>();
            BasicEntityFactory factory = new BasicEntityFactory();
            for (int i = 0; i < columnNum; ++i)
            {
                columnVecs.Add(factory.createVectorWithDefaultValue(colTypes[i], 0));
            }

            try
            {
                rwLock_.EnterReadLock();
                int rowNum = destTables_.Count();
                foreach (KeyValuePair<Tuple<string, string>, DestTable> t in destTables_)
                {
                    columnVecs[0].add(t.Value.dbName);
                    columnVecs[1].add(t.Value.tableName);
                    columnVecs[2].add(t.Value.writeQueue.Count());
                    columnVecs[3].add(t.Value.sendedRows);
                    columnVecs[4].add(t.Value.destroy);
                    columnVecs[5].add(t.Value.finished);
                }
            }
            finally
            {
                rwLock_.ExitReadLock();
            }
            BasicTable ret = new BasicTable(colNames, columnVecs);
            return ret;
        }

        /**
         * Release the resouces occupied by the specified table, including write queue and write thread. If this
         * function is called to add an in-memory table, the parameter dbName indicates the name of the in-memory
         * table, and the parameter tableName should be a null string.
         */
        public void removeTable(string dbName, string tableName = "")
        {
            DestTable destTable = null;
            try
            {
                rwLock_.EnterReadLock();
                if (destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                {
                    destTable = destTables_[Tuple.Create<string, string>(dbName, tableName)];
                    if (destTable.destroy)
                        return;
                    else
                        destTable.destroy = true;
                }
            }
            finally
            {
                rwLock_.ExitReadLock();
            }
            if (destTable != null)
            {
                destTable.writeThread.Join();
                destTable.conn.close();
                destTables_.Remove(Tuple.Create<string, string>(dbName, tableName));
            }
        }

        /**
         * Insert a row into the specified table. If this function is called to insert data into an in-memory table,
         * the parameter dbName indicates the name of the shared in-memory table, and one must pass in a null string
         * to the parameter tableName. If the number of parameters does not match the number of columns of added table,
         * this function throws an exception. If the type of the parameter and the type of the corresponding column
         * cannot be matched or converted, this function throws an exception. If error is raised on the server,
         * this function throws an exception. If the specified table is being removed, this function throws an exception.
         * If the background thread fails to write a row, it will print error message to std::cerr and return.
         */
        public void insert(string dbName, string tableName, List<IScalar> args)
        {
            DestTable destTable;
            {
                try
                {
                    rwLock_.EnterReadLock();
                    if (!destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                        throw new Exception("failed to insert into table, please use addtable to add infomation of database and table first.");
                    destTable = destTables_[Tuple.Create<string, string>(dbName, tableName)];
                }
                finally
                {
                    rwLock_.ExitReadLock();
                }
            }
            if (destTable.destroy)
                throw new Exception("failed to insert into table, the table is being removed.");
            int argsize = args.Count();
            if (argsize != destTable.columnNum)
                throw new Exception("failed to insert into table, number of arguments must match the number of columns of table.");
            if (argsize == 0)
                return;

            insertRecursive(destTable, args);
        }

        public BasicTable getUnwrittenData(string dbName, string tableName)
        {
            try
            {
                List<IVector> cols = new List<IVector>();
                List<List<IScalar>> data = new List<List<IScalar>>();
                rwLock_.EnterReadLock();
                if (!destTables_.ContainsKey(Tuple.Create<string, string>(dbName, tableName)))
                    throw new Exception("Failed to get unwritten data.Please use addTable to add infomation of database and table first.");
                DestTable destTable = destTables_[Tuple.Create<string, string>(dbName, tableName)];
                int columns = destTable.colTypes.Count();
                lock (destTable.failQueue)
                {
                    while (destTable.failQueue.Count() != 0)
                    {
                        if (destTable.failQueue.TryDequeue(out List<IScalar> queueData))
                        {
                            data.Add(queueData);
                        }
                    }
                }
                while (destTable.writeQueue.Count() != 0)
                {
                    if (destTable.writeQueue.TryDequeue(out List<IScalar> queueData))
                    {
                        data.Add(queueData);
                    }
                }
                int size = data.Count();
                BasicEntityFactory factory = new BasicEntityFactory();
                for (int i = 0; i < destTable.colTypes.Count(); ++i)
                {
                    IVector vector = factory.createVectorWithDefaultValue(destTable.colTypes[i], size);
                    for (int j = 0; j < size; ++j)
                    {
                        vector.set(j, data[j][i]);
                    }
                    cols.Add(vector);
                }
                BasicTable ret = new BasicTable(destTable.colNames, cols);
                return ret;
            }
            finally
            {
                rwLock_.ExitReadLock();
            }

        }

        void insertRecursive(DestTable destTable, List<IScalar> args)
        {
            if (destTable.finished)
            {
                throw new Exception("Failed to insert data. Error writing data in backgroud thread. Talbe (" + destTable.dbName + " " + destTable.tableName + ") will be removed.");
            }
            int cols = args.Count();
            for (int i = 0; i < cols; ++i)
            {
                if ((int)args[i].getDataType() != destTable.colDefsTypeInt.getInt(i))
                {
                    if (!(args[i].getDataType() == DATA_TYPE.DT_STRING && destTable.colDefsTypeInt.getInt(i) == (int)DATA_TYPE.DT_SYMBOL))
                        throw new Exception("Failed to insert data, the type of argument does not match the type of column at column: " + i.ToString());
                }
            }
            destTable.writeQueue.Enqueue(args);
        }

        private string hostName_;
        private int port_;
        private string userId_;
        private string password_;

        private IDictionary<Tuple<string, string>, DestTable> destTables_;
        ReaderWriterLockSlim rwLock_;
    }
}



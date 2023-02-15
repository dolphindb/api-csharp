using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using dolphindb.io;
using dolphindb;
using System.Collections;

namespace dolphindb.route
{
    public class PartitionedTableAppender
    {
        private BasicDictionary tableInfo;
        private Domain domain;
        private int partitionColumnIdx;
        private int cols;
        private DATA_CATEGORY[] columnCategories;
        private DATA_TYPE[] columnTypes;
        private int threadCount;
        private IDBConnectionPool pool;
        private List<List<int>> chunkIndices;
        private string appendScript;

        public PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, IDBConnectionPool pool):this(dbUrl, tableName, partitionColName, null, pool)
        {
            
        }
        public PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, IDBConnectionPool pool)
        {
            this.pool = pool;
            threadCount = pool.getConnectionCount();
            chunkIndices = new List<List<int>>(threadCount);
            for (int i = 0; i < threadCount; ++i)
                chunkIndices.Add(new List<int>());
            DBConnection conn = new DBConnection();
            IEntity partitionSchema;
            BasicTable colDefs;
            BasicIntVector typeInts;
            int partitionType;
            DATA_TYPE partitionColType;
            try
            {
                IDBTask task;
                if(dbUrl == null || dbUrl.Length == 0)
                {
                    task = new BasicDBTask("schema(" + tableName + ")");
                    appendScript = "tableInsert{" + tableName + "}";
                }
                else
                {
                    task = new BasicDBTask("schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))");
                    appendScript = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
                }
                if(appendFunction != null && appendFunction.Length != 0)
                {
                    appendScript = appendFunction;
                }
                pool.execute(task);
                if (!task.isSuccessful())
                    throw new Exception(task.getErrorMsg());
                tableInfo = (BasicDictionary)task.getResults();

                IEntity partColNames = null;
                try
                {
                    partColNames = tableInfo.get(new BasicString("partitionColumnName"));
                }
                catch(Exception e)
                {
                }
                if (partColNames == null)
                    throw new Exception("Can't find specified partition column name.");
                if (partColNames.isScalar())
                {
                    
                    if ( !((BasicString)partColNames).getString().Equals(partitionColName, StringComparison.OrdinalIgnoreCase) )
                        throw new Exception("Can't find specified partition column name.");
                    partitionColumnIdx = ((BasicInt)tableInfo.get(new BasicString("partitionColumnIndex"))).getValue();
                    partitionSchema = tableInfo.get(new BasicString("partitionSchema"));
                    partitionType = ((BasicInt)tableInfo.get(new BasicString("partitionType"))).getValue();
                    //
                    partitionColType = (DATA_TYPE)((BasicInt)tableInfo.get(new BasicString("partitionColumnType"))).getValue();
                    
                }


                else
                {
                    BasicStringVector vec = (BasicStringVector)partColNames;
                    int dims = vec.rows();
                    int index = -1;
                    for (int i = 0; i < dims; ++i)
                    {
                        if (vec.getString(i).Equals(partitionColName, StringComparison.OrdinalIgnoreCase))
                        {
                            index = i;
                            break;
                        }
                    }
                    if (index < 0)
                        throw new Exception("Can't find specified partition column name.");
                    partitionColumnIdx = ((BasicIntVector)tableInfo.get(new BasicString("partitionColumnIndex"))).getInt(index);
                    partitionSchema = ((BasicAnyVector)tableInfo.get(new BasicString("partitionSchema"))).getEntity(index);
                    partitionType = ((BasicIntVector)tableInfo.get(new BasicString("partitionType"))).getInt(index);
                    //
                    partitionColType = (DATA_TYPE)((BasicIntVector)tableInfo.get(new BasicString("partitionColumnType"))).getInt(index);

                }
                colDefs = ((BasicTable)tableInfo.get(new BasicString("colDefs")));
                this.cols = colDefs.getColumn(0).rows();
                typeInts = (BasicIntVector)colDefs.getColumn("typeInt");
                this.columnCategories = new DATA_CATEGORY[this.cols];
                this.columnTypes = new DATA_TYPE[this.cols];
                for (int i = 0; i < cols; ++i)
                {
                    this.columnTypes[i] = (DATA_TYPE)typeInts.getInt(i);
                    this.columnCategories[i] = Utils.typeToCategory(this.columnTypes[i]);
                   
                }
                domain = DomainFactory.createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);

            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                conn.close();
            }

        }

        public int append(ITable table)
        {
            if(cols != table.columns())
                throw new Exception("The input table doesn't match the schema of the target table.");
            for (int i = 0; i < cols; ++i)
            {
                IVector curCol = table.getColumn(i);
                checkColumnType(i, curCol.getDataCategory(), curCol.getDataType());
            }
            for (int i = 0; i < threadCount; ++i)
                chunkIndices[i].Clear();
            List<int> keys = domain.getPartitionKeys(table.getColumn(partitionColumnIdx));
            int rows = keys.Count;
            for(int i = 0; i < rows; ++i)
            {
                int key = keys[i];
                if (key >= 0)
                    chunkIndices[key % threadCount].Add(i);
            }
            List<IDBTask> tasks = new List<IDBTask>(threadCount);
            for(int i = 0; i < threadCount; ++i)
            {
                List<int> chunk = chunkIndices[i];
                if (chunk.Count == 0)
                    continue;
                int count = chunk.Count;
                int[] array = new int[count];
                for (int j = 0; j < count; ++j)
                    array[j] = chunk[j];
                ITable subTable = table.getSubTable(array);
                List<IEntity> args = new List<IEntity>(1);
                args.Add(subTable);
                tasks.Add(new BasicDBTask(appendScript, args));
            }
            pool.execute(tasks);
            int affected = 0;
            for (int i = 0; i < tasks.Count; ++i)
            {
                IDBTask task = tasks[i];
                if (task.isSuccessful())
                {
                    IEntity re = task.getResults();
                    if (re.getDataType() == DATA_TYPE.DT_VOID)
                    {
                        affected = 0;
                    }
                    else
                    {
                        affected += ((BasicInt)task.getResults()).getValue();
                    }
                }
            }
            return affected;
        }

        private void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type)
        {
            DATA_CATEGORY expectCategory = this.columnCategories[col];
            DATA_TYPE expectType = this.columnTypes[col];
            if (category != expectCategory)
            {
                throw new Exception("column " + col + ", expect category " + expectCategory.ToString() + ", got category " + category.ToString());
            }
            else if (category == DATA_CATEGORY.TEMPORAL && type != expectType)
            {
                throw new Exception("column " + col + ", temporal column must have exactly the same type, expect " + expectType.ToString() + ", got " + type.ToString());
            }
        }


    }
}

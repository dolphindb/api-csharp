using dolphindb;
using dolphindb.data;
using System;
using System.Collections.Generic;
using System.Text;

namespace dolphindb_csharpapi_net_core.src.route
{
    public class AutoFitTableUpsert
    {
        private DBConnection connection_;
        private string upsertScript_;
        private int cols_;
        private List<DATA_CATEGORY> columnCategories_ = new List<DATA_CATEGORY>();
        private List<DATA_TYPE> columnTypes_ = new List<DATA_TYPE>();
        private List<string> colNames_ = new List<string>();

        public AutoFitTableUpsert(string dbUrl, string tableName, DBConnection connection, bool ignoreNull, string[] pkeyColNames, string[] psortColumns)
        {
            connection_ = connection;
            BasicTable colDefs;
            BasicIntVector colTypesInt;
            BasicDictionary tableInfo;
            BasicStringVector colNames;
            try {
                string task;
                if (dbUrl.Equals(""))
                {
                    task = "schema(" + tableName + ")";
                    upsertScript_ = "upsert!{" + tableName + "";
                }
                else
                {
                    task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
                    upsertScript_ = "upsert!{loadTable('" + dbUrl + "', '" + tableName + "')";
                }
                upsertScript_ += ",";
                if (!ignoreNull)
                    upsertScript_ += ",ignoreNull=false";
                else
                    upsertScript_ += ",ignoreNull=true";
                int ignoreParamCount = 0;
                if (pkeyColNames != null && pkeyColNames.Length > 0)
                {
                    upsertScript_ += ",keyColNames=";
                    foreach (string one in pkeyColNames)
                    {
                        upsertScript_ += "`" + one;
                    }
                }
                else
                {
                    ignoreParamCount++;
                }
                if (psortColumns != null && psortColumns.Length > 0)
                {
                    while (ignoreParamCount > 0)
                    {
                        upsertScript_ += ",";
                        ignoreParamCount--;
                    }
                    upsertScript_ += ",sortColumns=";
                    foreach (string one in psortColumns)
                    {
                        upsertScript_ += "`" + one;
                    }
                }
                upsertScript_ += "}";
                tableInfo = (BasicDictionary)connection_.run(task);
                colDefs = (BasicTable)tableInfo.get(new BasicString("colDefs"));
                cols_ = colDefs.rows();
                colTypesInt = (BasicIntVector)colDefs.getColumn("typeInt");
                colNames = (BasicStringVector)colDefs.getColumn("name");
                for (int i = 0; i < cols_; i++)
                {
                    columnTypes_.Add((DATA_TYPE)colTypesInt.getInt(i));
                    columnCategories_.Add(Utils.getCategory(columnTypes_[i]));
                    colNames_.Add(colNames.getString(i));
                }
            }catch (Exception e){
                throw e;
            }
        }

        public int upsert(BasicTable table)
        {
            if (cols_ != table.columns())
                throw new Exception("The input table columns doesn't match the columns of the target table.");
            List<IVector> colums = new List<IVector> ();
            for (int i = 0; i < cols_; i++)
            {
                IVector curCol = table.getColumn(i);
                checkColumnType(i, curCol.getDataCategory(), curCol.getDataType());
                if (columnCategories_[i] == DATA_CATEGORY.TEMPORAL && curCol.getDataType() != columnTypes_[i])
                {
                    colums.Add((IVector)Utils.castDateTime(curCol, columnTypes_[i]));
                }
                else
                {
                    colums.Add(curCol);
                }
            }
            BasicTable tableToInsert = new BasicTable(colNames_, colums);
            List<IEntity> args = new List<IEntity>();
            args.Add(tableToInsert);
            IEntity res = connection_.run(upsertScript_, args);
            if (res.getDataType() == DATA_TYPE.DT_INT && res.getDataForm() == DATA_FORM.DF_SCALAR)
            {
                return ((BasicInt)res).getInt();
            }
            else
                return 0;
        }

        private void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type)
        {
            if (columnTypes_[col] != type)
            {
                DATA_CATEGORY expectCateGory = columnCategories_[col];
                if (category != expectCateGory)
                    throw new Exception("column " + col + ", expect category " + expectCateGory + ", got category " + category);
            }
        }
    }
}

using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

namespace dolphindb.data
{
    public class BasicTable : AbstractEntity, ITable
    {
        private IList<IVector> columns_ = new List<IVector>();
        private IList<string> names_ = new List<string>();
        private IDictionary<string, int?> name2index_ = new Dictionary<string, int?>();

        //
        private string _tableName = "tmpTb_" + System.Guid.NewGuid();
        public BasicTable(DataTable dt)
        {
            this.loadDataTable(dt);
        }
        public BasicTable(ExtendedDataInput @in)
        {
            int rows = @in.readInt();
            int cols = @in.readInt();
            _tableName = @in.readString();

            //read column names
            for (int i = 0; i < cols; ++i)
            {
                string name = @in.readString();
                name2index_[name] = name2index_.Count;
                names_.Add(name);
            }

            BasicEntityFactory factory = new BasicEntityFactory();
            //read columns
            for (int i = 0; i < cols; ++i)
            {
                short flag = @in.readShort();
                int form = flag >> 8;
                int type = flag & 0xff;

                DATA_FORM df = (DATA_FORM)form;
                DATA_TYPE dt = (DATA_TYPE)type;
                if (df != DATA_FORM.DF_VECTOR)
                {
                    throw new IOException("Invalid form for column [" + names_[i] + "] for table " + _tableName);
                }
                Console.WriteLine("vector create " + i + ":" + DateTime.Now);
                IVector vector = (IVector)factory.createEntity(df, dt, @in);
                Console.WriteLine("vector end create " + i + ":" + DateTime.Now);
                if (vector.rows() != rows && vector.rows() != 1)
                {
                    throw new IOException("The number of rows for column " + names_[i] + " is not consistent with other columns");
                }
                columns_.Add(vector);
            }
        }

        public BasicTable(IList<string> colNames, IList<IVector> cols)
        {
            this.setColName(colNames);
            this.setColumns(cols);
        }

        public virtual void setColName(IList<string> value)
        {
            names_.Clear();
            foreach (string name in value)
            {
                names_.Add(name);
            }
        }

        public virtual void setColumns(IList<IVector> value)
        {
            columns_.Clear();
            foreach (IVector vector in value)
            {
                columns_.Add(vector);
            }
        }

        public virtual DATA_CATEGORY getDataCategory()
        {
            return DATA_CATEGORY.MIXED;
        }

        public virtual DATA_TYPE getDataType()
        {
            return DATA_TYPE.DT_DICTIONARY;
        }

        public override DATA_FORM getDataForm()
        {
            return DATA_FORM.DF_TABLE;
        }

        public virtual int rows()
        {
            if (columns() <= 0)
            {
                return 0;
            }
            else
            {
                return columns_[0].rows();
            }
        }

        public virtual int columns()
        {
            return columns_.Count;
        }

        public IList<IVector> getColumns()
        {
            return this.columns_;
        }

        public virtual IVector getColumn(int index)
        {
            return columns_[index];
        }

        public virtual IVector getColumn(string name)
        {
            int? index = name2index_[name];
            if (index == null)
            {
                return null;
            }
            else
            {
                return getColumn(index.Value);
            }
        }

        public virtual string getColumnName(int index)
        {
            return names_[index];
        }

        public virtual string getString()
        {
            int rows = Math.Min(Utils.DISPLAY_ROWS, this.rows());
            int strColMaxWidth = Utils.DISPLAY_WIDTH / Math.Min(columns(), Utils.DISPLAY_COLS) + 5;
            int length = 0;
            int curCol = 0;
            int maxColWidth;
            StringBuilder[] list = new StringBuilder[rows + 1];
            StringBuilder separator = new StringBuilder();
            string[] listTmp = new string[rows + 1];
            int i, curSize;

            for (i = 0; i < list.Length; ++i)
            {
                list[i] = new StringBuilder();
            }

            while (length < Utils.DISPLAY_WIDTH && curCol < columns())
            {
                listTmp[0] = getColumnName(curCol);
                maxColWidth = 0;
                for (i = 0; i < rows; i++)
                {
                    listTmp[i + 1] = getColumn(curCol).get(i).getString();
                    if (listTmp[i + 1].Length > maxColWidth)
                    {
                        maxColWidth = listTmp[i + 1].Length;
                    }
                }
                if (maxColWidth > strColMaxWidth && getColumn(curCol).getDataCategory() == DATA_CATEGORY.LITERAL)
                {
                    maxColWidth = strColMaxWidth;
                }
                if ((int)listTmp[0].Length > maxColWidth)
                {
                    maxColWidth = Math.Min(strColMaxWidth, (int)listTmp[0].Length);
                }

                if (length + maxColWidth > Utils.DISPLAY_WIDTH && curCol + 1 < columns())
                {
                    break;
                }

                for (int k = 0; k < maxColWidth; ++k)
                {
                    separator.Append('-');
                }
                if (curCol < columns() - 1)
                {
                    maxColWidth++;
                    separator.Append(' ');
                }

                for (i = 0; i <= rows; i++)
                {
                    curSize = listTmp[i].Length;
                    if (curSize <= maxColWidth)
                    {
                        list[i].Append(listTmp[i]);
                        if (curSize < maxColWidth)
                        {
                            for (int j = 0; j < (maxColWidth - curSize); j++)
                            {
                                list[i].Append(' ');
                            }
                        }
                    }
                    else
                    {
                        if (maxColWidth > 3)
                        {
                            list[i].Append(listTmp[i].Substring(0, maxColWidth - 3));
                        }
                        list[i].Append("...");
                        separator.Append("---");
                    }
                }
                length += maxColWidth;
                curCol++;
            }

            if (curCol < columns())
            {
                for (i = 0; i <= rows; i++)
                {
                    list[i].Append("...");
                }
            }

            StringBuilder resultStr = list[0]; //
            resultStr.Append("\n");
            resultStr.Append(separator);
            resultStr.Append("\n");
            for (i = 1; i <= rows; i++)
            {
                resultStr.Append(list[i]);
                resultStr.Append("\n");
            }
            if (rows < this.rows())
            {
                resultStr.Append("...\n");
            }
            return resultStr.ToString();
        }

        public virtual void write(ExtendedDataOutput @out)
        {
            int flag = ((int)DATA_FORM.DF_TABLE << 8) + (int)getDataType();
            @out.writeShort(flag);
            @out.writeInt(rows());
            @out.writeInt(columns());
            @out.writeString(""); //table name
            foreach (string colName in names_)
            {
                @out.writeString(colName);
            }
            foreach (IVector vector in columns_)
            {
                vector.write(@out);
            }
        }
        /// <summary>
        /// transfer data to datatable
        /// </summary>
        /// <returns></returns>
        public DataTable toDataTable()
        {
            DataTable dt = new DataTable(_tableName);
            foreach (string fieldName in names_)
            {
                int i = name2index_[fieldName].Value;
                DATA_TYPE dtype = columns_[i].getDataType();
                DataColumn dc = new DataColumn(fieldName, Utils.getSystemType(dtype));
                dt.Columns.Add(dc);
            }
            if (columns_.Count == 0) return null;//table columns not exists
            int rowCount = columns_[0].rows();

            for (int rowIndex = 0; rowIndex < this.rows(); rowIndex++)
            {
                DataRow dr = dt.NewRow();

                for (int colIndex = 0; colIndex < this.columns(); colIndex++)
                {
                    DATA_TYPE dtype = columns_[colIndex].getDataType();
                    switch (dtype)
                    {
                        case DATA_TYPE.DT_BOOL:
                            dr[colIndex] = ((BasicBoolean)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_BYTE:
                            dr[colIndex] = ((BasicByte)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_SHORT:
                            dr[colIndex] = ((BasicShort)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_INT:
                            dr[colIndex] = ((BasicInt)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_LONG:
                            dr[colIndex] = ((BasicLong)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_FLOAT:
                            dr[colIndex] = ((BasicFloat)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        case DATA_TYPE.DT_DOUBLE:
                            dr[colIndex] = ((BasicDouble)columns_[colIndex].get(rowIndex)).getValue();
                            break;
                        default:
                            dr[colIndex] = columns_[colIndex].get(rowIndex).getString();
                            break;
                    }
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }
        /// <summary>
        /// load data from datatable
        /// </summary>
        /// <param name="dt">the datatable to load into basicTable</param>
        public void loadDataTable(DataTable dt)
        {
            string tableName = dt.TableName;
            DataView dv = dt.DefaultView;
            int rowCount = dt.Rows.Count;
            int colCount = dt.Columns.Count;
            for (int colIndex = 0; colIndex < colCount; colIndex++)
            {

                Type t = dt.Columns[colIndex].DataType;
                IVector curColumn = getDolphinDBVectorBySystemType(t, rowCount);

                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    curColumn.set(rowIndex, getDolphinDBScalarBySystemType(t, dv[rowIndex][colIndex]));
                }
                this.columns_.Add(curColumn);
                this.names_.Add(dt.Columns[colIndex].ColumnName);
                KeyValuePair<String, int?> colNameIndex = new KeyValuePair<string, int?>(dt.Columns[colIndex].ColumnName, colIndex);
                this.name2index_.Add(colNameIndex);
            }
        }


        private IVector getDolphinDBVectorBySystemType(Type stype, int rowCount)
        {
            IVector v = null;
            if (stype == Type.GetType("System.Boolean"))
            {
                v = new BasicBooleanVector(rowCount);
            }
            else if (stype == Type.GetType("System.Byte"))
            {
                v = new BasicByteVector(rowCount);
            }
            else if (stype == Type.GetType("System.Double"))
            {
                v = new BasicDoubleVector(rowCount);
            }
            else if (stype == Type.GetType("System.DateTime"))
            {
                v = new BasicDateTimeVector(rowCount);
            }
            else if (stype == Type.GetType("System.TimeSpan"))
            {
                v = new BasicTimeVector(rowCount);
            }
            else if (stype == Type.GetType("System.Int16"))
            {
                v = new BasicShortVector(rowCount);
            }
            else if (stype == Type.GetType("System.Int32"))
            {
                v = new BasicIntVector(rowCount);
            }
            else if (stype == Type.GetType("System.Int64"))
            {
                v = new BasicLongVector(rowCount);
            }
            else
            {
                v = new BasicStringVector(rowCount);
            }
            return v;
        }

        private IScalar getDolphinDBScalarBySystemType(Type stype, object value)
        {
            IScalar data = null;
            if (stype == Type.GetType("System.Boolean"))
            {
                data = new BasicBoolean(Convert.ToBoolean(value));
            }
            else if (stype == Type.GetType("System.Byte"))
            {
                data = new BasicByte(Convert.ToByte(value));
            }
            else if (stype == Type.GetType("System.Double"))
            {
                data = new BasicDouble(Convert.ToDouble(value));
            }
            else if (stype == Type.GetType("System.DateTime"))
            {
                data = new BasicDateTime(Convert.ToDateTime(value));
            }
            else if (stype == Type.GetType("System.TimeSpan"))
            {
                data = new BasicTime((TimeSpan)value);
            }

            else if (stype == Type.GetType("System.Int16"))
            {
                data = new BasicShort(Convert.ToInt16(value));
            }
            else if (stype == Type.GetType("System.Int32"))
            {
                data = new BasicInt(Convert.ToInt32(value));
            }
            else if (stype == Type.GetType("System.Int64"))
            {
                data = new BasicLong(Convert.ToInt64(value));
            }
            else
            {
                data = new BasicString(value.ToString());
            }
            return data;
        }

        public object getObject()
        {
            throw new NotImplementedException();
        }
    }

}
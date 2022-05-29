using dolphindb.io;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using dolphindb.compression;

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
            if (dt.Columns.Count == 0)
            {
                throw new Exception("DataTable must contain at least one column");
            }
            loadDataTable(dt);
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
                bool extended = type >= 128;
                if (type >= 128)
                    type -= 128;

                DATA_FORM df = (DATA_FORM)form;
                DATA_TYPE dt = (DATA_TYPE)type;
                VectorDecompressor decompressor = null;
                SymbolBaseCollection collection = null;
                if (df != DATA_FORM.DF_VECTOR)
                {
                    throw new IOException("Invalid form for column [" + names_[i] + "] for table " + _tableName);
                }
                IVector vector;
                if (dt == DATA_TYPE.DT_SYMBOL && extended)
                {
                    if (collection == null)
                        collection = new SymbolBaseCollection();
                    vector = new BasicSymbolVector(df, @in, collection);
                }else if (dt == DATA_TYPE.DT_COMPRESS)
                {
                    if (decompressor == null)
                        decompressor = new VectorDecompressor();
                    vector = decompressor.Decompress(factory, @in, false, true);
                }
                else
                {
                    vector = (IVector)factory.createEntity(df, dt, @in, extended);
                }
                if (vector.rows() != rows && vector.rows() != 1)
                {
                    int tmp = vector.rows();
                    throw new IOException("The number of rows for column " + names_[i] + " is not consistent with other columns");
                }
                columns_.Add(vector);
            }
        }

        public BasicTable(IList<string> colNames, IList<IVector> cols)
        {
            setColName(colNames);
            setColumns(cols);
        }

        public virtual void setColName(IList<string> value)
        {
            names_.Clear();
            name2index_.Clear();
            foreach (string name in value)
            {
                names_.Add(name);
                name2index_[name] = name2index_.Count;
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

        //2021.01.26 cwj
        public void change_column(string name, IVector col_new)
        {
            int? index = name2index_[name];
            columns_[(int)index] = col_new;

        }
        //

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
            return columns_;
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

        public virtual IList<string> getColumnNames()
        {
            return names_;
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
        
        public void writeCompressed(ExtendedDataOutput output){

        short flag = ((short)(DATA_FORM.DF_TABLE) << 8 | 8 & 0xff); //8: table type TODO: add table type
        output.writeShort(flag);

		int rows = this.rows();
        int cols = this.columns();
        output.writeInt(rows);
		output.writeInt(cols);
		output.writeString(""); //table name
		for (int i = 0; i<cols; i++) {
			output.writeString(this.getColumnName(i));
        }

		for (int i = 0; i<cols; i++) {
			AbstractVector v = (AbstractVector)this.getColumn(i);
            if (v.getDataType() == DATA_TYPE.DT_SYMBOL)
                    v.write((ExtendedDataOutput)output);
            else
                v.writeCompressed((ExtendedDataOutput)output);
            output.flush();
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

            for (int rowIndex = 0; rowIndex < rows(); rowIndex++)
            {
                DataRow dr = dt.NewRow();

                for (int colIndex = 0; colIndex < columns(); colIndex++)
                {
                    DATA_TYPE dtype = columns_[colIndex].getDataType();
                    switch (dtype)
                    {
                        case DATA_TYPE.DT_BOOL:
                            BasicBoolean bbobj = (BasicBoolean)columns_[colIndex].get(rowIndex);
                            if (bbobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = bbobj.getValue();
                            break;
                        case DATA_TYPE.DT_BYTE:
                            BasicByte bobj = (BasicByte)columns_[colIndex].get(rowIndex);
                            if (bobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = bobj.getValue();
                            break;
                        case DATA_TYPE.DT_SHORT:
                            BasicShort bsobj = (BasicShort)columns_[colIndex].get(rowIndex);
                            if (bsobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = bsobj.getValue();
                            break;
                        case DATA_TYPE.DT_INT:
                            BasicInt biobj = (BasicInt)columns_[colIndex].get(rowIndex);
                            if (biobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = biobj.getValue();
                            break;
                        case DATA_TYPE.DT_LONG:
                            BasicLong blobj = (BasicLong)columns_[colIndex].get(rowIndex);
                            if (blobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = blobj.getValue();
                            break;
                        case DATA_TYPE.DT_FLOAT:
                            BasicFloat bfobj = (BasicFloat)columns_[colIndex].get(rowIndex);
                            if (bfobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = bfobj.getValue();
                            break;
                        case DATA_TYPE.DT_DOUBLE:
                            BasicDouble bdobj = (BasicDouble)columns_[colIndex].get(rowIndex);
                            if (bdobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = bdobj.getValue();
                            break;
                        default:
                            IScalar eobj = (IScalar)columns_[colIndex].get(rowIndex);
                            if (eobj.isNull())
                                dr[colIndex] = DBNull.Value;
                            else
                                dr[colIndex] = eobj.getString();
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
                columns_.Add(curColumn);
                names_.Add(dt.Columns[colIndex].ColumnName);
                KeyValuePair<string, int?> colNameIndex = new KeyValuePair<string, int?>(dt.Columns[colIndex].ColumnName, colIndex);
                name2index_.Add(colNameIndex);
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
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicBoolean(false);
                }
                else
                {
                    data = new BasicBoolean(Convert.ToBoolean(value));
                }
            }
            else if (stype == Type.GetType("System.Byte"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicByte(0);
                }
                else
                {
                    data = new BasicByte(Convert.ToByte(value));
                }
               
            }
            else if (stype == Type.GetType("System.Double"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicDouble(0);
                    data.setNull();
                }
                else
                {
                    data = new BasicDouble(Convert.ToDouble(value));
                }
            }
            else if (stype == Type.GetType("System.DateTime"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicDateTime(0);
                }
                else
                {
                    data = new BasicDateTime(Convert.ToDateTime(value));
                }
                
            }
            else if (stype == Type.GetType("System.TimeSpan"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicTime(0);
                }
                else
                {
                    data = new BasicTime((TimeSpan)value);
                }
            }

            else if (stype == Type.GetType("System.Int16"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicShort(0);
                    data.setNull();
                }
                else
                {
                    data = new BasicShort(Convert.ToInt16(value));
                }
                
            }
            else if (stype == Type.GetType("System.Int32"))
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicInt(0);
                    data.setNull();
                }
                else
                {
                    data = new BasicInt(Convert.ToInt32(value));
                }
                
            }
            else if (stype == Type.GetType("System.Int64"))
            {

                if (value == null || value == DBNull.Value)
                {
                    data = new BasicLong(0);
                    data.setNull();
                }
                else
                {
                    data = new BasicLong(Convert.ToInt64(value));
                }
                
            }
            else
            {
                if (value == null || value == DBNull.Value)
                {
                    data = new BasicString("");
                    data.setNull();
                }
                else
                {
                    data = new BasicString(value.ToString());
                }
                
            }
            return data;
        }

        public object getObject()
        {
            throw new NotImplementedException();
        }

        public void add(BasicRow br)
        {
            for(int i = 0; i < columns_.Count; i++)
            {
                columns_[i].add(br[i]);
            }
        }

        public ITable getSubTable(int[] indices)
        {
            int colCount = columns_.Count;
            List<IVector> cols = new List<IVector>(colCount);
            for (int i = 0; i < colCount; ++i)
                cols.Add(columns_[i].getSubVector(indices));
            return new BasicTable(names_, cols);
        }

    }

    public class BasicRow : List<object>
    {

        public BasicRow(int size):base(size)
        {
            
        }
        public new object this[int index]
        {
            get
            {
                return base[index];
            }
            set
            {
                base[index] = value;
            }
        }
    }

}
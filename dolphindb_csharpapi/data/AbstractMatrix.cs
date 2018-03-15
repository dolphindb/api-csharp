using dolphindb.io;
using System;
using System.IO;
using System.Text;

namespace dolphindb.data
{

	public abstract class AbstractMatrix : AbstractEntity, IMatrix
	{
        public abstract DATA_TYPE getDataType();
        public abstract DATA_CATEGORY getDataCategory();
        public abstract Type getElementClass();
		public abstract IScalar get(int row, int column);
		public abstract void setNull(int row, int column);
		public abstract bool isNull(int row, int column);
		private IVector rowLabels = null;
		private IVector columnLabels = null;
		protected int _rows;
		protected int _columns;

		protected internal abstract void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput @in);

        protected internal abstract void writeVectorToOutputStream(ExtendedDataOutput @out);

		protected internal AbstractMatrix(int rows, int columns)
		{
			this._rows = rows;
			this._columns = columns;
		}

		protected internal AbstractMatrix(ExtendedDataInput @in)
		{
			byte hasLabels = @in.readByte();

			BasicEntityFactory factory = null;
			DATA_TYPE[] types = Enum.GetValues(typeof(DATA_TYPE)) as DATA_TYPE[];
			if (hasLabels > 0)
			{
				factory = new BasicEntityFactory();
			}
            short flag;
            int form;
            int type;
            if ((hasLabels & 1) == 1)
			{
				//contain row labels
				 flag = @in.readShort();
				 form = flag >> 8;
				 type = flag & 0xff;
				if (form != (int)DATA_FORM.DF_VECTOR)
				{
					throw new IOException("The form of matrix row labels must be vector");
				}
				if (type < 0 || type >= types.Length)
				{
					throw new IOException("Invalid data type for matrix row labels: " + type);
				}
				rowLabels = (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);
			}

            if ((hasLabels & 2) == 2)
			{
				//contain columns labels
				flag = @in.readShort();
				form = flag >> 8;
				type = flag & 0xff;
				if (form != (int)DATA_FORM.DF_VECTOR)
				{
					throw new IOException("The form of matrix columns labels must be vector");
				}
				if (type < 0 || type >= types.Length)
				{
					throw new IOException("Invalid data type for matrix column labels: " + type);
				}
				columnLabels = (IVector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], @in);
			}

			flag = @in.readShort();
			type = flag & 0xff;
			if (type < 0 || type >= types.Length)
			{
				throw new IOException("Invalid data type for matrix: " + type);
			}
			_rows = @in.readInt();
			_columns = @in.readInt();
			readMatrixFromInputStream(_rows, _columns, @in);
		}

		protected internal virtual int getIndex(int row, int column)
		{
			return column * _rows + row;
		}

		public virtual bool hasRowLabel()
		{
			return rowLabels != null;
		}

		public virtual bool hasColumnLabel()
		{
			return columnLabels != null;
		}

		public virtual IScalar getRowLabel(int index)
		{
			return rowLabels.get(index);
		}

		public virtual IScalar getColumnLabel(int index)
		{
			return columnLabels.get(index);
		}

		public virtual IVector getRowLabels()
        {
				return rowLabels;
        }
        public virtual void setRowLabels(IVector vector)
        {
				if (vector.rows() != _rows)
				{
					throw new System.ArgumentException("the row label size doesn't equal to the row number of the matrix.");
				}
				rowLabels = vector;
		}


		public virtual IVector getColumnLabels()
		{
			return columnLabels;
		}

        public virtual void setColumnLabels(IVector vector)
        {
			if (vector.rows() != _columns)
			{
				throw new System.ArgumentException("the column label size doesn't equal to the column number of the matrix.");
			}
			columnLabels = vector;
		}

		public virtual string getString()
		{
				int rows = Math.Min(Utils.DISPLAY_ROWS,this.rows());
				int limitColMaxWidth = 25;
				int length = 0;
				int curCol = 0;
				int maxColWidth;
				StringBuilder[] list = new StringBuilder[rows + 1];
				string[] listTmp = new string[rows + 1];
				int i, curSize;
    
				for (i = 0; i < list.Length; ++i)
				{
					list[i] = new StringBuilder();
				}
    
				//display row label
				if (rowLabels != null)
				{
					listTmp[0] = "";
					maxColWidth = 0;
					for (i = 0;i < rows;i++)
					{
						listTmp[i + 1] = rowLabels.get(i).getString();
						if (listTmp[i + 1].Length > maxColWidth)
						{
							maxColWidth = listTmp[i + 1].Length;
						}
					}
					maxColWidth++;
					for (i = 0;i <= rows;i++)
					{
						curSize = listTmp[i].Length;
						if (curSize <= maxColWidth)
						{
							list[i].Append(listTmp[i]);
							if (curSize < maxColWidth)
							{
								for (int j = 0; j < maxColWidth - curSize; ++j)
								{
									list[i].Append(' ');
								}
							}
						}
						else
						{
							if (maxColWidth > 3)
							{
								list[i].Append(listTmp[i].Substring(0,maxColWidth - 3));
							}
							list[i].Append("...");
						}
					}
					length += maxColWidth;
				}
    
				while (length < Utils.DISPLAY_WIDTH && curCol < columns())
				{
					listTmp[0] = columnLabels == null ?"#" + curCol : columnLabels.get(curCol).getString();
					maxColWidth = 0;
					for (i = 0;i < rows;i++)
					{
						listTmp[i + 1] = get(i, curCol).getString();
						if (listTmp[i + 1].Length > maxColWidth)
						{
							maxColWidth = listTmp[i + 1].Length;
						}
					}
					if (maxColWidth > limitColMaxWidth)
					{
						maxColWidth = limitColMaxWidth;
					}
					if ((int)listTmp[0].Length > maxColWidth)
					{
						maxColWidth = Math.Min(limitColMaxWidth, listTmp[0].Length);
					}
					if (curCol < columns() - 1)
					{
						maxColWidth++;
					}
    
					if (length + maxColWidth > Utils.DISPLAY_WIDTH && curCol + 1 < columns())
					{
						break;
					}
    
					for (i = 0;i <= rows;i++)
					{
						curSize = listTmp[i].Length;
						if (curSize <= maxColWidth)
						{
							list[i].Append(listTmp[i]);
							if (curSize < maxColWidth)
							{
								for (int j = 0; j < maxColWidth - curSize; ++j)
								{
									list[i].Append(' ');
								}
							}
						}
						else
						{
							if (maxColWidth > 3)
							{
								list[i].Append(listTmp[i].Substring(0,maxColWidth - 3));
							}
							list[i].Append("...");
						}
					}
					length += maxColWidth;
					curCol++;
				}
    
				if (curCol < _columns)
				{
					for (i = 0;i <= rows;i++)
					{
						list[i].Append("...");
					}
				}
    
				StringBuilder resultStr = new StringBuilder();
				for (i = 0;i <= rows;i++)
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

		public override DATA_FORM getDataForm()
		{
			return DATA_FORM.DF_MATRIX;
		}

		public virtual int rows()
		{
			return _rows;
		}

		public virtual int columns()
		{
			return _columns;
		}

		public virtual void write(ExtendedDataOutput @out)
		{
			int flag = ((int)DATA_FORM.DF_MATRIX << 8) + (int)getDataType();
			@out.writeShort((short)flag);
			byte labelFlag = (byte)((hasRowLabel() ? 1 : 0) + (hasColumnLabel() ? 2 : 0));
			@out.writeByte(labelFlag);
			if (hasRowLabel())
			{
				rowLabels.write(@out);
			}
			if (hasColumnLabel())
			{
				columnLabels.write(@out);
			}
			@out.writeShort((short)flag);
			@out.writeInt(rows());
			@out.writeInt(columns());
			writeVectorToOutputStream(@out);
		}
	}

}
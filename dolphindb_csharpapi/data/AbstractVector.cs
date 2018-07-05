using dolphindb.io;
using System;
using System.Text;

namespace dolphindb.data
{

	public abstract class AbstractVector : AbstractEntity, IVector
	{
		public abstract int rows();
        public abstract DATA_TYPE getDataType();
        public abstract DATA_CATEGORY getDataCategory();
        public abstract Type getElementClass();
		public abstract void set(int index, IScalar value);
		public abstract IScalar get(int index);
        public abstract void setNull(int index);
		public abstract bool isNull(int index);
		private DATA_FORM df_;


		protected internal abstract void writeVectorToOutputStream(ExtendedDataOutput @out);

		public AbstractVector(DATA_FORM df)
		{
			df_ = df;
		}

		public override DATA_FORM getDataForm()
		{
				return df_;
		}

		public virtual int columns()
		{
			return 1;
		}

		public virtual string getString()
		{
				StringBuilder sb = new StringBuilder("[");
				int size = Math.Min(Vector_Fields.DISPLAY_ROWS, rows());
				if (size > 0)
				{
					sb.Append(get(0).ToString());
				}
				for (int i = 1; i < size; ++i)
				{
					sb.Append(',');
					sb.Append(get(i).ToString());
				}
				if (size < rows())
				{
					sb.Append(",...");
				}
				sb.Append("]");
				return sb.ToString();
		}

		public virtual void write(ExtendedDataOutput @out)
		{
			int flag = ((int)df_ << 8) + (int)getDataType();
			@out.writeShort((short)flag);
			@out.writeInt(rows());
			@out.writeInt(columns());
			writeVectorToOutputStream(@out);
		}
	}

}
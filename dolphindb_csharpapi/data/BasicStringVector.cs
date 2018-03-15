using dolphindb.io;
using System;
using System.Collections.Generic;

namespace dolphindb.data
{

	public class BasicStringVector : AbstractVector
	{
		private string[] values;
		private bool isSymbol;

		public BasicStringVector(int size) : this(DATA_FORM.DF_VECTOR, size, false)
		{
		}

		public BasicStringVector(IList<string> list) : base(DATA_FORM.DF_VECTOR)
		{
			if (list != null)
			{
				values = new string[list.Count];
				for (int i = 0; i < list.Count; ++i)
				{
					values[i] = list[i];
				}
			}
			this.isSymbol = false;
		}

		public BasicStringVector(string[] array) : base(DATA_FORM.DF_VECTOR)
		{
			values = array.Clone() as string[];
			this.isSymbol = false;
		}

		protected internal BasicStringVector(DATA_FORM df, int size, bool isSymbol) : base(df)
		{
			values = new string[size];
			this.isSymbol = isSymbol;
		}

		protected internal BasicStringVector(DATA_FORM df, ExtendedDataInput @in) : base(df)
		{
			int rows = @in.readInt();
			int columns = @in.readInt();
			int size = rows * columns;
			values = new string[size];
			for (int i = 0; i < size; ++i)
			{
				values[i] = @in.readString();
			}
		}

		public override IScalar get(int index)
		{
			return new BasicString(values[index]);
		}

		public virtual string getString(int index)
		{
			return values[index];
		}

		public override void set(int index, IScalar value)
		{
			values[index] = value.ToString();
		}

		public virtual void setString(int index, string value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return string.ReferenceEquals(values[index], null) || values[index].Length == 0;
		}

		public override void setNull(int index)
		{
			values[index] = "";
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.LITERAL;
		}

		public override DATA_TYPE getDataType()
		{
			return isSymbol ? DATA_TYPE.DT_SYMBOL : DATA_TYPE.DT_STRING;
		}

		public override Type getElementClass()
		{
			return typeof(BasicString);
		}

		public override int rows()
		{
			return values.Length;
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (string str in values)
			{
				@out.writeString(str);
			}
		}
	}

}
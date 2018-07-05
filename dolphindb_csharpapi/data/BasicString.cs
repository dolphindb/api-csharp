using System;
using dolphindb.io;

namespace dolphindb.data
{

	public class BasicString : AbstractScalar, IComparable<BasicString>
	{
		private string value;

		public BasicString(string value)
		{
			this.value = value;
		}

		public BasicString(ExtendedDataInput @in)
		{
			value = @in.readString();
		}

        public String getValue()
        {
            return value;
        }

        protected void setValue(String value)
        {
            this.value = value;
        }


        public override bool isNull()
		{
			return value.Length == 0;
		}

		public override void setNull()
		{
			value = "";
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.LITERAL;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_STRING;
		}

		public override Number getNumber()
		{
			throw new Exception("Imcompatible data type");
		}

		public override object getTemporal()
		{
			throw new Exception("Imcompatible data type");
		}

		public override bool Equals(object o)
		{
			if (!(o is BasicString) || o == null)
			{
				return false;
			}
			else
			{
				return value.Equals(((BasicString)o).value);
			}
		}

		public override int GetHashCode()
		{
			return value.GetHashCode();
		}


		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeString(value);
		}

		public virtual int CompareTo(BasicString o)
		{
			return value.CompareTo(o.value);
		}

        public override string getString()
        {
            return value;
        }
    }

}
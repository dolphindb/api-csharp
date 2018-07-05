using dolphindb.io;

namespace dolphindb.data
{
    public class Void : AbstractScalar
	{

		public override bool isNull()
		{
				return true;
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.NOTHING;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_VOID;
		}

        public override string getString()
		{
				return "";
		}

		public override bool Equals(object o)
		{
			if (!(o is Void) || o == null)
			{
				return false;
			}
			else
			{
				return true;
			}
		}

		public override int GetHashCode()
		{
			return 0;
		}


		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			@out.writeBoolean(true); //explicit null value
		}
	}

}
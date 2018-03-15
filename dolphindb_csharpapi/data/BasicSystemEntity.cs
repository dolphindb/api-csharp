using dolphindb.io;
using System.IO;

namespace dolphindb.data
{
	public class BasicSystemEntity : BasicString
	{
		private DATA_TYPE type;

		public BasicSystemEntity(ExtendedDataInput @in, DATA_TYPE type) : base("")
		{
			this.type = type;
			if (type == DATA_TYPE.DT_FUNCTIONDEF)
			{
				@in.readByte();
			}
			base.setValue(@in.readString());
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.SYSTEM;
		}

		public override DATA_TYPE getDataType()
		{
			return type;
		}
		protected override void writeScalarToOutputStream(ExtendedDataOutput @out)
		{
			throw new IOException("System entity is not supposed to serialize.");
		}
	}

}
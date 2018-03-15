using dolphindb.io;
using System;
using System.Text;

namespace dolphindb.data
{
	public class BasicAnyVector : AbstractVector
	{
		private IEntity[] values;


		public BasicAnyVector(int size) : base(DATA_FORM.DF_VECTOR)
		{
			values = new IEntity[size];
		}

		protected internal BasicAnyVector(ExtendedDataInput @in) : base(DATA_FORM.DF_VECTOR)
		{
			int rows = @in.readInt();
			int cols = @in.readInt();
			int size = rows * cols;
			values = new IEntity[size];

			BasicEntityFactory factory = new BasicEntityFactory();
			for (int i = 0; i < size; ++i)
			{
				short flag = @in.readShort();
				int form = flag >> 8;
				int type = flag & 0xff;
				//if (form != 1)
					//assert (form == 1);
				//if (type != 4)
					//assert(type == 4);
				IEntity obj = factory.createEntity((DATA_FORM)form, (DATA_TYPE)type, @in);
				values[i] = obj;
			}

		}

		public virtual IEntity getEntity(int index)
		{
			return values[index];
		}

		public override IScalar get(int index)
		{
			if (values[index].isScalar())
			{
				return (IScalar)values[index];
			}
			else
			{
				throw new Exception("The element of the vector is not a scalar object.");
			}
		}

		public override void set(int index, IScalar value)
		{
			values[index] = value;
		}

		public virtual void setEntity(int index, IEntity value)
		{
			values[index] = value;
		}

		public override bool isNull(int index)
		{
			return values[index] == null || (values[index].isScalar() && ((IScalar)values[index]).isNull());
		}

		public override void setNull(int index)
		{
			values[index] = new Void();
		}

		public override DATA_CATEGORY getDataCategory()
		{
			return DATA_CATEGORY.MIXED;
		}

		public override DATA_TYPE getDataType()
		{
			return DATA_TYPE.DT_ANY;
		}

		public override int rows()
		{
			return values.Length;
		}

		public override string getString()
		{
				StringBuilder sb = new StringBuilder("(");
				int size = Math.Min(10, rows());
				if (size > 0)
				{
					sb.Append(getEntity(0).getString());
				}
				for (int i = 1; i < size; ++i)
				{
					sb.Append(',');
					sb.Append(getEntity(i).getString());
				}
				if (size < rows())
				{
					sb.Append(",...");
				}
				sb.Append(")");
				return sb.ToString();
		}

		public override Type getElementClass()
		{
				return typeof(IEntity);
		}

		protected internal override void writeVectorToOutputStream(ExtendedDataOutput @out)
		{
			foreach (IEntity value in values)
			{
				value.write(@out);
			}
		}
	}

}
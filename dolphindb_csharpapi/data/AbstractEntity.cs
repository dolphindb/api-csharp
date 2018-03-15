namespace dolphindb.data
{

	public abstract class AbstractEntity
	{
        public abstract DATA_FORM getDataForm();

		public bool isScalar()
		{
			return getDataForm() == DATA_FORM.DF_SCALAR;
		}
		public bool isVector()
		{
			return getDataForm() == DATA_FORM.DF_VECTOR;
		}
		public bool isPair()
		{
			return getDataForm() == DATA_FORM.DF_PAIR;
		}
		public bool isTable()
		{
			return getDataForm() == DATA_FORM.DF_TABLE;
		}
		public bool isMatrix()
		{
			return getDataForm() == DATA_FORM.DF_MATRIX;
		}
		public bool isDictionary()
		{
			return getDataForm() == DATA_FORM.DF_DICTIONARY;
		}
		public bool isChart()
		{
			return getDataForm() == DATA_FORM.DF_CHART;
		}
		public bool isChunk()
		{
			return getDataForm() == DATA_FORM.DF_CHUNK;
		}

		protected internal DATA_CATEGORY getDataCategory(DATA_TYPE valueType)
		{
			if (valueType == DATA_TYPE.DT_BOOL)
			{
				return DATA_CATEGORY.LOGICAL;
			}
			else if (valueType == DATA_TYPE.DT_STRING || valueType == DATA_TYPE.DT_SYMBOL)
			{
				return DATA_CATEGORY.LITERAL;
			}
			else if (valueType == DATA_TYPE.DT_DOUBLE || valueType == DATA_TYPE.DT_FLOAT)
			{
				return DATA_CATEGORY.FLOATING;
			}
			else if (valueType == DATA_TYPE.DT_BYTE || valueType == DATA_TYPE.DT_SHORT || valueType == DATA_TYPE.DT_INT || valueType == DATA_TYPE.DT_LONG)
			{
				return DATA_CATEGORY.INTEGRAL;
			}
			else if (valueType == DATA_TYPE.DT_FUNCTIONDEF || valueType == DATA_TYPE.DT_HANDLE)
			{
				return DATA_CATEGORY.SYSTEM;
			}
			else if (valueType == DATA_TYPE.DT_VOID)
			{
				return DATA_CATEGORY.NOTHING;
			}
			else if (valueType == DATA_TYPE.DT_ANY)
			{
				return DATA_CATEGORY.MIXED;
			}
			else if (valueType == DATA_TYPE.DT_TIMESTAMP)
			{
				return DATA_CATEGORY.TEMPORAL;
			}
			else
			{
				return DATA_CATEGORY.TEMPORAL;
			}
		}
	}

}
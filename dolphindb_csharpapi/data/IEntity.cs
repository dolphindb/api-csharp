using dolphindb.io;

namespace dolphindb.data
{
	public interface IEntity
	{
        DATA_FORM getDataForm();
        DATA_CATEGORY getDataCategory();
        DATA_TYPE getDataType();
		int rows();
		int columns();
        string getString();
		void write(ExtendedDataOutput output);

        bool isScalar();
        bool isVector();
        bool isPair();
        bool isTable();
        bool isMatrix();
        bool isDictionary();
        bool isChart();
        bool isChunk();
	}

	public enum DATA_TYPE
	{
		DT_VOID,
		DT_BOOL,
		DT_BYTE,
		DT_SHORT,
		DT_INT,
		DT_LONG,
		DT_DATE,
		DT_MONTH,
		DT_TIME,
		DT_MINUTE,
		DT_SECOND,
		DT_DATETIME,
		DT_TIMESTAMP,
		DT_NANOTIME,
		DT_NANOTIMESTAMP,
		DT_FLOAT,
		DT_DOUBLE,
		DT_SYMBOL,
		DT_STRING,
        DT_UUID,
        DT_FUNCTIONDEF,
		DT_HANDLE,
		DT_CODE,
		DT_DATASOURCE,
		DT_RESOURCE,
		DT_ANY,
		DT_DICTIONARY,
		DT_OBJECT
	}

	public enum DATA_CATEGORY
	{
		NOTHING,
		LOGICAL,
		INTEGRAL,
		FLOATING,
		TEMPORAL,
		LITERAL,
		SYSTEM,
		MIXED
	}

	public enum DATA_FORM
	{
		DF_SCALAR,
		DF_VECTOR,
		DF_PAIR,
		DF_MATRIX,
		DF_SET,
		DF_DICTIONARY,
		DF_TABLE,
		DF_CHART,
		DF_CHUNK
	}

	public enum PARTITION_TYPE
	{
		SEQ,
		VALUE,
		RANGE,
		LIST
	}

}
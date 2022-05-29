using dolphindb.io;
using System.Data;

namespace dolphindb.data
{
    public interface IEntity
    {
        DATA_FORM getDataForm();
        DATA_CATEGORY getDataCategory();
        DATA_TYPE getDataType();
        object getObject();
        int rows();
        int columns();
        string getString();
        void write(ExtendedDataOutput output);
        void writeCompressed(ExtendedDataOutput output);
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
        DT_COMPRESS,
        DT_DICTIONARY,
        DT_DATEHOUR,
        DT_DATEMINUTE,
        DT_IPADDR,
        DT_INT128,
        //2021.01.19 cwj 顺序！
        DT_BLOB,
        //
        DT_OBJECT,
        
        DT_BOOL_ARRAY = 65,
        DT_BYTE_ARRAY,
        DT_SHORT_ARRAY,
        DT_INT_ARRAY,
        DT_LONG_ARRAY,
        DT_DATE_ARRAY,
        DT_MONTH_ARRAY,
        DT_TIME_ARRAY,
        DT_MINUTE_ARRAY,
        DT_SECOND_ARRAY,
        DT_DATETIME_ARRAY,
        DT_TIMESTAMP_ARRAY,
        DT_NANOTIME_ARRAY,
        DT_NANOTIMESTAMP_ARRAY,
        DT_FLOAT_ARRAY,
        DT_DOUBLE_ARRAY,
        DT_SYMBOL_ARRAY,
        DT_STRING_ARRAY,
        DT_UUID_ARRAY,
        DT_FUNCTIONDEF_ARRAY,
        DT_HANDLE_ARRAY,
        DT_CODE_ARRAY,
        DT_DATASOURCE_ARRAY,
        DT_RESOURCE_ARRAY,
        DT_ANY_ARRAY,
        DT_COMPRESS_ARRAY,
        DT_DICTIONARY_ARRAY,
        DT_DATEHOUR_ARRAY,
        DT_DATEMINUTE_ARRAY,
        DT_IPADDR_ARRAY,
        DT_INT128_ARRAY,
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
        MIXED,
        BINARY
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
        LIST,
        COMPO,
        HASH
    }


    



}
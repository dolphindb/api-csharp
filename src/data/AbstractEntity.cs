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
    }

}
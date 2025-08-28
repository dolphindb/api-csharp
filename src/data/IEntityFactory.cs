using dolphindb.io;

namespace dolphindb.data
{
    public interface IEntityFactory
    {
        IEntity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput @in, bool extended);
        IMatrix createMatrixWithDefaultValue(DATA_TYPE type, int rows, int columns, int extra = -1);
        IVector createVectorWithDefaultValue(DATA_TYPE type, int size, int extra = -1);
        IVector createPairWithDefaultValue(DATA_TYPE type);
        IScalar createScalarWithDefaultValue(DATA_TYPE type);
    }

}
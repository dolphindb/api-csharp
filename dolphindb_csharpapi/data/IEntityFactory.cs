using dolphindb.io;

namespace dolphindb.data
{
	public interface IEntityFactory
	{
		IEntity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput @in);
		IMatrix createMatrixWithDefaultValue(DATA_TYPE type, int rows, int columns);
		IVector createVectorWithDefaultValue(DATA_TYPE type, int size);
		IVector createPairWithDefaultValue(DATA_TYPE type);
		IScalar createScalarWithDefaultValue(DATA_TYPE type);
	}

}
using dolphindb.io;

namespace dolphindb.data
{
	public interface IEntityFactory
	{
		IEntity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput @in);
		//Matrix createMatrixWithDefaultValue(DATA_TYPE type, int rows, int columns);
		//Vector createVectorWithDefaultValue(DATA_TYPE type, int size);
		//Vector createPairWithDefaultValue(DATA_TYPE type);
		IScalar createScalarWithDefaultValue(DATA_TYPE type);
	}

}
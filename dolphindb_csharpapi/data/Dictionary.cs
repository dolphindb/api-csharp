namespace dolphindb.data
{

	/*
	 * Interface for dictionary object
	 */

	public interface IDictionary : IEntity
	{
		DATA_TYPE KeyDataType {get;}
		IEntity get(IScalar key);
		bool put(IScalar key, IEntity value);
	}

}
using dolphindb.data;

namespace dolphindb.data
{
	public interface ISet : IEntity
	{
		bool contains(IScalar key);
		bool add(IScalar key);
	}

}
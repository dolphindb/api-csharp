namespace com.xxdb.streaming.client
{

	using Entity = com.xxdb.data.Entity;

	public interface IMessage
	{
		string Topic {get;}
		long Offset {get;}
		Entity getEntity(int colIndex);
		T getValue<T>(int colIndex);
	}

}
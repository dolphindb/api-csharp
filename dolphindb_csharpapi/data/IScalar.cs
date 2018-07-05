namespace dolphindb.data
{

	public interface IScalar : IEntity
	{
        bool isNull(); 
		void setNull();

        Number getNumber();
        object getTemporal();
	}

}
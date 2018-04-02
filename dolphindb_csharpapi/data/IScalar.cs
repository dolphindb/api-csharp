using System.Data;

namespace dolphindb.data
{

	public interface IScalar : IEntity
	{
        bool isNull(); 
		void setNull();
        DataTable toDataTable();
        Number getNumber();
        object getTemporal();
	}

}
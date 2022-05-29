using System.Data;

namespace dolphindb.data
{

    public interface IScalar : IEntity
    {
        bool isNull();
        void setNull();
        void setObject(object value);
        DataTable toDataTable();
        Number getNumber();
        object getTemporal();
        int hashBucket(int buckets);
    }

}
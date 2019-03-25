using System.Collections.Generic;

namespace dolphindb.data
{
    public interface ITable : IEntity
    {
        IVector getColumn(int index);
        IVector getColumn(string name);
        string getColumnName(int index);
        IList<string> getColumnNames();
    }

}
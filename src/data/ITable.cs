using System.Collections.Generic;
using dolphindb.io;

namespace dolphindb.data
{
    public interface ITable : IEntity
    {
        IVector getColumn(int index);
        IVector getColumn(string name);
        string getColumnName(int index);
        IList<string> getColumnNames();
        ITable getSubTable(int[] indices);
        void writeCompressed(ExtendedDataOutput output);
    }

}
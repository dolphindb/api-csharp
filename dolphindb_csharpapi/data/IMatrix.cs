using dolphindb.data;
using System;

namespace dolphindb.data
{
	public interface IMatrix : IEntity
	{
		bool isNull(int row, int column);
		void setNull(int row, int column);
		IScalar getRowLabel(int row);
        IScalar getColumnLabel(int column);
        IScalar get(int row, int column);
        IVector getRowLabels();
        IVector getColumnLabels();
		bool hasRowLabel();
		bool hasColumnLabel();
        Type getElementClass();
	}

}
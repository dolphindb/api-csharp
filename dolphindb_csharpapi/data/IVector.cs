using System;

namespace dolphindb.data
{

	public interface IVector : IEntity
	{

		bool isNull(int index);
        void setNull(int index);
		IScalar get(int index);
		void set(int index, IScalar value);
        Type getElementClass();
	}

	public static class Vector_Fields
	{
		public const int DISPLAY_ROWS = 10;
	}

}
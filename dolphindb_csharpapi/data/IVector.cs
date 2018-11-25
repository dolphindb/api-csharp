using System;
using System.Data;
using System.Collections.Generic;
namespace dolphindb.data
{

    public interface IVector : IEntity
    {

        bool isNull(int index);
        void setNull(int index);
        IScalar get(int index);
        void set(int index, IScalar value);
        Type getElementClass();
        DataTable toDataTable();
        object getList();
        void set(int index, string value);
    }

    public static class Vector_Fields
    {
        public const int DISPLAY_ROWS = 10;
    }

}
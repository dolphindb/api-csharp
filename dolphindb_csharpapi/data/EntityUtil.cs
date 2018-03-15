
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.data
{
    public static class EntityUtil
    {
        public static DATA_CATEGORY typeToCategory(DATA_TYPE type)
        {
            if (type == DATA_TYPE.DT_TIME || type == DATA_TYPE.DT_SECOND || type == DATA_TYPE.DT_MINUTE || type == DATA_TYPE.DT_DATE
                    || type == DATA_TYPE.DT_DATETIME || type == DATA_TYPE.DT_MONTH || type == DATA_TYPE.DT_TIMESTAMP || type == DATA_TYPE.DT_NANOTIME || type == DATA_TYPE.DT_NANOTIMESTAMP)
                return DATA_CATEGORY.TEMPORAL;
            else if (type == DATA_TYPE.DT_INT || type == DATA_TYPE.DT_LONG || type == DATA_TYPE.DT_SHORT || type == DATA_TYPE.DT_BYTE)
                return DATA_CATEGORY.INTEGRAL;
            else if (type == DATA_TYPE.DT_BOOL)
                return DATA_CATEGORY.LOGICAL;
            else if (type == DATA_TYPE.DT_DOUBLE || type == DATA_TYPE.DT_FLOAT)
                return DATA_CATEGORY.FLOATING;
            else if (type == DATA_TYPE.DT_STRING || type == DATA_TYPE.DT_SYMBOL)
                return DATA_CATEGORY.LITERAL;
            else if (type == DATA_TYPE.DT_ANY)
                return DATA_CATEGORY.MIXED;
            else if (type == DATA_TYPE.DT_VOID)
                return DATA_CATEGORY.NOTHING;
            else
                return DATA_CATEGORY.SYSTEM;
        }
    }
}

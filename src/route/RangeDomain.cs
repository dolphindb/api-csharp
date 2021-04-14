using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;
using System.Collections.Generic;

namespace dolphindb.route
{
    public class RangeDomain:Domain
    {
        private DATA_TYPE type;
        private DATA_CATEGORY cat;
        private IVector range;

        public RangeDomain(IVector range, DATA_TYPE type, DATA_CATEGORY cat)
        {
            this.type = type;
            this.cat = cat;
            this.range = range;
        }

        public List<int> getPartitionKeys(IVector partitionCol)
        {
            if (partitionCol.getDataCategory() != cat)
                throw new Exception("Data category incompatible.");
            if (cat == DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
            {
                DATA_TYPE old = partitionCol.getDataType();
                partitionCol = (IVector)Utils.castDateTime(partitionCol, type);
                if (partitionCol == null)
                    throw new Exception("Can't convert type from " + old.ToString() + " to " + type.ToString());
            }
            int partitions = range.rows() - 1;
            int rows = partitionCol.rows();
            List<int> keys = new List<int>(rows);
            for (int i = 0; i < rows; ++i)
            {
                int index = range.asof(partitionCol.get(i));
                if (index >= partitions)
                    keys.Add(-1);
                else
                    keys.Add(index);
            }
            return keys;
        }


    }
}

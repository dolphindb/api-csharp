using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;

namespace dolphindb.route
{
    public class ValueDomain:Domain
    {
        private DATA_TYPE type;
        private DATA_CATEGORY cat;

        public ValueDomain(IVector partitionScheme, DATA_TYPE type, DATA_CATEGORY cat)
        {
            this.type = type;
            this.cat = cat;
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
            if (type == DATA_TYPE.DT_LONG)
                throw new Exception("Long type value can't be used as a partition column.");

            int rows = partitionCol.rows();
            List<int> keys = new List<int>(rows);
            for (int i = 0; i < rows; ++i)
                keys.Add(partitionCol.hashBucket(i, 1048576));
            return keys;
        }

        public int getPartitionKey(IScalar partitionCol)
        {
            if (partitionCol.getDataCategory() != cat)
                throw new Exception("Data category incompatible.");
            if (cat == DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
            {
                DATA_TYPE old = partitionCol.getDataType();
                partitionCol = (IScalar)Utils.castDateTime(partitionCol, type);
                if (partitionCol == null)
                    throw new Exception("Can't convert type from " + old.ToString() + " to " + type.ToString());
            }
            if (type == DATA_TYPE.DT_LONG)
                throw new Exception("Long type value can't be used as a partition column.");
            int key = partitionCol.hashBucket(1048576);
            return key;
        }

    }
}

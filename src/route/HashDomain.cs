using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;

namespace dolphindb.route
{
    public class HashDomain: Domain
    {
        private int buckets;
        private DATA_TYPE type;
        private DATA_CATEGORY cat;

        public HashDomain(int buckets, DATA_TYPE type, DATA_CATEGORY cat)
        {
            this.buckets = buckets;
            this.type = type;
            this.cat = cat;
        }
        public List<int> getPartitionKeys(IVector partitionCol)
        {
            if(partitionCol.getDataCategory() != cat)
                throw new Exception("Data category incompatible.");
            if(cat == DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
            {
                //类型转化
                DATA_TYPE old = partitionCol.getDataType();
                partitionCol = (IVector)Utils.castDateTime(partitionCol, type);
                if (partitionCol == null)
                    throw new Exception("Can't convert type from " + old.ToString() + " to " + type.ToString());
            }
            int rows = partitionCol.rows();
            List<int> keys = new List<int>(rows);
            for (int i = 0; i < rows; ++i)
                keys.Add(partitionCol.hashBucket(i, buckets));
            return keys;
        }
    }
}

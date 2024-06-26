﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb.data;


namespace dolphindb.route
{
    public class ListDomain:Domain
    {
        private DATA_TYPE type;
        private DATA_CATEGORY cat;
        private Dictionary<IScalar, int> dict;

        public ListDomain(IVector list, DATA_TYPE type, DATA_CATEGORY cat)
        {
            this.type = type;
            this.cat = cat;
            if(list.getDataType() != DATA_TYPE.DT_ANY)
            {
                throw new Exception("The input list must be a tuple.");
            }
            dict = new Dictionary<IScalar, int>();

            BasicAnyVector values = (BasicAnyVector)list;
            int partitions = values.rows();
            for (int i = 0; i < partitions; ++i)
            {
                IEntity cur = values.getEntity(i);
                if (cur.isScalar())
                    dict.Add((IScalar)cur, i);
                else
                {
                    IVector vec = (IVector)cur;
                    for (int j = 0; j < vec.rows(); ++j)
                        dict.Add(vec.get(j), i);
                }
            }

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
            int index = 0;
            if (dict.ContainsKey(partitionCol))
                {
                    index = (int)dict[partitionCol];
                }
                else
                {
                    index = -1;
                }
            return index;
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

            int rows = partitionCol.rows();
            List<int> keys = new List<int>(rows);
            for (int i = 0; i < rows; ++i)
            {
                if (dict.ContainsKey(partitionCol.get(i)))
                {
                    keys.Add((int)dict[partitionCol.get(i)]);
                }
                else
                {
                    keys.Add(-1);
                }
            }
            return keys;
        }

    }
}

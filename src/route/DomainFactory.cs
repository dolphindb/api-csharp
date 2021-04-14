using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dolphindb;
using dolphindb.data;

namespace dolphindb.route
{
    public class DomainFactory
    {
        public static Domain createDomain(PARTITION_TYPE type, DATA_TYPE partitionColType, IEntity partitionSchema)
        {
            if (type == PARTITION_TYPE.HASH)
            {
                DATA_TYPE dataType = partitionColType;
                DATA_CATEGORY dataCat = Utils.getCategory(dataType);
                int buckets = ((BasicInt)partitionSchema).getValue();
                return new HashDomain(buckets, dataType, dataCat);
            }
            else if (type == PARTITION_TYPE.VALUE)
            {
                DATA_TYPE dataType = partitionSchema.getDataType();
                DATA_CATEGORY dataCat = Utils.getCategory(dataType);
                return new ValueDomain((IVector)partitionSchema, dataType, dataCat);
            }
            else if (type == PARTITION_TYPE.RANGE)
            {
                DATA_TYPE dataType = partitionSchema.getDataType();
                DATA_CATEGORY dataCat = Utils.getCategory(dataType);
                return new RangeDomain((IVector)partitionSchema, dataType, dataCat);
            }
            else if (type == PARTITION_TYPE.LIST)
            {
                DATA_TYPE dataType = partitionSchema.getDataType();
                DATA_CATEGORY dataCat = Utils.getCategory(dataType);
                return new ListDomain((IVector)partitionSchema, dataType, dataCat);
            }
            throw new Exception("Unsupported partition type " + type.ToString());
        }
    }
}

using System;

namespace com.xxdb.route
{

	using AbstractVector = com.xxdb.data.AbstractVector;
	using BasicAnyVector = com.xxdb.data.BasicAnyVector;
	using Entity = com.xxdb.data.Entity;

	public class TableRouterFacotry
	{
		public static TableRouter createRouter(com.xxdb.data.Entity_PARTITION_TYPE type, AbstractVector values, BasicAnyVector locations)
		{
			if (type == com.xxdb.data.Entity_PARTITION_TYPE.RANGE)
			{
				if (values.DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.INTEGRAL)
				{
					return new IntegralRangePartitionedTableRouter(values, locations);
				}
				else if (values.DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.LITERAL)
				{
					return new LiteralRangePartitionedTableRouter(values, locations);
				}
			}
			else if (type == com.xxdb.data.Entity_PARTITION_TYPE.VALUE)
			{
				if (values.DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.INTEGRAL)
				{
					return new IntegralValuePartitionedTableRouter(values, locations);
				}
				else if (values.DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.LITERAL)
				{
					return new LiteralValuePartitionedTableRouter(values, locations);
				}
			}
			else if (type == com.xxdb.data.Entity_PARTITION_TYPE.LIST)
			{
				BasicAnyVector schema = (BasicAnyVector)values;
				if (schema.getEntity(0).DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.INTEGRAL)
				{
					return new IntegralListPartitionedTableRouter(values, locations);
				}
				else if (schema.getEntity(0).DataCategory == com.xxdb.data.Entity_DATA_CATEGORY.LITERAL)
				{
					return new LiteralListPartitionedTableRouter(values, locations);
				}
			}
			throw new Exception("Unsupported partition type " + type.ToString());
		}
	}

}
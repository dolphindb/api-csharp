using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;

	internal class IntegralValuePartitionedTableRouter : TableRouter
	{
		private IDictionary<long?, IList<string>> map;
		internal IntegralValuePartitionedTableRouter(AbstractVector values, BasicAnyVector locations)
		{
			if (values.DataCategory != Entity_DATA_CATEGORY.INTEGRAL)
			{
				throw new Exception("expect a vector of integrals");
			}
			IList<long?> longs = new List<long?>();
			for (int i = 0; i < values.rows(); ++i)
			{
				try
				{
					longs.Add(Convert.ToInt64(values.get(i).Number.longValue()));
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
					Console.Write(e.StackTrace);
					throw new Exception(e);
				}
			}
			initialize(longs, locations);
		}

		private void initialize(IList<long?> longs, BasicAnyVector locations)
		{
			if (locations.rows() <= 0)
			{
				throw new Exception("requires at least one location");
			}
			if (locations.getEntity(0).DataType != Entity_DATA_TYPE.DT_STRING)
			{
				throw new Exception("location must be a string");
			}
			if (longs.Count != locations.rows())
			{
				throw new Exception("expect # locations == # values");
			}
			map = new Dictionary<>();
			bool isScalar = locations.getEntity(0).DataForm == Entity_DATA_FORM.DF_SCALAR;
			if (isScalar)
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicString location = (BasicString) locations.get(i);
					long val = longs[i].Value;
					map[val] = location.String;
				}
			}
			else
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
					long val = longs[i].Value;
					map[val] = new List<>();
					for (int j = 0; j < locationVector.rows(); ++j)
					{
						map[val].Add(locationVector.getString(j));
					}
				}
			}
		}

		public virtual string route(Scalar partitionColumn)
		{
			if (partitionColumn.DataCategory != Entity_DATA_CATEGORY.INTEGRAL)
			{
				throw new Exception("invalid column category type" + partitionColumn.DataCategory.name() + ", expect Integral category.");
			}
			try
			{
				long longVal = partitionColumn.Number.longValue();
				IList<string> locations = map[longVal];
				if (locations == null)
				{
					throw new Exception(partitionColumn.Number.longValue() + " does not match any partitioning values!");
				}
				return locations[0];
			}
			catch (Exception e)
			{
				throw new Exception(e);
			}
		}
	}

}
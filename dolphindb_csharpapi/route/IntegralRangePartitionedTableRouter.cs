using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;

	internal class IntegralRangePartitionedTableRouter : AbstractRangePartitionedTableRouter
	{
		private long?[] ranges;
		private IList<IList<string>> locations;
		internal IntegralRangePartitionedTableRouter(AbstractVector values, BasicAnyVector locations)
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

		private static bool isSorted<T>(IList<T> theList) where T : IComparable
		{
			T previous = null;
			foreach (T t in theList)
			{
				if (previous != null && t.compareTo(previous) < 0)
				{
					return false;
				}
				previous = t;
			}
			return true;
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
			if (isSorted(longs) == false)
			{
				throw new Exception("ranges " + longs.ToString() + " not sorted!");
			}
			if (longs.Count != locations.rows() + 1)
			{
				throw new Exception("expect # locations == # range values - 1");
			}
			ranges = longs.ToArray();
			this.locations = new List<>();
			bool isScalar = locations.getEntity(0).DataForm == Entity_DATA_FORM.DF_SCALAR;
			if (isScalar)
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicString location = (BasicString)locations.get(i);
					this.locations.Add(location.String);
				}
			}
			else
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
					this.locations.Add(new List<>());
					for (int j = 0; j < locationVector.rows(); ++j)
					{
						this.locations[this.locations.Count - 1].Add(((BasicString)locationVector.get(j)).String);
					}
				}
			}
		}

		public override string route(Scalar partitionColumn)
		{
			if (partitionColumn.DataCategory != Entity_DATA_CATEGORY.INTEGRAL)
			{
				throw new Exception("invalid column category type" + partitionColumn.DataCategory.name() + ", expect Integral category.");
			}
			try
			{
				long longVal = partitionColumn.Number.longValue();
				int pos = lowerBound(this.ranges, longVal);
				if (0 <= pos && pos < ranges.Length)
				{
					if (ranges[pos] == longVal)
					{
						return this.locations[pos][0];
					}
					else
					{
						return this.locations[pos - 1][0];
					}
				}
				throw new Exception(partitionColumn.Number.longValue() + " not in partitioning range!");
			}
			catch (Exception e)
			{
				throw new Exception(e);
			}
		}
	}

}
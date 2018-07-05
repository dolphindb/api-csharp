using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;


	internal class LiteralRangePartitionedTableRouter : AbstractRangePartitionedTableRouter
	{
		private string[] ranges;
		private IList<IList<string>> locations;
		internal LiteralRangePartitionedTableRouter(AbstractVector values, BasicAnyVector locations)
		{
			if (values.DataCategory != Entity_DATA_CATEGORY.LITERAL)
			{
				throw new Exception("expect a vector of literals");
			}
			IList<string> strings = new List<string>();
			for (int i = 0; i < values.rows(); ++i)
			{
				try
				{
					strings.Add(values.get(i).String);
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
					Console.Write(e.StackTrace);
					throw new Exception(e);
				}
			}
			initialize(strings, locations);
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
		private void initialize(IList<string> strings, BasicAnyVector locations)
		{
			if (locations.rows() <= 0)
			{
				throw new Exception("requires at least one location");
			}
			if (locations.getEntity(0).DataType != Entity_DATA_TYPE.DT_STRING)
			{
				throw new Exception("location must be a string");
			}
			if (isSorted(strings) == false)
			{
				throw new Exception("ranges " + strings.ToString() + " not sorted!");
			}
			if (strings.Count != locations.rows() + 1)
			{
				throw new Exception("expect # locations == # range values - 1");
			}
			ranges = strings.ToArray();
			this.locations = new List<>();
			bool isScalar = locations.getEntity(0).DataForm == Entity_DATA_FORM.DF_SCALAR;
			if (isScalar)
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicString location = (BasicString)locations.get(i);
					this.locations.Add(Arrays.asList(location.String));
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
			if (partitionColumn.DataCategory != Entity_DATA_CATEGORY.LITERAL)
			{
				throw new Exception("invalid column category " + partitionColumn.DataCategory.name() + ", expect Literal category.");
			}
			try
			{
				string stringVal = partitionColumn.String;
				int pos = lowerBound(this.ranges, stringVal);
				if (0 <= pos && pos < ranges.Length)
				{
					if (ranges[pos].Equals(stringVal))
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
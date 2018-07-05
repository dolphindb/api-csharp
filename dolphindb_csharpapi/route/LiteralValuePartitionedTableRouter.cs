using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;

	internal class LiteralValuePartitionedTableRouter : TableRouter
	{
		private IDictionary<string, IList<string>> map;
		internal LiteralValuePartitionedTableRouter(AbstractVector values, BasicAnyVector locations)
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
			if (strings.Count != locations.rows())
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
					string val = strings[i];
					map[val] = location.String;
				}
			}
			else
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
					string val = strings[i];
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
			if (partitionColumn.DataCategory != Entity_DATA_CATEGORY.LITERAL)
			{
				throw new Exception("invalid column category type" + partitionColumn.DataCategory.name() + ", expect Literal category.");
			}
			try
			{
				string stringVal = partitionColumn.String;
				IList<string> locations = map[stringVal];
				if (locations == null)
				{
					throw new Exception(partitionColumn.String + " does not match any partitioning values!");
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
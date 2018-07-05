using System;
using System.Collections.Generic;

namespace com.xxdb.route
{

	using com.xxdb.data;

	internal class LiteralListPartitionedTableRouter : TableRouter
	{
		private IDictionary<string, IList<string>> locationMap;
		internal LiteralListPartitionedTableRouter(AbstractVector values, BasicAnyVector locations)
		{
			initialize(values, locations);
		}

		private void initialize(AbstractVector values, BasicAnyVector locations)
		{
			if (values.DataType != Entity_DATA_TYPE.DT_ANY || values.DataForm != Entity_DATA_FORM.DF_VECTOR)
			{
				throw new Exception("expect a vector of partitioning lists");
			}
			if (values.rows() <= 0)
			{
				throw new Exception("requires at least one partitioning list");
			}
			if (locations.rows() <= 0)
			{
				throw new Exception("requires at least one location");
			}
			if (locations.getEntity(0).DataType != Entity_DATA_TYPE.DT_STRING)
			{
				throw new Exception("location must be a string");
			}
			if (values.rows() != locations.rows())
			{
				throw new Exception("expect # locations == # partitioning lists");
			}
			this.locationMap = new Dictionary<>();
			IList<string>[] locationListArray = new IList[locations.rows()];
			bool isScalar = locations.getEntity(0).DataForm == Entity_DATA_FORM.DF_SCALAR;
			if (isScalar)
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicString location = (BasicString)locations.get(i);
					locationListArray[i] = location.String;
				}
			}
			else
			{
				for (int i = 0; i < locations.rows(); ++i)
				{
					BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
					locationListArray[i] = new List<>();
					for (int j = 0; j < locationVector.rows(); ++j)
					{
						BasicString location = (BasicString)locationVector.get(j);
						locationListArray[i].Add(location.String);
					}
				}
			}

			for (int i = 0; i < values.rows(); ++i)
			{
				AbstractVector partitioningList = (AbstractVector)((BasicAnyVector) values).getEntity(i);
				if (partitioningList.rows() <= 0)
				{
					throw new Exception("expect partitioning list to be nonempty");
				}
				if (partitioningList.DataCategory != Entity_DATA_CATEGORY.LITERAL)
				{
					throw new Exception("expect partitioning column values to be LITERAL.");
				}
				for (int j = 0; j < partitioningList.rows(); ++j)
				{
					try
					{
						string val = partitioningList.get(j).String;
						locationMap[val] = locationListArray[i];
					}
					catch (Exception e)
					{
						throw new Exception(e);
					}
				}
			}
		}

		public virtual string route(Scalar partitionColumn)
		{
			if (partitionColumn.DataCategory != Entity_DATA_CATEGORY.LITERAL)
			{
				throw new Exception("invalid column category type" + partitionColumn.DataCategory.name() + ", expect LITERAL category.");
			}
			try
			{
				string stringVal = partitionColumn.String;
				IList<string> locations = locationMap[stringVal];
				if (locations == null)
				{
					throw new Exception(partitionColumn.Number.longValue() + " not in any partitioning list!");
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
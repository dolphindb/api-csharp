namespace com.xxdb.route
{

	internal abstract class AbstractRangePartitionedTableRouter : TableRouter
	{
		public abstract string route(com.xxdb.data.Scalar partitionColumn);
		/// <summary>
		/// java equivalent of c++ lower_bound. </summary>
		/// <param name="A"> list to be searched on </param>
		/// <param name="x"> element for which is being searched </param>
		/// @param <T> types that implements Comparable<T> interface </param>
		/// <returns> index of the first element that is no less than x, or -1 if:
		///           1) list is empty
		///           2) list has only one element
		///           3) x < list.get(0) or x >= list[list.size() - 1]
		///  </returns>
		protected internal virtual int lowerBound<T>(T[] A, T x) where T : IComparable<T>
		{
			if (A.Length == 0 || A.Length == 1 || x.compareTo(A[0]) < 0 || x.compareTo(A[A.Length - 1]) >= 0)
			{
				return -1;
			}
			int l = 0, h = A.Length;
			while (l < h)
			{
				int m = l + ((h - l) >> 1);
				if (A[m].compareTo(x) < 0)
				{ // A[m] < x
					l = m + 1;
				}
				else
				{ // A[m] >= x
					h = m;
				}
			}
			return l;
		}
	}

}
namespace com.xxdb.route
{

	using Scalar = com.xxdb.data.Scalar;

	internal interface TableRouter
	{
		// given a partition column value, return "host:port:alias"
		string route(Scalar partitionColumn);
	}

}
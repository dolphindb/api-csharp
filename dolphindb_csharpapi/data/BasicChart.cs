using dolphindb.io;
using System;

namespace dolphindb.data
{


	public class BasicChart : BasicDictionary, Chart
	{
		private static BasicString KEY_CHARTTYPE = new BasicString("chartType");
		private static BasicString KEY_DATA = new BasicString("data");
		private static BasicString KEY_TITLE = new BasicString("title");


		public BasicChart(ExtendedDataInput @in) : base(DATA_TYPE.DT_ANY, @in)
		{
		}

		public BasicChart(int capacity) : base(DATA_TYPE.DT_STRING, DATA_TYPE.DT_ANY, capacity)
		{
		}

		public BasicChart() : this(0)
		{
		}

		public override DATA_FORM DataForm
		{
			get
			{
				return DATA_FORM.DF_CHART;
			}
		}

//JAVA TO C# CONVERTER WARNING: Method 'throws' clauses are not available in .NET:
//ORIGINAL LINE: @Override public Chart_CHART_TYPE getChartType() throws Exception
		public virtual Chart_CHART_TYPE ChartType
		{
			get
			{
				Entity chartType = get(KEY_CHARTTYPE);
				if (chartType == null || !chartType.Scalar)
				{
					throw new Exception("Invalid chart object. Chart type is not defined.");
				}
				return Enum.GetValues(typeof(Chart_CHART_TYPE))[((Scalar)chartType).Number.intValue()];
			}
		}

		public virtual Matrix Data
		{
			get
			{
				Entity data = get(KEY_DATA);
				if (data == null || !data.Matrix)
				{
					throw new Exception("Invalid chart object. Chart data is not set.");
				}
				return (Matrix)data;
			}
		}

		public virtual string Title
		{
			get
			{
				Entity title = get(KEY_TITLE);
				if (title == null || (!title.Scalar && !title.Vector))
				{
					return "";
				}
				else if (title.Scalar)
				{
					return title.String;
				}
				else
				{
					return ((Vector)title).get(0).String;
				}
			}
		}

		public virtual string XAxisName
		{
			get
			{
				Entity title = get(KEY_TITLE);
				if (title == null || !title.Vector || title.rows() < 2)
				{
					return "";
				}
				else
				{
					return ((Vector)title).get(1).String;
				}
			}
		}

		public virtual string YAxisName
		{
			get
			{
				Entity title = get(KEY_TITLE);
				if (title == null || !title.Vector || title.rows() < 3)
				{
					return "";
				}
				else
				{
					return ((Vector)title).get(2).String;
				}
			}
		}


	}

}
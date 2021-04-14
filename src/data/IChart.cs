namespace dolphindb.data
{


	public interface IChart : Dictionary
	{

		Chart_CHART_TYPE ChartType {get;}
		Matrix Data {get;}
		string Title {get;}
		string XAxisName {get;}
		string YAxisName {get;}

	}

	public enum Chart_CHART_TYPE
	{
		CT_AREA,
		CT_BAR,
		CT_COLUMN,
		CT_HISTOGRAM,
		CT_LINE,
		CT_PIE,
		CT_SCATTER,
		CT_TREND
	}

}
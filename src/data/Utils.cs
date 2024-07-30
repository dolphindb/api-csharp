using System;
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Numerics;

namespace dolphindb.data
{


    public class Utils
    {
        private static readonly int DEFALUT_YEAR = 1970;
        private static readonly int DEFAULT_MONTH = 1;
        private static readonly int DEFAULT_DAY = 1;
        //private static readonly int DEFAULT_HOUR = 0;
        //private static readonly int DEFAULT_MINUTE = 0;
        private static readonly int DEFAULT_SECOND = 0;
        //private static readonly int DEFAULT_MILLIONSECOND = 0;


        public const int DISPLAY_ROWS = 20;
        public const int DISPLAY_COLS = 100;
        public const int DISPLAY_WIDTH = 100;

        private static readonly int[] cumMonthDays = new int[] { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };
        private static readonly int[] cumLeapMonthDays = new int[] { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 };
        private static readonly int[] monthDays = new int[] { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        private static readonly int[] leapMonthDays = new int[] { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        private static string API_VERSION = "3.00.1.1";

        public static string getAPIVersion()
        {
            return API_VERSION;
        }
        
        public static int countMonths(DateTime date)
        {
            return date.Year * 12 + date.Month - 1;
        }

        public static int countMonths(int year, int month)
        {
            return year * 12 + month-1;
        }

        public static DateTime parseMonth(int value)
        {
            try
            {
                int year = divide(value, 12);
                int month = value % 12+1;
                return new DateTime(year, month, 1);
            }
            catch (Exception)
            {
                System.Console.Out.WriteLine("Failed to parseMonth " + value + " to DateTime. ");
                throw;
            }
        }

        public static int countDays(DateTime date)
        {
            return countDays(date.Year, date.Month, date.Day);
        }

        public static int countDays(int year, int month, int day)
        {
            if (month < 1 || month > 12 || day < 0)
                return int.MinValue;
            int divide400Years = year / 400;
            int offset400Years = year % 400;
            int days = divide400Years * 146097 + offset400Years * 365 - 719529;
            if (offset400Years > 0) days += (offset400Years - 1) / 4 + 1 - (offset400Years - 1) / 100;
            if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
            {
                days += cumLeapMonthDays[month - 1];
                return day <= leapMonthDays[month - 1] ? days + day : int.MinValue;
            }
            else
            {
                days += cumMonthDays[month - 1];
                return day <= monthDays[month - 1] ? days + day : int.MinValue;
            }
        }

        public static DateTime parseDate(int days)
        {
            int year, month, day;
            days += 719529;
            int circleIn400Years = days / 146097;
            int offsetIn400Years = days % 146097;
            int resultYear = circleIn400Years * 400;
            int similarYears = offsetIn400Years / 365;
            int tmpDays = similarYears * 365;
            if (similarYears > 0) tmpDays += (similarYears - 1) / 4 + 1 - (similarYears - 1) / 100;
            if (tmpDays >= offsetIn400Years) --similarYears;
            year = similarYears + resultYear;
            days -= circleIn400Years * 146097 + tmpDays;
            bool leap = ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0);
            if (days <= 0)
            {
                days += leap ? 366 : 365;
            }
            if (leap)
            {
                month = days / 32 + 1;
                if (days > cumLeapMonthDays[month])
                    month++;
                day = days - cumLeapMonthDays[month - 1];
            }
            else
            {
                month = days / 32 + 1;
                if (days > cumMonthDays[month])
                    month++;
                day = days - cumMonthDays[month - 1];
            }

            return new DateTime(year, month, day);
        }

        public static int countSeconds(DateTime dt)
        {
            return countSeconds(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
        }

        public static int countSeconds(TimeSpan ts)
        {
            return countSeconds(ts.Hours, ts.Minutes, ts.Seconds);
        }

        public static int countSeconds(int year, int month, int day, int hour, int minute, int second)
        {
            int days = countDays(year, month, day);
            return days * 86400 + (hour * 60 + minute) * 60 + second;
        }

        public static DateTime parseDateTime(int seconds)
        {
            int days = divide(seconds, 86400);
            DateTime date = Utils.parseDate(days);
            seconds = seconds % 86400;
            if (seconds < 0)
            {
                seconds += 86400;
            }
            int hour = seconds / 3600;
            seconds = seconds % 3600;
            int minute = seconds / 60;
            int second = seconds % 60;
            return new DateTime(date.Year, date.Month, date.Day, hour, minute, second);
        }

        public static int countHours(DateTime dt)
        {
            return countHours(dt.Year, dt.Month, dt.Day, dt.Hour);
        }

        public static int countHours(int year, int month, int day, int hour)
        {
            int days = countDays(year, month, day);
            return days * 24 + hour;
        }

        public static DateTime parseDateHour(int hours)
        {
            return new DateTime(hours * 3600 * 10000000L + 621355968000000000L);
        }

        public static long countMilliseconds(DateTime dt)
        {
            int seconds = countSeconds(dt);
            long re = (long)seconds * 1000 + dt.Millisecond;
            return re;
        }

        public static int countMilliseconds(TimeSpan ts)
        {
            int seconds = countSeconds(ts);
            int re = (int)seconds * 1000 + ts.Milliseconds;
            return re;
        }

        public static long countMilliseconds(int year, int month, int day, int hour, int minute, int second, int millisecond)
        {
            return countSeconds(year, month, day, hour, minute, second) * 1000 + millisecond;
        }

        public static long countNanoseconds(DateTime dt)
        {
            return (dt.Ticks - 621355968000000000) * 100;
        }

        public static long countNanoseconds(TimeSpan ts)
        {
            return ts.Ticks * 100;
        }

        /// <summary>
        /// 1 <==> 1970.01.01 00:00:00.001
        /// 0 <==> 1970.01.01 00:00:00.000
        /// -1 <==> 1969.12.31 23:59:59.999
        /// ...
        /// </summary>
        public static DateTime parseTimestamp(long milliseconds)
        {
            int days = (int)divide(milliseconds, 86400000L);
            DateTime date = Utils.parseDate(days);

            milliseconds = milliseconds % 86400000L;
            if (milliseconds < 0)
            {
                milliseconds += 86400000;
            }
            int millisecond = (int)(milliseconds % 1000);
            int seconds = (int)(milliseconds / 1000);
            int hour = seconds / 3600;
            seconds = seconds % 3600;
            int minute = seconds / 60;
            int second = seconds % 60;
            return new DateTime(date.Year, date.Month, date.Day, hour, minute, second, millisecond);
        }

        public const int HOURS_PER_DAY = 24;
        public const int MINUTES_PER_HOUR = 60;
        public const int SECONDS_PER_MINUTE = 60;
        public const long NANOS_PER_SECOND = 1000000000L;
        public static readonly long NANOS_PER_MINUTE = NANOS_PER_SECOND * SECONDS_PER_MINUTE;
        public static readonly long NANOS_PER_HOUR = NANOS_PER_MINUTE * MINUTES_PER_HOUR;
        public static readonly long NANOS_PER_DAY = NANOS_PER_HOUR * HOURS_PER_DAY;
        public static readonly long MILLS_PER_DAY = NANOS_PER_DAY / 1000000;
        /// <summary>
        /// 1 <==> 1970.01.01 00:00:00.000000001
        /// 0 <==> 1970.01.01 00:00:00.000000000
        /// -1 <==> 1969.12.31 23:59:59.999999999
        /// ...
        /// </summary>
        public static DateTime parseNanoTimestamp(long nanoseconds)
        {
            return new DateTime(new DateTime(DEFALUT_YEAR, DEFAULT_MONTH, DEFAULT_DAY).Ticks + (long)(nanoseconds / 100));
        }

        public static int countMilliseconds(int hour, int minute, int second, int millisecond)
        {
            return ((hour * 60 + minute) * 60 + second) * 1000 + millisecond;
        }

        public static TimeSpan parseTime(int milliseconds)
        {
            return new TimeSpan(0, milliseconds / 3600000, milliseconds / 60000 % 60, milliseconds / 1000 % 60, milliseconds % 1000);
        }

        public static TimeSpan parseNanoTime(long nanoOfDay)
        {
            DateTime dt = new DateTime(new DateTime(DEFALUT_YEAR, DEFAULT_MONTH, DEFAULT_DAY).Ticks + (long)(nanoOfDay / 100));
            return dt.TimeOfDay;
        }

        public static int countSeconds(int hour, int minute, int second)
        {
            return (hour * 60 + minute) * 60 + second;
        }

        public static TimeSpan parseSecond(int seconds)
        {
            return new TimeSpan(seconds / 3600, seconds % 3600 / 60, seconds % 60);
        }

        public static int countMinutes(TimeSpan time)
        {
            return countMinutes(time.Hours, time.Minutes);
        }

        public static int countMinutes(int hour, int minute)
        {
            return hour * 60 + minute;
        }

        public static TimeSpan parseMinute(int minutes)
        {
            return new TimeSpan(minutes / 60, minutes % 60, DEFAULT_SECOND);
        }



        public static Type getSystemType(DATA_TYPE dtype)
        {
            Type colType = null;
            switch (dtype)
            {
                case DATA_TYPE.DT_BOOL:
                    colType = Type.GetType("System.Boolean");
                    break;
                case DATA_TYPE.DT_BYTE:
                    colType = Type.GetType("System.Byte");
                    break;
                case DATA_TYPE.DT_SHORT:
                    colType = Type.GetType("System.Int16");
                    break;
                case DATA_TYPE.DT_INT:
                    colType = Type.GetType("System.Int32");
                    break;
                case DATA_TYPE.DT_LONG:
                    colType = Type.GetType("System.Int64");
                    break;
                case DATA_TYPE.DT_DATE:
                case DATA_TYPE.DT_MONTH:

                case DATA_TYPE.DT_DATETIME:
                case DATA_TYPE.DT_TIMESTAMP:
                case DATA_TYPE.DT_NANOTIMESTAMP:
                case DATA_TYPE.DT_DATEHOUR:
                    colType = Type.GetType("System.DateTime");
                    break;
                case DATA_TYPE.DT_TIME:
                case DATA_TYPE.DT_MINUTE:
                case DATA_TYPE.DT_SECOND:
                case DATA_TYPE.DT_NANOTIME:
                    colType = Type.GetType("System.TimeSpan");
                    break;
                case DATA_TYPE.DT_FLOAT:
                    colType = Type.GetType("System.Double");
                    break;
                case DATA_TYPE.DT_DOUBLE:
                    colType = Type.GetType("System.Double");
                    break;
                case DATA_TYPE.DT_SYMBOL:
                case DATA_TYPE.DT_STRING:
                case DATA_TYPE.DT_FUNCTIONDEF:
                case DATA_TYPE.DT_HANDLE:
                case DATA_TYPE.DT_CODE:
                case DATA_TYPE.DT_DATASOURCE:
                case DATA_TYPE.DT_RESOURCE:
                case DATA_TYPE.DT_ANY:
                case DATA_TYPE.DT_DICTIONARY:
                case DATA_TYPE.DT_OBJECT:
                default:
                    colType = Type.GetType("System.String");
                    break;
            }
            return colType;
        }

        public static DATA_TYPE getDolphinDBType(Type stype)
        {

            if (stype == Type.GetType("System.Boolean"))
            {
                return DATA_TYPE.DT_BOOL;
            }
            else if (stype == Type.GetType("System.Byte"))
            {
                return DATA_TYPE.DT_BYTE;
            }
            else if (stype == Type.GetType("System.Double"))
            {
                return DATA_TYPE.DT_DOUBLE;
            }
            else if (stype == Type.GetType("System.DateTime"))
            {
                return DATA_TYPE.DT_DATETIME;
            }
            else if (stype == Type.GetType("System.TimeSpan"))
            {
                return DATA_TYPE.DT_SECOND;
            }
            else if (stype == Type.GetType("System.Int16"))
            {
                return DATA_TYPE.DT_SHORT;
            }
            else if (stype == Type.GetType("System.Int32"))
            {
                return DATA_TYPE.DT_INT;
            }
            else if (stype == Type.GetType("System.Int64"))
            {
                return DATA_TYPE.DT_LONG;
            }
            else
            {
                return DATA_TYPE.DT_STRING;
            }

        }


        public static BasicTable fillSchema(DataTable dt, Dictionary<string, DATA_TYPE> schema, Dictionary<string, int> extras = null)
        {
            BasicTable bt = new BasicTable(dt);
            return fillSchema(bt, schema, extras);
        }

        public static BasicTable fillSchema(BasicTable dt, Dictionary<string, DATA_TYPE> schema, Dictionary<string, int> extras = null)
        {
            Dictionary<string, int> colIndex = new Dictionary<string, int>();
            int cols = dt.columns();
            BasicTable bt = dt;
            foreach (KeyValuePair<string, DATA_TYPE> kvp in schema)
            {
                if (kvp.Value == DATA_TYPE.DT_DATE)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicDateVector col_new = new BasicDateVector(col.rows());

                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicDateTime ti = (BasicDateTime)col.get(i);
                            col_new.setDate(i, ti.getValue());

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        throw new Exception("illeagl convertion from TimeSpan to DT_DATE");
                    }




                }
                else if (kvp.Value == DATA_TYPE.DT_MONTH)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicMonthVector col_new = new BasicMonthVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicDateTime ti = (BasicDateTime)col.get(i);
                            col_new.setMonth(i, ti.getValue());
                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        throw new Exception("illeagl convertion from TimeSpan to DT_MONTH");
                    }

                }

                else if (kvp.Value == DATA_TYPE.DT_TIME)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicTimeVector col_new = new BasicTimeVector(col.rows());

                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setInt(i, (ti.getValue() % 86400) * 1000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        ;
                    }

                }
                else if (kvp.Value == DATA_TYPE.DT_MINUTE)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicMinuteVector col_new = new BasicMinuteVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setInt(i, (ti.getValue() / 60) % 1440);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setInt(i, (ti.getValue() / 1000) / 60);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                }
                else if (kvp.Value == DATA_TYPE.DT_SECOND)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicSecondVector col_new = new BasicSecondVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setInt(i, ti.getValue() % 86400);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setInt(i, ti.getValue() / 1000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                }
                else if (kvp.Value == DATA_TYPE.DT_DATETIME)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicDateTimeVector col_new = new BasicDateTimeVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        ;
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        throw new Exception("illeagl convertion from TimeSpan to DT_DATETIME");
                    }
                }
                else if (kvp.Value == DATA_TYPE.DT_TIMESTAMP)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicTimestampVector col_new = new BasicTimestampVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setLong(i, (long)ti.getValue() * 1000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        throw new Exception("illeagl convertion from TimeSpan to DT_TIMESTAMP");
                    }
                }
                else if (kvp.Value == DATA_TYPE.DT_NANOTIME)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicNanoTimeVector col_new = new BasicNanoTimeVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setLong(i, (long)(ti.getValue() % 86400) * 1000000000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setLong(i, (long)ti.getValue() * 1000000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                }
                else if (kvp.Value == DATA_TYPE.DT_NANOTIMESTAMP)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE t = col.getDataType();
                    BasicNanoTimestampVector col_new = new BasicNanoTimestampVector(col.rows());
                    if (t == DATA_TYPE.DT_DATETIME)
                    {
                        for (int i = 0; i < col.rows(); ++i)
                        {
                            BasicInt ti = (BasicInt)col.get(i);
                            col_new.setLong(i, (long)(ti.getValue()) * 1000000000);

                        }
                        bt.change_column(kvp.Key, col_new);
                    }
                    else if (t == DATA_TYPE.DT_TIME)
                    {
                        throw new Exception("illeagl convertion from TimeSpan to DT_NANOTIMESTAMP");
                    }
                }else if(getCategory(kvp.Value) == DATA_CATEGORY.DENARY)
                {
                    IVector col = bt.getColumn(kvp.Key);
                    DATA_TYPE originType = col.getDataType();
                    if(originType == DATA_TYPE.DT_STRING)
                    {
                        IVector newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(kvp.Value, 0, extras[kvp.Key]);
                        int rows = col.rows();
                        for(int i = 0; i < rows; i++)
                        {
                            newVector.add(col.get(i).getString());
                        }
                        bt.change_column(kvp.Key, newVector);
                    }
                }


            }


            return bt;

        }

        public static DATA_CATEGORY typeToCategory(DATA_TYPE type)
        {
            if ((int)type >= AbstractVector.ARRAY_VECTOR_BASE)
                return DATA_CATEGORY.ARRAY;
            if (type == DATA_TYPE.DT_TIME || type == DATA_TYPE.DT_SECOND || type == DATA_TYPE.DT_MINUTE || type == DATA_TYPE.DT_DATE
                    || type == DATA_TYPE.DT_DATETIME || type == DATA_TYPE.DT_MONTH || type == DATA_TYPE.DT_TIMESTAMP || type == DATA_TYPE.DT_NANOTIME || type == DATA_TYPE.DT_NANOTIMESTAMP
                    || type == DATA_TYPE.DT_DATEHOUR || type == DATA_TYPE.DT_DATEMINUTE)
                return DATA_CATEGORY.TEMPORAL;
            else if (type == DATA_TYPE.DT_INT || type == DATA_TYPE.DT_LONG || type == DATA_TYPE.DT_SHORT || type == DATA_TYPE.DT_BYTE)
                return DATA_CATEGORY.INTEGRAL;
            else if (type == DATA_TYPE.DT_BOOL)
                return DATA_CATEGORY.LOGICAL;
            else if (type == DATA_TYPE.DT_DOUBLE || type == DATA_TYPE.DT_FLOAT)
                return DATA_CATEGORY.FLOATING;
            else if (type == DATA_TYPE.DT_STRING || type == DATA_TYPE.DT_SYMBOL || type == DATA_TYPE.DT_BLOB)
                return DATA_CATEGORY.LITERAL;
            else if (type == DATA_TYPE.DT_INT128 || type == DATA_TYPE.DT_UUID || type == DATA_TYPE.DT_IPADDR)
                return DATA_CATEGORY.BINARY;
            else if (type == DATA_TYPE.DT_ANY)
                return DATA_CATEGORY.MIXED;
            else if (type == DATA_TYPE.DT_VOID)
                return DATA_CATEGORY.NOTHING;
            else if (type == DATA_TYPE.DT_DECIMAL32 || type == DATA_TYPE.DT_DECIMAL64 || type == DATA_TYPE.DT_DECIMAL128)
                return DATA_CATEGORY.DENARY;
            else
                return DATA_CATEGORY.SYSTEM;
        }

        public static DATA_CATEGORY getCategory(DATA_TYPE type)
        {
            return typeToCategory(type);
        }

        public static IEntity castDateTime(IEntity source, DATA_TYPE newDateTimeType)
        {
            if (source.getDataForm() != DATA_FORM.DF_VECTOR && source.getDataForm() != DATA_FORM.DF_SCALAR)
                throw new Exception("The source data must be a temporal scalar/vector.");
            switch (newDateTimeType)
            {

                case DATA_TYPE.DT_MONTH:
                    return toMonth(source);
                case DATA_TYPE.DT_DATE:
                    return toDate(source);
                case DATA_TYPE.DT_DATEHOUR:
                    return toDateHour(source);
                default:
                    throw new Exception("The target date/time type supports MONTH/DATE only for time being.");
            }
        }

        public static IEntity toDateHour(IEntity source)
        {
            if (source.isScalar())
            {
                long scaleFactor = 1;
                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 3600000000000L;
                        return new BasicDateHour((int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor));
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 3600000;
                        return new BasicDateHour((int)divide(((BasicTimestamp)source).getLong(), scaleFactor));
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 3600;
                        return new BasicDateHour(divide(((BasicDateTime)source).getInt(), (int)scaleFactor));
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
                }
            }
            else
            {
                long scaleFactor = 1;
                int rows = source.rows();
                int[] values = new int[rows];

                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 3600000000000L;
                        BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = (int)divide(ntsVec.getLong(i), scaleFactor);
                        }
                        return new BasicDateHourVector(values);
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 3600000;
                        BasicTimestampVector tsVec = (BasicTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = (int)divide(tsVec.getLong(i), scaleFactor);
                        }
                        return new BasicDateHourVector(values);
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 3600;
                        BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = divide(dtVec.getInt(i), (int)scaleFactor);
                        }
                        return new BasicDateHourVector(values);
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
                }
            }
        }

        public static IEntity toMonth(IEntity source)
        {
            long scaleFactor = 1;
            int days;

            if (source.isScalar())
            {
                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 86400000000000L;
                        days = (int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor);
                        return new BasicMonth(countMonths(days));
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 86400000;
                        days = (int)divide(((BasicTimestamp)source).getLong(), scaleFactor);
                        return new BasicMonth(countMonths(days));
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 86400;
                        days = divide(((BasicDateTime)source).getInt(), (int)scaleFactor);
                        return new BasicMonth(countMonths(days));
                    case DATA_TYPE.DT_DATEHOUR:
                        scaleFactor = 24;
                        days = divide(((BasicDateHour)source).getInt(), (int)scaleFactor);
                        return new BasicMonth(countMonths(days));
                    case DATA_TYPE.DT_DATE:
                        return new BasicMonth(countMonths(((BasicDate)source).getInt()));
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.");
                }
            }
            else
            {
                int rows = source.rows();
                int[] values = new int[rows];

                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 86400000000000L;
                        BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = countMonths((int)divide(ntsVec.getLong(i), scaleFactor));
                        }
                        return new BasicMonthVector(values);
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 86400000;
                        BasicTimestampVector tsVec = (BasicTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = countMonths((int)divide(tsVec.getLong(i), scaleFactor));
                        }
                        return new BasicMonthVector(values);
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 86400;
                        BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = countMonths(divide(dtVec.getInt(i), (int)scaleFactor));
                        }
                        return new BasicMonthVector(values);
                    case DATA_TYPE.DT_DATEHOUR:
                        scaleFactor = 24;
                        BasicDateHourVector dhVec = (BasicDateHourVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = countMonths(divide(dhVec.getInt(i), (int)scaleFactor));
                        }
                        return new BasicMonthVector(values);
                    case DATA_TYPE.DT_DATE:
                        BasicDateVector dVec = (BasicDateVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = countMonths(dVec.getInt(i));
                        }
                        return new BasicMonthVector(values);
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATETIME, or DATE.");
                }
            }
        }

        public static IEntity toDate(IEntity source)
        {
            if (source.isScalar())
            {
                long scaleFactor = 1;
                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 86400000000000L;
                        return new BasicDate((int)divide(((BasicNanoTimestamp)source).getLong(), scaleFactor));
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 86400000;
                        return new BasicDate((int)divide(((BasicTimestamp)source).getLong(), scaleFactor));
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 86400;
                        return new BasicDate(divide(((BasicDateTime)source).getInt(), (int)scaleFactor));
                    case DATA_TYPE.DT_DATEHOUR:
                        scaleFactor = 24;
                        return new BasicDate(divide(((BasicDateHour)source).getInt(), (int)scaleFactor));
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
                }
            }
            else
            {
                long scaleFactor = 1;
                int rows = source.rows();
                int[] values = new int[rows];

                switch (source.getDataType())
                {
                    case DATA_TYPE.DT_NANOTIMESTAMP:
                        scaleFactor = 86400000000000L;
                        BasicNanoTimestampVector ntsVec = (BasicNanoTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = (int)divide(ntsVec.getLong(i), scaleFactor);
                        }
                        return new BasicDateVector(values);
                    case DATA_TYPE.DT_TIMESTAMP:
                        scaleFactor = 86400000;
                        BasicTimestampVector tsVec = (BasicTimestampVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = (int)divide(tsVec.getLong(i), scaleFactor);
                        }
                        return new BasicDateVector(values);
                    case DATA_TYPE.DT_DATETIME:
                        scaleFactor = 86400;
                        BasicDateTimeVector dtVec = (BasicDateTimeVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = divide(dtVec.getInt(i), (int)scaleFactor);
                        }
                        return new BasicDateVector(values);
                    case DATA_TYPE.DT_DATEHOUR:
                        scaleFactor = 24;
                        BasicDateHourVector dhVec = (BasicDateHourVector)source;
                        for (int i = 0; i < rows; ++i)
                        {
                            values[i] = divide(dhVec.getInt(i), (int)scaleFactor);
                        }
                        return new BasicDateVector(values);
                    default:
                        throw new Exception("The data type of the source data must be NANOTIMESTAMP, TIMESTAMP, or DATETIME.");
                }
            }
        }

        public static int divide(int x, int y)
        {
            if(x == int.MinValue)
                return int.MinValue;
            int tmp = x / y;
            if (x >= 0)
                return tmp;
            else if (x % y < 0)
                return tmp - 1;
            else
                return tmp;
        }

        public static long divide(long x, long y)
        {
            if(x == long.MinValue)
                return long.MinValue;
            long tmp = x / y;
            if (x >= 0)
                return tmp;
            else if (x % y < 0)
                return tmp - 1;
            else
                return tmp;
        }


        public static int countMonths(int days)
        {
            int year, month;
            days += 719529;
            int circleIn400Years = days / 146097;
            int offsetIn400Years = days % 146097;
            int resultYear = circleIn400Years * 400;
            int similarYears = offsetIn400Years / 365;
            int tmpDays = similarYears * 365;
            if (similarYears > 0) tmpDays += (similarYears - 1) / 4 + 1 - (similarYears - 1) / 100;
            if (tmpDays >= offsetIn400Years) --similarYears;
            year = similarYears + resultYear;
            days -= circleIn400Years * 146097 + tmpDays;
            bool leap = ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0);
            if (days <= 0)
            {
                days += leap ? 366 : 365;
            }
            if (leap)
            {
                month = days / 32 + 1;
                if (days > cumLeapMonthDays[month])
                    month++;
            }
            else
            {
                month = days / 32 + 1;
                if (days > cumMonthDays[month])
                    month++;
            }

            return year * 12 + month - 1;
        }

        public static string getDataTypeString(DATA_TYPE dt)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_VOID:
                    return "VOID";
                case DATA_TYPE.DT_BOOL:
                    return "BOOL";
                case DATA_TYPE.DT_BYTE:
                    return "BYTE";
                case DATA_TYPE.DT_SHORT:
                    return "SHORT";
                case DATA_TYPE.DT_INT:
                    return "INT";
                case DATA_TYPE.DT_LONG:
                    return "LONG";
                case DATA_TYPE.DT_FLOAT:
                    return "FLOAT";
                case DATA_TYPE.DT_DOUBLE:
                    return "DOUBLE";
                case DATA_TYPE.DT_NANOTIME:
                    return "NANOTIME";
                case DATA_TYPE.DT_NANOTIMESTAMP:
                    return "NANOTIMESTAMP";
                case DATA_TYPE.DT_TIMESTAMP:
                    return "TIMESTAMP";
                case DATA_TYPE.DT_DATE:
                    return "DATE";
                case DATA_TYPE.DT_MONTH:
                    return "MONTH";
                case DATA_TYPE.DT_TIME:
                    return "TIME";
                case DATA_TYPE.DT_SECOND:
                    return "SECOND";
                case DATA_TYPE.DT_MINUTE:
                    return "MINUTE";
                case DATA_TYPE.DT_DATETIME:
                    return "DATETIME";
                case DATA_TYPE.DT_INT128:
                    return "INT128";
                case DATA_TYPE.DT_IPADDR:
                    return "IPADDR";
                case DATA_TYPE.DT_UUID:
                    return "UUID";
                case DATA_TYPE.DT_STRING:
                    return "STRING";
                case DATA_TYPE.DT_SYMBOL:
                    return "DT_SYMBOL";
                case DATA_TYPE.DT_DECIMAL32:
                    return "DECIMAL32";
                case DATA_TYPE.DT_DECIMAL64:
                    return "DECIMAL64";
                case DATA_TYPE.DT_DECIMAL128:
                    return "DECIMAL128";
                default:
                    return "data type " + ((int)dt).ToString();
            }
        }


        public static string getDataFormString(DATA_FORM df)
        {
            switch(df){
                case DATA_FORM.DF_SCALAR:
                    return "SCALAR";
                case DATA_FORM.DF_VECTOR:
                    return "VECTOR";
                case DATA_FORM.DF_PAIR:
                    return "PAIR";
                    case DATA_FORM.DF_MATRIX:
                    return "MATRIX";
                case DATA_FORM.DF_SET:
                    return "SET";
                case DATA_FORM.DF_DICTIONARY:

                    return "DICTIONARY";
                case DATA_FORM.DF_TABLE:
                    return "TABLE";
                default:
                    return ((int)df).ToString();
            }
        }
        public static IEntity createObject(DATA_TYPE dt, Object value, int extraParam)
        {
            if(value == null){
                return createNULLObject(dt);
            }
            else if (value is bool)
            {
                return createObject(dt, (bool)value);
            }
            else if (value is bool[])
            {
                return createObject(dt, (bool[])value, extraParam);
            }
            else if (value is byte)
            {
                return createObject(dt, (byte)value);
            }
            else if (value is byte[])
            {
                return createObject(dt, (byte[])value, extraParam);
            }
            else if (value is short)
            {
                return createObject(dt, (short)value);
            }
            else if (value is short[])
            {
                return createObject(dt, (short[])value, extraParam);
            }
            else if (value is int)
            {
                return createObject(dt, (int)value, extraParam);
            }
            else if (value is int[])
            {
                return createObject(dt, (int[])value, extraParam);
            }
            else if(value is long)
            {
                return createObject(dt, (long)value, extraParam);
            }
            else if (value is long[])
            {
                return createObject(dt, (long[])value, extraParam);
            }
            else if (value is float)
            {
                return createObject(dt, (float)value, extraParam);
            }
            else if (value is float[])
            {
                return createObject(dt, (float[])value, extraParam);
            }
            else if (value is double)
            {
                return createObject(dt, (double)value, extraParam);
            }
            else if (value is double[])
            {
                return createObject(dt, (double[])value, extraParam);
            }
            else if (value is string)
            {
                return createObject(dt, (string)value, extraParam);
            }
            else if (value is string[])
            {
                return createObject(dt, (string[])value, extraParam);
            }
            else if (value is DateTime)
            {
                return createObject(dt, (DateTime)value);
            }
            else if (value is DateTime[])
            {
                return createObject(dt, (DateTime[])value, extraParam);
            }
            else if (value is TimeSpan)
            {
                return createObject(dt, (TimeSpan)value);
            }
            else if (value is TimeSpan[])
            {
                return createObject(dt, (TimeSpan[])value, extraParam);
            }
            else if (value is decimal)
            {
                return createObject(dt, (decimal)value, extraParam);
            }
            else if (value is decimal[])
            {
                return createObject(dt, (decimal[])value, extraParam);
            }
            else if(value is IEntity)
            {
                return (IEntity)value;
            }
            else if (value is IEntity[])
            {
                return createObject(dt, (IEntity[])value, extraParam);
            }
            throw new Exception("Failed to insert data,  Cannot convert " + value.GetType().ToString() + " to " + getDataTypeString(dt));
        }

        public static IEntity createObject(DATA_TYPE dt, bool value)
        {

            switch (dt)
            {
                case DATA_TYPE.DT_BOOL:
                    return new BasicBoolean(value);
                default: break;
            }
            throw new Exception("Failed to insert data. Cannot convert bool to" + getDataTypeString(dt));
        }

        public static IEntity createObject(DATA_TYPE dt, long value, string typeName)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_LONG:
                    return new BasicLong(value);
                case DATA_TYPE.DT_INT:
                    if (value >= int.MinValue && value <= int.MaxValue)
                        return new BasicInt((int)value);
                    else
                        throw new Exception("Failed to insert data. " + typeName + " cannot be converted because it exceeds the range of " + getDataTypeString(dt));
                case DATA_TYPE.DT_SHORT:
                    if (value >= short.MinValue && value <= short.MaxValue)
                        return new BasicShort((short)value);
                    else
                        throw new Exception("Failed to insert data. " + typeName + " cannot be converted because it exceeds the range of " + getDataTypeString(dt));
                case DATA_TYPE.DT_BYTE:
                    if (value >= byte.MinValue && value <= byte.MaxValue)
                        return new BasicByte((byte)value);
                    else
                        throw new Exception("Failed to insert data. " + typeName + " cannot be converted because it exceeds the range of " + getDataTypeString(dt));
                default:
                    throw new Exception("Failed to insert data. Cannot convert " + typeName +" to " + getDataTypeString(dt));
            }
        }

        public static IEntity createObject(DATA_TYPE dt, byte value)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_BYTE:
                    return new BasicByte(value);
                default:
                    return createObject(dt, (long)value, "byte");
            }
        }

        public static IEntity createObject(DATA_TYPE dt, short value)
        {
            return createObject(dt, (long)value, "short");
        }

        public static IEntity createObject(DATA_TYPE dt, int value, int extraParam)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_INT:
                    return new BasicInt(value);
                default:
                    return createObject(dt, (long)value, "int");
            }
        }

        public static IEntity createObject(DATA_TYPE dt, string value, int extraParam = -1)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_STRING:
                case DATA_TYPE.DT_SYMBOL:
                    return new BasicString(value);
                case DATA_TYPE.DT_INT128:
                    return BasicInt128.fromString(value);
                case DATA_TYPE.DT_UUID:
                    return BasicUuid.fromString(value);
                case DATA_TYPE.DT_IPADDR:
                    return BasicIPAddr.fromString(value);
                case DATA_TYPE.DT_BLOB:
                    return new BasicString(value, true);
                case DATA_TYPE.DT_DECIMAL32:
                    return new BasicDecimal32(value, extraParam);
                case DATA_TYPE.DT_DECIMAL64:
                    return new BasicDecimal64(value, extraParam);
                case DATA_TYPE.DT_DECIMAL128:
                    return new BasicDecimal128(value, extraParam);
                default:
                    throw new Exception("Failed to insert data. Cannot convert string to " + getDataTypeString(dt));
            }
        }

        public static IEntity createObject(DATA_TYPE dt, long value, int extraParam)
        {
            switch (dt)
            {
                default:
                    return createObject(dt, value, "long");
            }
        }

        public static IEntity createObject(DATA_TYPE dt, float value, int extraParam)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_FLOAT:
                    return new BasicFloat(value);
                case DATA_TYPE.DT_DOUBLE:
                    return new BasicDouble(value);
                default:
                    throw new Exception("Failed to insert data. Cannot convert float to " + getDataTypeString(dt));
            }
        }

        public static IEntity createObject(DATA_TYPE dt, double value, int extraParam)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_FLOAT:
                    if(value <= float.MaxValue && value >= float.MinValue)
                    return new BasicFloat((float)value);
                    break;
                case DATA_TYPE.DT_DOUBLE:
                    return new BasicDouble(value);
                default:
                    throw new Exception("Failed to insert data. Cannot convert double to " + getDataTypeString(dt));
            }
            throw new Exception("Failed to insert data. Cannot convert double to " + getDataTypeString(dt));
        }

        public static IEntity createObject(DATA_TYPE dt, decimal value, int extraParam)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_DECIMAL32:
                    return new BasicDecimal32(value, extraParam);
                case DATA_TYPE.DT_DECIMAL64:
                    return new BasicDecimal64(value, extraParam);
                case DATA_TYPE.DT_DECIMAL128:
                    return new BasicDecimal128(value, extraParam);
            }
            throw new Exception("Failed to insert data. Cannot convert decimal to " + getDataTypeString(dt));
        }

        public static IEntity createObject(DATA_TYPE dt, DateTime value)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_NANOTIMESTAMP:
                    return new BasicNanoTimestamp(value);
                case DATA_TYPE.DT_TIMESTAMP:
                    return new BasicTimestamp(value);
                case DATA_TYPE.DT_DATE:
                        return new BasicDate(value);
                case DATA_TYPE.DT_MONTH:
                        return new BasicMonth(value);
                case DATA_TYPE.DT_DATETIME:
                        return new BasicDateTime(value);
                case DATA_TYPE.DT_DATEHOUR:
                    return new BasicDateHour(value);
                default:
                    throw new Exception("Failed to insert data. Cannot convert DateTime to " + getDataTypeString(dt));
            }
            throw new Exception("Failed to insert data. Cannot convert DateTime to " + getDataTypeString(dt));
        }

        public static IEntity createObject(DATA_TYPE dt, TimeSpan value)
        {
            switch (dt)
            {
                case DATA_TYPE.DT_NANOTIME:
                    return new BasicNanoTime(value);
                case DATA_TYPE.DT_TIME:
                        return new BasicTime(value);
                case DATA_TYPE.DT_SECOND:
                        return new BasicSecond(value);
                case DATA_TYPE.DT_MINUTE:
                        return new BasicMinute(value);
                default:
                    throw new Exception("Failed to insert data. Cannot convert TimeSpan to " + getDataTypeString(dt));
            }
            throw new Exception("Failed to insert data. Cannot convert TimeSpan to " + getDataTypeString(dt));
        }

        public static IEntity createObject<T>(DATA_TYPE dt, T[] value, int extra)
        {
            if (dt < DATA_TYPE.DT_BOOL_ARRAY)
                throw new Exception(value.GetType().ToString() + "must convert to array vector");
            int count = value.Length;
            BasicEntityFactory factory = (BasicEntityFactory)BasicEntityFactory.instance();
            IVector ret = factory.createVectorWithDefaultValue(dt - 64, count, extra);
            for(int i = 0; i < count; ++i)
            {
                IScalar t = (IScalar)createObject(dt - 64, value[i], extra);
                ret.set(i, t);
            }
            return ret;
        }

        public static IEntity createNULLObject(DATA_TYPE dt)
        {
            BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();
            IScalar scalar = basicEntityFactory.createScalarWithDefaultValue(dt);
            scalar.setNull();
            return scalar;
        }

        public static int UIntMoveRight(int x, int y)
        {
            int mask = 0x7fffffff;
            for (int i = 0; i < y; i++)
            {
                x >>= 1;
                x &= mask;
            }
            return x;
        }

        public static object getRowDataTableObject(IEntity entity)
        {
            if(entity.isScalar())
            {
                if (((IScalar)entity).isNull())
                    return DBNull.Value;
                else
                {
                    DATA_TYPE dType = entity.getDataType();
                    DATA_CATEGORY category = Utils.getCategory(dType);
                    if(category == DATA_CATEGORY.TEMPORAL)
                    {
                        return ((IScalar)entity).getTemporal();
                    }
                    switch (dType)
                    {
                        case DATA_TYPE.DT_BOOL:
                            return ((BasicBoolean)entity).getValue();
                        case DATA_TYPE.DT_BYTE:
                            return ((BasicByte)entity).getValue();
                        case DATA_TYPE.DT_SHORT:
                            return ((BasicShort)entity).getValue();
                        case DATA_TYPE.DT_INT:
                            return ((BasicInt)entity).getValue();
                        case DATA_TYPE.DT_LONG:
                            return ((BasicLong)entity).getValue();
                        case DATA_TYPE.DT_FLOAT:
                            return ((BasicFloat)entity).getValue();
                        case DATA_TYPE.DT_DOUBLE:
                            return ((BasicDouble)entity).getValue();
                        default:
                            return entity.getString();
                    }
                }
            }
            else
            {
                return entity.getString();
            }
            
        }

        public static object getRowDataTableObject(IVector vector, int rowIndex)
        {
            DATA_TYPE dType = vector.getDataType();
            if (dType == DATA_TYPE.DT_ANY)
                return vector.get(rowIndex).getString();
            DATA_CATEGORY category = Utils.getCategory(dType);
            return getRowDataTableObject(vector.getEntity(rowIndex));
        }

        public static BasicDecimal32 createBasicDecimal32(int rawData, int scale)
        {
            return new BasicDecimal32(rawData, scale);
        }

        public static BasicDecimal64 createBasicDecimal64(long rawData, int scale)
        {
            return new BasicDecimal64(rawData, scale);
        }

        public static BasicDecimal128 createBasicDecimal128(BigInteger rawData, int scale)
        {
            return new BasicDecimal128(rawData, scale);
        }

        internal static void checkParamIsNull(object param, string paramName)
        {
            if(param == null)
            {
                throw new Exception(paramName + " must be non-null.");
            }
        }

        internal static void checkStringNotNullAndEmpty (string param, string paramName)
        {
            checkParamIsNull(param, paramName);
            if (param == "")
            {
                throw new Exception(paramName + " must be non-empty.");
            }
        }
    }

}
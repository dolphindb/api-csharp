using System;
using System.Diagnostics;

namespace dolphindb.data
{


	public class Utils
	{
        private static readonly int DEFALUT_YEAR = 1970;
        private static readonly int DEFAULT_MONTH = 1;
        private static readonly int DEFAULT_DAY = 1;
        private static readonly int DEFAULT_HOUR = 0;
        private static readonly int DEFAULT_MINUTE = 0;
        private static readonly int DEFAULT_SECOND = 0;
        private static readonly int DEFAULT_MILLIONSECOND = 0;


        public const int DISPLAY_ROWS = 20;
		public const int DISPLAY_COLS = 100;
		public const int DISPLAY_WIDTH = 100;

		private static readonly int[] cumMonthDays = new int[] {0,31,59,90,120,151,181,212,243,273,304,334,365};
		private static readonly int[] cumLeapMonthDays = new int[] {0,31,60,91,121,152,182,213,244,274,305,335,366};
		private static readonly int[] monthDays = new int[] {31,28,31,30,31,30,31,31,30,31,30,31};
		private static readonly int[] leapMonthDays = new int[] {31,29,31,30,31,30,31,31,30,31,30,31};

		public static int countMonths(DateTime date)
		{
			return date.Year * 12 + date.Month -1;
		}

		public static int countMonths(int year, int month)
		{
			return year * 12 + month - 1;
		}

		public static DateTime parseMonth(int value)
		{
			return new DateTime(value / 12, value % 12 + 1,1);
		}

		public static int countDays(DateTime date)
		{
			return countDays(date.Year, date.Month,date.Day);
		}

		public static int countDays(int year, int month, int day)
		{
			//1999.12.31 return 0
			if (month < 1 || month>12 || day < 0)
			{
				return int.MinValue;
			}

			int days = 10956 + (year - 2000) / 4 * 1461;
			year = (year - 2000) % 4;
			days += 365 * year;
			if (year == 0)
			{
				//leap year
				days += cumLeapMonthDays[month - 1];
				return day <= leapMonthDays[month - 1] ? days + day : int.MinValue;
			}
			else
			{
				if (year >= 0)
				{
					days++;
				}
				days += cumMonthDays[month - 1];
				return day <= monthDays[month - 1] ? days + day : int.MinValue;
			}
		}

		public static DateTime parseDate(int days)
		{
			int year, month, day;
			bool leap = false;

			days -= 10956;
			year = 2000 + (days / 1461) * 4;
			days = days % 1461;
			if (days < 0)
			{
				year -= 4;
				days += 1461;
			}
			if (days > 366)
			{
				year += 1;
				days = days - 366;
				year += days / 365;
				days = days % 365;
			}
			else
			{
				leap = true;
			}
			if (days == 0)
			{
				year = year - 1;
				month = 12;
				day = 31;
			}
			else
			{
				if (leap)
				{
					month = days / 32 + 1;
					if (days > cumLeapMonthDays[month])
					{
						month++;
					}
					day = days - cumLeapMonthDays[month - 1];
				}
				else
				{
					month = days / 32 + 1;
					if (days > cumMonthDays[month])
					{
						month++;
					}
					day = days - cumMonthDays[month - 1];
				}
			}

			return DateTime.Parse(year.ToString() + "." + month.ToString() + "." + day.ToString());
		}

		public static int countSeconds(DateTime dt)
		{
			return countSeconds(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
		}

		public static int countSeconds(int year, int month, int day, int hour, int minute, int second)
		{
			int days = countDays(year, month, day);
			return days * 86400 + (hour * 60 + minute) * 60 + second;
		}

		public static DateTime parseDateTime(int seconds)
		{
			int days = seconds / 86400;
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

		public static int countMilliseconds(DateTime dt)
		{
            int seconds = countSeconds(dt);
			return seconds * 1000 + dt.Millisecond;
		}

		public static int countMilliseconds(int year, int month, int day, int hour, int minute, int second, int millisecond)
		{
			return countSeconds(year, month, day, hour, minute, second) * 1000 + millisecond;
		}
		public static int countNanoseconds(DateTime dt)
		{
            int seconds = countSeconds(dt);
			return seconds * 1000000000 + dt.Millisecond * 1000000;
		}

		/// <summary>
		/// 1 <==> 1970.01.01 00:00:00.001
		/// 0 <==> 1970.01.01 00:00:00.000
		/// -1 <==> 1969.12.31 23:59:59.999
		/// ...
		/// </summary>
		public static DateTime parseTimestamp(long milliseconds)
		{
			int days = (int)Math.Floor(((double)milliseconds / 86400000.0));
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
            //throw new NotImplementedException();
            return new DateTime(nanoseconds / 100);
			//int days = (int)Math.Floor(((double)nanoseconds / NANOS_PER_DAY));
			//DateTime date = Utils.parseDate(days);
			//nanoseconds = nanoseconds % NANOS_PER_DAY;
			//if (nanoseconds < 0)
			//{
			//	nanoseconds += NANOS_PER_DAY;
			//}
			//DateTime time = Utils.parseNanoTime(nanoseconds % NANOS_PER_DAY);
			//return new DateTime();
		}
		//public static int countMilliseconds(DateTime time)
		//{
		//	return countMilliseconds(time.Hour, time.Minute, time.Second, time.Nano / 1000000);
		//}

		public static int countMilliseconds(int hour, int minute, int second, int millisecond)
		{
			return ((hour * 60 + minute) * 60 + second) * 1000 + millisecond;
		}

        //public static long countNanoseconds(DateTime time)
        //{
        //	return (long)countMilliseconds(time.Hour, time.Minute, time.Second, 0) * 1000000 + time.Nano;
        //}

        public static DateTime parseTime(int milliseconds)
        {
            return new DateTime(1970,1,1,milliseconds / 3600000, milliseconds / 60000 % 60, milliseconds / 1000 % 60, milliseconds % 1000);
        }

        public static DateTime parseNanoTime(long nanoOfDay)
        {
            return new DateTime(nanoOfDay);
        }

        //public static int countSeconds(DateTime time)
        //{
        //    return countSeconds(time.Hour, time.Minute, time.Second);
        //}

        public static int countSeconds(int hour, int minute, int second)
		{
			return (hour * 60 + minute) * 60 + second;
		}

		public static DateTime parseSecond(int seconds)
		{
            //throw new NotImplementedException();
			return new DateTime(DEFALUT_YEAR, DEFAULT_MONTH, DEFAULT_DAY,seconds / 3600, seconds % 3600 / 60, seconds % 60);
		}

		public static int countMinutes(DateTime time)
		{
			return countMinutes(time.Hour, time.Minute);
		}

		public static int countMinutes(int hour, int minute)
		{
			return hour * 60 + minute;
		}

		public static DateTime parseMinute(int minutes)
		{
		    return new DateTime(DEFALUT_YEAR,DEFAULT_MONTH,DEFAULT_DAY, minutes / 60, minutes % 60,DEFAULT_SECOND);
		}
	}

}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureRepositories.Candles
{
    public static class DateTimeUtils
    {
        /// <summary>
        /// Returns date of the first week's monday in the specified year.
        /// </summary>
        public static DateTime GetFirstWeekOfYear(int year, DateTimeKind kind = DateTimeKind.Utc)
        {
            var dt = new DateTime(year, 1, 1, 0, 0, 0, kind);
            if (dt.DayOfWeek == DayOfWeek.Monday)
            {
                return dt;
            }

            int diff = DayOfWeek.Monday - dt.DayOfWeek;
            return dt.AddDays(diff == 1 ? 1 : 7 + diff);
        }

        /// <summary>
        /// Defines in which year starts the week of the specified date.
        /// Returns date of the first weeks' monday in that year.
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static DateTime GetFirstWeekOfYear(DateTime dt)
        {
            int diff = dt.DayOfWeek - DayOfWeek.Monday;
            int year = dt.AddDays(diff == -1 ? -6 : -diff).Year;
            return GetFirstWeekOfYear(year, dt.Kind);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices;

namespace AzureRepositories.Candles.Tests
{
    public class DateTimeUtilsTests
    {
        [Fact]
        public void GetFirstWeekOfYear1_BasicChecks()
        {
            Assert.Equal(new DateTime(2006, 1, 2, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2006));
            Assert.Equal(new DateTime(2007, 1, 1, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2007));
            Assert.Equal(new DateTime(2009, 1, 5, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2009));
            Assert.Equal(new DateTime(2010, 1, 4, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2010));
            Assert.Equal(new DateTime(2012, 1, 2, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2012));
            Assert.Equal(new DateTime(2016, 1, 4, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2016));
            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), DateTimeUtils.GetFirstWeekOfYear(2017));
        }

        public void GetFirstWeekOfYear2_BasicChecks()
        {
            Assert.Equal(new DateTime(2016, 1, 4), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2016, 12, 25)));
            Assert.Equal(new DateTime(2016, 1, 4), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2016, 12, 26)));
            Assert.Equal(new DateTime(2016, 1, 4), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2017, 1, 1)));
            Assert.Equal(new DateTime(2017, 1, 2), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2017, 1, 2)));
            Assert.Equal(new DateTime(2017, 1, 2), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2017, 1, 8)));
            Assert.Equal(new DateTime(2017, 1, 2), DateTimeUtils.GetFirstWeekOfYear(new DateTime(2017, 2, 8)));
        }
    }
}

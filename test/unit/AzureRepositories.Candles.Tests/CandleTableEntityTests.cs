//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using Xunit;
//using Lykke.Domain.Prices;

//namespace AzureRepositories.Candles.Tests
//{
//    public class CandleTableEntityTests
//    {

//        [Fact]
//        public void TestsAreCoveringAllIntervals()
//        {
//            // Tests are written for TimeInterval with 9 values
//            Assert.Equal(13, Enum.GetValues(typeof(TimeInterval)).Cast<int>().Count());
//        }

//        [Fact]
//        public void PropertiesParsedFromKeys()
//        {
//            // Month
//            var entityMonth = new CandleTableEntity("BTCRUB", "SELL_Month_636188256000000000"); // "2017"
//            Assert.Equal("BTCRUB", entityMonth.Asset);
//            Assert.False(entityMonth.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc), entityMonth.DateTime);

//            // Week
//            var entityWeek = new CandleTableEntity("BTCRUB", "SELL_Week_636189120000000000"); // "02-jan-2017"
//            Assert.Equal("BTCRUB", entityWeek.Asset);
//            Assert.False(entityWeek.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityWeek.DateTime);

//            // Day
//            var entityDay = new CandleTableEntity("BTCRUB", "BUY_Day_636188256000000000"); // "2017-01"
//            Assert.Equal("BTCRUB", entityDay.Asset);
//            Assert.True(entityDay.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc), entityDay.DateTime);

//            // Hour
//            var entityHour = new CandleTableEntity("BTCRUB", "SELL_Hour_636189120000000000"); // "2017-01-02"
//            Assert.Equal("BTCRUB", entityHour.Asset);
//            Assert.False(entityHour.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityHour.DateTime);

//            // Hour4
//            var entityHour4 = new CandleTableEntity("BTCRUB", "SELL_Hour_636189120000000000"); // "2017-01-02"
//            Assert.Equal("BTCRUB", entityHour4.Asset);
//            Assert.False(entityHour4.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityHour4.DateTime);

//            // Min30
//            var entityMin30 = new CandleTableEntity("BTCRUB", "buy_Min30_636189120000000000"); // "2017-01-02T00"
//            Assert.Equal("BTCRUB", entityMin30.Asset);
//            Assert.True(entityMin30.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc), entityMin30.DateTime);

//            // Min15
//            var entityMin15 = new CandleTableEntity("BTCRUB", "sell_Min15_636189156000000000"); // "2017-01-02T01"
//            Assert.Equal("BTCRUB", entityMin15.Asset);
//            Assert.False(entityMin15.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 1, 0, 0, DateTimeKind.Utc), entityMin15.DateTime);

//            // Min5
//            var entityMin5 = new CandleTableEntity("BTCRUB", "BUY_Min5_636189192000000000"); // "2017-01-02T02"
//            Assert.Equal("BTCRUB", entityMin5.Asset);
//            Assert.True(entityMin5.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 2, 0, 0, DateTimeKind.Utc), entityMin5.DateTime);

//            // Min
//            var entityMinute = new CandleTableEntity("BTCRUB", "SELL_Minute_636189228000000000"); // "2017-01-02T03"
//            Assert.Equal("BTCRUB", entityMinute.Asset);
//            Assert.False(entityMinute.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 3, 0, 0, DateTimeKind.Utc), entityMinute.DateTime);

//            // Sec
//            var entitySec = new CandleTableEntity("BTCRUB", "BUY_Sec_636189191400000000");// "2017-01-02T01:59"
//            Assert.Equal("BTCRUB", entitySec.Asset);
//            Assert.True(entitySec.IsBuy);
//            Assert.Equal(new DateTime(2017, 1, 2, 1, 59, 0, DateTimeKind.Utc), entitySec.DateTime);
//        }

//        private static string[] fields1 = new string[] { "Data0" };
//        private static string[] fields2 = new string[] { "Data0", "Data1" };
//        private static string[] fields12 = Enumerable.Range(0, 12).Select(i => "Data" + i).ToArray();
//        private static string[] fields24 = Enumerable.Range(0, 24).Select(i => "Data" + i).ToArray();
//        private static string[] fields31 = Enumerable.Range(0, 31).Select(i => "Data" + i).ToArray();
//        private static string[] fields60 = Enumerable.Range(0, 60).Select(i => "Data" + i).ToArray();

//        [Fact]
//        public void GetStoreFieldsWithNormalData()
//        {
//            var baseTime = new DateTime(2017, 1, 1);

//            // Sec
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Sec, baseTime, baseTime.AddSeconds(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Sec, baseTime, baseTime.AddSeconds(61)));
//            Assert.Equal(fields60, CandleTableEntity.GetStoreFields(TimeInterval.Sec, baseTime, baseTime.AddMinutes(59).AddSeconds(1)));

//            // Min
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Minute, baseTime, baseTime.AddMinutes(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Minute, baseTime, baseTime.AddMinutes(61)));
//            Assert.Equal(fields24, CandleTableEntity.GetStoreFields(TimeInterval.Minute, baseTime, baseTime.AddHours(23).AddMinutes(1)));

//            // Min5
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Min5, baseTime, baseTime.AddMinutes(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Min5, baseTime, baseTime.AddMinutes(61)));
//            Assert.Equal(fields24, CandleTableEntity.GetStoreFields(TimeInterval.Min5, baseTime, baseTime.AddHours(23).AddMinutes(1)));

//            // Min15
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Min15, baseTime, baseTime.AddMinutes(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Min15, baseTime, baseTime.AddMinutes(61)));
//            Assert.Equal(fields24, CandleTableEntity.GetStoreFields(TimeInterval.Min15, baseTime, baseTime.AddHours(23).AddMinutes(1)));

//            // Min30
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Min30, baseTime, baseTime.AddMinutes(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Min30, baseTime, baseTime.AddMinutes(61)));
//            Assert.Equal(fields24, CandleTableEntity.GetStoreFields(TimeInterval.Min30, baseTime, baseTime.AddHours(23).AddMinutes(1)));

//            // Hour
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Hour, baseTime, baseTime.AddHours(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Hour, baseTime, baseTime.AddHours(25)));
//            Assert.Equal(fields31, CandleTableEntity.GetStoreFields(TimeInterval.Hour, baseTime, baseTime.AddDays(30).AddHours(1)));

//            // Hour4
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Hour4, baseTime, baseTime.AddHours(1)));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Hour4, baseTime, baseTime.AddHours(25)));
//            Assert.Equal(fields31, CandleTableEntity.GetStoreFields(TimeInterval.Hour4, baseTime, baseTime.AddDays(30).AddHours(1)));

//            // Day
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Day, baseTime, baseTime));
//            Assert.Equal(fields2, CandleTableEntity.GetStoreFields(TimeInterval.Day, baseTime, baseTime.AddDays(32)));
//            Assert.Equal(fields12, CandleTableEntity.GetStoreFields(TimeInterval.Day, baseTime, baseTime.AddMonths(11).AddDays(1)));

//            // Week
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Week, baseTime, baseTime));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Week, baseTime.AddDays(1), baseTime.AddDays(1)));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Week, baseTime.AddDays(1), baseTime.AddDays(8)));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Week, baseTime.AddDays(1), baseTime.AddDays(364)));

//            // Month
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Month, baseTime, baseTime));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Month, baseTime, baseTime.AddMonths(1)));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Month, baseTime, baseTime.AddMonths(13)));
//            Assert.Equal(fields1, CandleTableEntity.GetStoreFields(TimeInterval.Month, baseTime, baseTime.AddYears(10).AddMonths(1)));
//        }

//        [Fact]
//        public void GenerateRowKey_BasicTests()
//        {
//            // Week
//            Assert.Equal("BUY_Week_0635874624000000000", CandleTableEntity.GenerateRowKey(new DateTime(2016, 12, 26), true, TimeInterval.Week)); // 04-Jan-2016
//            Assert.Equal("BUY_Week_0635874624000000000", CandleTableEntity.GenerateRowKey(new DateTime(2016, 12, 27), true, TimeInterval.Week)); // 04-Jan-2016
//            Assert.Equal("BUY_Week_0635874624000000000", CandleTableEntity.GenerateRowKey(new DateTime(2017, 1, 1), true, TimeInterval.Week)); // 04-Jan-2016
//            Assert.Equal("BUY_Week_0636189120000000000", CandleTableEntity.GenerateRowKey(new DateTime(2017, 1, 2), true, TimeInterval.Week)); // 02-Jan-2017
//            Assert.Equal("BUY_Week_0636189120000000000", CandleTableEntity.GenerateRowKey(new DateTime(2017, 1, 30), true, TimeInterval.Week)); // 02-Jan-2017
//            Assert.Equal("BUY_Week_0636189120000000000", CandleTableEntity.GenerateRowKey(new DateTime(2017, 2, 1), true, TimeInterval.Week)); // 02-Jan-2017
//            Assert.Equal("BUY_Week_0636189120000000000", CandleTableEntity.GenerateRowKey(new DateTime(2017, 2, 6), true, TimeInterval.Week)); // 02-Jan-2017
//        }
//    }
//}

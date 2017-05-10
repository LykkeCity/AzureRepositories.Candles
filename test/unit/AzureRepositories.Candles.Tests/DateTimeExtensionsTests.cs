using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Lykke.Domain.Prices;

namespace AzureRepositories.Candles.Tests
{
    public class DateTimeExtensionTests
    {
        [Fact]
        public void TestsAreCoveringAllIntervals()
        {
            // Tests are written for TimeInterval with 9 values
            Assert.Equal(13, Enum.GetValues(typeof(TimeInterval)).Cast<int>().Count());
        }

        [Fact]
        public void GetIntervalTick_BasicChecks()
        {
            // Sec
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 0, 1).GetIntervalTick(TimeInterval.Sec));
            Assert.Equal(59, new DateTime(2017, 2, 8, 11, 12, 59, 1).GetIntervalTick(TimeInterval.Sec));
            // Minute
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 0, 59, 1).GetIntervalTick(TimeInterval.Minute));
            Assert.Equal(59, new DateTime(2017, 2, 8, 11, 59, 59, 1).GetIntervalTick(TimeInterval.Minute));
            // Min5
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 0, 13, 111).GetIntervalTick(TimeInterval.Min5));
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 1, 13, 111).GetIntervalTick(TimeInterval.Min5));
            Assert.Equal(2, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min5));
            // Min15
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min15));
            Assert.Equal(3, new DateTime(2017, 2, 8, 11, 59, 13, 111).GetIntervalTick(TimeInterval.Min15));
            // Min30
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 12, 13, 111).GetIntervalTick(TimeInterval.Min30));
            Assert.Equal(1, new DateTime(2017, 2, 8, 11, 59, 13, 111).GetIntervalTick(TimeInterval.Min30));
            // Hour
            Assert.Equal(11, new DateTime(2017, 2, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Hour));
            // Hour4
            Assert.Equal(0, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalTick(TimeInterval.Hour4));
            Assert.Equal(0, new DateTime(2017, 2, 8, 1, 50, 13, 111).GetIntervalTick(TimeInterval.Hour4));
            Assert.Equal(5, new DateTime(2017, 2, 8, 21, 50, 13, 111).GetIntervalTick(TimeInterval.Hour4));
            // Hour6
            Assert.Equal(0, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalTick(TimeInterval.Hour6));
            Assert.Equal(0, new DateTime(2017, 2, 8, 1, 50, 13, 111).GetIntervalTick(TimeInterval.Hour6));
            Assert.Equal(1, new DateTime(2017, 2, 8, 6, 50, 13, 111).GetIntervalTick(TimeInterval.Hour6));
            Assert.Equal(3, new DateTime(2017, 2, 8, 21, 50, 13, 111).GetIntervalTick(TimeInterval.Hour6));
            // Hour12
            Assert.Equal(0, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalTick(TimeInterval.Hour12));
            Assert.Equal(0, new DateTime(2017, 2, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Hour12));
            Assert.Equal(1, new DateTime(2017, 2, 8, 21, 50, 13, 111).GetIntervalTick(TimeInterval.Hour12));
            // Day
            Assert.Equal(1, new DateTime(2017, 1, 1, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Day));
            Assert.Equal(31, new DateTime(2017, 1, 31, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Day));
            // Week
            Assert.Equal(50, new DateTime(2016, 12, 25, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(51, new DateTime(2016, 12, 26, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(51, new DateTime(2017, 1, 1, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(0, new DateTime(2017, 1, 2, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(0, new DateTime(2017, 1, 3, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(1, new DateTime(2017, 1, 9, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(4, new DateTime(2017, 1, 31, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(4, new DateTime(2017, 2, 5, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(5, new DateTime(2017, 2, 6, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            Assert.Equal(6, new DateTime(2017, 2, 13, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Week));
            // Month
            Assert.Equal(1, new DateTime(2017, 1, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Month));
            Assert.Equal(12, new DateTime(2017, 12, 8, 11, 50, 13, 111).GetIntervalTick(TimeInterval.Month));
        }

        //[Fact]
        //public void GetIntervalCell_BasicChecks()
        //{
        //    // Hour
        //    Assert.Equal(7, new DateTime(2017, 2, 8, 11, 50, 13, 111).GetIntervalCell(TimeInterval.Hour));
        //    // Hour4
        //    Assert.Equal(7, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalCell(TimeInterval.Hour4));
        //    // Hour6
        //    Assert.Equal(7, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalCell(TimeInterval.Hour6));
        //    // Hour12
        //    Assert.Equal(7, new DateTime(2017, 2, 8, 0, 50, 13, 111).GetIntervalCell(TimeInterval.Hour12));
        //    // Week
        //    Assert.Equal(0, new DateTime(2017, 1, 1).GetIntervalCell(TimeInterval.Week));
        //    Assert.Equal(0, new DateTime(2017, 1, 2).GetIntervalCell(TimeInterval.Week));
        //    Assert.Equal(0, new DateTime(2017, 1, 31).GetIntervalCell(TimeInterval.Week));
        //    Assert.Equal(0, new DateTime(2017, 2, 1).GetIntervalCell(TimeInterval.Week));
        //    Assert.Equal(0, new DateTime(2017, 2, 6).GetIntervalCell(TimeInterval.Week));
        //}

        //[Fact]
        //public void AddIntervalTicks_BasicChecks()
        //{
        //    DateTime baseTime = new DateTime(2017, 1, 1, 0, 0, 0);

        //    // Sec
        //    Assert.Equal(baseTime.AddSeconds(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Sec));
        //    Assert.Equal(baseTime.AddSeconds(1), baseTime.AddIntervalTicks(0, 1, TimeInterval.Sec));
        //    // Minute
        //    Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Minute));
        //    Assert.Equal(baseTime.AddMinutes(1), baseTime.AddIntervalTicks(0, 1, TimeInterval.Minute));
        //    // Min5
        //    Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Min5));
        //    Assert.Equal(baseTime.AddMinutes(5), baseTime.AddIntervalTicks(0, 1, TimeInterval.Min5));
        //    // Min15
        //    Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Min15));
        //    Assert.Equal(baseTime.AddMinutes(15), baseTime.AddIntervalTicks(0, 1, TimeInterval.Min15));
        //    // Min30
        //    Assert.Equal(baseTime.AddMinutes(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Min30));
        //    Assert.Equal(baseTime.AddMinutes(30), baseTime.AddIntervalTicks(0, 1, TimeInterval.Min30));
        //    // Hour
        //    Assert.Equal(baseTime.AddHours(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Hour));
        //    Assert.Equal(baseTime.AddHours(1), baseTime.AddIntervalTicks(0, 1, TimeInterval.Hour));
        //    Assert.Equal(baseTime.AddDays(1).AddHours(0), baseTime.AddIntervalTicks(1, 0, TimeInterval.Hour));
        //    Assert.Equal(baseTime.AddDays(1).AddHours(1), baseTime.AddIntervalTicks(1, 1, TimeInterval.Hour));

        //    // Hour4
        //    Assert.Equal(baseTime.AddHours(0), baseTime.AddIntervalTicks(0, 0, TimeInterval.Hour4));
        //    Assert.Equal(baseTime.AddHours(4), baseTime.AddIntervalTicks(0, 1, TimeInterval.Hour4));
        //    Assert.Equal(baseTime.AddDays(1).AddHours(0), baseTime.AddIntervalTicks(1, 0, TimeInterval.Hour4));
        //    Assert.Equal(baseTime.AddDays(1).AddHours(4), baseTime.AddIntervalTicks(1, 1, TimeInterval.Hour4));

        //    // Days ticks are in [1..31]
        //    Assert.Equal(baseTime.AddDays(0), baseTime.AddIntervalTicks(0, 1, TimeInterval.Day));
        //    Assert.Equal(baseTime.AddDays(1), baseTime.AddIntervalTicks(0, 2, TimeInterval.Day));

        //    Assert.Equal(baseTime.AddMonths(1).AddDays(0), baseTime.AddIntervalTicks(1, 1, TimeInterval.Day));
        //    Assert.Equal(baseTime.AddMonths(1).AddDays(1), baseTime.AddIntervalTicks(1, 2, TimeInterval.Day));

        //    // Week
        //    Assert.Equal(baseTime.AddDays(7), baseTime.AddIntervalTicks(0, 1, TimeInterval.Week));
        //    Assert.Equal(baseTime.AddDays(14), baseTime.AddIntervalTicks(0, 2, TimeInterval.Week));
        //    Assert.Equal(baseTime.AddDays(7*52), baseTime.AddIntervalTicks(0, 52, TimeInterval.Week));

        //    Assert.Equal(baseTime.AddDays(7), baseTime.AddIntervalTicks(1, 1, TimeInterval.Week));

        //    // Months ticks are in [1..12]
        //    Assert.Equal(baseTime.AddMonths(0), baseTime.AddIntervalTicks(0, 1, TimeInterval.Month));
        //    Assert.Equal(baseTime.AddMonths(1), baseTime.AddIntervalTicks(0, 2, TimeInterval.Month));
        //}
    }
}

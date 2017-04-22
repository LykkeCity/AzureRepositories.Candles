using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Model;

namespace AzureRepositories.Candles
{
    internal static class CandleItemExtensions
    {
        public static IFeedCandle ToCandle(this CandleItem candle, bool isBuy, DateTime baseTime, TimeInterval interval)
        {
            if (candle != null)
            {
                return new FeedCandle()
                {
                    Open = candle.Open,
                    Close = candle.Close,
                    High = candle.High,
                    Low = candle.Low,
                    IsBuy = isBuy,
                    DateTime = baseTime.AddIntervalTicks(candle.Cell, candle.Tick, interval)
                };
            }
            return null;
        }
    }

    internal static class CandleExtensions
    {
        public static CandleItem ToItem(this IFeedCandle candle, TimeInterval interval)
        {
            return new CandleItem()
            {
                Open = candle.Open,
                Close = candle.Close,
                High = candle.High,
                Low = candle.Low,
                Tick = candle.DateTime.GetIntervalTick(interval),
                Cell = candle.DateTime.GetIntervalCell(interval)
            };
        }

        public static string PartitionKey(this IFeedCandle candle, string asset)
        {
            if (candle == null)
            {
                throw new ArgumentNullException(nameof(candle));
            }
            return CandleTableEntity.GeneratePartitionKey(asset);
        }

        public static string RowKey(this IFeedCandle candle, TimeInterval interval)
        {
            if (candle == null)
            {
                throw new ArgumentNullException(nameof(candle));
            }
            return CandleTableEntity.GenerateRowKey(candle.DateTime, candle.IsBuy, interval);
        }
    }

    internal static class CandleTableEntityExtensions
    {
        public static void MergeCandles(this CandleTableEntity entity, IEnumerable<IFeedCandle> candles, TimeInterval interval)
        {
            foreach (var candle in candles)
            {
                entity.MergeCandle(candle, interval);
            }
        }

        public static void MergeCandles(this CandleTableEntity entity, IEnumerable<CandleItem> candles, TimeInterval interval)
        {
            foreach (var candle in candles)
            {
                entity.MergeCandle(candle, interval);
            }
        }

        public static void MergeCandle(this CandleTableEntity entity, IFeedCandle candle, TimeInterval interval)
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            // 1. Check if candle with specified time already exist
            // 2. If found - merge, else - add to list
            //
            var cell = candle.DateTime.GetIntervalCell(interval);
            var tick = candle.DateTime.GetIntervalTick(interval);
            var existingCandle = entity.Candles.FirstOrDefault(ci => ci.Tick == tick && ci.Cell == cell);

            if (existingCandle != null)
            {
                // Merge in list
                var mergedCandle = existingCandle
                    .ToCandle(entity.IsBuy, entity.DateTime, interval)
                    .MergeWith(candle);

                entity.Candles.Remove(existingCandle);
                entity.Candles.Add(mergedCandle.ToItem(interval));
            }
            else
            {
                // Add to list
                entity.Candles.Add(candle.ToItem(interval));
            }
        }

        public static void MergeCandle(this CandleTableEntity entity, CandleItem candle, TimeInterval interval)
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }

            IFeedCandle fc = candle.ToCandle(entity.IsBuy, entity.DateTime, interval);
            entity.MergeCandle(fc, interval);
        }
    }
}

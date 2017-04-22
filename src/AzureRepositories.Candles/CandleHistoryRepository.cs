using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading;
using Common;

namespace AzureRepositories.Candles
{
    /// <summary>
    /// Implementation of ICandleHistoryRepository for Azure
    /// </summary>
    public sealed class CandleHistoryRepository : ICandleHistoryRepository
    {
        private readonly INoSQLTableStorage<CandleTableEntity> _tableStorage;

        public CandleHistoryRepository(INoSQLTableStorage<CandleTableEntity> tableStorage)
        {
            _tableStorage = tableStorage;
        }

        public async Task InsertOrMergeAsync(IFeedCandle candle, string asset, TimeInterval interval)
        {
            if (candle == null) { throw new ArgumentNullException(nameof(candle)); }
            if (string.IsNullOrEmpty(asset)) { throw new ArgumentNullException(nameof(asset)); }

            await InsertOrMergeAsync(new Dictionary<TimeInterval, IEnumerable<IFeedCandle>>()
            {
                { interval, new IFeedCandle[] { candle } }
            }, asset);
        }

        public async Task InsertOrMergeAsync(IEnumerable<IFeedCandle> candles, string asset, TimeInterval interval)
        {
            if (candles == null) { throw new ArgumentNullException(nameof(candles)); }
            if (string.IsNullOrEmpty(asset)) { throw new ArgumentNullException(nameof(asset)); }
            if (!candles.Any()) { return; }

            await InsertOrMergeAsync(new Dictionary<TimeInterval, IEnumerable<IFeedCandle>>()
            {
                { interval, candles }
            }, asset);
        }

        public async Task InsertOrMergeAsync(IReadOnlyDictionary<TimeInterval, IEnumerable<IFeedCandle>> dict, string asset)
        {
            if (dict == null) { throw new ArgumentNullException(nameof(dict)); }
            if (string.IsNullOrEmpty(asset)) { throw new ArgumentNullException(nameof(asset)); }
            if (!dict.Any() && dict.Values.All(e => e != null) && dict.Values.Any(e => e.Count() > 0)) { return; }

            var partitionKey = CandleTableEntity.GeneratePartitionKey(asset);
            var rowKeys = new List<string>();
            var fields = new HashSet<string>(); // which fields to read from table

            // 1. Read all { pkey, rowkey } rows
            //
            var updateEntities = new List<CandleTableEntity>();

            foreach (var interval in dict.Keys)
            {
                var candles = dict[interval];
                if (candles != null && candles.Any())
                {
                    // Inside one interval group all candles by distinct row
                    var groups = candles.GroupBy(candle => candle.RowKey(interval));

                    rowKeys.AddRange(groups.Select(g => g.Key));

                    foreach (var group in groups)
                    {
                        // Create entity with candles and add it to list
                        var e = new CandleTableEntity(partitionKey, group.Key);  // group.Key = rowKey
                        e.MergeCandles(group, interval);
                        updateEntities.Add(e);

                        // update field
                        var dates = group.Select(c => c.DateTime);
                        fields.UnionWith(CandleTableEntity.GetStoreFields(interval, dates.Min(), dates.Max()));
                    }
                }
            }

            // ... prepare get query
            // ... partitionKey = ? AND (rokey = ? OR rowkey = ? OR rowkey = ? OR ...)
            TableQuery<CandleTableEntity> query = new TableQuery<CandleTableEntity>();
            string pkeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);

            var rowkeyFilters = rowKeys.Select(rowKey => TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));
            var rowkeyFilter = rowkeyFilters.Aggregate((cond1, cond2) => TableQuery.CombineFilters(cond1, TableOperators.Or, cond2));
            query.FilterString = TableQuery.CombineFilters(pkeyFilter, TableOperators.And, rowkeyFilter);
            query.SelectColumns = fields.ToList();

            // ... reading rows from azure table
            List<CandleTableEntity> origEntities = new List<CandleTableEntity>(1);
            await _tableStorage.ScanDataAsync(query, list =>
            {
                origEntities.AddRange(list);
                return Task.FromResult(0);
            });

            // 2. Update rows (merge entities)
            //
            var listToUpdate = new List<CandleTableEntity>();
            foreach (var updateEntity in updateEntities)
            {
                var origEntity = origEntities.Where(e => e.PartitionKey == updateEntity.PartitionKey && e.RowKey == updateEntity.RowKey).FirstOrDefault();
                if (origEntity != null)
                {
                    origEntity.MergeCandles(updateEntity.Candles, updateEntity.Interval);
                    listToUpdate.Add(origEntity);
                }
                else
                {
                    listToUpdate.Add(updateEntity);
                }
            }
            
            // 3. Write rows in batch
            // ... Only 100 records with the same pKey can be updated in one batch operation
            foreach (var collection in listToUpdate.ToPieces(100))
            {
                await _tableStorage.InsertOrMergeBatchAsync(collection);
            }
        }

        public async Task<IFeedCandle> GetCandleAsync(string asset, TimeInterval interval, bool isBuy, DateTime dateTime)
        {
            if (string.IsNullOrEmpty(asset)) { throw new ArgumentNullException(nameof(asset)); }

            // 1. Get candle table entity
            string partitionKey = CandleTableEntity.GeneratePartitionKey(asset);
            string rowKey = CandleTableEntity.GenerateRowKey(dateTime, isBuy, interval);

            //CandleTableEntity entity = await _tableStorage.GetDataAsync(partitionKey, rowKey);

            //---------------
            TableQuery<CandleTableEntity> query = new TableQuery<CandleTableEntity>();
            string pkeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            string rowkeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey);
            query.FilterString = TableQuery.CombineFilters(pkeyFilter, TableOperators.And, rowkeyFilter);
            query.SelectColumns = CandleTableEntity.GetStoreFields(interval, dateTime);

            List<CandleTableEntity> entities = new List<CandleTableEntity>(1);
            await _tableStorage.ScanDataAsync(query, list =>
            {
                entities.AddRange(list);
                return Task.FromResult(0);
            });
            //-----------

            // 2. Find required candle in candle list by tick
            if (entities.Count > 0)
            {
                var cell = dateTime.GetIntervalCell(interval);
                var tick = dateTime.GetIntervalTick(interval);
                var candleItem = entities[0].Candles.FirstOrDefault(ci => ci.Tick == tick && ci.Cell == cell);
                return candleItem.ToCandle(isBuy, entities[0].DateTime, interval);
            }
            return null;
        }

        public async Task<IEnumerable<IFeedCandle>> GetCandlesAsync(string asset, TimeInterval interval, bool isBuy, DateTime from, DateTime to)
        {
            if (string.IsNullOrEmpty(asset)) { throw new ArgumentNullException(nameof(asset)); }

            string partitionKey = CandleTableEntity.GeneratePartitionKey(asset);
            string rowKeyFrom = CandleTableEntity.GenerateRowKey(from, isBuy, interval);
            string rowKeyTo = CandleTableEntity.GenerateRowKey(to, isBuy, interval);

            //IEnumerable<CandleTableEntity> candleEntities = await _tableStorage.WhereAsync(partitionKey, from, to, ToIntervalOption.ExcludeTo, null, includeTime: true);

            //---------------
            TableQuery<CandleTableEntity> query = new TableQuery<CandleTableEntity>();
            string pkeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            string fromFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, rowKeyFrom);
            string toFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, rowKeyTo);

            query.FilterString = TableQuery.CombineFilters(pkeyFilter, TableOperators.And,
                TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter));

            List<CandleTableEntity> entities = new List<CandleTableEntity>(1);
            await _tableStorage.ScanDataAsync(query, list =>
            {
                entities.AddRange(list);
                return Task.FromResult(0);
            });
            //-----------

            var result = from e in entities
                         select e.Candles.Select(ci => ci.ToCandle(e.IsBuy, e.DateTime, interval));

            return result
                .SelectMany(c => c)
                .Where(c => c.DateTime >= from && c.DateTime < to);
        }
    }
}

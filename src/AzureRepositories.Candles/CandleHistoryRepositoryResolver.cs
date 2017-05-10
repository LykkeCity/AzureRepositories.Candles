using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Lykke.Domain.Prices;
using Lykke.Domain.Prices.Contracts;
using Lykke.Domain.Prices.Repositories;

namespace AzureRepositories.Candles
{
    public delegate INoSQLTableStorage<CandleTableEntity> CreateStorage(string asset, string tableName);

    public class CandleHistoryRepositoryResolver : ICandleHistoryRepository
    {
        private readonly CreateStorage _createStorage;
        private readonly Dictionary<string, CandleHistoryRepository> _repoTable = new Dictionary<string, CandleHistoryRepository>();
        private readonly object _sync = new object();

        public CandleHistoryRepositoryResolver(CreateStorage createStorage)
        {
            _createStorage = createStorage;
        }

        /// <summary>
        /// Insert or merge candle value.
        /// </summary>
        public async Task InsertOrMergeAsync(IFeedCandle feedCandle, string asset, TimeInterval interval, PriceType priceType)
        {
            ValidateAndThrow(asset, interval, priceType);
            var repo = GetRepo(asset, interval);
            await repo.InsertOrMergeAsync(feedCandle, priceType, interval);
        }

        /// <summary>
        /// Insert or merge candle value.
        /// </summary>
        public async Task InsertOrMergeAsync(IEnumerable<IFeedCandle> candles, string asset, TimeInterval interval, PriceType priceType)
        {
            ValidateAndThrow(asset, interval, priceType);
            var repo = GetRepo(asset, interval);
            await repo.InsertOrMergeAsync(candles, priceType, interval);
        }

        /// <summary>
        /// Returns buy or sell candle value for the specified interval in the specified time.
        /// </summary>
        public async Task<IFeedCandle> GetCandleAsync(string asset, TimeInterval interval, PriceType priceType, DateTime dateTime)
        {
            ValidateAndThrow(asset, interval, priceType);
            var repo = GetRepo(asset, interval);
            return await repo.GetCandleAsync(priceType, interval, dateTime);
        }

        /// <summary>
        /// Returns buy or sell candle values for the specified interval from the specified time range.
        /// </summary>
        public async Task<IEnumerable<IFeedCandle>> GetCandlesAsync(string asset, TimeInterval interval, PriceType priceType, DateTime from, DateTime to)
        {
            ValidateAndThrow(asset, interval, priceType);
            var repo = GetRepo(asset, interval);
            return await repo.GetCandlesAsync(priceType, interval, from, to);
        }

        private CandleHistoryRepository GetRepo(string asset, TimeInterval interval)
        {
            string tableName = interval.ToString().ToLowerInvariant();
            string key = asset.ToLowerInvariant() + "_" + tableName;
            CandleHistoryRepository repo;
            if (!_repoTable.TryGetValue(key, out repo))
            {
                lock (_sync)
                {
                    if (!_repoTable.TryGetValue(key, out repo))
                    {
                        repo = new CandleHistoryRepository(_createStorage(asset, tableName));
                        _repoTable.Add(key, repo);
                    }
                }
            }
            return repo;
        }

        private void ValidateAndThrow(string asset, TimeInterval interval, PriceType priceType)
        {
            if (string.IsNullOrEmpty(asset))
            {
                throw new ArgumentNullException(nameof(asset));
            }
            if (interval == TimeInterval.Unspecified)
            {
                throw new ArgumentOutOfRangeException(nameof(interval), "Time interval is not specified");
            }
            if (priceType == PriceType.Unspecified)
            {
                throw new ArgumentOutOfRangeException(nameof(priceType), "Price type is not specified");
            }
        }
    }
}

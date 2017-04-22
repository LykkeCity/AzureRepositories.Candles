using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Domain.Prices.Repositories;

namespace AzureRepositories.Candles
{
    public class RepoFactory
    {
        public ICandleHistoryRepository CreateCandleHistoryRepository(string connectionString, ILog log)
        {
            return new CandleHistoryRepository(new AzureTableStorage<CandleTableEntity>(connectionString, "CandlesHistory", log));
        }

        #region "Singleton implementation"

        private static volatile RepoFactory _instance;
        private static object _sync = new object();

        private RepoFactory()
        {
        }

        public static RepoFactory Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (_sync)
                    {
                        if (_instance == null)
                            _instance = new RepoFactory();
                    }
                }
                return _instance;
            }
        }

        #endregion
    }
}

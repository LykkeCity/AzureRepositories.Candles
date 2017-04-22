using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Autofac;
using Autofac.Builder;
using Common.Log;
using Lykke.Domain.Prices.Repositories;

namespace AzureRepositories.Candles
{
    using IRegistrationBuilder = IRegistrationBuilder<ICandleHistoryRepository, SimpleActivatorData, SingleRegistrationStyle>;

    public sealed class RepoBinder
    {
        public IRegistrationBuilder RegisterInstance(ContainerBuilder container, string connectionString, ILog log)
        {
            return container.RegisterInstance(
                RepoFactory.Instance.CreateCandleHistoryRepository(connectionString, log));
        }

        public IServiceCollection AddSingleton(IServiceCollection services, string connectionString, ILog log)
        {
            return services.AddSingleton(RepoFactory.Instance.CreateCandleHistoryRepository(connectionString, log));
        }

        #region "Singleton implementation"

        private static volatile RepoBinder _instance;
        private static object _sync = new object();

        private RepoBinder()
        {
        }

        public static RepoBinder Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (_sync)
                    {
                        if (_instance == null)
                            _instance = new RepoBinder();
                    }
                }
                return _instance;
            }
        }

        #endregion
    }
}

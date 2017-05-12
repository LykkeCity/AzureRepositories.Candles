using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Lykke.Domain.Prices;

namespace AzureRepositories.Candles
{
    public class CandleTableEntity : ITableEntity
    {
        public CandleTableEntity()
        {
        }

        public CandleTableEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        #region ITableEntity properties

        public string ETag { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }

        #endregion

        public DateTime DateTime
        {
            get
            {
                // extract from RowKey + Interval from PKey
                if (!string.IsNullOrEmpty(this.RowKey))
                {
                    return ParseRowKey(this.RowKey, DateTimeKind.Utc);
                }
                return default(DateTime);
            }
        }

        public PriceType PriceType
        {
            get
            {
                if (!string.IsNullOrEmpty(this.PartitionKey))
                {
                    PriceType value;
                    if (Enum.TryParse(this.PartitionKey, out value))
                    {
                        return value;
                    }
                }
                return PriceType.Unspecified;
            }
        }

        public List<CandleItem> Candles { get; set; } = new List<CandleItem>();

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this.Candles.Clear();

            EntityProperty property;
            if (properties.TryGetValue("Data", out property))
            {
                string json = property.StringValue;
                if (!string.IsNullOrEmpty(json))
                {
                    this.Candles.AddRange(JsonConvert.DeserializeObject<List<CandleItem>>(json));
                }
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            // Serialize candles
            string json = JsonConvert.SerializeObject(this.Candles);

            var dict = new Dictionary<string, EntityProperty>();
            dict.Add("Data", new EntityProperty(json));
            return dict;
        }

        public static string GeneratePartitionKey(PriceType priceType)
        {
            return $"{priceType}";
        }

        public static string GenerateRowKey(DateTime date, TimeInterval interval)
        {
            DateTime time;
            switch (interval)
            {
                case TimeInterval.Month: time = new DateTime(date.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc); break;
                case TimeInterval.Week: time = DateTimeUtils.GetFirstWeekOfYear(date); break;
                case TimeInterval.Day: time = new DateTime(date.Year, date.Month, 1, 0, 0, 0, DateTimeKind.Utc); break;
                case TimeInterval.Hour12:
                case TimeInterval.Hour6:
                case TimeInterval.Hour4:
                case TimeInterval.Hour: time = new DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc); break;
                case TimeInterval.Min30:
                case TimeInterval.Min15:
                case TimeInterval.Min5:
                case TimeInterval.Minute: time = new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0, DateTimeKind.Utc); break;
                case TimeInterval.Sec: time = new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, 0, DateTimeKind.Utc); break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
            }
            return FormatRowKey(time);
        }

        private static string FormatRowKey(DateTime dateUtc)
        {
            return dateUtc.ToString("s"); // sortable format
        }

        private static DateTime ParseRowKey(string value, DateTimeKind kind)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentNullException(nameof(value));
            }

            return DateTime.ParseExact(value, "s", System.Globalization.DateTimeFormatInfo.InvariantInfo);
        }
    }
}

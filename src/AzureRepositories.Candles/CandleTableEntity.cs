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
                    string[] splits = this.RowKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                    if (splits.Length > 2)
                    {
                        return new DateTime(long.Parse(splits[2]), DateTimeKind.Utc);
                    }
                }
                return default(DateTime);
            }
        }

        public string Asset
        {
            get
            {
                return this.PartitionKey ?? string.Empty;
                //// extract from PartitionKey
                //if (!string.IsNullOrEmpty(this.PartitionKey))
                //{
                //    string[] splits = this.PartitionKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                //    if (splits.Length > 0)
                //    {
                //        return splits[0];
                //    }
                //}
                //return string.Empty;
            }

        }
        public bool IsBuy
        {
            get
            {
                // extract from Partition key
                if (!string.IsNullOrEmpty(this.RowKey))
                {
                    string[] splits = this.RowKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                    if (splits.Length > 0)
                    {
                        return string.Compare(splits[0], "BUY", true) == 0;
                    }
                }
                return false;
            }
        }

        public TimeInterval Interval
        {
            get
            {
                // extract from Partition key
                if (!string.IsNullOrEmpty(this.RowKey))
                {
                    string[] splits = this.RowKey.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                    if (splits.Length > 1)
                    {
                        return (TimeInterval)Enum.Parse(typeof(TimeInterval), splits[1]);
                    }
                }
                return TimeInterval.Unspecified;
            }
        }

        public List<CandleItem> Candles { get; set; } = new List<CandleItem>();

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this.Candles.Clear();

            // Fields are expected to be: Data0, Data1, Data2, ... , DataN
            // Deserialize each field and initialize Cell property.
            //
            IEnumerable<string> dataFields = properties.Keys.Where(key => key.StartsWith("Data"));

            foreach (string dataField in dataFields)
            {
                EntityProperty property;
                int cell;
                if (properties.TryGetValue(dataField, out property) && Int32.TryParse(dataField.Substring(4), out cell))
                {
                    string json = property.StringValue;
                    if (!string.IsNullOrEmpty(json))
                    {
                        var items = JsonConvert.DeserializeObject<List<CandleItem>>(json);
                        items.ForEach(item => item.Cell = cell);
                        this.Candles.AddRange(items);
                    }
                }
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            // Serialize candles
            var dict = new Dictionary<string, EntityProperty>();

            // Group by cells
            var groups = this.Candles.GroupBy(c => c.Cell);

            // Update cells: Data0, Data1, ... DataN
            foreach (var group in groups)
            {
                dict.Add("Data" + group.Key, new EntityProperty(JsonConvert.SerializeObject(group)));
            }

            return dict;
        }

        public static string GeneratePartitionKey(string assetPairId)
        {
            return $"{assetPairId}";
        }

        public static string GenerateRowKey(DateTime date, bool isBuy, TimeInterval interval)
        {
            string time = "";
            switch (interval)
            {
                case TimeInterval.Month: time = new DateTime(date.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks.ToString("d19"); break;
                case TimeInterval.Week: time = DateTimeUtils.GetFirstWeekOfYear(date).Ticks.ToString("d19"); break;
                case TimeInterval.Day: time = new DateTime(date.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks.ToString("d19"); break;
                case TimeInterval.Hour12:
                case TimeInterval.Hour6:
                case TimeInterval.Hour4:
                case TimeInterval.Hour: time = new DateTime(date.Year, date.Month, 1, 0, 0, 0, DateTimeKind.Utc).Ticks.ToString("d19"); break;
                case TimeInterval.Min30:
                case TimeInterval.Min15:
                case TimeInterval.Min5:
                case TimeInterval.Minute: time = new DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc).Ticks.ToString("d19"); break;
                case TimeInterval.Sec: time = new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0, DateTimeKind.Utc).Ticks.ToString("d19"); break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
            }
            return $"{(isBuy ? "BUY" : "SELL")}_{interval}_" + time;
        }

        public static string[] GetStoreFields(TimeInterval interval, DateTime dateTime)
        {
            return GetStoreFields(interval, dateTime, dateTime);
        }

        /// <summary>
        /// Returns array of fields' names that store specified time period.
        /// </summary>
        /// <exception cref="System.ArgumentException"></exception>
        /// <exception cref="System.ArgumentOutOfRangeException"></exception>
        public static string[] GetStoreFields(TimeInterval interval, DateTime from, DateTime to)
        {
            if (from > to)
            {
                throw new ArgumentException("Date \"from\" should be less or equal than date \"to\".", nameof(from));
            }

            int fromIndex = 0;
            int toIndex = 0;

            switch (interval)
            {
                case TimeInterval.Sec:
                    if ((to - from).TotalHours > 1)
                    {
                        throw new ArgumentOutOfRangeException(string.Format("Date range exceeds one hour {{from: {0}, to:{1}}}.", from, to));
                    }
                    fromIndex = from.GetIntervalCell(interval);
                    toIndex = to.GetIntervalCell(interval);
                    break;
                case TimeInterval.Minute:
                case TimeInterval.Min5:
                case TimeInterval.Min15:
                case TimeInterval.Min30:
                    if ((to - from).TotalDays > 1)
                    {
                        throw new ArgumentOutOfRangeException(string.Format("Date range exceeds one day {{from: {0}, to:{1}}}.", from, to));
                    }
                    fromIndex = from.GetIntervalCell(interval);
                    toIndex = to.GetIntervalCell(interval);
                    break;
                case TimeInterval.Hour12:
                case TimeInterval.Hour6:
                case TimeInterval.Hour4:
                case TimeInterval.Hour:
                    if (from.Month != to.Month && from.Year != to.Year)
                    {
                        throw new ArgumentOutOfRangeException(string.Format("Date range exceeds one month {{from: {0}, to:{1}}}.", from, to));
                    }
                    fromIndex = from.GetIntervalCell(interval);
                    toIndex = to.GetIntervalCell(interval);
                    break;
                case TimeInterval.Day:
                    if (from.Year != to.Year)
                    {
                        throw new ArgumentOutOfRangeException(string.Format("Date range exceeds one year {{from: {0}, to:{1}}}.", from, to));
                    }
                    fromIndex = from.GetIntervalCell(interval);
                    toIndex = to.GetIntervalCell(interval);
                    break;
                case TimeInterval.Week:
                case TimeInterval.Month:
                    fromIndex = 0;
                    toIndex = 0;
                    break;
                default:
                    throw new ArgumentException("Unexpected interval value", nameof(interval));
            }
            return Enumerable.Range(fromIndex, (toIndex - fromIndex) + 1).Select(i => "Data" + i).ToArray();
        }
    }
}

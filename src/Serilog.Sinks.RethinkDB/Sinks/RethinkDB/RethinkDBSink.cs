// Copyright 2014 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.using System;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RethinkDb.Driver.Net;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.RethinkDB
{
    /// <summary>
    /// Writes log events as documents to a RethinkDB database.
    /// </summary>
    public class RethinkDBSink : PeriodicBatchingSink
    {
        private readonly string _databaseName;
        private readonly string _tableName;
        private static readonly RethinkDb.Driver.RethinkDB R = RethinkDb.Driver.RethinkDB.R;
        private readonly Connection _connection;
        /// <summary>
        /// A reasonable default for the number of events posted in
        /// each batch.
        /// </summary>
        public const int DefaultBatchPostingLimit = 50;

        /// <summary>
        /// A reasonable default time to wait between checking for event batches.
        /// </summary>
        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(2);

        /// <summary>
        /// The default hostname for the RethinkDb connecton
        /// </summary>
        public static readonly string DefaultHostName = "localhost";

        /// <summary>
        /// Default port number for the RethinkDB connection
        /// </summary>
        public static readonly int DefaultPort = 28015;

        /// <summary>
        /// The default name for the logging database.
        /// </summary>
        public static readonly string DefaultDbName = "logging";

        /// <summary>
        /// The default name for the log table.
        /// </summary>
        public static readonly string DefaultTableName = "log";

        /// <summary>
        /// Create new instance of the RethinkDB sink
        /// </summary>
        /// <param name="port"></param>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <param name="batchPostingLimit"></param>
        /// <param name="period"></param>
        /// <param name="hostname"></param>
        public RethinkDBSink(string hostname,
            int port,
            string databaseName,
            string tableName,
            int batchPostingLimit,
            TimeSpan period) : base(batchPostingLimit, period)
        {
            _databaseName = databaseName;
            _tableName = tableName;
            _connection = R.Connection().Hostname(hostname).Port(port).Connect();
        }
        private async void EnsureDatabaseAndTable()
        {
            var dbExists = await R.DbList().Contains(_databaseName).RunAsync(_connection);
            if (!dbExists)
                R.DbCreate(_databaseName);

            var tableExists = await R.Db(_databaseName).TableList().Contains(_tableName).RunAsync(_connection);

            if (!tableExists)
                R.Db(_databaseName).TableCreate(_tableName);
        }

        /// <summary>
        /// Emit a batch of log events, running asynchronously.
        /// </summary>
        /// <param name="events">The events to emit.</param>
        /// <remarks>
        /// Override either <see cref="M:Serilog.Sinks.PeriodicBatching.PeriodicBatchingSink.EmitBatch(System.Collections.Generic.IEnumerable{Serilog.Events.LogEvent})"/> or <see cref="M:Serilog.Sinks.PeriodicBatching.PeriodicBatchingSink.EmitBatchAsync(System.Collections.Generic.IEnumerable{Serilog.Events.LogEvent})"/>,
        ///             not both. Overriding EmitBatch() is preferred.
        /// </remarks>
        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            EnsureDatabaseAndTable();

            foreach (var logEvent in events)
            {
                await R.Db(_databaseName).Table(_tableName).Insert(new RethinkDbLogEvent
                {
                    Id = Guid.NewGuid(),
                    Timestamp = logEvent.Timestamp,
                    Message = logEvent.RenderMessage(),
                    MessageTemplate = logEvent.MessageTemplate.Text,
                    Level = logEvent.Level,
                    Exception = logEvent?.Exception?.ToString(),
                    Props = logEvent.Properties.ToDictionary<KeyValuePair<string, LogEventPropertyValue>, string, object>(k => k.Key, v => v.Value)
                }).RunAsync(_connection);
            }
        }
    }
}

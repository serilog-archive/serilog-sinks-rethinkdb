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
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RethinkDb;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.RethinkDB
{
    /// <summary>
    /// Writes log events as documents to a RethinkDB database.
    /// </summary>
    public class RethinkDBSink : PeriodicBatchingSink
    {
        private IConnectionFactory _connectionFactory;
        private IDatabaseQuery _db;
        private ITableQuery<RethinkDBLogEvent> _table;

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
        /// The default name for the logging database.
        /// </summary>
        public static readonly string DefaultDbName = "logging";

        /// <summary>
        /// The default name for the log table.
        /// </summary>
        public static readonly string DefaultTableName = "log";

        /// <summary>
        /// Construct a sink posting to the specified database.
        /// </summary>
        /// <param name="connectionFactory">The connection factory for connecting to RethinkDB.</param>
        /// <param name="batchSizeLimit">The maximum number of events to post in a single batch.</param>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="databaseName">Name of the RethinkDB database to use for the log.</param>
        /// <param name="tableName">Name of the RethinkDB collection to use for the log. Default is "log".</param>
        public RethinkDBSink(IConnectionFactory connectionFactory, string databaseName, string tableName, int batchSizeLimit, TimeSpan period)
            : base(batchSizeLimit, period)
        {
            if (connectionFactory == null) throw new ArgumentNullException("connectionFactory");

            _connectionFactory = connectionFactory;

            _db = Query.Db(databaseName);
            _table = _db.Table<RethinkDBLogEvent>(tableName);

            EnsureDbCreated(databaseName, tableName);
        }

        /// <summary>
        /// Makes sure that the Database and Table is created in RethinkDB.
        /// </summary>
        /// <param name="databaseName">The name of the database to create in RethinkDB if it doesn't exist.</param>
        /// <param name="tableName">The name of the table to create in RethinkDB if it doesn't exist.</param>
        private void EnsureDbCreated(string databaseName, string tableName)
        {
            using (IConnection connection = _connectionFactory.Get())
            {
                if (!connection.Run(Query.DbList()).Contains(databaseName))
                {
                    connection.Run(Query.DbCreate(databaseName));
                }

                if (!connection.Run(_db.TableList()).Contains(tableName))
                {
                    connection.Run(_db.TableCreate(tableName));
                }
            }
        }

        /// <summary>
        /// Emit a batch of log events, running to completion asynchronously.
        /// </summary>
        /// <param name="events">The events to emit.</param>
        /// <remarks>Override either <see cref="PeriodicBatchingSink.EmitBatch"/> or <see cref="PeriodicBatchingSink.EmitBatchAsync"/>,
        /// not both.</remarks>
        protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
        {
            using (IConnection connection = await _connectionFactory.GetAsync())
            {
                var logs = events.Select(logEvent => new RethinkDBLogEvent
                {
                    Id = Guid.NewGuid(),
                    Message = logEvent.RenderMessage(),
                    MessageTemplate = logEvent.MessageTemplate.Text,
                    Level = logEvent.Level,
                    Timestamp = logEvent.Timestamp,
                    Exception = logEvent.Exception != null ? logEvent.Exception.ToString() : null,
                    Props = Convert(logEvent.Properties),
                });

                await connection.RunAsync(_table.Insert(logs));
            }
        }

        private static Dictionary<string, object> Convert(IEnumerable<KeyValuePair<string, LogEventPropertyValue>> properties)
        {
            return properties.ToDictionary<KeyValuePair<string, LogEventPropertyValue>, string, object>(property => property.Key, property => property.Value);
        }
    }
}

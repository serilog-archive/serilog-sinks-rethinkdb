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
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Sinks.RethinkDB;

namespace Serilog
{
    /// <summary>
    /// Adds the WriteTo.RethinkDB() extension method to <see cref="LoggerConfiguration"/>.
    /// </summary>
    public static class LoggerConfigurationRethinkDBExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="loggerConfiguration"></param>
        /// <param name="port"></param>
        /// <param name="databaseName"></param>
        /// <param name="tableName"></param>
        /// <param name="restrictedToMinimumLevel"></param>
        /// <param name="batchPostingLimit"></param>
        /// <param name="period"></param>
        /// <param name="hostname"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static LoggerConfiguration RethinkDB(
            this LoggerSinkConfiguration loggerConfiguration, 
            string hostname = null,
            int? port = null,
            string databaseName = null,
            string tableName = null,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            int batchPostingLimit = RethinkDBSink.DefaultBatchPostingLimit,
            TimeSpan? period = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException("loggerConfiguration");
            
            var rethinkDbSink = new RethinkDBSink(
                hostname ?? RethinkDBSink.DefaultHostName,
                port ?? RethinkDBSink.DefaultPort,
                databaseName ?? RethinkDBSink.DefaultDbName,
                tableName ?? RethinkDBSink.DefaultTableName, 
                batchPostingLimit,
                period ?? RethinkDBSink.DefaultPeriod
            );

            return loggerConfiguration.Sink(rethinkDbSink, restrictedToMinimumLevel);
        }
    }
}

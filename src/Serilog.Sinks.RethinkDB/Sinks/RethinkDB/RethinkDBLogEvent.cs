using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Serilog.Events;

namespace Serilog.Sinks.RethinkDB
{
    /// <summary>
    /// A log event.
    /// </summary>
    [DataContract]
    public class RethinkDBLogEvent
    {
        /// <summary>
        /// The Id of the event.
        /// </summary>
        [JsonProperty("id", NullValueHandling = NullValueHandling.Ignore)]
        public Guid Id;

        /// <summary>
        /// The time at which the event occurred.
        /// </summary>
        [JsonProperty]
        public DateTimeOffset Timestamp;

        /// <summary>
        /// The level of the event.
        /// </summary>
        [JsonProperty]
        public LogEventLevel Level;

        /// <summary>
        /// The message describing the event
        /// </summary>
        [JsonProperty]
        public string Message { get; set; }

        /// <summary>
        /// The message template describing the event.
        /// </summary>
        [JsonProperty]
        public string MessageTemplate;

        /// <summary>
        /// Properties associated with the event, including those presented in <see cref="LogEvent.MessageTemplate"/>.
        /// </summary>
        [JsonProperty]
        public Dictionary<string, object> Props;
        
        /// <summary>
        /// An exception associated with the event, or null.
        /// </summary>
        [JsonProperty]
        public string Exception;
    }
}
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace StompDotNet
{

    /// <summary>
    /// Base implementation of a factory for STOMP connections.
    /// </summary>
    public abstract class StompConnectionFactory
    {

        readonly StompConnectionOptions options;
        readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="logger"></param>
        protected StompConnectionFactory(StompConnectionOptions options, ILogger logger)
        {
            this.options = options ?? throw new System.ArgumentNullException(nameof(options));
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Opens a new connection to the specified endpoint.
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public abstract ValueTask<StompConnection> OpenAsync(EndPoint endpoint, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken);

        /// <summary>
        /// Opens the underlying connection.
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected async ValueTask<StompConnection> OpenAsync(StompTransport transport, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            var connection = new StompConnection(transport, options, logger);
            await connection.OpenAsync(cancellationToken);
            await connection.ConnectAsync(null, null, null, headers, cancellationToken);
            return connection;
        }

    }

}
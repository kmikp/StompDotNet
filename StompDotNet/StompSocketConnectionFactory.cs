using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using StompDotNet.Internal;

namespace StompDotNet
{

    /// <summary>
    /// Provides <see cref="StompConnection"/> instances for socket connections.
    /// </summary>
    public abstract class StompSocketConnectionFactory : StompConnectionFactory
    {

        readonly ProtocolType protocolType;
        readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="protocolType"></param>
        /// <param name="options"></param>
        /// <param name="logger"></param>
        public StompSocketConnectionFactory(ProtocolType protocolType, StompConnectionOptions options, ILogger logger) : base(options, logger)
        {
            this.protocolType = protocolType;
            this.logger = logger;
        }

        /// <summary>
        /// Opens a new connection to the specified endpoint.
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async ValueTask<StompConnection> OpenAsync(EndPoint endpoint, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            var ip = endpoint as IPEndPoint;
            if (ip == null)
                throw new StompException("A STOMP socket connection requires an IP endpoint.");

            var socket = new Socket(endpoint.AddressFamily, SocketType.Stream, protocolType);
            await socket.ConnectAsync(endpoint, cancellationToken);
            return await OpenAsync(new StompSocketTransport(ip, socket, new StompBinaryProtocol(), logger), headers, cancellationToken);
        }

    }
}
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;

namespace StompDotNet
{

    /// <summary>
    /// Provides <see cref="StompConnection"/> instances for web socket connections.
    /// </summary>
    public class StompWebSocketConnectionFactory : StompConnectionFactory
    {

        readonly Action<ClientWebSocketOptions> configure;
        readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="configure"></param>
        /// <param name="logger"></param>
        public StompWebSocketConnectionFactory(StompConnectionOptions options, Action<ClientWebSocketOptions> configure, ILogger logger) :
            base(options, logger)
        {
            this.configure = configure ?? throw new ArgumentNullException(nameof(configure));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="configure"></param>
        /// <param name="logger"></param>
        public StompWebSocketConnectionFactory(Action<ClientWebSocketOptions> configure, ILogger logger) :
            this(new StompConnectionOptions(), configure, logger)
        {

        }

        /// <summary>
        /// Converts the given endpoint to a 
        /// </summary>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        UriEndPoint ConvertToUriEndpoint(EndPoint endpoint)
        {
            if (endpoint is UriEndPoint u)
                return u;
            if (endpoint is DnsEndPoint d)
                return new UriEndPoint(new Uri($"ws://{d.Host}:{d.Port}"));

            return null;
        }

        /// <summary>
        /// Opens a new connection to the specified endpoint.
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async ValueTask<StompConnection> OpenAsync(EndPoint endpoint, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (endpoint is null)
                throw new ArgumentNullException(nameof(endpoint));

            var uri = ConvertToUriEndpoint(endpoint);
            if (uri == null)
                throw new StompException("A Stomp Web Socket connection requires a URI endpoint.");
            if (uri.Uri.Scheme != "ws" && uri.Uri.Scheme != "wss")
                throw new StompException("A Stomp Web Socket connection requires a 'ws' or 'wss' endpoint.");

            // open new web socket
            var socket = new ClientWebSocket();
            configure?.Invoke(socket.Options);
            await socket.ConnectAsync(uri.Uri, cancellationToken);

            // return new connection
            return await OpenAsync(new StompWebSocketTransport(endpoint, socket, new StompBinaryProtocol(), logger), headers, cancellationToken);
        }

    }

}
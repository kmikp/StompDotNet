using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StompDotNet.Sample.Console
{

    public class SampleService : IHostedService
    {

        readonly ILogger<SampleService> logger;

        StompConnection connection;
        StompTransaction transaction;
        CancellationTokenSource cts;
        Task run;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="logger"></param>
        public SampleService(ILogger<SampleService> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var f = new StompWebSocketConnectionFactory(c => { c.Credentials = new NetworkCredential("admin", "admin"); }, logger);
            var e = new UriEndPoint(new Uri("ws://localhost:44112/websockets/messaging/websocket"));
            connection = await f.OpenAsync(e, [], cancellationToken);
            transaction = await connection.BeginAsync(cancellationToken: cancellationToken);

            cts = new CancellationTokenSource();
            run = Task.Run(() => RunAsync(cts.Token));
        }

        async Task RunAsync(CancellationToken cancellationToken)
        {
            while (cancellationToken.IsCancellationRequested == false)
            {
                var r = new Guid("5b10d7c7-589f-428d-b23b-3f02458585f7");
                await using var s = await connection.SubscribeAsync($"/topic/tagBlinkLite.{r:D}", StompAckMode.ClientIndividual, cancellationToken: cancellationToken);

                while (await s.WaitToReadAsync(cancellationToken))
                    await HandleMessageAsync(await s.ReadAsync(cancellationToken), cancellationToken);
            }
        }

        async Task HandleMessageAsync(StompMessage message, CancellationToken cancellationToken)
        {
            logger.LogInformation("RECEIVED: {Message}", Encoding.UTF8.GetString(message.Body.Span));
            await message.AckAsync(transaction: transaction, cancellationToken: cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await connection.CloseAsync(cancellationToken);
            await connection.DisposeAsync();
        }

    }

}
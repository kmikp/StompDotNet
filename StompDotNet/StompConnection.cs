using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

using StompDotNet.Internal;

namespace StompDotNet
{

    /// <summary>
    /// Maintains a connection to a STOMP server.
    /// </summary>
    public class StompConnection :
#if NETSTANDARD2_1
        IAsyncDisposable,
#endif
        IDisposable
    {

        /// <summary>
        /// Describes a condition and a handle to resume when the condition is met.
        /// </summary>
        class FrameRouter
        {

            readonly Func<StompFrame, bool> match;
            readonly Func<StompFrame, CancellationToken, ValueTask<bool>> routeAsync;
            readonly Func<Exception, CancellationToken, ValueTask> abortAsync;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="match"></param>
            /// <param name="writeAsync"></param>
            /// <param name="abortAsync"></param>
            public FrameRouter(Func<StompFrame, bool> match, Func<StompFrame, CancellationToken, ValueTask<bool>> writeAsync, Func<Exception, CancellationToken, ValueTask> abortAsync)
            {
                this.match = match ?? throw new ArgumentNullException(nameof(match));
                this.routeAsync = writeAsync ?? throw new ArgumentNullException(nameof(writeAsync));
                this.abortAsync = abortAsync ?? throw new ArgumentNullException(nameof(abortAsync));
            }

            /// <summary>
            /// Filter to execute against received frames in order to decide whether to resume the handle.
            /// </summary>
            public Func<StompFrame, bool> Match => match;

            /// <summary>
            /// Action to be invoked when a matching frame is received. Returns <c>true</c> or <c>false</c> to signal whether to maintain the listener.
            /// </summary>
            public Func<StompFrame, CancellationToken, ValueTask<bool>> RouteAsync => routeAsync;

            /// <summary>
            /// Action to be invoked when the router is aborted.
            /// </summary>
            public Func<Exception, CancellationToken, ValueTask> AbortAsync => abortAsync;

        }

        readonly StompTransport transport;
        readonly StompConnectionOptions options;
        readonly ILogger logger;

        readonly AsyncLock stateLock = new AsyncLock();

        // channels that store frames as they move in and out of the system
        readonly Channel<StompFrame> recv = Channel.CreateUnbounded<StompFrame>();
        readonly Channel<StompFrame> send = Channel.CreateUnbounded<StompFrame>();

        // internal frame routing
        readonly LinkedList<FrameRouter> routers = new LinkedList<FrameRouter>();
        readonly AsyncLock routersLock = new AsyncLock();

        StompConnectionState state = StompConnectionState.Closed;
        Task runner;
        CancellationTokenSource runnerCts;
        StompVersion version;
        string session;
        int prevReceiptId = 0;
        int prevSubscriptionId = 0;
        int prevTransactionId = 0;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="options"></param>
        /// <param name="logger"></param>
        public StompConnection(StompTransport transport, StompConnectionOptions options, ILogger logger)
        {
            this.transport = transport ?? throw new ArgumentNullException(nameof(transport));
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Invoked when the state of the connection is changed.
        /// </summary>
        public event StompConnectionStateChangeEventHandler StateChanged;

        /// <summary>
        /// Invokes the StateChanged event.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        async ValueTask OnStateChanged(StompConnectionStateChangeArgs args)
        {
            if (args.CancellationToken.IsCancellationRequested == false)
                foreach (StompConnectionStateChangeEventHandler handler in StateChanged.GetInvocationList())
                    await handler(this, args);
        }

        /// <summary>
        /// Changes the state of the connection.
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        ValueTask SetState(StompConnectionState state, Exception exception = null, CancellationToken cancellationToken = default)
        {
            if (this.state == state)
                return ValueTaskHelper.CompletedTask;

            // update state and let any listeners know
            this.state = state;
            if (StateChanged != null)
                return OnStateChanged(new StompConnectionStateChangeArgs(state, exception, cancellationToken));

            return ValueTaskHelper.CompletedTask;
        }

        /// <summary>
        /// Starts the connection.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async Task OpenAsync(CancellationToken cancellationToken)
        {
            using (await stateLock.LockAsync(cancellationToken))
            {
                if (runner != null)
                    throw new StompException("Connection has already been started.");

                await SetState(StompConnectionState.Opening, null, cancellationToken);

                // ensure there are no listeners hanging around
                using (await routersLock.LockAsync(cancellationToken))
                    routers.Clear();

                // begin new run loop
                runnerCts = new CancellationTokenSource();
                runner = Task.Run(() => RunAsync(runnerCts.Token), CancellationToken.None);
                await SetState(StompConnectionState.Opened, null, cancellationToken);
            }
        }

        /// <summary>
        /// Shuts down the connection.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task ShutdownAsync(CancellationToken cancellationToken)
        {
            // disconnect from the transport
            try
            {
                // don't bother disconnecting if the runner has been shut down
                if (runner != null)
                {
                    // disconnect, but only allow 2 seconds
                    var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token;
                    var combine = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout).Token;
                    await DisconnectAsync(cancellationToken: cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // ignore, cancellation might cause connection drop and timeout
            }
            catch (StompConnectionAbortedException)
            {
                // ignore, connection ends up closed without receipt for disconnect
            }
            catch (StompException e)
            {
                logger.LogError(e, "Unexpected exception disconnecting from the STOMP server.");
            }

            // abort any outstanding routers, they'll never complete
            await AbortRoutersAsync(new StompConnectionAbortedException("STOMP connection has been aborted."), cancellationToken);

            // signal cancellation of runner
            runnerCts.Cancel();

            // if we're not already clear, wait on the runner to finish (can call itself during abort)
            if (runner != null)
                await runner;

            // clean up the runner state
            runner = null;
            runnerCts = null;

            // close and dispose of the transport
            await transport.CloseAsync(cancellationToken);
            await transport.DisposeAsync();
        }

        /// <summary>
        /// Stops the connection.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task CloseAsync(CancellationToken cancellationToken)
        {
            return CloseAsync(cancellationToken, false);
        }

        /// <summary>
        /// Stops the connection.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task CloseAsync(CancellationToken cancellationToken, bool disposing = false)
        {
            using (await stateLock.LockAsync(cancellationToken))
            {
                // during disposal, we don't want to do any work if we're already closed
                if (runner == null && disposing)
                    return;

                // but otherwise, we should throw an exception about the wrong state
                if (runner == null)
                    throw new StompException("Connection is not started.");

                await SetState(StompConnectionState.Closing, null, cancellationToken);
                await ShutdownAsync(cancellationToken);
                await SetState(StompConnectionState.Closed, null, cancellationToken);
            }
        }

        /// <summary>
        /// Aborts the connection.
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task AbortAsync(Exception exception, CancellationToken cancellationToken)
        {
            using (await stateLock.LockAsync(cancellationToken))
            {
                await SetState(StompConnectionState.Closing, null, cancellationToken);
                await AbortRoutersAsync(exception, cancellationToken);
                await ShutdownAsync(cancellationToken);
                await SetState(StompConnectionState.Aborted, exception, cancellationToken);
            }
        }

        /// <summary>
        /// Background processing for the connection.
        /// </summary>
        /// <returns></returns>
        async Task RunAsync(CancellationToken cancellationToken)
        {
            while (cancellationToken.IsCancellationRequested == false)
            {
                try
                {
                    var readTask = transport.ReceiveAsync(recv.Writer, cancellationToken).AsTask();
                    var recvTask = RunReceiveAsync(cancellationToken);
                    var sendTask = RunSendAsync(cancellationToken);
                    await Task.WhenAll(readTask, recvTask, sendTask);
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }
        }

        /// <summary>
        /// Processes outgoing items to the transport.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task RunSendAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (await send.Reader.WaitToReadAsync(cancellationToken))
                    await OnSendAsync(await send.Reader.ReadAsync(cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            catch (Exception e)
            {
                logger.LogError(e, "Exception during connection processing. Aborting connection.");

                try
                {
                    runner = null;
                    await AbortAsync(e, CancellationToken.None);
                }
                catch (Exception e2)
                {
                    logger.LogError(e2, "Exception received during abort.");
                }
            }
        }

        /// <summary>
        /// Handles outgoing messages to the transport.
        /// </summary>
        /// <param name="frame"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask OnSendAsync(StompFrame frame, CancellationToken cancellationToken)
        {
            if (frame.Command == StompCommand.Heartbeat)
                logger.LogTrace("Sending heartbeat");
            else
                logger.LogDebug("Sending STOMP frame: {Command}", frame.Command);

            await transport.SendAsync(frame, cancellationToken);
        }

        /// <summary>
        /// Processes incoming items from the transport.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task RunReceiveAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (await recv.Reader.WaitToReadAsync(cancellationToken))
                    await OnReceiveAsync(await recv.Reader.ReadAsync(cancellationToken), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            catch (Exception e)
            {
                logger.LogError(e, "Exception during connection processing. Aborting connection.");

                try
                {
                    runner = null;
                    await AbortAsync(e, CancellationToken.None);
                }
                catch (Exception e2)
                {
                    logger.LogError(e2, "Exception received during abort.");
                }
            }
        }

        /// <summary>
        /// Handles incoming received messages from the transport.
        /// </summary>
        /// <param name="frame"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask OnReceiveAsync(StompFrame frame, CancellationToken cancellationToken)
        {
            logger.LogDebug("Received STOMP frame: {Command}", frame.Command);

            // route the message to any registered routers
            using (await routersLock.LockAsync(cancellationToken))
            {
                if (TryGetRouterNode(frame, out var node))
                {
                    if (await TryRouteAsync(node.Value, frame, cancellationToken) == false)
                        if (node.List != null)
                            routers.Remove(node);

                    return;
                }
            }

            // unhandled ERROR frame
            if (frame.Command == StompCommand.Error)
                throw new StompErrorFrameException(frame);

            // unhandled frame
            throw new StompFrameException(frame);
        }

        /// <summary>
        /// Attempts to execute the listener and return whether or not it should be removed. If the listener throws an exception, it is logged and marked for removal.
        /// </summary>
        /// <param name="router"></param>
        /// <param name="frame"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask<bool> TryRouteAsync(FrameRouter router, StompFrame frame, CancellationToken cancellationToken)
        {
            try
            {
                return await router.RouteAsync(frame, cancellationToken);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Unhandled exception occurred dispatching frame to trigger.");
                return false;
            }
        }

        /// <summary>
        /// Invoked when the send or receive loops are finished.
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask AbortRoutersAsync(Exception exception, CancellationToken cancellationToken)
        {
            using (await routersLock.LockAsync(cancellationToken))
            {
                // send abort exception to all of the routers
                for (var node = routers.First; node != null; node = node.Next)
                    await TryAbortRouter(node.Value, exception, cancellationToken); // result does not matter, always error

                // clear the list of listeners
                routers.Clear();
            }
        }

        /// <summary>
        /// Attempts to pass an error to the router and return whether or not it should be removed.
        /// </summary>
        /// <param name="router"></param>
        /// <param name="exception"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask TryAbortRouter(FrameRouter router, Exception exception, CancellationToken cancellationToken)
        {
            try
            {
                await router.AbortAsync(exception, cancellationToken);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Unhandled exception occurred dispatching exception to listener.");
            }
        }

        /// <summary>
        /// Finds the node of the handle list that matches the given frame.
        /// </summary>
        /// <param name="frame"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        bool TryGetRouterNode(StompFrame frame, out LinkedListNode<FrameRouter> node)
        {
            for (node = routers.First; node != null; node = node.Next)
                if (node.Value.Match(frame))
                    return true;

            return false;
        }

        /// <summary>
        /// Sends the specified frame.
        /// </summary>
        /// <param name="frame"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask SendFrameAsync(StompFrame frame, CancellationToken cancellationToken)
        {
            if (state == StompConnectionState.Aborted)
                throw new StompConnectionAbortedException("Cannot send, connection in aborted state.");
            if (state != StompConnectionState.Opened && state != StompConnectionState.Closing)
                throw new StompException("Cannot send, connection is not open.");

            await send.Writer.WriteAsync(frame, cancellationToken);
        }

        /// <summary>
        /// Sends the specified frame, and registers a completion for the next frame that matches the specified conditions.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="headers"></param>
        /// <param name="body"></param>
        /// <param name="response"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask<StompFrame> SendFrameAndWaitAsync(StompCommand command, IEnumerable<KeyValuePair<string, string>> headers, ReadOnlyMemory<byte> body, Func<StompFrame, bool> response, CancellationToken cancellationToken)
        {
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();

            // schedule a router for the frame filter
            var tcs = new TaskCompletionSource<StompFrame>();
            ValueTask<bool> WriteAsync(StompFrame frame, CancellationToken cancellationToken) { tcs.SetResult(frame); return new ValueTask<bool>(false); }
            ValueTask AbortAsync(Exception exception, CancellationToken cancellationToken) { tcs.SetException(exception); return ValueTaskHelper.CompletedTask; }
            var hnd = new FrameRouter(response, WriteAsync, AbortAsync);

            // handle cancellation through new task
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

            // subscribe to matching frames
            var node = await RegisterRouterAsync(hnd, cancellationToken);

            try
            {
                // send initial frame and wait for resumption
                await SendFrameAsync(new StompFrame(command, headers, body), cancellationToken);
                return await tcs.Task;
            }
            finally
            {
                // ensure listener is removed upon completion
                if (node.List != null)
                    using (await routersLock.LockAsync())
                        if (node.List != null)
                            routers.Remove(node);
            }
        }

        /// <summary>
        /// Sends the specified frame, and registers a completion for the response frame by receipt ID.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="headers"></param>
        /// <param name="body"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<StompFrame> SendFrameAndWaitWithReceiptAsync(StompCommand command, IEnumerable<KeyValuePair<string, string>> headers, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
        {
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();

            var receiptIdText = Interlocked.Increment(ref prevReceiptId).ToString();
            headers = headers.Prepend(new KeyValuePair<string, string>("receipt", receiptIdText));
            cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, new CancellationTokenSource(options.ReceiptTimeout).Token).Token;
            return SendFrameAndWaitAsync(command, headers, body, f => f.GetHeaderValue("receipt-id") == receiptIdText, cancellationToken);
        }

        /// <summary>
        /// Registers a trigger for a frame condition.
        /// </summary>
        /// <param name="trigger"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async ValueTask<LinkedListNode<FrameRouter>> RegisterRouterAsync(FrameRouter trigger, CancellationToken cancellationToken)
        {
            using (await routersLock.LockAsync(cancellationToken))
                return routers.AddLast(trigger);
        }

        /// <summary>
        /// Initiates the connection to the STOMP server.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="login"></param>
        /// <param name="passcode"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async Task ConnectAsync(string host, string login, string passcode, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();

            if (GetStompVersionHeader() is string acceptVersion)
                headers = headers.Prepend(new KeyValuePair<string, string>("accept-version", acceptVersion));
            if (host != null)
                headers = headers.Prepend(new KeyValuePair<string, string>("host", host));

            logger.LogInformation("Initating STOMP connection: Host={Host}", host);

            var result = await SendFrameAndWaitAsync(StompCommand.Connect, headers, null, frame => frame.Command == StompCommand.Connected || frame.Command == StompCommand.Error, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompException($"ERROR waiting for CONNECTED response: {Encoding.UTF8.GetString(result.Body.Span)}");
            if (result.Command != StompCommand.Connected)
                throw new StompException("Did not receive CONNECTED response.");

            version = result.GetHeaderValue("version") is string _version ? ParseStompVersionHeader(_version) : StompVersion.Stomp_1_0;
            session = result.GetHeaderValue("session");
            logger.LogInformation("STOMP connection established: Version={Version} Session={Session}", StompVersionHeaderToString(version), session);
        }

        /// <summary>
        /// Gets the version header value.
        /// </summary>
        /// <returns></returns>
        string GetStompVersionHeader()
        {
            var l = new List<string>(4);
            if (options.MaximumVersion >= StompVersion.Stomp_1_0)
                l.Add("1.0");
            if (options.MaximumVersion >= StompVersion.Stomp_1_1)
                l.Add("1.1");
            if (options.MaximumVersion >= StompVersion.Stomp_1_2)
                l.Add("1.2");

            return string.Join(",", l);
        }

        /// <summary>
        /// Converts a <see cref="StompVersion"/> into a string.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        string StompVersionHeaderToString(StompVersion version) => version switch
        {
            StompVersion.Stomp_1_0 => "1.0",
            StompVersion.Stomp_1_1 => "1.1",
            StompVersion.Stomp_1_2 => "1.2",
            _ => throw new NotImplementedException(),
        };

        /// <summary>
        /// Parses the version header value.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        StompVersion ParseStompVersionHeader(string value) => value switch
        {
            "1.0" => StompVersion.Stomp_1_0,
            "1.1" => StompVersion.Stomp_1_1,
            "1.2" => StompVersion.Stomp_1_2,
            _ => throw new NotImplementedException(),
        };

        /// <summary>
        /// Initiates a disconnection from the STOMP server.
        /// </summary>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async Task DisconnectAsync(IEnumerable<KeyValuePair<string, string>> headers = null, CancellationToken cancellationToken = default)
        {
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();

            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Disconnect, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive DISCONNECT receipt.");
        }

        /// <summary>
        /// Sends a message to a destination in the messaging system.
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="transaction"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask SendAsync(string destination, StompTransaction transaction = null, IEnumerable<KeyValuePair<string, string>> headers = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(destination))
                throw new ArgumentException($"'{nameof(destination)}' cannot be null or whitespace.", nameof(destination));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            if (transaction != null)
                headers = headers.Prepend(new KeyValuePair<string, string>("transaction", transaction.Id));
            headers = headers.Prepend(new KeyValuePair<string, string>("destination", destination));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Send, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive SEND receipt.");
        }

        public async ValueTask HeartbeatAsync(CancellationToken cancellationToken)
        {
            var frame = new StompFrame(StompCommand.Heartbeat, headers: null, body: null);
            await SendFrameAsync(frame, cancellationToken);
        }

        /// <summary>
        /// Creates a subscription where each individual message requires an acknowledgement. This implements the 'client-individual' STOMP ack method.
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask<StompSubscription> SubscribeAsync(string destination, StompAckMode ack, IEnumerable<KeyValuePair<string, string>> headers = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(destination))
                throw new ArgumentException($"'{nameof(destination)}' cannot be null or whitespace.", nameof(destination));

            var id = Interlocked.Increment(ref prevSubscriptionId).ToString();
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            headers = headers.Prepend(new KeyValuePair<string, string>("destination", destination));
            headers = headers.Prepend(new KeyValuePair<string, string>("id", id));
            headers = headers.Prepend(new KeyValuePair<string, string>("ack", GetAckModeHeaderValue(ack)));

            // establish a router for inbound messages, and direct them to a new channel for the subscription
            var channel = Channel.CreateUnbounded<StompFrame>();
            async ValueTask<bool> RouteAsync(StompFrame frame, CancellationToken cancellationToken) { await channel.Writer.WriteAsync(frame, cancellationToken); return true; }
            ValueTask AbortAsync(Exception exception, CancellationToken cancellationToken) { channel.Writer.Complete(exception); return ValueTaskHelper.CompletedTask; }
            var router = await RegisterRouterAsync(new FrameRouter(frame => frame.GetHeaderValue("subscription") == id, RouteAsync, AbortAsync), cancellationToken);

            // completes the channel and removes the listener
            async ValueTask CompleteAsync(Exception exception)
            {
                // remove the listener to stop sending
                if (router.List != null)
                    using (await routersLock.LockAsync(CancellationToken.None))
                        if (router.List != null)
                            routers.Remove(router);

                // signal to channel that we're done with messages
                channel.Writer.Complete(exception);
            }

            try
            {
                // send SUBSCRIBE command
                var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Subscribe, headers, null, cancellationToken);
                if (result.Command == StompCommand.Error)
                    throw new StompErrorFrameException(result);
                if (result.Command != StompCommand.Receipt)
                    throw new StompException("Did not receive SUBSCRIBE receipt.");
            }
            catch (Exception e)
            {
                // complete the channel with an error if we can't even establish it
                await CompleteAsync(e);
                throw;
            }

            // caller obtains a subscription reference that closes the channel upon completion
            return new StompSubscription(this, id, channel.Reader, CompleteAsync);
        }

        /// <summary>
        /// Translates an <see cref="StompAckMode"/> value into the header text.
        /// </summary>
        /// <param name="ack"></param>
        /// <returns></returns>
        string GetAckModeHeaderValue(StompAckMode ack) => ack switch
        {
            StompAckMode.Auto => "auto",
            StompAckMode.Client => "client",
            StompAckMode.ClientIndividual => "client-individual",
            _ => throw new NotImplementedException(),
        };

        /// <summary>
        /// Unsubscribes the specified subscription.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask UnsubscribeAsync(string id, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException($"'{nameof(id)}' cannot be null or empty.", nameof(id));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            headers = headers.Prepend(new KeyValuePair<string, string>("id", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Unsubscribe, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive UNSUBSCRIBE receipt.");
        }

        /// <summary>
        /// Unsubscribes the specified subscription.
        /// </summary>
        /// <param name="subscription"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask UnsubscribeAsync(StompSubscription subscription, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (subscription is null)
                throw new ArgumentNullException(nameof(subscription));

            await UnsubscribeAsync(subscription.Id, headers, cancellationToken);
            await subscription.CompleteAsync(null);
        }

        /// <summary>
        /// Acknowleges the consumption of a message.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask AckAsync(string id, StompTransaction transaction, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (id is null)
                throw new ArgumentNullException(nameof(id));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            if (transaction != null)
                headers = headers.Prepend(new KeyValuePair<string, string>("transaction", transaction.Id));
            headers = headers.Prepend(new KeyValuePair<string, string>("message-id", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Ack, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive ACK receipt.");
        }

        /// <summary>
        /// Informs the server that a message was not consumed.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="transaction"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask NackAsync(string id, StompTransaction transaction, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (id is null)
                throw new ArgumentNullException(nameof(id));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            if (transaction != null)
                headers = headers.Prepend(new KeyValuePair<string, string>("transaction", transaction.Id));
            headers = headers.Prepend(new KeyValuePair<string, string>("message-id", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Nack, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive NACK receipt.");
        }

        /// <summary>
        /// Begins a new STOMP transaction.
        /// </summary>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async ValueTask<StompTransaction> BeginAsync(IEnumerable<KeyValuePair<string, string>> headers = null, CancellationToken cancellationToken = default)
        {
            var id = Interlocked.Increment(ref prevTransactionId).ToString();
            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            headers = headers.Prepend(new KeyValuePair<string, string>("transaction", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Begin, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive BEGIN receipt.");

            return new StompTransaction(this, id);
        }

        /// <summary>
        /// Commmits the specified transaction.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask CommitAsync(string id, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException($"'{nameof(id)}' cannot be null or empty.", nameof(id));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            headers = headers.Prepend(new KeyValuePair<string, string>("transaction", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Commit, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive COMMIT receipt.");
        }

        /// <summary>
        /// Aborts the specified transaction.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="headers"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        internal async ValueTask AbortAsync(string id, IEnumerable<KeyValuePair<string, string>> headers, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(id))
                throw new ArgumentException($"'{nameof(id)}' cannot be null or empty.", nameof(id));

            headers ??= Enumerable.Empty<KeyValuePair<string, string>>();
            headers = headers.Prepend(new KeyValuePair<string, string>("transaction", id));
            var result = await SendFrameAndWaitWithReceiptAsync(StompCommand.Abort, headers, null, cancellationToken);
            if (result.Command == StompCommand.Error)
                throw new StompErrorFrameException(result);
            if (result.Command != StompCommand.Receipt)
                throw new StompException("Did not receive ABORT receipt.");
        }

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            await CloseAsync(CancellationToken.None, true);
        }

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        /// <returns></returns>
        public void Dispose()
        {
            Task.Run(() => DisposeAsync()).ConfigureAwait(false).GetAwaiter().GetResult();
        }

    }

}
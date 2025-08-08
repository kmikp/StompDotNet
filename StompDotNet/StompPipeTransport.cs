using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using StompDotNet.Internal;

namespace StompDotNet
{

    /// <summary>
    /// Describes a class capable of sending and receiving messages from a STOMP server.
    /// </summary>
    public abstract class StompPipeTransport : StompTransport
    {

        readonly StompBinaryProtocol protocol;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="protocol"></param>
        /// <param name="logger"></param>
        protected StompPipeTransport(StompBinaryProtocol protocol, ILogger logger) :
            base(logger)
        {
            this.protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        }

        /// <summary>
        /// Begins receiving data from the transport and invokes <paramref name="onReceiveAsync"/> when a frame is available.
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override ValueTask ReceiveAsync(ChannelWriter<StompFrame> writer, CancellationToken cancellationToken)
        {
            if (writer is null)
                throw new ArgumentNullException(nameof(writer));

            var pipe = new Pipe();
            var fill = FillPipeAsync(pipe.Writer, cancellationToken);
            var read = ReadPipeAsync(pipe.Reader, writer, cancellationToken);
            return new ValueTask(Task.WhenAll(fill, read));
        }

        /// <summary>
        /// Reads incoming data and writes it to the <see cref="PipeWriter"/>.
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public abstract Task FillPipeAsync(PipeWriter writer, CancellationToken cancellationToken);

        /// <summary>
        /// Reads buffered data and converts it to frames.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="writer"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        async Task ReadPipeAsync(PipeReader reader, ChannelWriter<StompFrame> writer, CancellationToken cancellationToken)
        {
            try
            {
                while (cancellationToken.IsCancellationRequested == false)
                {
                    var result = await reader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;

                    // the read was canceled
                    if (result.IsCanceled)
                        break;

                    // attempt to parse frame from data currently available
                    if (TryReadFrame(buffer, out var frame, out var position) == false)
                    {
                        reader.AdvanceTo(buffer.Start, buffer.End); // record no movement
                        continue;
                    }

                    // handle the received frame
                    await writer.WriteAsync(frame, cancellationToken);

                    // no more data remaining to be processed
                    if (result.IsCompleted)
                        writer.Complete();

                    // record that we read the frame's bytes
                    reader.AdvanceTo(position, result.Buffer.End);
                }
            }
            finally
            {
                await reader.CompleteAsync();
                writer.Complete();
            }
        }

        /// <summary>
        /// Attempts to read the next frame and reports back the position within the sequence.
        /// </summary>
        /// <param name="sequence"></param>
        /// <param name="frame"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        bool TryReadFrame(ReadOnlySequence<byte> sequence, out StompFrame frame, out SequencePosition position)
        {
            var reader = new SequenceReader<byte>(sequence);
            if (protocol.TryReadFrame(ref reader, out frame))
            {
                position = reader.Position;
                return true;
            }

            position = default;
            return false;
        }

    }

}
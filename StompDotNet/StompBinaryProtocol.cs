using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using StompDotNet.Internal;

namespace StompDotNet
{

    /// <summary>
    /// Provides for reading and writing of StompFrames across a binary connection.
    /// </summary>
    public class StompBinaryProtocol
    {

        static readonly char[] CHAR_CR = new char[] { '\r' };
        static readonly char[] CHAR_LF = new char[] { '\n' };
        static readonly char[] CHAR_COLON = new char[] { ':' };
        static readonly char[] CHAR_BACKSLASH = new char[] { '\\' };

        static readonly byte[] BYTE_NULL = new byte[] { 0x00 };
        static readonly byte[] BYTE_CR = Encoding.UTF8.GetBytes("\r");
        static readonly byte[] BYTE_LF = Encoding.UTF8.GetBytes("\n");
        static readonly byte[] BYTE_BACKSLASH = Encoding.UTF8.GetBytes("\\");
        static readonly byte[] BYTE_COLON = Encoding.UTF8.GetBytes(":");
        static readonly byte[] BYTE_R = Encoding.UTF8.GetBytes("r");
        static readonly byte[] BYTE_N = Encoding.UTF8.GetBytes("n");
        static readonly byte[] BYTE_C = Encoding.UTF8.GetBytes("c");

        static readonly byte[] COMMAND_CONNECT = Encoding.UTF8.GetBytes("CONNECT\n");
        static readonly byte[] COMMAND_STOMP = Encoding.UTF8.GetBytes("STOMP\n");
        static readonly byte[] COMMAND_CONNECTED = Encoding.UTF8.GetBytes("CONNECTED\n");
        static readonly byte[] COMMAND_SEND = Encoding.UTF8.GetBytes("SEND\n");
        static readonly byte[] COMMAND_SUBSCRIBE = Encoding.UTF8.GetBytes("SUBSCRIBE\n");
        static readonly byte[] COMMAND_UNSUBSCRIBE = Encoding.UTF8.GetBytes("UNSUBSCRIBE\n");
        static readonly byte[] COMMAND_ACK = Encoding.UTF8.GetBytes("ACK\n");
        static readonly byte[] COMMAND_NACK = Encoding.UTF8.GetBytes("NACK\n");
        static readonly byte[] COMMAND_BEGIN = Encoding.UTF8.GetBytes("BEGIN\n");
        static readonly byte[] COMMAND_DISCONNECT = Encoding.UTF8.GetBytes("DISCONNECT\n");
        static readonly byte[] COMMAND_MESSAGE = Encoding.UTF8.GetBytes("MESSAGE\n");
        static readonly byte[] COMMAND_RECEIPT = Encoding.UTF8.GetBytes("RECEIPT\n");
        static readonly byte[] COMMAND_ERROR = Encoding.UTF8.GetBytes("ERROR\n");

        /// <summary>
        /// Attempts to parse a single frame from the given sequence.
        /// </summary>
        /// <param name="sequence"></param>
        /// <param name="frame"></param>
        /// <returns></returns>
        public bool TryReadFrame(ref SequenceReader<byte> sequence, out StompFrame frame)
        {
            frame = new StompFrame(StompCommand.Unknown, null, null);

            StompCommand command;
            List<KeyValuePair<string, string>> headers = new List<KeyValuePair<string, string>>();
            Memory<byte> body = null;

            // attempt to read initial command line
            if (TryReadCommand(ref sequence, out command) == false)
                return false;

            // read headers
            while (sequence.End == false)
            {
                // read the current header
                if (TryReadHeader(ref sequence, out var header, command != StompCommand.Connect && command != StompCommand.Connected) == false)
                    return false;

                // null key indicates end of headers
                if (header.Key == null)
                    break;

                headers.Add(header);
            }

            // content length header can help us decide how far to read
            var contenLengthHeader = headers.FirstOrDefault(i => i.Key == "content-length");
            if (contenLengthHeader.Value != null)
            {
                if (int.TryParse(contenLengthHeader.Value, out var contentLength) == false)
                    throw new StompProtocolException("Invalid 'content-length'. Not an integer.");

                // allocate new buffer and copy body contents
                body = new byte[contentLength];
                if (sequence.TryCopyTo(body.Span) == false)
                    return false;
                sequence.Advance(contentLength);

                // body should be terminated by a null character
                if (sequence.TryRead(out var term) == false)
                    return false;
                if (term != 0x00)
                    throw new StompProtocolException("Body not terminated by null.");
            }
            else
            {
                if (sequence.TryReadTo(out ReadOnlySpan<byte> buffer, (byte)0x00) == false)
                    return false;

                // allocate new buffer and copy body contents
                body = new byte[buffer.Length];
                if (buffer.TryCopyTo(body.Span) == false)
                    return false;
            }

            frame = new StompFrame(command, headers, body);
            return true;
        }

        /// <summary>
        /// Attempts to read the next line from the sequence.
        /// </summary>
        /// <param name="sequence"></param>
        /// <param name="text"></param>
        /// <returns></returns>
        bool TryReadCommand(ref SequenceReader<byte> sequence, out StompCommand command)
        {
            command = StompCommand.Unknown;

            // read up to line ending
            if (sequence.TryReadTo(out ReadOnlySequence<byte> itemBytes, (byte)'\n') == false)
                return false;

            // Received heartbeat
            if (itemBytes.IsEmpty)
                return true;

            // if so, it should be the command name
            if (TryParseCommand(Encoding.UTF8.GetString(itemBytes).TrimEnd(), out command) == false)
                throw new StompProtocolException("Cannot parse STOMP command.");

            return true;
        }

        /// <summary>
        /// Parses the command line string.
        /// </summary>
        /// <param name="line"></param>
        /// <param name="command"></param>
        /// <returns></returns>
        bool TryParseCommand(string line, out StompCommand command)
        {
            switch (line)
            {
                case "CONNECT":
                    command = StompCommand.Connect;
                    return true;
                case "STOMP":
                    command = StompCommand.Stomp;
                    return true;
                case "CONNECTED":
                    command = StompCommand.Connected;
                    return true;
                case "SEND":
                    command = StompCommand.Send;
                    return true;
                case "SUBSCRIBE":
                    command = StompCommand.Subscribe;
                    return true;
                case "UNSUBSCRIBE":
                    command = StompCommand.Unsubscribe;
                    return true;
                case "ACK":
                    command = StompCommand.Ack;
                    return true;
                case "NACK":
                    command = StompCommand.Nack;
                    return true;
                case "BEGIN":
                    command = StompCommand.Begin;
                    return true;
                case "DISCONNECT":
                    command = StompCommand.Disconnect;
                    return true;
                case "MESSAGE":
                    command = StompCommand.Message;
                    return true;
                case "RECEIPT":
                    command = StompCommand.Receipt;
                    return true;
                case "ERROR":
                    command = StompCommand.Error;
                    return true;
                default:
                    command = StompCommand.Unknown;
                    return false;
            };
        }

        /// <summary>
        /// Attempts to read the next line from the sequence.
        /// </summary>
        /// <param name="sequence"></param>
        /// <param name="header"></param>
        /// <returns></returns>
        bool TryReadHeader(ref SequenceReader<byte> sequence, out KeyValuePair<string, string> header, bool escapeCrLfCol)
        {
            header = new KeyValuePair<string, string>(null, null);

            // read up to line end from separator
            if (sequence.TryReadTo(out ReadOnlySequence<byte> lineBytes, (byte)'\n') == false)
                return false;
            if (lineBytes.Length > 255)
                throw new StompProtocolException("Max size reached for header line.");

            var line = Encoding.UTF8.GetString(lineBytes).TrimEnd('\r');
            if (line.Length == 0)
                return true;

            var k = new ArrayBufferWriter<char>(32);
            var v = new ArrayBufferWriter<char>(32);

            // finite state machine, switch from escaped to non, append characters to buffer writer
            var e = false;
            var d = k;
            var z = (Span<char>)stackalloc char[1];

            foreach (var c in line)
            {
                if (e == false)
                {
                    switch (c)
                    {
                        case '\\':
                            e = true;
                            continue;
                        case ':':
                            d = v; // swap target buffer to value
                            break;
                        default:
                            z[0] = c;
                            d.Write(z);
                            break;
                    }
                }
                else
                {
                    switch (c)
                    {
                        case '\\':
                            d.Write(CHAR_BACKSLASH);
                            continue;
                        case 'r':
                            d.Write(CHAR_CR);
                            continue;
                        case 'n':
                            d.Write(CHAR_LF);
                            continue;
                        case 'c':
                            d.Write(CHAR_COLON);
                            continue;
                        default:
                            throw new StompProtocolException("Invalid escape sequence.");
                    }
                }
            }

            header = new KeyValuePair<string, string>(k.WrittenSpan.ToString(), v.WrittenSpan.ToString());
            return true;
        }

        /// <summary>
        /// Writes the specified frame to the writer.
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="frame"></param>
        public void Write(ArrayBufferWriter<byte> writer, StompFrame frame)
        {
            // write command
            switch (frame.Command)
            {
                case StompCommand.Connect:
                    writer.Write(COMMAND_CONNECT);
                    break;
                case StompCommand.Stomp:
                    writer.Write(COMMAND_STOMP);
                    break;
                case StompCommand.Connected:
                    writer.Write(COMMAND_CONNECTED);
                    break;
                case StompCommand.Send:
                    writer.Write(COMMAND_SEND);
                    break;
                case StompCommand.Subscribe:
                    writer.Write(COMMAND_SUBSCRIBE);
                    break;
                case StompCommand.Unsubscribe:
                    writer.Write(COMMAND_UNSUBSCRIBE);
                    break;
                case StompCommand.Ack:
                    writer.Write(COMMAND_ACK);
                    break;
                case StompCommand.Nack:
                    writer.Write(COMMAND_NACK);
                    break;
                case StompCommand.Begin:
                    writer.Write(COMMAND_BEGIN);
                    break;
                case StompCommand.Disconnect:
                    writer.Write(COMMAND_DISCONNECT);
                    break;
                case StompCommand.Message:
                    writer.Write(COMMAND_MESSAGE);
                    break;
                case StompCommand.Receipt:
                    writer.Write(COMMAND_RECEIPT);
                    break;
                case StompCommand.Error:
                    writer.Write(COMMAND_ERROR);
                    break;
                case StompCommand.Heartbeat:
                    break;
                default:
                    throw new StompProtocolException("Unknown command.");
            }

            // write headers
            foreach (var header in frame.Headers)
            {
                WriteEscapedHeaderValue(writer, header.Key);
                writer.Write(BYTE_COLON);
                WriteEscapedHeaderValue(writer, header.Value);
                writer.Write(BYTE_LF);
            }

            // headers separated from body by a new line
            writer.Write(BYTE_LF);

            // body followed by NULL
            if (frame.Command != StompCommand.Heartbeat)
            {
                writer.Write(frame.Body.Span);
                writer.Write(BYTE_NULL);
            }
        }

        void WriteEscapedHeaderValue(ArrayBufferWriter<byte> writer, string value)
        {
            var z = (Span<char>)stackalloc char[1];

            foreach (var c in value)
            {
                switch (c)
                {
                    case '\r':
                        writer.Write(BYTE_BACKSLASH);
                        writer.Write(BYTE_R);
                        break;
                    case '\n':
                        writer.Write(BYTE_BACKSLASH);
                        writer.Write(BYTE_N);
                        break;
                    case ':':
                        writer.Write(BYTE_BACKSLASH);
                        writer.Write(BYTE_C);
                        break;
                    case '\\':
                        writer.Write(BYTE_BACKSLASH);
                        writer.Write(BYTE_BACKSLASH);
                        break;
                    default:
                        z[0] = c;
                        Encoding.UTF8.GetBytes(z, writer);
                        break;
                }
            }
        }

    }

}
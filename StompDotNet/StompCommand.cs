namespace StompDotNet
{

    public enum StompCommand
    {
        Unknown,
        Connect,
        Stomp,
        Connected,
        Send,
        Subscribe,
        Unsubscribe,
        Ack,
        Nack,
        Begin,
        Commit,
        Abort,
        Disconnect,
        Message,
        Receipt,
        Error,
        Heartbeat
    }

}
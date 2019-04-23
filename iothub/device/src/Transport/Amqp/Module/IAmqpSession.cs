namespace Microsoft.Azure.Devices.Client.Transport.Amqp
{ 
    internal interface IAmqpSession
    {
        IAmqpLink AllocatAmqpLink();
    }
}

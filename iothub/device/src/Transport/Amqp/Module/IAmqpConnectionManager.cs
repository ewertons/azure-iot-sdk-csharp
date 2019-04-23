namespace Microsoft.Azure.Devices.Client.Transport.Amqp
{
    internal interface IAmqpConnectionManager
    {
        IAmqpConnection AllocateAmqpConnection();
    }
}

namespace Microsoft.Azure.Devices.Client.Transport.Amqp

{
    internal interface IAmqpConnection
    {
        IAmqpSession AllocateAmqpSession();
    }
}

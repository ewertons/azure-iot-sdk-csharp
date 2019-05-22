namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpConnectionResourceHolderManager
    {
        IAmqpConnectionResourceHolder AllocateAmqpConnectionResourceHolder(DeviceIdentity deviceIdentity);
    }
}

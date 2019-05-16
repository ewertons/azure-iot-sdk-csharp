namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpConnectionResourceHolder : IResourceHolder<IAmqpConnectionResource>, IResourceAllocator<IAmqpSessionResource>
    {
    }
}

using Microsoft.Azure.Devices.Client.Transport.Stateful;

namespace Microsoft.Azure.Devices.Client.Transport.Amqp
{
    internal interface IAmqpConnectionResource : IResourceHolder, IResourceAllocator
    {
    }
}

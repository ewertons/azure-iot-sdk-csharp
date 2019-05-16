using System;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IAmqpConnectionResource : IResource, IResourceAllocator<IAmqpSessionResource>
    {
        Task<IAmqpSessionResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpSessionResource> resourceStatusListener, TimeSpan timeout);
    }
}

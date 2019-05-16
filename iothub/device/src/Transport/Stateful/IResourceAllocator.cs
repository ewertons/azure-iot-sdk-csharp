using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceAllocator<T> where T : IResource
    {
        Task<T> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<T> resourceStatusListener, TimeSpan timeout);
    }
}

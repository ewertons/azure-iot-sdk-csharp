using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceHolder : IResource, IResourceStatusListener, IDisposable
    {
        Task<IResource> AllocateOrRetrieveResourceAsync(TimeSpan timeout);
    }
}

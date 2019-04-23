using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceAllocator
    {
        Task<IResource> AllocateResourceAsync(TimeSpan timeout);
        void OnResourceOperationStatusChange(IResource resource, OperationStatus operationStatus);
    }
}

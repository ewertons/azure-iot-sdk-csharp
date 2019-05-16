using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IResourceHolder<T> : IDisposable where T : IResource
    {
        Task<T> OpenAsync(DeviceIdentity deviceIdentity, TimeSpan timeout);
        Task CloseAsync(TimeSpan timeout);
        Task<T> EnsureResourceAsync(DeviceIdentity deviceIdentity, TimeSpan timeout);
        void Abort();
    }
}

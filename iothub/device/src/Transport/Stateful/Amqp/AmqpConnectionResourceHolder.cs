using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpConnectionResourceHolder : ResourceHolder<IAmqpConnectionResource>, IAmqpConnectionResourceHolder
    {
        #region Members-Constructor
        internal AmqpConnectionResourceHolder(Action onResourceDisconnection) : base(AmqpConnectionAllocator.GetInstance(), onResourceDisconnection)
        {
        }
        #endregion

        #region IResourceAllocator<IAmqpSessionResource>
        public async Task<IAmqpSessionResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpSessionResource> resourceStatusListener, TimeSpan timeout)
        {
            IAmqpConnectionResource resource = await EnsureResourceAsync(deviceIdentity, timeout).ConfigureAwait(false);
            return await resource.AllocateResourceAsync(deviceIdentity, resourceStatusListener, timeout).ConfigureAwait(false);
        }
        #endregion
    }
}

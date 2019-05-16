using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpSendingLinkResourceHolder : ResourceHolder<IAmqpSendingLinkResource>
    {
        #region Members-Constructor
        public AmqpSendingLinkResourceHolder(
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder, 
            AmqpLinkSettings amqpLinkSettings, 
            Action onResourceDisconnection) : base(new AmqpSendingLinkResourceAllocator(amqpSessionResourceHolder, amqpLinkSettings), onResourceDisconnection)
        {
        }
        #endregion

        #region IResourceAllocator<IAmqpSendingLinkResource>
        private class AmqpSendingLinkResourceAllocator : IResourceAllocator<IAmqpSendingLinkResource>
        {
            private readonly IResourceHolder<IAmqpSessionResource> _amqpSessionResourceHolder;
            private readonly AmqpLinkSettings _amqpLinkSettings;

            internal AmqpSendingLinkResourceAllocator(IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder, AmqpLinkSettings amqpLinkSettings)
            {
                _amqpSessionResourceHolder = amqpSessionResourceHolder;
                _amqpLinkSettings = amqpLinkSettings;
            }

            public async Task<IAmqpSendingLinkResource> AllocateResourceAsync(
                DeviceIdentity deviceIdentity,
                IResourceStatusListener<IAmqpSendingLinkResource> resourceStatusListener,
                TimeSpan timeout)
            {
                if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");

                IAmqpSessionResource amqpSessionResource = await _amqpSessionResourceHolder.EnsureResourceAsync(deviceIdentity, timeout).ConfigureAwait(false);
                _amqpLinkSettings.UpsertProperty(IotHubAmqpProperty.TimeoutName, timeout.TotalMilliseconds);
                IAmqpSendingLinkResource amqpSendingLinkResource = await amqpSessionResource.AllocateSendingLinkAsync(_amqpLinkSettings, resourceStatusListener, timeout).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Associate(amqpSessionResource, amqpSendingLinkResource, $"{nameof(AllocateResourceAsync)}");
                if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpSendingLinkResource, $"{nameof(AllocateResourceAsync)}");
                if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
                return amqpSendingLinkResource;
            }
        }
        #endregion
    }

}

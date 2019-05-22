using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpReceivingLinkResourceHolder : ResourceHolder<IAmqpReceivingLinkResource>, IAmqpReceivingLinkResourceHolder
    {
        #region Members-Constructor
        public AmqpReceivingLinkResourceHolder(
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            AmqpLinkSettings amqpLinkSettings,
            Action<AmqpMessage> messageListener,
            Action onResourceDisconnection
        ) : base(new AmqpReceivingLinkResourceAllocator(amqpSessionResourceHolder, amqpLinkSettings, messageListener), onResourceDisconnection)
        {
        }
        #endregion

        #region IAmqpReceivingLinkResourceHolder
        public void DisposeDelivery(AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, $"{nameof(DisposeDelivery)}");
            IAmqpReceivingLinkResource resource = _resource;
            if (resource.IsValid())
            {
                resource.DisposeDelivery(amqpMessage);
            }
            if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, $"{nameof(DisposeDelivery)}");
        }
        #endregion

        #region IResourceAllocator<IAmqpReceivingLinkResource>
        private class AmqpReceivingLinkResourceAllocator : IResourceAllocator<IAmqpReceivingLinkResource>
        {
            private readonly IResourceHolder<IAmqpSessionResource> _amqpSessionResourceHolder;
            private readonly AmqpLinkSettings _amqpLinkSettings;
            private readonly Action<AmqpMessage> _messageListener;

            internal AmqpReceivingLinkResourceAllocator(
                IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
                AmqpLinkSettings amqpLinkSettings,
                Action<AmqpMessage> messageListener
            )
            {
                _amqpSessionResourceHolder = amqpSessionResourceHolder;
                _amqpLinkSettings = amqpLinkSettings;
                _messageListener = messageListener;
            }

            public async Task<IAmqpReceivingLinkResource> AllocateResourceAsync(DeviceIdentity deviceIdentity, IResourceStatusListener<IAmqpReceivingLinkResource> resourceStatusListener, TimeSpan timeout)
            {
                if (Logging.IsEnabled) Logging.Enter(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
                IAmqpSessionResource amqpSessionResource = await _amqpSessionResourceHolder.EnsureResourceAsync(deviceIdentity, timeout).ConfigureAwait(false);
                IAmqpReceivingLinkResource amqpReceivingLinkResource = await amqpSessionResource.AllocateReceivingLinkAsync(
                    _amqpLinkSettings,
                    resourceStatusListener,
                    timeout
                ).ConfigureAwait(false);
                if (_messageListener != null)
                {
                    amqpReceivingLinkResource.RegisterMessageListener(_messageListener);
                }

                if (Logging.IsEnabled) Logging.Associate(amqpSessionResource, amqpReceivingLinkResource, $"{nameof(AllocateResourceAsync)}");
                if (Logging.IsEnabled) Logging.Associate(deviceIdentity, amqpReceivingLinkResource, $"{nameof(AllocateResourceAsync)}");
                if (Logging.IsEnabled) Logging.Exit(this, deviceIdentity, timeout, $"{nameof(AllocateResourceAsync)}");
                return amqpReceivingLinkResource;
            }
        }
        #endregion
    }
}

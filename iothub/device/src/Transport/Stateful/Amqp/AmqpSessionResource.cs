using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpSessionResource : IAmqpSessionResource
    {
        #region Members-Constructor
        internal static readonly Exception s_amqpSessionDisconnectedException = new IotHubCommunicationException("AMQP Session is disconnected.");

        private AmqpSession _amqpSession;
        private IAmqpAuthenticationRefresher _amqpAuthenticationRefresher;
        private bool _disposed;

        internal AmqpSessionResource(AmqpSession amqpSession, IAmqpAuthenticationRefresher amqpAuthenticationRefresher)
        {
            _amqpSession = amqpSession;
            _amqpAuthenticationRefresher = amqpAuthenticationRefresher;
            if (Logging.IsEnabled) Logging.Associate(this, _amqpSession, $"{nameof(AmqpSessionResource)}");
            if (Logging.IsEnabled) Logging.Associate(this, _amqpAuthenticationRefresher, $"{nameof(AmqpSessionResource)}");
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            _amqpAuthenticationRefresher?.Dispose();
            _amqpSession.SafeClose();
            if (Logging.IsEnabled) Logging.Exit(this, $"Aborting {this}", $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            return !(_amqpSession?.IsClosing() ?? true);
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;

            if (Logging.IsEnabled) Logging.Enter(this, disposing, $"{nameof(Dispose)}");
            if (disposing)
            {
                _amqpAuthenticationRefresher?.Dispose();
                _amqpSession.SafeClose();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region IAmqpSessionResource
        public async Task<IAmqpReceivingLinkResource> AllocateReceivingLinkAsync(
            AmqpLinkSettings amqpLinkSettings,
            IResourceStatusListener<IAmqpReceivingLinkResource> resourceStatusListener,
            TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpLinkSettings, timeout, $"{nameof(AllocateReceivingLinkAsync)}");
            if (!IsValid())
            {
                throw s_amqpSessionDisconnectedException;
            }

            ReceivingAmqpLink receivingAmqpLink = new ReceivingAmqpLink(amqpLinkSettings);

            try
            {

                receivingAmqpLink.AttachTo(_amqpSession);
                await receivingAmqpLink.OpenAsync(timeout).ConfigureAwait(false);

                IAmqpReceivingLinkResource amqpReceivingLinkResource = new AmqpReceivingLinkResource(receivingAmqpLink);
                receivingAmqpLink.Closed += (obj, args) =>
                {
                    resourceStatusListener?.OnResourceStatusChange(amqpReceivingLinkResource, ResourceStatus.Disconnected);
                };

                // safty check, incase connection was closed before event handler attached
                if (receivingAmqpLink.IsClosing())
                {
                    amqpReceivingLinkResource.Abort();
                    throw AmqpReceivingLinkResource.s_receivingAmqpLinkDisconnectedException;
                }

                if (Logging.IsEnabled) Logging.Exit(this, amqpLinkSettings, timeout, $"{nameof(AllocateReceivingLinkAsync)}");
                return amqpReceivingLinkResource;
            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_amqpSessionDisconnectedException;
                }
            }
        }

        public async Task<IAmqpSendingLinkResource> AllocateSendingLinkAsync(
            AmqpLinkSettings amqpLinkSettings,
            IResourceStatusListener<IAmqpSendingLinkResource> resourceStatusListener,
            TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpLinkSettings, timeout, $"{nameof(AllocateSendingLinkAsync)}");
            if (!IsValid())
            {
                throw s_amqpSessionDisconnectedException;
            }

            SendingAmqpLink sendingAmqpLink = new SendingAmqpLink(amqpLinkSettings);
            try
            {

                sendingAmqpLink.AttachTo(_amqpSession);
                await sendingAmqpLink.OpenAsync(timeout).ConfigureAwait(false);

                IAmqpSendingLinkResource amqpSendingLinkResource = new AmqpSendingLinkResource(sendingAmqpLink);
                sendingAmqpLink.Closed += (obj, args) =>
                {
                    resourceStatusListener?.OnResourceStatusChange(amqpSendingLinkResource, ResourceStatus.Disconnected);
                };

                // safty check, incase connection was closed before event handler attached
                if (sendingAmqpLink.IsClosing())
                {
                    amqpSendingLinkResource.Abort();
                    throw AmqpSendingLinkResource.s_sendingAmqpLinkDisconnectedException;
                }

                if (Logging.IsEnabled) Logging.Exit(this, amqpLinkSettings, timeout, $"{nameof(AllocateSendingLinkAsync)}");
                return amqpSendingLinkResource;
            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_amqpSessionDisconnectedException;
                }
            }
        }
        #endregion

    }
}

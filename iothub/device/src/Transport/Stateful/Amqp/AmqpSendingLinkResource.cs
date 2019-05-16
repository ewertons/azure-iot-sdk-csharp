using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpSendingLinkResource : IAmqpSendingLinkResource
    {
        #region Members-Constructor
        internal static readonly Exception s_sendingAmqpLinkDisconnectedException = new IotHubCommunicationException("AMQP sending link is disconnected");

        private SendingAmqpLink _sendingAmqpLink;
        private volatile bool _disposed;

        internal AmqpSendingLinkResource(SendingAmqpLink sendingAmqpLink)
        {
            _sendingAmqpLink = sendingAmqpLink;
            if (Logging.IsEnabled) Logging.Associate(this, _sendingAmqpLink, $"{nameof(AmqpSendingLinkResource)}");
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            _sendingAmqpLink.SafeClose();
            if (Logging.IsEnabled) Logging.Exit(this, $"Aborting {this}", $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            return !_sendingAmqpLink.IsClosing();
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
                _sendingAmqpLink.SafeClose();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region IAmqpSendingLinkResource 
        public async Task<Outcome> SendAmqpMessageAsync(AmqpMessage message, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(SendAmqpMessageAsync)}");
            try
            {
                Outcome outcome = await _sendingAmqpLink.SendMessageAsync(
                    message,
                    new ArraySegment<byte>(Guid.NewGuid().ToByteArray()),
                    AmqpConstants.NullBinary,
                    timeout
                ).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(SendAmqpMessageAsync)}");
                return outcome;
            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_sendingAmqpLinkDisconnectedException;
                }
            }
        }
        #endregion
    }
}

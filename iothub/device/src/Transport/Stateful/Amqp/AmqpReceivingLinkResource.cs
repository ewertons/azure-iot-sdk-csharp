using System;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpReceivingLinkResource : IAmqpReceivingLinkResource
    {
        #region Members-Constructor
        internal static readonly Exception s_receivingAmqpLinkDisconnectedException = new IotHubCommunicationException("AMQP receiving link is disconnected");
        private readonly ReceivingAmqpLink _receivingAmqpLink;
        private volatile bool _disposed;

        internal AmqpReceivingLinkResource(ReceivingAmqpLink receivingAmqpLink)
        {
            _receivingAmqpLink = receivingAmqpLink;
            if (Logging.IsEnabled) Logging.Associate(this, _receivingAmqpLink, $"{nameof(AmqpReceivingLinkResource)}");
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            _receivingAmqpLink.SafeClose();
            if (Logging.IsEnabled) Logging.Exit(this, $"Aborting {this}", $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            return !_receivingAmqpLink.IsClosing();
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
                _receivingAmqpLink.SafeClose();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region IAmqpReceivingLinkResource 
        public void DisposeDelivery(AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, $"{nameof(DisposeDelivery)}");
            try
            {
                _receivingAmqpLink.DisposeDelivery(amqpMessage, true, AmqpConstants.AcceptedOutcome);
            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw new IotHubCommunicationException("");
                }
            }
            if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, $"{nameof(DisposeDelivery)}");
        }

        public async Task<Outcome> DisposeMessageAsync(string lockToken, Outcome outcome, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(DisposeMessageAsync)}");
            try
            {
                ArraySegment<byte> deliveryTag = ConvertToDeliveryTag(lockToken);
                Outcome disposeOutcome = await _receivingAmqpLink.DisposeMessageAsync(deliveryTag, outcome, true, timeout).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(DisposeMessageAsync)}");
                return disposeOutcome;
            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_receivingAmqpLinkDisconnectedException;
                }
            }
        }

        public async Task<AmqpMessage> ReceiveAmqpMessageAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(ReceiveAmqpMessageAsync)}");
            try
            {
                AmqpMessage amqpMessage = await _receivingAmqpLink.ReceiveMessageAsync(timeout).ConfigureAwait(false);
                if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(ReceiveAmqpMessageAsync)}");
                return amqpMessage;

            }
            catch (InvalidOperationException)
            {
                if (IsValid())
                {
                    throw;
                }
                else
                {
                    throw s_receivingAmqpLinkDisconnectedException;
                }
            }
        }

        public void RegisterMessageListener(Action<AmqpMessage> messageListener)
        {
            if (Logging.IsEnabled) Logging.Enter(this, messageListener, $"{nameof(RegisterMessageListener)}");
            _receivingAmqpLink.RegisterMessageListener(messageListener);
            if (!IsValid())
            {
                throw s_receivingAmqpLinkDisconnectedException;
            }
            if (Logging.IsEnabled) Logging.Exit(this, messageListener, $"{nameof(RegisterMessageListener)}");
        }
        #endregion

        #region Private helper functions
        private static ArraySegment<byte> ConvertToDeliveryTag(string lockToken)
        {
            if (lockToken == null)
            {
                throw new ArgumentNullException("lockToken");
            }

            if (!Guid.TryParse(lockToken, out Guid lockTokenGuid))
            {
                throw new ArgumentException("Should be a valid Guid", "lockToken");
            }

            return new ArraySegment<byte>(lockTokenGuid.ToByteArray());
        }
        #endregion
    }
}

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Shared;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpTransport : IAmqpTransport
    {
        #region Members-Constructor

        private readonly DeviceIdentity _deviceIdentity;
        private readonly IAmqpConnectionResourceHolder _amqpConnectionResourceHolder;


        private readonly IResourceHolder<IAmqpSessionResource> _amqpSessionResourceHolder;
        // Same link name and path for IoT edge(called Events) and regular device(called Telemetry)
        private readonly IResourceHolder<IAmqpSendingLinkResource> _messageSendingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpSendingLinkResource> _methodsSendingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpSendingLinkResource> _twinSendingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpSendingLinkResource> _streamSendingLinkResourceHolder;
        
        // Different link name and path for IoT edge(called Events) and regular device(called C2D) 
        private readonly IResourceHolder<IAmqpReceivingLinkResource> _messageReceivingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpReceivingLinkResource> _methodsReceivingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpReceivingLinkResource> _twinReceivingLinkResourceHolder;
        private readonly IResourceHolder<IAmqpReceivingLinkResource> _streamReceivingLinkResourceHolder;

        private readonly object _stateLock;

        private bool _disposed;

        internal AmqpTransport(DeviceIdentity deviceIdentity, IAmqpConnectionResourceHolder amqpConnectionResourceHolder)
        {
            _deviceIdentity = deviceIdentity;
            _amqpConnectionResourceHolder = amqpConnectionResourceHolder;
            _amqpSessionResourceHolder = new AmqpSessionResourceHolder(_amqpConnectionResourceHolder, () => { /* TODO */ });

            string correlationId = Guid.NewGuid().ToString();
            _messageSendingLinkResourceHolder = IoTAmqpLinkFactory.CreateMessageSendingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, null);
            _methodsSendingLinkResourceHolder = IoTAmqpLinkFactory.CreateMethodsSendingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, null);
            _twinSendingLinkResourceHolder = IoTAmqpLinkFactory.CreateTwinSendingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, null);
            _streamSendingLinkResourceHolder = IoTAmqpLinkFactory.CreateStreamSendingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, null);

            _messageReceivingLinkResourceHolder = IoTAmqpLinkFactory.CreateMessageReceivingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, () => { /* TODO */});
            _methodsReceivingLinkResourceHolder = IoTAmqpLinkFactory.CreateMethodsReceivingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, (amqpMessage) => {/* TODO handle message*/}, () => { /* TODO */});
            _twinReceivingLinkResourceHolder = IoTAmqpLinkFactory.CreateTwinReceivingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, (amqpMessage) => {/* TODO handle message*/}, () => { /* TODO */});
            _streamReceivingLinkResourceHolder = IoTAmqpLinkFactory.CreateStreamReceivingLinkResourceHolder(_deviceIdentity, _amqpSessionResourceHolder, correlationId, (amqpMessage) => {/* TODO handle message*/}, () => { /* TODO */});

            _stateLock = new object();
            if (Logging.IsEnabled) Logging.Associate(this, _amqpConnectionResourceHolder, $"{nameof(AmqpTransport)}");
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, _amqpConnectionResourceHolder, $"{nameof(AmqpTransport)}");
        }
        #endregion

        #region Usability
        public bool IsUsable()
        {
            lock (_stateLock)
            {
                return !_disposed;
            }
        }
        #endregion

        #region Open-Close
        public async Task OpenAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(OpenAsync)}");
            await _messageSendingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(OpenAsync)}");
        }
        public async Task CloseAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(CloseAsync)}");
            await _messageSendingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _methodsSendingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _twinSendingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _streamSendingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _messageReceivingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _methodsReceivingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _twinReceivingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _streamReceivingLinkResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            await _amqpSessionResourceHolder.CloseAsync(timeout).ConfigureAwait(false);

            if (!_deviceIdentity.AmqpTransportSettings.AmqpConnectionPoolSettings.Pooling)
            {
                await _amqpConnectionResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            }
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(CloseAsync)}");
        }
        #endregion

        #region Message
        public async Task<Outcome> SendMessageAsync(AmqpMessage message, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(SendMessageAsync)}");
            IAmqpSendingLinkResource messageSendingLinkResource = await _messageSendingLinkResourceHolder.EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome outcome = await messageSendingLinkResource.SendAmqpMessageAsync(message, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(SendMessageAsync)}");
            return outcome;
        }
        public async Task<Message> ReceiveMessageAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(ReceiveMessageAsync)}");
            IAmqpReceivingLinkResource messageReceivingLinkResource = await _messageReceivingLinkResourceHolder.EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            AmqpMessage amqpMessage = await messageReceivingLinkResource.ReceiveAmqpMessageAsync(timeout).ConfigureAwait(false);
            Message message = null;
            if (amqpMessage != null)
            {
                message = new Message(amqpMessage)
                {
                    LockToken = new Guid(amqpMessage.DeliveryTag.Array).ToString()
                };
            }
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(ReceiveMessageAsync)}");
            return message;
        }

        public async Task<Outcome> DisposeMessageAsync(string lockToken, Outcome outcome, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(DisposeMessageAsync)}");
            IAmqpReceivingLinkResource messageReceivingLinkResource = await _messageReceivingLinkResourceHolder.EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome disposeOutcome = await messageReceivingLinkResource.DisposeMessageAsync(lockToken, outcome, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(DisposeMessageAsync)}");
            return disposeOutcome;
        }
        #endregion

        #region Event
        public async Task EnableEventReceiveAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableEventReceiveAsync)}");
            await _messageReceivingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableEventReceiveAsync)}");
        }
        #endregion

        #region Method
        public async Task EnableMethodsAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableMethodsAsync)}");
            await Task.WhenAll(
                _messageSendingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout),
                _messageReceivingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableMethodsAsync)}");
        }
        public async Task DisableMethodsAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(DisableMethodsAsync)}");
            await Task.WhenAll(
                _messageSendingLinkResourceHolder.CloseAsync(timeout),
                _messageReceivingLinkResourceHolder.CloseAsync(timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(DisableMethodsAsync)}");
        }
        public async Task<Outcome> SendMethodResponseAsync(AmqpMessage message, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(SendMethodResponseAsync)}");
            IAmqpSendingLinkResource methodsSendingLinkResource = await _methodsSendingLinkResourceHolder.EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome outcome = await methodsSendingLinkResource.SendAmqpMessageAsync(message, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(SendMethodResponseAsync)}");
            return outcome;
        }
        #endregion

        #region Twin
        public async Task EnableTwinPatchAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableTwinPatchAsync)}");
            await Task.WhenAll(
                _twinSendingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout),
                _twinReceivingLinkResourceHolder.OpenAsync(_deviceIdentity, timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableTwinPatchAsync)}");
        }
        public async Task<Outcome> SendTwinMessageAsync(AmqpMessage message, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(SendTwinMessageAsync)}");
            IAmqpSendingLinkResource twinSendingLinkResource = await _twinSendingLinkResourceHolder.EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome outcome = await twinSendingLinkResource.SendAmqpMessageAsync(message, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(SendTwinMessageAsync)}");
            return outcome;
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            _messageSendingLinkResourceHolder.Abort();
            _methodsSendingLinkResourceHolder.Abort();
            _twinSendingLinkResourceHolder.Abort();
            _streamSendingLinkResourceHolder.Abort();
            _messageReceivingLinkResourceHolder.Abort();
            _methodsReceivingLinkResourceHolder.Abort();
            _twinReceivingLinkResourceHolder.Abort();
            _streamReceivingLinkResourceHolder.Abort();
            _amqpSessionResourceHolder.Abort();

            if (!_deviceIdentity.AmqpTransportSettings.AmqpConnectionPoolSettings.Pooling)
            {
                _amqpConnectionResourceHolder.Abort();
            }

            if (Logging.IsEnabled) Logging.Exit(this, $"Aborting {this}", $"{nameof(Abort)}");
        }

        public bool IsValid()
        {
            return !_disposed;
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
                _messageSendingLinkResourceHolder.Dispose();
                _methodsSendingLinkResourceHolder.Dispose();
                _twinSendingLinkResourceHolder.Dispose();
                _streamSendingLinkResourceHolder.Dispose();
                _messageReceivingLinkResourceHolder.Dispose();
                _methodsReceivingLinkResourceHolder.Dispose();
                _twinReceivingLinkResourceHolder.Dispose();
                _streamReceivingLinkResourceHolder.Dispose();
                _amqpSessionResourceHolder.Dispose();

                if (!_deviceIdentity.AmqpTransportSettings.AmqpConnectionPoolSettings.Pooling)
                {
                    _amqpConnectionResourceHolder.Dispose();
                }
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpTransport : IAmqpTransport
    {
        #region Members-Constructor
        private readonly DeviceIdentity _deviceIdentity;
        private readonly Func<IRetryScheduler> _reconnectSchedulerSupplier;
        private readonly Action _onDisconnected;

        private readonly IAmqpConnectionResourceHolder _amqpConnectionResourceHolder;
        private readonly IResourceHolder<IAmqpSessionResource> _amqpSessionResourceHolder;
        //Caution: Same link name and path for IoT edge(called Events) and regular device(called Telemetry)
        private IReadOnlyDictionary<IoTHubTopic, IResourceHolder<IAmqpSendingLinkResource>> _sendingLinkHolders;
        //Caution: Different link name and path for IoT edge(called Events) and regular device(called C2D)
        private readonly IReadOnlyDictionary<IoTHubTopic, IAmqpReceivingLinkResourceHolder> _receivingLinkHolders;

        private readonly object _stateLock;
        private readonly bool _pooling;
        private readonly SemaphoreSlim _reconnectLock;

        private volatile bool _disposed;

        internal AmqpTransport(
            DeviceIdentity deviceIdentity, 
            IAmqpConnectionResourceHolder amqpConnectionResourceHolder, 
            Func<IRetryScheduler> reconnectSchedulerSupplier,
            Action<IoTHubTopic, AmqpMessage> onAmqpMessageReceived,
            Action onDisconnected)
        {
            _deviceIdentity = deviceIdentity;
            _reconnectSchedulerSupplier = reconnectSchedulerSupplier;
            _onDisconnected = onDisconnected;

            _amqpConnectionResourceHolder = amqpConnectionResourceHolder;
            _amqpSessionResourceHolder = new AmqpSessionResourceHolder(_amqpConnectionResourceHolder, OnSessionDisconnected);

            string correlationId = Guid.NewGuid().ToString();

            IResourceHolder<IAmqpSendingLinkResource> messageSendingLink = IoTAmqpLinkFactory.CreateMessageSendingLinkResourceHolder(
                _deviceIdentity,
                _amqpSessionResourceHolder,
                () => OnSendingLinkDisconnected(IoTHubTopic.Message));
            IResourceHolder<IAmqpSendingLinkResource> methodSendingLink = IoTAmqpLinkFactory.CreateMethodsSendingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId, 
                () => OnSendingLinkDisconnected(IoTHubTopic.Method));
            IResourceHolder<IAmqpSendingLinkResource> twinSendingLink = IoTAmqpLinkFactory.CreateTwinSendingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId, 
                () => OnSendingLinkDisconnected(IoTHubTopic.Twin));
            IResourceHolder<IAmqpSendingLinkResource> deviceStreamingSendingLink = IoTAmqpLinkFactory.CreateStreamSendingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId, 
                () => OnSendingLinkDisconnected(IoTHubTopic.DeviceStreaming));
            _sendingLinkHolders = new Dictionary<IoTHubTopic, IResourceHolder<IAmqpSendingLinkResource>>()
            {
                [IoTHubTopic.Message] = messageSendingLink,
                [IoTHubTopic.Method] = methodSendingLink,
                [IoTHubTopic.Twin] = twinSendingLink,
                [IoTHubTopic.DeviceStreaming] = deviceStreamingSendingLink
            };

            IAmqpReceivingLinkResourceHolder messageReceivingLink = IoTAmqpLinkFactory.CreateMessageReceivingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder,
                amqpMessage => onAmqpMessageReceived(IoTHubTopic.Message, amqpMessage),
                () => OnReceivingLinkDisconnected(IoTHubTopic.Message));
            IAmqpReceivingLinkResourceHolder methodReceivingLink = IoTAmqpLinkFactory.CreateMethodsReceivingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId,
                amqpMessage => onAmqpMessageReceived(IoTHubTopic.Method, amqpMessage), 
                () => OnReceivingLinkDisconnected(IoTHubTopic.Method));
            IAmqpReceivingLinkResourceHolder twinReceivingLink = IoTAmqpLinkFactory.CreateTwinReceivingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId,
                amqpMessage => onAmqpMessageReceived(IoTHubTopic.Twin, amqpMessage),
                () => OnReceivingLinkDisconnected(IoTHubTopic.Twin));
            IAmqpReceivingLinkResourceHolder deviceStreamingReceivingLink = IoTAmqpLinkFactory.CreateStreamReceivingLinkResourceHolder(
                _deviceIdentity, 
                _amqpSessionResourceHolder, 
                correlationId,
                amqpMessage => onAmqpMessageReceived(IoTHubTopic.DeviceStreaming, amqpMessage),
                () => OnReceivingLinkDisconnected(IoTHubTopic.DeviceStreaming));
            _receivingLinkHolders = new Dictionary<IoTHubTopic, IAmqpReceivingLinkResourceHolder>()
            {
                [IoTHubTopic.Message] = messageReceivingLink,
                [IoTHubTopic.Method] = methodReceivingLink,
                [IoTHubTopic.Twin] = twinReceivingLink,
                [IoTHubTopic.DeviceStreaming] = deviceStreamingReceivingLink
            };

            _pooling = deviceIdentity.AuthenticationModel != AuthenticationModel.X509 && (deviceIdentity?.AmqpTransportSettings?.AmqpConnectionPoolSettings?.Pooling ?? false);
            _stateLock = new object();

            _reconnectLock = new SemaphoreSlim(1, 1);

            if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpUnitCreated();
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
            await _amqpConnectionResourceHolder.OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            await _amqpSessionResourceHolder.OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            await _sendingLinkHolders[IoTHubTopic.Message].OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(OpenAsync)}");
        }

        public async Task CloseAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(CloseAsync)}");

            List<Task> tasks = new List<Task>();
            foreach (IResourceHolder<IAmqpSendingLinkResource> sendingLinkHolder in _sendingLinkHolders.Values)
            {
                tasks.Add(sendingLinkHolder.CloseAsync(timeout));
            }

            foreach (IResourceHolder<IAmqpReceivingLinkResource> receivingLinkHolder in _receivingLinkHolders.Values)
            {
                tasks.Add(receivingLinkHolder.CloseAsync(timeout));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            await _amqpSessionResourceHolder.CloseAsync(timeout).ConfigureAwait(false);

            if (!_pooling)
            {
                await _amqpConnectionResourceHolder.CloseAsync(timeout).ConfigureAwait(false);
            }

            _onDisconnected?.Invoke();
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(CloseAsync)}");
        }
        #endregion

        #region Message
        public async Task<Message> ReceiveMessageAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(ReceiveMessageAsync)}");
            IAmqpReceivingLinkResource messageReceivingLinkResource = await _receivingLinkHolders[IoTHubTopic.Message].EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
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
            IAmqpReceivingLinkResource messageReceivingLinkResource = await _receivingLinkHolders[IoTHubTopic.Message].EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome disposeOutcome = await messageReceivingLinkResource.DisposeMessageAsync(lockToken, outcome, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(DisposeMessageAsync)}");
            return disposeOutcome;
        }
        #endregion

        #region Event
        public async Task EnableEventReceiveAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableEventReceiveAsync)}");
            await _receivingLinkHolders[IoTHubTopic.Message].OpenAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableEventReceiveAsync)}");
        }
        #endregion

        #region Method
        public async Task EnableMethodsAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableMethodsAsync)}");
            await Task.WhenAll(
                _sendingLinkHolders[IoTHubTopic.Method].OpenAsync(_deviceIdentity, timeout),
                _receivingLinkHolders[IoTHubTopic.Method].OpenAsync(_deviceIdentity, timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableMethodsAsync)}");
        }

        public async Task DisableMethodsAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(DisableMethodsAsync)}");
            await Task.WhenAll(
                _sendingLinkHolders[IoTHubTopic.Method].CloseAsync(timeout),
                _receivingLinkHolders[IoTHubTopic.Method].CloseAsync(timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(DisableMethodsAsync)}");
        }
        #endregion

        #region Twin
        public async Task EnableTwinPatchAsync(TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, $"{nameof(EnableTwinPatchAsync)}");
            await Task.WhenAll(
                _sendingLinkHolders[IoTHubTopic.Twin].OpenAsync(_deviceIdentity, timeout),
                _receivingLinkHolders[IoTHubTopic.Twin].OpenAsync(_deviceIdentity, timeout)
            ).ConfigureAwait(false);

            if (Logging.IsEnabled) Logging.Exit(this, timeout, $"{nameof(EnableTwinPatchAsync)}");
        }
        #endregion

        #region Send/Dispose AMQP message
        public async Task<Outcome> SendMessageAsync(IoTHubTopic topic, AmqpMessage message, TimeSpan timeout)
        {
            if (Logging.IsEnabled) Logging.Enter(this, topic, timeout, $"{nameof(SendMessageAsync)}");
            IAmqpSendingLinkResource sendingLinkResource = await _sendingLinkHolders[topic].EnsureResourceAsync(_deviceIdentity, timeout).ConfigureAwait(false);
            Outcome outcome = await sendingLinkResource.SendAmqpMessageAsync(message, timeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, topic, timeout, $"{nameof(SendMessageAsync)}");
            return outcome;
        }
        public void DisposeDelivery(IoTHubTopic topic, AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, topic, amqpMessage, $"{nameof(DisposeDelivery)}");
            _receivingLinkHolders[topic].DisposeDelivery(amqpMessage);
            if (Logging.IsEnabled) Logging.Exit(this, topic, amqpMessage, $"{nameof(DisposeDelivery)}");
        }
        #endregion

        #region IResource
        public void Abort()
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"Aborting {this}", $"{nameof(Abort)}");
            foreach (IResourceHolder<IAmqpSendingLinkResource> sendingLinkHolder in _sendingLinkHolders.Values)
            {
                sendingLinkHolder.Abort();
            }

            foreach (IResourceHolder<IAmqpReceivingLinkResource> receivingLinkHolder in _receivingLinkHolders.Values)
            {
                receivingLinkHolder.Abort();
            }

            _amqpSessionResourceHolder.Abort();

            if (!_pooling)
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
                foreach (IResourceHolder<IAmqpSendingLinkResource> sendingLinkHolder in _sendingLinkHolders.Values)
                {
                    sendingLinkHolder.Dispose();
                }

                foreach (IResourceHolder<IAmqpReceivingLinkResource> receivingLinkHolder in _receivingLinkHolders.Values)
                {
                    receivingLinkHolder.Dispose();
                }

                _amqpSessionResourceHolder.Dispose();

                if (!_deviceIdentity.AmqpTransportSettings.AmqpConnectionPoolSettings.Pooling)
                {
                    _amqpConnectionResourceHolder.Dispose();
                }

                if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpUnitDisposed();
            }

            if (Logging.IsEnabled) Logging.Exit(this, disposing, $"{nameof(Dispose)}");
        }
        #endregion

        #region Unexpected disconnection events
        private void OnSessionDisconnected()
        {
            if (Logging.IsEnabled) Logging.Enter(this, _amqpSessionResourceHolder, $"{nameof(OnSessionDisconnected)}");
            if (DeviceEventCounter.IsEnabled) DeviceEventCounter.OnAmqpSessionDisconnected();
            _ = ReconnectAsync();

            if (Logging.IsEnabled) Logging.Exit(this, _amqpSessionResourceHolder, $"{nameof(OnSessionDisconnected)}");
        }

        private void OnSendingLinkDisconnected(IoTHubTopic topic)
        {
            if (Logging.IsEnabled) Logging.Enter(this, topic, $"{nameof(OnSendingLinkDisconnected)}");
            _ = ReconnectAsync();

            if (Logging.IsEnabled) Logging.Exit(this, topic, $"{nameof(OnSendingLinkDisconnected)}");
        }

        private void OnReceivingLinkDisconnected(IoTHubTopic topic)
        {
            if (Logging.IsEnabled) Logging.Enter(this, topic, $"{nameof(OnReceivingLinkDisconnected)}");
            _ = ReconnectAsync();

            if (Logging.IsEnabled) Logging.Exit(this, topic, $"{nameof(OnReceivingLinkDisconnected)}");
        }
        #endregion

        #region Connection Guard
        private async Task ReconnectAsync()
        {
            if (Logging.IsEnabled) Logging.Enter(this, _disposed, $"{nameof(ReconnectAsync)}");

            
            IRetryScheduler retryScheduler = _reconnectSchedulerSupplier();
            while (!_disposed && retryScheduler.HasNext())
            {
                if (Logging.IsEnabled) Logging.Info(this, "Attempting to reconnect.", $"{nameof(ReconnectAsync)}");

                await _reconnectLock.WaitAsync().ConfigureAwait(false);

                try
                {
                    List<Task> tasks = new List<Task>();
                    foreach (IResourceHolder<IAmqpSendingLinkResource> sendingLinkHolder in _sendingLinkHolders.Values)
                    {
                        if (sendingLinkHolder.GetOperationStatus() == OperationStatus.Active)
                        {
                            tasks.Add(sendingLinkHolder.EnsureResourceAsync(_deviceIdentity, retryScheduler.GetOperationTimeout()));
                        }
                    }

                    foreach (IResourceHolder<IAmqpReceivingLinkResource> receivingLinkHolder in _receivingLinkHolders.Values)
                    {
                        if (receivingLinkHolder.GetOperationStatus() == OperationStatus.Active)
                        {
                            tasks.Add(receivingLinkHolder.EnsureResourceAsync(_deviceIdentity, retryScheduler.GetOperationTimeout()));
                        }
                    }

                    if (tasks.Count > 0)
                    {
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                        if (Logging.IsEnabled) Logging.Info(this, "Reconnected.", $"{nameof(ReconnectAsync)}");
                        // TODO reconnected, notify connected?
                    }
                    break;
                }
                catch(Exception e)
                {
                        
                    if (e is IotHubException && (e as IotHubException).IsTransient && retryScheduler.HasNext())
                    {
                        TimeSpan timewait = retryScheduler.NextInterval();
                        if (Logging.IsEnabled) Logging.Info(this, $"Reconnect attempt failed: {e}. Waiting {timewait} for next attempt.", $"{nameof(ReconnectAsync)}");
                        await Task.Delay(timewait).ConfigureAwait(false);
                    }
                    else
                    {
                        if (Logging.IsEnabled) Logging.Info(this, $"Reconnect failed: {e}. Now disconnected.", $"{nameof(ReconnectAsync)}");
                        _onDisconnected?.Invoke();
                        break;
                    }
                }
                finally
                {
                    _reconnectLock.Release();
                }
            }

            if (Logging.IsEnabled) Logging.Exit(this, _disposed, $"{nameof(ReconnectAsync)}");
        }
        #endregion
    }
}

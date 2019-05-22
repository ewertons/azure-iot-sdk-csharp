using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Exceptions;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpTransportHandler : TransportHandler
    {
        #region Members-Constructor
        private const string ResponseStatusName = "status";
        private readonly TimeSpan ResponseTimeout = TimeSpan.FromMinutes(5);
        private readonly TimeSpan _operationTimeout;
        private readonly IAmqpTransport _amqpTransport;

        private readonly Func<string, Message, Task> _eventListener;
        private readonly Func<MethodRequestInternal, Task> _methodHandler;
        private readonly Action<TwinCollection> _desiredPropertyListener;
        private ConcurrentDictionary<string, TaskCompletionSource<AmqpMessage>> _twinResponseCompletions;

        internal AmqpTransportHandler(
            IPipelineContext context,
            IotHubConnectionString connectionString,
            AmqpTransportSettings transportSettings,
            Func<MethodRequestInternal, Task> methodHandler = null,
            Action<TwinCollection> desiredPropertyListener = null,
            Func<string, Message, Task> eventListener = null)
            : base(context, transportSettings)
        {
            _operationTimeout = transportSettings.OperationTimeout;

            _eventListener = eventListener;
            _methodHandler = methodHandler;
            _desiredPropertyListener = desiredPropertyListener;

            DeviceIdentity deviceIdentity = new DeviceIdentity(connectionString, transportSettings, context.Get<ProductInfo>());
            _twinResponseCompletions = new ConcurrentDictionary<string, TaskCompletionSource<AmqpMessage>>();

            IAmqpConnectionResourceHolder amqpConnectionResourceHolder = AmqpConnectionResourceHolderManager.GetInstance().AllocateAmqpConnectionResourceHolder(deviceIdentity);
            _amqpTransport = new AmqpTransport(
                deviceIdentity,
                amqpConnectionResourceHolder,
                CreatReconnectSchedulerSupplier,
                OnAmqpMessageReceived,
                OnTransportClosedGracefully
            );

            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, this, $"{nameof(AmqpTransportHandler)}");
        }
        #endregion

        private IRetryScheduler CreatReconnectSchedulerSupplier()
        {
            return new ExponentialBackoffRetryScheduler(
                int.MaxValue, 
                _operationTimeout, 
                TimeSpan.FromSeconds(1), 
                TimeSpan.FromMinutes(5),
                0.25D, 
                1.25D);
        }

        #region Handle received AMQP message
        private void OnAmqpMessageReceived(IoTHubTopic topic, AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, topic, amqpMessage, $"{nameof(OnAmqpMessageReceived)}");
            if (topic == IoTHubTopic.Message)
            {
                OnEventMessageReceived(amqpMessage);
            }
            else if (topic == IoTHubTopic.Method)
            {
                OnMethodMessageReceived(amqpMessage);
            }
            else if (topic == IoTHubTopic.Twin)
            {
                OnTwinMessageReceived(amqpMessage);
            }
            else if (topic == IoTHubTopic.DeviceStreaming)
            {
                OnDeviceStreamingMessageReceived(amqpMessage);
            }
            if (Logging.IsEnabled) Logging.Exit(this, topic, amqpMessage, $"{nameof(OnAmqpMessageReceived)}");
        }

        private void OnMethodMessageReceived(AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, $"{nameof(OnMethodMessageReceived)}");
            MethodRequestInternal methodRequestInternal = MethodConverter.ConstructMethodRequestFromAmqpMessage(amqpMessage, new CancellationToken(false));
            _amqpTransport.DisposeDelivery(IoTHubTopic.Method, amqpMessage);
            _methodHandler?.Invoke(methodRequestInternal);
            if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, $"{nameof(OnMethodMessageReceived)}");
        }

        private void OnEventMessageReceived(AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, $"{nameof(OnEventMessageReceived)}");
            Message message = new Message(amqpMessage)
            {
                LockToken = new Guid(amqpMessage.DeliveryTag.Array).ToString()
            };

            _eventListener?.Invoke(message.InputName, message);
            if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, $"{nameof(OnEventMessageReceived)}");
        }

        private void OnTwinMessageReceived(AmqpMessage amqpMessage)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, $"{nameof(OnTwinMessageReceived)}");
            string correlationId = amqpMessage.Properties?.CorrelationId?.ToString();
            if (correlationId != null)
            {
                // If we have a correlation id, it must be a response, complete the task.
                TaskCompletionSource<AmqpMessage> task;
                if (_twinResponseCompletions.TryRemove(correlationId, out task))
                {
                    task.SetResult(amqpMessage);
                }
            }
            else
            {
                // No correlation id? Must be a patch.
                if (_desiredPropertyListener != null)
                {
                    using (StreamReader reader = new StreamReader(amqpMessage.BodyStream, System.Text.Encoding.UTF8))
                    {
                        string patch = reader.ReadToEnd();
                        var props = JsonConvert.DeserializeObject<TwinCollection>(patch);
                        _desiredPropertyListener(props);
                    }
                }
            }
            if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, $"{nameof(OnTwinMessageReceived)}");
        }

        private void OnDeviceStreamingMessageReceived(AmqpMessage amqpMessage)
        {
            // TODO
        }
        #endregion


        public override bool IsUsable => !_disposed;

        #region Open-Close
        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(OpenAsync)}");
            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.OpenAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(OpenAsync)}");
        }

        public override async Task CloseAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, $"{nameof(CloseAsync)}");

            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.CloseAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, $"{nameof(CloseAsync)}");
        }
        #endregion

        #region Telemetry
        public override async Task SendEventAsync(Message message, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, message, cancellationToken, $"{nameof(SendEventAsync)}");

            cancellationToken.ThrowIfCancellationRequested();
            Outcome outcome = await _amqpTransport.SendMessageAsync(IoTHubTopic.Message, message.ToAmqpMessage(), _operationTimeout).ConfigureAwait(false);
            if (outcome != null && outcome.DescriptorCode != Accepted.Code)
            {
                throw AmqpErrorMapper.GetExceptionFromOutcome(outcome);
            }

            if (Logging.IsEnabled) Logging.Exit(this, message, cancellationToken, $"{nameof(SendEventAsync)}");
        }

        public override async Task SendEventAsync(IEnumerable<Message> messages, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, messages, cancellationToken, $"{nameof(SendEventAsync)}");

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                // List to hold messages in Amqp friendly format
                var messageList = new List<Data>();
                foreach (Message message in messages)
                {
                    using (AmqpMessage amqpMessage = message.ToAmqpMessage())
                    {
                        var data = new Data()
                        {
                            Value = MessageConverter.ReadStream(amqpMessage.ToStream())
                        };
                        messageList.Add(data);
                    }
                }

                Outcome outcome;
                using (AmqpMessage amqpMessage = AmqpMessage.Create(messageList))
                {
                    amqpMessage.MessageFormat = AmqpConstants.AmqpBatchedMessageFormat;
                    outcome = await _amqpTransport.SendMessageAsync(IoTHubTopic.Message, amqpMessage, _operationTimeout).ConfigureAwait(false);
                }

                if (outcome != null)
                {
                    if (outcome.DescriptorCode != Accepted.Code)
                    {
                        throw AmqpErrorMapper.GetExceptionFromOutcome(outcome);
                    }
                }
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(this, messages, cancellationToken, $"{nameof(SendEventAsync)}");
            }
        }

        public override async Task<Message> ReceiveAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, timeout, cancellationToken, $"{nameof(ReceiveAsync)}");
            Message message = null;
            while (message == null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                message = await _amqpTransport.ReceiveMessageAsync(timeout).ConfigureAwait(false);
                if (message == null)
                {
                    if (Logging.IsEnabled) Logging.Info(this, "Null message received, looping...");
                }
            }

            if (Logging.IsEnabled) Logging.Exit(this, timeout, cancellationToken, $"{nameof(ReceiveAsync)}");
            return message;
        }
        #endregion

        #region Methods
        public override async Task EnableMethodsAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(EnableMethodsAsync)}");
            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.EnableMethodsAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(EnableMethodsAsync)}");
        }

        public override async Task DisableMethodsAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(DisableMethodsAsync)}");
            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.DisableMethodsAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(DisableMethodsAsync)}");
        }

        public override async Task SendMethodResponseAsync(MethodResponseInternal methodResponse, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, methodResponse, cancellationToken, $"{nameof(SendMethodResponseAsync)}");

            cancellationToken.ThrowIfCancellationRequested();

            using (AmqpMessage amqpMessage = methodResponse.ToAmqpMessage())
            {
                Outcome outcome = await _amqpTransport.SendMessageAsync(IoTHubTopic.Method, amqpMessage, _operationTimeout).ConfigureAwait(false);
                if (outcome.DescriptorCode != Accepted.Code)
                {
                    throw AmqpErrorMapper.GetExceptionFromOutcome(outcome);
                }
            }
            if (Logging.IsEnabled) Logging.Exit(this, methodResponse, cancellationToken, $"{nameof(SendMethodResponseAsync)}");
        }
        #endregion

        #region Twin
        public override async Task EnableTwinPatchAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(EnableTwinPatchAsync)}");
            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.EnableTwinPatchAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(EnableTwinPatchAsync)}");
        }

        public override async Task<Twin> SendTwinGetAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(SendTwinGetAsync)}");
            await EnableTwinPatchAsync(cancellationToken).ConfigureAwait(false);
            AmqpMessage amqpMessage = AmqpMessage.Create();
            amqpMessage.MessageAnnotations.Map["operation"] = "GET";
            AmqpMessage response = await RoundTripTwinMessage(amqpMessage, cancellationToken).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(SendTwinGetAsync)}");
            return TwinFromResponse(response);
        }

        public override async Task SendTwinPatchAsync(TwinCollection reportedProperties, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, reportedProperties, cancellationToken, $"{nameof(SendTwinPatchAsync)}");
            await EnableTwinPatchAsync(cancellationToken).ConfigureAwait(false);
            string body = JsonConvert.SerializeObject(reportedProperties);
            MemoryStream bodyStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(body));
            AmqpMessage amqpMessage = AmqpMessage.Create(bodyStream, true);
            amqpMessage.MessageAnnotations.Map["operation"] = "PATCH";
            amqpMessage.MessageAnnotations.Map["resource"] = "/properties/reported";
            amqpMessage.MessageAnnotations.Map["version"] = null;
            AmqpMessage response = await RoundTripTwinMessage(amqpMessage, cancellationToken).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, reportedProperties, cancellationToken, $"{nameof(SendTwinPatchAsync)}");
        }

        private async Task<AmqpMessage> RoundTripTwinMessage(AmqpMessage amqpMessage, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, amqpMessage, cancellationToken, $"{nameof(RoundTripTwinMessage)}");
            string correlationId = Guid.NewGuid().ToString();
            AmqpMessage response = null;

            try
            {
                amqpMessage.Properties.CorrelationId = correlationId;
                var taskCompletionSource = new TaskCompletionSource<AmqpMessage>();
                _twinResponseCompletions[correlationId] = taskCompletionSource;
                Outcome outcome = await _amqpTransport.SendMessageAsync(IoTHubTopic.Twin, amqpMessage, _operationTimeout).ConfigureAwait(false);

                if (outcome.DescriptorCode != Accepted.Code)
                {
                    throw AmqpErrorMapper.GetExceptionFromOutcome(outcome);
                }

                Task<AmqpMessage> receivingTask = taskCompletionSource.Task;
                if (await Task.WhenAny(receivingTask, Task.Delay(ResponseTimeout, cancellationToken)).ConfigureAwait(false) == receivingTask)
                {
                    // Task completed within timeout.
                    // Consider that the task may have faulted or been canceled.
                    // We re-await the task so that any exceptions/cancellation is rethrown.
                    response = await receivingTask.ConfigureAwait(false);
                }
                else
                {
                    // Timeout happen
                    throw new TimeoutException();
                }
                if (Logging.IsEnabled) Logging.Exit(this, amqpMessage, cancellationToken, $"{nameof(RoundTripTwinMessage)}");
            }
            finally
            {
                _twinResponseCompletions.TryRemove(correlationId, out _);
            }

            return response;
        }
        #endregion

        #region Events
        public override async Task EnableEventReceiveAsync(CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, cancellationToken, $"{nameof(EnableEventReceiveAsync)}");
            cancellationToken.ThrowIfCancellationRequested();
            await _amqpTransport.EnableEventReceiveAsync(_operationTimeout).ConfigureAwait(false);
            if (Logging.IsEnabled) Logging.Exit(this, cancellationToken, $"{nameof(EnableEventReceiveAsync)}");
        }

        #endregion

        #region Accept-Dispose
        public override Task CompleteAsync(string lockToken, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, lockToken, cancellationToken, $"{nameof(CompleteAsync)}");
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return DisposeMessageAsync(lockToken, AmqpConstants.AcceptedOutcome, cancellationToken);
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(this, lockToken, cancellationToken, $"{nameof(CompleteAsync)}");
            }
        }

        public override Task AbandonAsync(string lockToken, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, lockToken, cancellationToken, $"{nameof(AbandonAsync)}");

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return DisposeMessageAsync(lockToken, AmqpConstants.ReleasedOutcome, cancellationToken);
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(this, lockToken, cancellationToken, $"{nameof(AbandonAsync)}");
            }
        }

        public override Task RejectAsync(string lockToken, CancellationToken cancellationToken)
        {
            if (Logging.IsEnabled) Logging.Enter(this, lockToken, cancellationToken, $"{nameof(RejectAsync)}");

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return DisposeMessageAsync(lockToken, AmqpConstants.RejectedOutcome, cancellationToken);
            }
            finally
            {
                if (Logging.IsEnabled) Logging.Exit(this, lockToken, cancellationToken, $"{nameof(RejectAsync)}");
            }
        }

        private async Task DisposeMessageAsync(string lockToken, Outcome outcome, CancellationToken cancellationToken)
        {
            Outcome disposeOutcome;
            
            // Currently, the same mechanism is used for sending feedback for C2D messages and events received by modules.
            // However, devices only support C2D messages (they cannot receive events), and modules only support receiving events
            // (they cannot receive C2D messages). So we use this to distinguish whether to dispose the message (i.e. send outcome on)
            // the DeviceBoundReceivingLink or the EventsReceivingLink. 
            // If this changes (i.e. modules are able to receive C2D messages, or devices are able to receive telemetry), this logic 
            // will have to be updated.
            disposeOutcome = await _amqpTransport.DisposeMessageAsync(lockToken, outcome, _operationTimeout).ConfigureAwait(false);
            
            if (disposeOutcome.DescriptorCode != Accepted.Code)
            {
                if (disposeOutcome.DescriptorCode == Rejected.Code)
                {
                    var rejected = (Rejected)disposeOutcome;

                    // Special treatment for NotFound amqp rejected error code in case of DisposeMessage 
                    if (rejected.Error != null && rejected.Error.Condition.Equals(AmqpErrorCode.NotFound))
                    {
                        Error error = new Error
                        {

                            Condition = IotHubAmqpErrorCode.MessageLockLostError
                        };
                        throw AmqpErrorMapper.ToIotHubClientContract(error);
                    }
                }

                throw AmqpErrorMapper.GetExceptionFromOutcome(disposeOutcome);
            }
        }
        #endregion

        #region Helpers
        private void VerifyResponseMessage(AmqpMessage response)
        {
            if (response != null)
            {
                if (response.MessageAnnotations.Map.TryGetValue(ResponseStatusName, out int status))
                {
                    if (status >= 400)
                    {
                        throw new InvalidOperationException("Service rejected the message with status: " + status);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Service response is null.");
            }
        }

        private Twin TwinFromResponse(AmqpMessage message)
        {
            using (StreamReader reader = new StreamReader(message.BodyStream, System.Text.Encoding.UTF8))
            {
                string body = reader.ReadToEnd();
                var props = JsonConvert.DeserializeObject<TwinProperties>(body);
                return new Twin(props);
            }
        }

        #endregion

        #region IDispose
        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            base.Dispose(disposing);
            if (disposing)
            {
                _amqpTransport.Dispose();
            }

        }
        #endregion
    }
}

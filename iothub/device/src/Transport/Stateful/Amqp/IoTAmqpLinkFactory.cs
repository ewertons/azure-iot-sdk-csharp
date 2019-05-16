using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices.Client.Extensions;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Globalization;
using System.Net;
using System.Text;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal static class IoTAmqpLinkFactory
    {
        private const string TelemetrySenderLinkSuffix = "_TelemetrySenderLink";
        private const string TelemetryReceiveLinkSuffix = "_TelemetryReceiverLink";
        private const string EventsReceiverLinkSuffix = "_EventsReceiverLink";
        private const string MethodsSenderLinkSuffix = "_MethodsSenderLink";
        private const string MethodsReceiverLinkSuffix = "_MethodsReceiverLink";
        private const string TwinSenderLinkSuffix = "_TwinSenderLink";
        private const string TwinReceiverLinkSuffix = "_TwinReceiverLink";
        private const string StreamsSenderLinkSuffix = "_StreamsSenderLink:";
        private const string StreamsReceiverLinkSuffix = "_StreamsReceiverLink:";

        private const string MethodsCorrelationIdPrefix = "methods:";
        private const string TwinCorrelationIdPrefix = "twin:";
        private const string StreamsCorrelationIdPrefix = "streams:";

        #region AmqpSendingLink
        internal static IResourceHolder<IAmqpSendingLinkResource> CreateMessageSendingLinkResourceHolder(
            DeviceIdentity deviceIdentity, 
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder, 
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateSendingAmqpLinkSettings(
                deviceIdentity,
                CommonConstants.DeviceEventPathTemplate,
                CommonConstants.ModuleEventPathTemplate,
                null,
                null,
                TelemetrySenderLinkSuffix,
                null
            );
            IResourceHolder<IAmqpSendingLinkResource> messageSendingLinkResource = new AmqpSendingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, messageSendingLinkResource, $"{nameof(CreateMessageSendingLinkResourceHolder)}");
            return messageSendingLinkResource;
        }

        internal static IResourceHolder<IAmqpSendingLinkResource> CreateMethodsSendingLinkResourceHolder(
            DeviceIdentity deviceIdentity, 
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder, 
            string correlationId, 
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateSendingAmqpLinkSettings(
                deviceIdentity,
                CommonConstants.DeviceMethodPathTemplate,
                CommonConstants.ModuleMethodPathTemplate,
                (byte)SenderSettleMode.Settled,
                (byte)ReceiverSettleMode.First,
                MethodsSenderLinkSuffix,
                MethodsCorrelationIdPrefix + correlationId
            );
            IResourceHolder<IAmqpSendingLinkResource> methodSendingLinkResource = new AmqpSendingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, methodSendingLinkResource, $"{nameof(CreateMethodsSendingLinkResourceHolder)}");
            return methodSendingLinkResource;
        }

        internal static IResourceHolder<IAmqpSendingLinkResource> CreateTwinSendingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            string correlationId,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateSendingAmqpLinkSettings(
                deviceIdentity,
                CommonConstants.DeviceTwinPathTemplate,
                CommonConstants.ModuleTwinPathTemplate,
                (byte)SenderSettleMode.Settled,
                (byte)ReceiverSettleMode.First,
                TwinSenderLinkSuffix,
                TwinCorrelationIdPrefix + correlationId
            );
            IResourceHolder<IAmqpSendingLinkResource> twinSendingLinkResource = new AmqpSendingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, twinSendingLinkResource, $"{nameof(CreateTwinSendingLinkResourceHolder)}");
            return twinSendingLinkResource;
        }

        internal static IResourceHolder<IAmqpSendingLinkResource> CreateStreamSendingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            string correlationId,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateSendingAmqpLinkSettings(
                deviceIdentity,
                CommonConstants.DeviceStreamsPathTemplate,
                CommonConstants.ModuleStreamsPathTemplate,
                (byte)SenderSettleMode.Settled,
                (byte)ReceiverSettleMode.First,
                StreamsSenderLinkSuffix,
                StreamsCorrelationIdPrefix + correlationId
            );
            IResourceHolder<IAmqpSendingLinkResource> streamSendingLinkResource = new AmqpSendingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, streamSendingLinkResource, $"{nameof(CreateStreamSendingLinkResourceHolder)}");
            return streamSendingLinkResource;
        }

        private static AmqpLinkSettings CreateSendingAmqpLinkSettings(
            DeviceIdentity deviceIdentity,
            string devicePathTemplate,
            string modulePathTemplate,
            byte? senderSettleMode,
            byte? receiverSettleMode,
            string linkSuffix,
            string correlationId
        )
        {
            string linkName = CommonResources.GetNewStringGuid(linkSuffix);
            string targetAddress = BuildLinkAddress(deviceIdentity, devicePathTemplate, modulePathTemplate);
            string sourceAddress = deviceIdentity.IotHubConnectionString.DeviceId;
            AmqpLinkSettings amqpLinkSettings = new AmqpLinkSettings
            {
                LinkName = linkName,
                Role = false,
                InitialDeliveryCount = 0,
                Target = new Target() { Address = targetAddress },
                Source = new Source() { Address = sourceAddress }
            };

            amqpLinkSettings.SndSettleMode = senderSettleMode;
            amqpLinkSettings.RcvSettleMode = receiverSettleMode;
            amqpLinkSettings.AddProperty(IotHubAmqpProperty.ClientVersion, deviceIdentity.ProductInfo.ToString());
            amqpLinkSettings.AddProperty(IotHubAmqpProperty.ApiVersion, ClientApiVersionHelper.ApiVersionString);
            if (correlationId != null)
            {
                amqpLinkSettings.AddProperty(IotHubAmqpProperty.ChannelCorrelationId, correlationId);
            }

            return amqpLinkSettings;
        }
        #endregion

        #region AmqpReceivingLink
        internal static IResourceHolder<IAmqpReceivingLinkResource> CreateMessageReceivingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings;

            if (deviceIdentity.IotHubConnectionString.ModuleId.IsNullOrWhiteSpace())
            {
                // regular device, C2D message
                if (Logging.IsEnabled) Logging.Info(deviceIdentity, "Creating C2D message receiving link.", $"{nameof(CreateMessageReceivingLinkResourceHolder)}");
                amqpLinkSettings = CreateReceivingAmqpLinkSettings(
                    deviceIdentity,
                    CommonConstants.DeviceBoundPathTemplate,
                    CommonConstants.ModuleBoundPathTemplate,
                    null,
                    (byte)ReceiverSettleMode.Second,
                    TelemetryReceiveLinkSuffix,
                    null
                );
            }
            else
            {
                // Module, Events
                if (Logging.IsEnabled) Logging.Info(deviceIdentity, "Creating event receiving link.", $"{nameof(CreateMessageReceivingLinkResourceHolder)}");
                amqpLinkSettings = CreateReceivingAmqpLinkSettings(
                    deviceIdentity,
                    CommonConstants.DeviceEventPathTemplate,
                    CommonConstants.ModuleEventPathTemplate,
                    null,
                    (byte)ReceiverSettleMode.First,
                    EventsReceiverLinkSuffix,
                    null
                );
            }

            IResourceHolder<IAmqpReceivingLinkResource> messageReceivingLinkResource = new AmqpReceivingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, null, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, messageReceivingLinkResource, $"{nameof(CreateMessageReceivingLinkResourceHolder)}");
            return messageReceivingLinkResource;
        }

        internal static IResourceHolder<IAmqpReceivingLinkResource> CreateMethodsReceivingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            string correlationId,
            Action<AmqpMessage> messageListener,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateReceivingAmqpLinkSettings(
               deviceIdentity,
               CommonConstants.DeviceMethodPathTemplate,
               CommonConstants.ModuleMethodPathTemplate,
               (byte)SenderSettleMode.Settled,
               (byte)ReceiverSettleMode.First,
               MethodsReceiverLinkSuffix,
               MethodsCorrelationIdPrefix + correlationId
           );
            IResourceHolder<IAmqpReceivingLinkResource> methodsReceivingLinkResource = new AmqpReceivingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, messageListener, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, methodsReceivingLinkResource, $"{nameof(CreateMethodsReceivingLinkResourceHolder)}");
            return methodsReceivingLinkResource;
        }

        internal static IResourceHolder<IAmqpReceivingLinkResource> CreateTwinReceivingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            string correlationId,
            Action<AmqpMessage> messageListener,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateReceivingAmqpLinkSettings(
               deviceIdentity,
               CommonConstants.DeviceTwinPathTemplate,
               CommonConstants.ModuleTwinPathTemplate,
               (byte)SenderSettleMode.Settled,
               (byte)ReceiverSettleMode.First,
               TwinReceiverLinkSuffix,
               TwinCorrelationIdPrefix + correlationId
           );
            IResourceHolder<IAmqpReceivingLinkResource> twinReceivingLinkResource = new AmqpReceivingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, messageListener, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, twinReceivingLinkResource, $"{nameof(CreateTwinReceivingLinkResourceHolder)}");
            return twinReceivingLinkResource;
        }

        internal static IResourceHolder<IAmqpReceivingLinkResource> CreateStreamReceivingLinkResourceHolder(
            DeviceIdentity deviceIdentity,
            IResourceHolder<IAmqpSessionResource> amqpSessionResourceHolder,
            string correlationId,
            Action<AmqpMessage> messageListener,
            Action onResourceDisconnected)
        {
            AmqpLinkSettings amqpLinkSettings = CreateReceivingAmqpLinkSettings(
               deviceIdentity,
               CommonConstants.DeviceStreamsPathTemplate,
               CommonConstants.ModuleStreamsPathTemplate,
               (byte)SenderSettleMode.Settled,
               (byte)ReceiverSettleMode.First,
               StreamsReceiverLinkSuffix,
               StreamsCorrelationIdPrefix + correlationId
           );
            IResourceHolder<IAmqpReceivingLinkResource> streamsReceivingLinkResource = new AmqpReceivingLinkResourceHolder(amqpSessionResourceHolder, amqpLinkSettings, messageListener, onResourceDisconnected);
            if (Logging.IsEnabled) Logging.Associate(deviceIdentity, streamsReceivingLinkResource, $"{nameof(CreateStreamReceivingLinkResourceHolder)}");
            return streamsReceivingLinkResource;
        }

        private static AmqpLinkSettings CreateReceivingAmqpLinkSettings(
            DeviceIdentity deviceIdentity,
            string devicePathTemplate,
            string modulePathTemplate,
            byte? senderSettleMode,
            byte? receiverSettleMode,
            string linkSuffix,
            string correlationId
        )
        {
            uint totalLinkCreadit = deviceIdentity.AmqpTransportSettings.PrefetchCount;
            string linkName = CommonResources.GetNewStringGuid(linkSuffix);
            string sourceAddress = BuildLinkAddress(deviceIdentity, devicePathTemplate, modulePathTemplate);
            AmqpLinkSettings amqpLinkSettings = new AmqpLinkSettings
            {
                LinkName = linkName,
                Role = true,
                TotalLinkCredit = totalLinkCreadit,
                AutoSendFlow = totalLinkCreadit > 0,
                Source = new Source() { Address = sourceAddress }
            };

            amqpLinkSettings.SndSettleMode = senderSettleMode;
            amqpLinkSettings.RcvSettleMode = receiverSettleMode;
            amqpLinkSettings.AddProperty(IotHubAmqpProperty.ClientVersion, deviceIdentity.ProductInfo.ToString());
            amqpLinkSettings.AddProperty(IotHubAmqpProperty.ApiVersion, ClientApiVersionHelper.ApiVersionString);
            if (correlationId != null)
            {
                amqpLinkSettings.AddProperty(IotHubAmqpProperty.ChannelCorrelationId, correlationId);
            }

            return amqpLinkSettings;
        }

        private static string BuildLinkAddress(DeviceIdentity deviceIdentity, string deviceTemplate, string moduleTemplate)
        {
            string path;
            if (string.IsNullOrEmpty(deviceIdentity.IotHubConnectionString.ModuleId))
            {
                path = string.Format(
                    CultureInfo.InvariantCulture,
                    deviceTemplate,
                    WebUtility.UrlEncode(deviceIdentity.IotHubConnectionString.DeviceId)
                );
            }
            else
            {
                path = string.Format(
                    CultureInfo.InvariantCulture,
                    moduleTemplate,
                    WebUtility.UrlEncode(deviceIdentity.IotHubConnectionString.DeviceId),
                    WebUtility.UrlEncode(deviceIdentity.IotHubConnectionString.ModuleId)
                );
            }

            UriBuilder builder = new UriBuilder(deviceIdentity.IotHubConnectionString.AmqpEndpoint)
            {
                Path = path,
            };

            return builder.Uri.AbsoluteUri;
        }
        #endregion
    }
}

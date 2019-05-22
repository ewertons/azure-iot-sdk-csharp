﻿
using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal interface IAmqpTransport : IResource
    {
        #region Usability
        bool IsUsable();
        #endregion

        #region Open-Close
        Task OpenAsync(TimeSpan timeout);
        Task CloseAsync(TimeSpan timeout);
        #endregion

        #region Message
        Task<Message> ReceiveMessageAsync(TimeSpan timeout);
        Task<Outcome> DisposeMessageAsync(string lockToken, Outcome outcome, TimeSpan timeout);
        #endregion

        #region Event
        Task EnableEventReceiveAsync(TimeSpan timeout);
        #endregion

        #region Method
        Task EnableMethodsAsync(TimeSpan timeout);
        Task DisableMethodsAsync(TimeSpan timeout);
        #endregion

        #region Twin
        Task EnableTwinPatchAsync(TimeSpan timeout);
        #endregion

        #region Send/Dispose AMQP message
        Task<Outcome> SendMessageAsync(IoTHubTopic topic, AmqpMessage message, TimeSpan timeout);
        void DisposeDelivery(IoTHubTopic topic, AmqpMessage amqpMessage);
        #endregion
    }
}

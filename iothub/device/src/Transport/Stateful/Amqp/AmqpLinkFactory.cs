using Microsoft.Azure.Amqp;
using Microsoft.Azure.Devices.Shared;
using System;
using System.Diagnostics;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful.Amqp
{
    internal class AmqpLinkFactory : ILinkFactory
    {
        private static AmqpLinkFactory s_instance = new AmqpLinkFactory();

        private AmqpLinkFactory()
        {
        }

        internal static AmqpLinkFactory GetInstance()
        {
            return s_instance;
        }

        public IAsyncResult BeginOpenLink(AmqpLink link, TimeSpan timeout, AsyncCallback callback, object state)
        {
            Debug.Fail($"{nameof(AmqpLinkFactory)} open link should not be used.");
            throw new NotImplementedException();
        }

        public AmqpLink CreateLink(AmqpSession session, AmqpLinkSettings settings)
        {
            if (Logging.IsEnabled) Logging.Info(this, session, $"{nameof(CreateLink)}");
            if (settings.IsReceiver())
            {
                return new ReceivingAmqpLink(session, settings);
            }
            else
            {
                return new SendingAmqpLink(session, settings);
            }
        }

        public void EndOpenLink(IAsyncResult result)
        {
            Debug.Fail($"{nameof(AmqpLinkFactory)} open link should not be used.");
            throw new NotImplementedException();
        }
    }
}

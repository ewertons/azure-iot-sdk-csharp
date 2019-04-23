using System.Diagnostics.Tracing;

namespace Microsoft.Azure.Devices.Client.Logger
{
    internal interface IEventCounterLogger
    {
        void OnDeviceClientCreated();
        void OnDeviceClientDisposed();
        void OnAmqpUnitCreated();
        void OnAmqpUnitDisposed();
        void OnAmqpConnectionEstablished();
        void OnAmqpConnectionDisconnected();
    }

#if NETSTANDARD2_0
    [EventSource(Name = "Microsoft-Azure-Devices-Client-Logger-Event-Counter")]
    internal class EventCounterLogger : EventSource, IEventCounterLogger
    {
        private static EventCounterLogger s_instance = new EventCounterLogger();

        private readonly EventCounter _deviceClientCreationCounter;
        private readonly EventCounter _deviceClientDisposeCounter;
        private readonly EventCounter _amqpUnitCreationCounter;
        private readonly EventCounter _amqpUnitDisposeCounter;
        private readonly EventCounter _amqpConnectionEstablishCounter;
        private readonly EventCounter _amqpConnectionDisconnectionCounter;

        public static IEventCounterLogger GetInstance()
        {
            return s_instance;
        }

        private EventCounterLogger() : base()
        {
            _deviceClientCreationCounter = new EventCounter("Device-Client-Creation", this);
            _deviceClientDisposeCounter = new EventCounter("Device-Client-Dispose", this);
            _amqpUnitCreationCounter = new EventCounter("AMQP-Unit-Creation", this);
            _amqpUnitDisposeCounter = new EventCounter("AMQP-Unit-Dispose", this);
            _amqpConnectionEstablishCounter = new EventCounter("AMQP-Connection-Establish", this);
            _amqpConnectionDisconnectionCounter = new EventCounter("AMQP-Connection-Disconnection", this);
        }

        public void OnDeviceClientCreated()
        {
            if (IsEnabled()) _deviceClientCreationCounter.WriteMetric(1);
        }

        public void OnDeviceClientDisposed()
        {
            if (IsEnabled()) _deviceClientDisposeCounter.WriteMetric(1);
        }

        public void OnAmqpUnitCreated()
        {
            if (IsEnabled()) _amqpUnitCreationCounter.WriteMetric(1);
        }
        public void OnAmqpUnitDisposed()
        {
            if (IsEnabled()) _amqpUnitDisposeCounter.WriteMetric(1);
        }

        public void OnAmqpConnectionEstablished()
        {
            if (IsEnabled()) _amqpConnectionEstablishCounter.WriteMetric(1);
        }
        public void OnAmqpConnectionDisconnected()
        {
            if (IsEnabled()) _amqpConnectionDisconnectionCounter.WriteMetric(1);
        }
    }
# else
    internal class EventCounterLogger : IEventCounterLogger
    {
        private static EventCounterLogger s_instance = new EventCounterLogger();

        public static IEventCounterLogger GetInstance()
        {
            return s_instance;
        }

        public void OnDeviceClientCreated()
        {
        }

        public void OnDeviceClientDisposed()
        {
        }
    
        public void OnAmqpUnitCreated()
        {
        }

        public void OnAmqpUnitDisposed()
        {
        }

        public void OnAmqpConnectionEstablished()
        {
        }

        public void OnAmqpConnectionDisconnected()
        {
        }
    }

#endif
}

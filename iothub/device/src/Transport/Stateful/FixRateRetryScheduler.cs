using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal class FixRateRetryScheduler : IRetryScheduler
    {
        private readonly int _retries;
        private readonly TimeSpan _interval;
        private readonly TimeSpan _operationTimeout;
        private int _attempts;

        internal FixRateRetryScheduler(int retries, TimeSpan interval, TimeSpan operationTimeout)
        {
            _retries = retries;
            _interval = interval;
            _operationTimeout = operationTimeout;
        }

        public TimeSpan NextInterval()
        {
            if (HasNext())
            {
                _attempts++;
                return _interval;
            }
            throw NoRetryScheduler.s_retryExhaustedException;
        }

        public bool HasNext()
        {
            return _attempts < _retries;
        }

        public TimeSpan GetOperationTimeout()
        {
            return _operationTimeout;
        }

    }
}

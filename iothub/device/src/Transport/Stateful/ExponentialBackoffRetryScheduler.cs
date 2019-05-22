using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    class ExponentialBackoffRetryScheduler : IRetryScheduler
    {
        private const double DEFAULT_RANDOMIZATION = 0.5D;
        private const double DEFAULT_MULTIPLIER = 1.5D;
        private const double DEFAULT_INIT_INTERVAL_MILLISECONDS = 1E3;
        private const double DEFAULT_MAX_INTERVAL_MILLISECONDS = 6E5;


        private readonly int _retries;
        private readonly Random _random;
        private readonly double _initIntervalMilliseconds;
        private readonly TimeSpan _maxInterval;
        private readonly double _maxIntervalMilliseconds;
        private readonly double _randomization;
        private readonly double _multiplier;
        private readonly TimeSpan _operationTimeout;
        private int _attempts;
        private double _currentIntervalMilliseconds;

        internal ExponentialBackoffRetryScheduler(
            int retries, 
            TimeSpan operationTimeout, 
            TimeSpan? initInterval, 
            TimeSpan? maxInterval, 
            double? randomization, 
            double? multiplier)
        {
            _retries = retries;
            _operationTimeout = operationTimeout;
            _initIntervalMilliseconds = initInterval?.TotalMilliseconds ?? DEFAULT_INIT_INTERVAL_MILLISECONDS;
            _maxIntervalMilliseconds = maxInterval?.TotalMilliseconds ?? DEFAULT_MAX_INTERVAL_MILLISECONDS;
            _maxInterval = TimeSpan.FromMilliseconds(_maxIntervalMilliseconds);
            _randomization = randomization ?? DEFAULT_RANDOMIZATION;
            _multiplier = multiplier ?? DEFAULT_MULTIPLIER;
            _currentIntervalMilliseconds = _initIntervalMilliseconds;
            _random = new Random();
        }

        public TimeSpan NextInterval()
        {
            if (HasNext())
            {
                _attempts++;
                double delta = _currentIntervalMilliseconds * _randomization;
                double intervalMilliseconds = _currentIntervalMilliseconds - delta + _random.NextDouble() * (delta * 2 + 1);
                if (intervalMilliseconds > _maxIntervalMilliseconds)
                {
                    return _maxInterval;
                }
                else
                {
                    return TimeSpan.FromMilliseconds(intervalMilliseconds);
                }
            }
            throw NoRetryScheduler.s_retryExhaustedException;
        }

        public TimeSpan GetOperationTimeout()
        {
            return _operationTimeout;
        }

        public bool HasNext()
        {
            return _attempts < _retries;
        }
    }
}

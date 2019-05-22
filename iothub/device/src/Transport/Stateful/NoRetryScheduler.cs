using Microsoft.Azure.Devices.Client.Exceptions;
using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal class NoRetryScheduler : IRetryScheduler
    {
        internal readonly static Exception s_retryExhaustedException = new IotHubException("Retry exhausted.", false);
        public TimeSpan NextInterval()
        {
            throw s_retryExhaustedException;
        }

        public TimeSpan GetOperationTimeout()
        {
            return TimeSpan.Zero;
        }

        public bool HasNext()
        {
            return false;
        }
    }
}

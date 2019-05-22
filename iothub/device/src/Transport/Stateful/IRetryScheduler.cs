using System;

namespace Microsoft.Azure.Devices.Client.Transport.Stateful
{
    internal interface IRetryScheduler
    {
        bool HasNext();
        TimeSpan NextInterval();
        TimeSpan GetOperationTimeout();
    }
}

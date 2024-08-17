using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.Utils;

public static class TestInterruptCount
{
    public static InterruptCount Create()
    {
        return new InterruptCount(value: 0, () => 0L.ToTask());
    }
}
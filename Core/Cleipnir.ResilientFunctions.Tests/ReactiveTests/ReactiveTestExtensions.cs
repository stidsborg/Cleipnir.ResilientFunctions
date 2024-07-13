using System;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Operators;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

internal static class ReactiveTestExtensions
{
    public static IReactiveChain<object> TakeUntilTimeout(this Source s, string timeoutEventId, TimeSpan expiresIn, bool overwrite = false)
        => new TimeoutOperator<object>(s, timeoutEventId, DateTime.UtcNow.Add(expiresIn), overwrite);
    
    public static IReactiveChain<object> TakeUntilTimeout(this Source s, string timeoutEventId, DateTime expiresAt, bool overwrite = false)
        => new TimeoutOperator<object>(s, timeoutEventId, expiresAt, overwrite);
}
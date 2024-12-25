using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Operators;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

internal static class ReactiveTestExtensions
{
    public static IReactiveChain<object> TakeUntilTimeout(this Source s, EffectId timeoutEventId, TimeSpan expiresIn)
        => new TimeoutOperator<object>(s, timeoutEventId, DateTime.UtcNow.Add(expiresIn));
    
    public static IReactiveChain<object> TakeUntilTimeout(this Source s, EffectId timeoutEventId, DateTime expiresAt)
        => new TimeoutOperator<object>(s, timeoutEventId, expiresAt);
}
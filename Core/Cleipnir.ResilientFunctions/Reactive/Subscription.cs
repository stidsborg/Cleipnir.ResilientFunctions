using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public record struct Subscription<T>(int SubscriptionId, Action<T> OnNext, Action OnCompletion, Action<Exception> OnException);
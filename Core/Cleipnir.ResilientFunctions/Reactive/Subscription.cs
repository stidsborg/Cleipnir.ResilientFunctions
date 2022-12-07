using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public record struct Subscription<T>(Action<T> OnNext, Action OnCompletion, Action<Exception> OnException);
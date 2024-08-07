﻿using System;
using System.Collections.Generic;
using System.Threading;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface IReactiveChain<out T>
{
    ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError);

    IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new StreamAsyncEnumerator<T>(reactiveChain: this, cancellationToken);
}
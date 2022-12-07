using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription : IDisposable
{
    int EventSourceTotalCount { get; }
    void Start();
    void ReplayUntil(int count);
}
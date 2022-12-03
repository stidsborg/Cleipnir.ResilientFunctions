using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription : IDisposable
{
    void Start();
}
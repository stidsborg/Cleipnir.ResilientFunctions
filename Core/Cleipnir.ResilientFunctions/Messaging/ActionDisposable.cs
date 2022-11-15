using System;

namespace Cleipnir.ResilientFunctions.Messaging;

public class ActionDisposable : IDisposable
{
    private readonly Action _dispose;

    public ActionDisposable(Action dispose) => _dispose = dispose;

    public void Dispose() => _dispose();
}
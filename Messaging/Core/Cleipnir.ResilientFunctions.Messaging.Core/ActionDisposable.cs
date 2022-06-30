namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class ActionDisposable : IDisposable
{
    private readonly System.Action _dispose;

    public ActionDisposable(System.Action dispose) => _dispose = dispose;

    public void Dispose() => _dispose();
}
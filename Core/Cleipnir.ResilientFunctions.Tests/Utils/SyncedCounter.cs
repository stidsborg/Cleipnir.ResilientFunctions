namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class SyncedCounter
{
    public int Current
    {
        get
        {
            lock (_sync)
                return _current;
        }
    }

    private int _current;
    private readonly object _sync = new();

    public void Increment()
    {
        lock (_sync)
            _current++;
    }
}
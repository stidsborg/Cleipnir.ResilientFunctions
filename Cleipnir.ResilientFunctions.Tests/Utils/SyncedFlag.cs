namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class SyncedFlag
{
    private readonly object _sync = new();
    private FlagPosition _position;
    public FlagPosition Position {
        get
        {
            lock (_sync)
                return _position;
        }
        set
        {
            lock (_sync)
                _position = value;
        } 
    }

    public void Raise() => Position = FlagPosition.Raised;
    public void Lower() => Position = FlagPosition.Lowered;
}

public enum FlagPosition
{
    Lowered, Raised
}
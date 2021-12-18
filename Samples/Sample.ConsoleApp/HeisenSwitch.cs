namespace ConsoleApp;

public class HeisenSwitch
{
    private bool _value;
    private readonly object _sync = new();
    
    public bool Value
    {
        get
        {
            lock (_sync)
            {
                var value = _value;
                _value = true;
                return value;
            }
        }
    }

    public static implicit operator bool(HeisenSwitch s) => s.Value;
}
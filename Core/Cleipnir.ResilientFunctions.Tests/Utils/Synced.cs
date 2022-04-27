namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    public class Synced<T>
    {
        private T? _t;
        private readonly object _sync = new();

        public T? Value
        {
            get
            {
                lock (_sync)
                    return _t;
            }
            set
            {
                lock (_sync)
                    _t = value;
            }
        }
    }
}
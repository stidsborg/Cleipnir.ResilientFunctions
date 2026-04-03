using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public class AsyncSignal
{
    private TaskCompletionSource _tcs = new();
    private readonly Lock _lock = new();
    
    public async Task Wait()
    {
        Task task;
        lock (_lock)
            task = _tcs.Task;

        await task;
    }

    public void Fire()
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            tcs = _tcs;
            _tcs = new();
        }
        
        tcs.SetResult();
    }
}
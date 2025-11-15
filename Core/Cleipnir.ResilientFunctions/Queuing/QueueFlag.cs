using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueFlag
{
    private readonly Lock _sync = new();
    private TaskCompletionSource _waiter = new();
    
    public async Task WaitForRaised()
    {
        TaskCompletionSource waiter;
        lock (_sync)
        {
            waiter = _waiter;
            if (waiter.Task.IsCompleted)
            {
                _waiter = new TaskCompletionSource();
                return;
            }
        }
        
        await waiter.Task;
        
        lock (_sync)
            _waiter = new();
    }

    public void Raise()
    {
        TaskCompletionSource waiter;
        lock (_sync)
            waiter = _waiter;
        
        waiter.TrySetResult();
    }
}
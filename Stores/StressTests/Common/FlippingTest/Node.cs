using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests.Common.FlippingTest;

public class Node
{
    private readonly int _id;
    private readonly CrashableFunctionStore _crashableFunctionStore;
    private readonly RFunctions _rFunctions;
    private readonly RFunc.Invoke<string, string> _rFunc;
    private volatile bool _stop;

    public Node(int id, IFunctionStore store)
    {
        _id = id;
        _crashableFunctionStore = new CrashableFunctionStore(store);
        _rFunctions = new RFunctions(
            _crashableFunctionStore,
            new Settings(
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(1_000),
                PostponedCheckFrequency: TimeSpan.FromMilliseconds(1_000)
            )
        );
        _rFunc = _rFunctions.RegisterFunc<string, Scrapbook, string>(
            "StressTest",
            async Task<Result<string>> (string param, Scrapbook scrapbook) =>
            {
                await Task.Delay(100);
                if (scrapbook.Postponed) return $"{_id}: {param.ToUpper()}";
                
                scrapbook.Postponed = true;
                return Postpone.For(1_000);
            }
        ).Invoke;
    }

    public async Task Start()
    {
        var i = 0;
        while (!_stop)
        {
            _ = _rFunc($"{_id}_{i}", $"hello world - {_id}_{i}");
            i++;
            await Task.Delay(5);
        }
    }

    public void Stop() => _stop = true;

    public void Crash()
    {
        _rFunctions.Dispose();
        _crashableFunctionStore.Crash();
    } 

    private class Scrapbook : RScrapbook
    {
        public bool Postponed { get; set; }
    }
}
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]

internal static class TestModuleInitializer
{
    [ModuleInitializer]
    public static void Init()
    {
        // Every test spins up FunctionsRegistry instances each running several Task.Delay-based watchdogs. Under a
        // small-core CI runner the thread pool grows only ~1 thread/sec, so these continuations - and the tests' own
        // timeout timers - queue behind pool growth, spiking message-delivery latency. Pre-size the pool so work is
        // serviced immediately instead of waiting for the pool to grow.
        ThreadPool.GetMinThreads(out _, out var completionPortThreads);
        ThreadPool.SetMinThreads(workerThreads: 200, completionPortThreads);
    }
}

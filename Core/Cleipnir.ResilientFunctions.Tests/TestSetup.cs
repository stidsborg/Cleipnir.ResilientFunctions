using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]

[TestClass]
public static class TestSetup
{
    [AssemblyInitialize]
    public static void Initialize(TestContext context)
    {
        // MSTest parallelizes test methods on the threadpool, and many tests
        // also spawn background Task.Run loops (FetchLoop, watchdogs). Without
        // a warm pool, those continuations queue behind tests and a 5s
        // Completion timeout can fire before a 250ms FetchLoop iteration runs.
        ThreadPool.SetMinThreads(128, 128);
    }
}

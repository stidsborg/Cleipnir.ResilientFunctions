using System;
using System.Threading.Tasks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    internal static class TestExtensions
    {
        public static void TaskShouldThrow<TException>(this Task t) where TException : Exception
        {
            Should.Throw<TException>(() =>
            {
                try
                {
                    t.Wait();
                }
                catch (AggregateException ae)
                {
                    var first = ae.InnerExceptions[0];
                    throw first;
                }
            });
        }
    }
}

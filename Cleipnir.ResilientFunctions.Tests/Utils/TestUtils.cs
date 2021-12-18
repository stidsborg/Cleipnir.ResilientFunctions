using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    public static class TestUtils
    {
        public static void SafeTry(Action a, Action<Exception>? onException = null)
        {
            try
            {
                a();
            }
            catch (Exception e)
            {
                onException?.Invoke(e);
            }
        }
        
        public static async Task SafeTryAsync(Func<Task> f, Action<Exception>? onException = null)
        {
            try
            {
                await f();
            }
            catch (Exception e)
            {
                onException?.Invoke(e);
            }
        }
    }
}
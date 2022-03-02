using System;
using System.Threading.Tasks;

namespace ConsoleApp.Utils;

public static class Safe
{
    public static async Task Try(Func<Task> f, Action<Exception> onException)
    {
        try
        {
            await f();
        } catch (Exception e)
        {
            onException(e);
        }
    }
}
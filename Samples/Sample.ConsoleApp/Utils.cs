using System;
using System.Threading.Tasks;

namespace ConsoleApp;

public static class Utils
{
    public static async Task SafeTry(Func<Task> f, Action<Exception> onException)
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
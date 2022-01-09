using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public static class Funcs
    {
        public static Task<RResult<string>> ToUpper(string s) => Succeed.WithResult(s.ToUpper()).ToTask();

        public static async Task<RResult<string>> ThrowsException(string _)
        {
            await Task.Delay(0);
            return Fail.WithException(new NullReferenceException());
        }
    }
}
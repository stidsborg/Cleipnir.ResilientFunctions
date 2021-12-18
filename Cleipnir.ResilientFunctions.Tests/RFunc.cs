using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.Tests
{
    public static class RFunc
    {
        public static Task<RResult<string>> ToUpper(string s) => Succeed.WithResult(s.ToUpper()).ToTask();

        public async static Task<RResult<string>> ThrowsException(string _)
        {
            await Task.Delay(0);
            return Fail.WithException(new NullReferenceException());
        }
    }
}
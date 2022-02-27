using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public static class Funcs
    {
        public static Task<Return<string>> ToUpper(string s) => Succeed.WithValue(s.ToUpper()).ToTask();

        public static async Task<Return<string>> ThrowsException(string _)
        {
            await Task.Delay(0);
            return Fail.WithException(new NullReferenceException());
        }
    }
}
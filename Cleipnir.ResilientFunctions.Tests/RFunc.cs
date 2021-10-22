using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.Tests
{
    public static class RFunc
    {
        public static Task<string> ToUpper(string s) => s.ToUpper().ToTask();
        
        public static Task<string> ThrowsException(string s) => Task.FromException<string>(new NullReferenceException());
    }
}
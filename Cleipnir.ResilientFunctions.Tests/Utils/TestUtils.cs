using System;
using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    public static class TestUtils
    {
        public static string ToJson<T>(this T t) => JsonSerializer.Serialize(t);

        public static void SafeTry(Action a)
        {
            try
            {
                a();
            }
            catch (Exception)
            {
                // ignored
            }
        }
        
        
    }
}
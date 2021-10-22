using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Storage;
using Moq;

namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    internal static class TestUtils
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

        public static SignOfLifeUpdaterFactory CreateNeverExecutionSignOfLifeUpdaterFactory()
            => new SignOfLifeUpdaterFactory(
                new Mock<IFunctionStore>().Object,
                _ => { },
                TimeSpan.Zero
            );
    }
}
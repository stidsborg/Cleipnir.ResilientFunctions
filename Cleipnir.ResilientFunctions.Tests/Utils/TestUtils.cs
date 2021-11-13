using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Storage;
using Moq;

namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    public static class TestUtils
    {
        public static string ToJson<T>(this T t) => JsonSerializer.Serialize(t);
        public static T? FromJsonTo<T>(this string json) => JsonSerializer.Deserialize<T>(json);

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

        internal static SignOfLifeUpdaterFactory CreateNeverExecutionSignOfLifeUpdaterFactory()
            => new SignOfLifeUpdaterFactory(
                new Mock<IFunctionStore>().Object,
                _ => { },
                TimeSpan.Zero
            );
    }
}
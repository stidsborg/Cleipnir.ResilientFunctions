using System;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class MiscUtils
{
    public static Type ResolveType(this string type) => Type.GetType(type, throwOnError: true)!;
}
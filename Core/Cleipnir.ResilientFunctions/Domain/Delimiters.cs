using System;

namespace Cleipnir.ResilientFunctions.Domain;

public static class Delimiters
{
    public const char FileSeparator = (char) 28;
    public const char GroupSeparator = (char) 29;
    public const char RecordSeparator = (char) 30;
    public const char UnitSeparator = (char) 31;

    public static void EnsureNoUnitSeparator(string s)
    {
        if (s.Contains(UnitSeparator))
            throw new ArgumentException("String contained ASCII separator character"); 
    }
}

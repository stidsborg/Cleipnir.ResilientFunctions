﻿namespace Cleipnir.ResilientFunctions.SqlServer;

public static class SqlError
{
    public const int UNIQUENESS_INDEX_VIOLATION = 2601;
    public const int UNIQUENESS_VIOLATION = 2627;
    public const int TABLE_ALREADY_EXISTS = 2714;
    public const int TABLE_DOES_NOT_EXIST = 3701;
    public const int DEADLOCK_VICTIM = 1205;
}
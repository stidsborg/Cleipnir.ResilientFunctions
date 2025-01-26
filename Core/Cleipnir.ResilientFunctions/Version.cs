using System;

namespace Cleipnir.ResilientFunctions;

public static class Version
{
    public const int CurrentMajor = 4;
    
    public static void EnsureSchemaVersion(int atVersion)
    {
        if (atVersion == CurrentMajor)
            return;
        
        throw new SchemaMigrationRequiredException($"Database schema must be migrated from '{atVersion}' -> '{CurrentMajor}'. See 'http://cleipnir.net/tree/main/Migrations'");
    } 
}

public class SchemaMigrationRequiredException : Exception
{
    public SchemaMigrationRequiredException(string? message) : base(message) { }
}
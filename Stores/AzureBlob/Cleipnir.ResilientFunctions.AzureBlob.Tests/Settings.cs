using System;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

public static class Settings
{
    public static string? ConnectionString { get; } = Environment.GetEnvironmentVariable("AZURE_BLOB_CONNECTIONSTRING");
}
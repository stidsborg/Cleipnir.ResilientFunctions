using System.Security.Cryptography;
using System.Text;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class Hashing
{
    public static string GenerateSHA256Hash(this string input)
    {
        // Convert the input string to a byte array and compute the hash.
        var bytes = Encoding.UTF8.GetBytes(input);
        using SHA256 sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(bytes);

        // Convert the byte array to a hexadecimal string.
        var builder = new StringBuilder(capacity: 64);
        foreach (var hashByte in hashBytes)
            builder.Append(hashByte.ToString("x2")); // "x2" for lowercase

        return builder.ToString();
    }
}
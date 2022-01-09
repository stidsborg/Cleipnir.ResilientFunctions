using System.Security.Cryptography;
using System.Text;

namespace Cleipnir.ResilientFunctions.Helpers
{
    public static class HashHelper
    {
        public static string SHA256Hash(string value)
        {
            var stringBuilder = new StringBuilder();

            using (var hash = SHA256.Create())            
            {
                var enc = Encoding.UTF8;
                var result = hash.ComputeHash(enc.GetBytes(value));

                foreach (var b in result)
                    stringBuilder.Append(b.ToString("x2"));
            }

            return stringBuilder.ToString();
        }
    }
}
using System.Text;

namespace CamusDB.Client;

internal static class CamusJsonContent
{
    public static StringContent Create(string json) =>
        new(json, Encoding.UTF8, "application/json");
}

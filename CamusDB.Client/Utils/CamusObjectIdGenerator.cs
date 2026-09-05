
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Runtime.CompilerServices;

namespace CamusDB.Core.Util.ObjectIds;

/**
 * ObjectId generation is inspired in the MongoId generator
 * 
 * The 12-byte ObjectId value consists of:
 * 
 * a 4-byte timestamp value, representing the ObjectId's creation, measured in seconds since the Unix epoch
 * a 5-byte random value generated once per process
 * a 3-byte counter, initialized to a random value
 * 
 * https://docs.mongodb.com/manual/reference/method/ObjectId/
 *
 * The 5-byte per-process value and the counter's starting point come from the cryptographic random
 * number generator. They used to be derived from the machine name's hash, the process id and
 * System.Random, all of which an outsider can guess or enumerate — so a row's identifier disclosed the
 * neighbouring identifiers. It is still an identifier and not a secret: the timestamp is in plain sight
 * and the counter is sequential by design. Nothing should treat one as a capability. But what can be
 * predicted from outside is now only what the format itself publishes.
 */
public sealed class CamusObjectIdGenerator
{
    /// <summary>The 5-byte per-process value, split the way it is laid out: 3 bytes in <c>b</c>'s high
    /// bits and 2 bytes spanning <c>b</c>'s low byte and <c>c</c>'s high byte.</summary>
    private static readonly int __staticMachine = RandomNumberGenerator.GetInt32(0x01000000);

    private static readonly short __staticPid = (short)RandomNumberGenerator.GetInt32(0x00010000);

    private static int __staticIncrement = RandomNumberGenerator.GetInt32(int.MaxValue);

    private static readonly DateTime epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetTimestampFromDateTime(DateTime dateTime)
    {
        return (int)((dateTime.ToUniversalTime() - epoch).TotalSeconds);
    }

    public static CamusObjectIdValue Generate()
    {
        int pid = __staticPid;
        int machine = __staticMachine;
        int timestamp = GetTimestampFromDateTime(DateTime.UtcNow);
        int increment = Interlocked.Increment(ref __staticIncrement) & 0x00ffffff; // only use low order 3 bytes

        if ((__staticMachine & 0xff000000) != 0)
            throw new ArgumentOutOfRangeException("machine", "The machine value must be between 0 and 16777215 (it must fit in 3 bytes).");

        if ((increment & 0xff000000) != 0)
            throw new ArgumentOutOfRangeException("increment", "The increment value must be between 0 and 16777215 (it must fit in 3 bytes).");

        int _a = timestamp;
        int _b = (machine << 8) | (((int)pid >> 8) & 0xff);
        int _c = ((int)pid << 24) | increment;

        return new CamusObjectIdValue(a: _a, b: _b, c: _c);
    }

    public static string GenerateAsString()
    {
        return Generate().ToString();
    }
}

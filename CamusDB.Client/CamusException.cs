
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

public class CamusException : Exception
{
	public string Code { get; }

	public CamusException(string code, string message) : base(message)
	{
		Code = code;
	}
}

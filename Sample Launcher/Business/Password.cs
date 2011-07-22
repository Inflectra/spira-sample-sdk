using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	public class Password
	{
		/// <summary>Rot13 encode the password, for simple security.</summary>
		/// <param name="Password">The password to encode.</param>
		/// <returns>The encoded password.</returns>
		public static string HashPassword(string Password)
		{
			List<char> retString = new List<char>();

			for (int i = 0; i < Password.Length; i++)
			{
				if (Password[i] <= 'Z' && Password[i] >= 'A')
					retString.Add((char)(((int)Password[i] - 'A' + 13) % 26 + 'A'));
				else if (Password[i] <= 'z' && Password[i] >= 'a')
					retString.Add((char)(((int)Password[i] - 'a' + 13) % 26 + 'a'));
				else
					retString.Add(Password[i]);
			}

			return new String(retString.ToArray());
		}

		/// <summary>Rot13 decode the password, for simple security.</summary>
		/// <param name="Password">The password to decode.</param>
		/// <returns>The decoded password.</returns>
		public static string UnHashPassword(string Password)
		{
			if (Password != null)
			{
				List<char> retString = new List<char>();

				for (int i = 0; i < Password.Length; i++)
				{
					char newLet = (char)(Password[i] - 13);
					if (Password[i] >= 'A' && Password[i] <= 'Z')
					{
						if (newLet < 'A') newLet = (char)(newLet + 26);
					}
					else if (Password[i] >= 'a' && Password[i] <= 'z')
					{
						if (newLet < 'a') newLet = (char)(newLet + 26);
					}
					else
						newLet = Password[i];

					retString.Add(newLet);
				}
				return new String(retString.ToArray());
			}
			else
			{
				return "";
			}

		}
	}
}

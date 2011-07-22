using System;

namespace Inflectra.Rapise.RapiseLauncher.Branding
{
	public static class BrandingExtensions
	{
		public static string Rebrand(this string str1)
		{
			if (!string.IsNullOrEmpty(str1))
			{
                if (Properties.Settings.Default.App_ApplyBranding)
				{
                    string rebrandedName = Resources.Main.App_CompanyName;
                    return str1.Replace("inflectra", rebrandedName.ToLowerInvariant()).Replace("Inflectra", rebrandedName);
				}
				else
				{
					return str1;
				}
			}
			else
			{
				return str1;
			}
		}
	}
}

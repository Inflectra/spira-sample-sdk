using System;
using System.Linq;
using Microsoft.VisualBasic.ApplicationServices;

using Inflectra.Rapise.RapiseLauncher.Business;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface
{
    /// <summary>
    /// Manager that ensures only one instance of the application is running
    /// </summary>
    public sealed class SingleInstanceManager : WindowsFormsApplicationBase
	{
        /// <summary>
        /// Constructor
        /// </summary>
		public SingleInstanceManager()
		{
			IsSingleInstance = true;

            StartupNextInstance += this_StartupNextInstance;
		}

        /// <summary>
        /// Starts up the next instance
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void this_StartupNextInstance(object sender, StartupNextInstanceEventArgs e)
        {
            try
            {
                Forms.frmMain form = MainForm as Forms.frmMain; //My derived form type
                form.Arguments = e.CommandLine;
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, "Error uncaught in base application:");
                throw ex;
            }
        }

        /// <summary>
        /// Creates the main form
        /// </summary>
        protected override void OnCreateMainForm()
        {
            try
            {
                MainForm = new Forms.frmMain();
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, "Error uncaught in base application:");
                throw ex;
            }
        }

 	}
}

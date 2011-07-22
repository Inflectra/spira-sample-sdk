using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using Inflectra.Rapise.RapiseLauncher.Business;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface
{
    /// <summary>
    /// Actually starts up the RapiseLauncher program
    /// </summary>
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            //Handle unhandled errors
            Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);
            Application.ThreadException += new System.Threading.ThreadExceptionEventHandler(Application_ThreadException);

            string[] args = Environment.GetCommandLineArgs();
            SingleInstanceManager manager = new SingleInstanceManager();
            manager.Run(args);
        }

        /// <summary>
        /// Called when background events throw an exception
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        static void Application_ThreadException(object sender, System.Threading.ThreadExceptionEventArgs e)
        {
            Logger.LogMessage(e.Exception, "Error uncaught in base application:");
            Application.Exit();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SampleAutomationEngine
{
    /// <summary>
    /// Contains some of the constants and enumerations used by the automation engine
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// The token reported by the automation and used to identify in SpiraTest
        /// </summary>
        public const string AUTOMATION_ENGINE_TOKEN = "SampleAutomationEngine"; // TODO: Need to change it to match the name of the engine

        /// <summary>
        /// The version number of the plugin.
        /// </summary>
        public const string AUTOMATION_ENGINE_VERSION = "1.0.0";    //TODO: Change to match your version numbering scheme.
    }
}

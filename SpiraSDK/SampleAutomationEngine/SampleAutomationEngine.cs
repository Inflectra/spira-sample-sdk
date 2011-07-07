using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.RemoteLaunch.Interfaces;
using Inflectra.RemoteLaunch.Interfaces.DataObjects;

namespace SampleAutomationEngine
{
    /// <summary>
    /// Sample data-synchronization provider that synchronizes incidents between SpiraTest/Plan/Team and an external system
    /// </summary>

    /// <summary>
    /// Sample test automation engine plugin that implements the IAutomationEngine class.
    /// This class is instantiated by the RemoteLaunch application
    /// </summary>
    /// <remarks>
    /// The AutomationEngine class provides some of the generic functionality
    /// </remarks>
    public class SampleAutomationEngine : AutomationEngine, IAutomationEngine
    {
        private const string CLASS_NAME = "SampleAutomationEngine";

        private bool TRACE_LOGGING_ENABLED = true;

        /// <summary>
        /// Constructor
        /// </summary>
        public SampleAutomationEngine()
        {
            //Set status to OK
            base.status = EngineStatus.OK;
        }


        /// <summary>
        /// Returns the author of the test automation engine
        /// </summary>
        public override string ExtensionAuthor
        {
            get
            {
                //TODO: Replace with your organization name
                return "Inflectra Corporation";
            }
        }

        /// <summary>
        /// The unique GUID that defines this automation engine
        /// </summary>
        public override Guid ExtensionID
        {
            get
            {
                //TODO: Generate a new GUID when you first create a new automation engine
                return new Guid("{5F98D722-6A83-42BE-AB2C-CF17762861A5}");
            }
        }

        /// <summary>
        /// Returns the display name of the automation engine
        /// </summary>
        public override string ExtensionName
        {
            get
            {
                //TODO: Change the display name to something meaningful for your engine
                return "Spira SDK Sample Automation Engine";
            }
        }

        /// <summary>
        /// Returns the unique token that identifies this automation engine to SpiraTest
        /// </summary>
        public override string ExtensionToken
        {
            get
            {
                return Constants.AUTOMATION_ENGINE_TOKEN;
            }
        }

        /// <summary>
        /// Returns the version number of this extension
        /// </summary>
        public override string ExtensionVersion
        {
            get
            {
                return Constants.AUTOMATION_ENGINE_VERSION;
            }
        }

        /// <summary>
        /// Adds a custom settings panel for allowing the user to set any engine-specific configuration values
        /// </summary>
        /// <remarks>
        /// 1) If you don't have any engine-specific settings, just comment out the entire Property
        /// 2) The SettingPanel needs to be implemented as a WPF XAML UserControl
        /// </remarks>
        public override System.Windows.UIElement SettingsPanel
        {
            get
            {
                return new AutomationEngineSettingsPanel();
            }
            set
            {
                AutomationEngineSettingsPanel settingsPanel = (AutomationEngineSettingsPanel)value;
                settingsPanel.SaveSettings();
            }
        }

        public override AutomatedTestRun StartExecution(AutomatedTestRun automatedTestRun)
        {
            throw new NotImplementedException();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.RemoteLaunch.Interfaces;
using Inflectra.RemoteLaunch.Interfaces.DataObjects;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

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

        /// <summary>
        /// This is the main method that is used to start automated test execution
        /// </summary>
        /// <param name="automatedTestRun">The automated test run object</param>
        /// <returns>Either the populated test run or an exception</returns>
        public override AutomatedTestRun StartExecution(AutomatedTestRun automatedTestRun)
        {
            //Set status to OK
            base.status = EngineStatus.OK;

            try
            {
                if (Properties.Settings.Default.TraceLogging)
                {
                    LogEvent("Starting test execution", EventLogEntryType.Information);
                }
                DateTime startDate = DateTime.Now;

                /*
                 * TODO: Instantiate the code/API used to access the external testing system
                 */

                //See if we have an attached or linked test script
                if (automatedTestRun.Type == AutomatedTestRun.AttachmentType.URL)
                {
                    //The "URL" of the test is actually the full file path of the file that contains the test script
                    //Some automation engines need additional parameters which can be provided by allowing the test script filename
                    //to consist of multiple elements separated by a specific character.
                    //Conventionally, most engines use the pipe (|) character to delimit the different elements

                    //To make it easier, we have certain shortcuts that can be used in the path
                    //This allows the same test to be run on different machines with different physical folder layouts
                    string path = automatedTestRun.FilenameOrUrl;
                    path = path.Replace("[MyDocuments]", Environment.GetFolderPath(System.Environment.SpecialFolder.MyDocuments));
                    path = path.Replace("[CommonDocuments]", Environment.GetFolderPath(System.Environment.SpecialFolder.CommonDocuments));
                    path = path.Replace("[DesktopDirectory]", Environment.GetFolderPath(System.Environment.SpecialFolder.DesktopDirectory));
                    path = path.Replace("[ProgramFiles]", Environment.GetFolderPath(System.Environment.SpecialFolder.ProgramFiles));
                    path = path.Replace("[ProgramFilesX86]", Environment.GetFolderPath(System.Environment.SpecialFolder.ProgramFilesX86));

                    //First make sure that the file exists
                    if (File.Exists(path))
                    {
                        if (Properties.Settings.Default.TraceLogging)
                        {
                            LogEvent("Executing " + Constants.EXTERNAL_SYSTEM_NAME + " test located at " + path, EventLogEntryType.Information);
                        }

                        /*
                         * TODO: Add the external-tool specific code to actually run the test located at the location specified by 'path'
                         */
                    }
                    else
                    {
                        throw new FileNotFoundException("Unable to find a " + Constants.EXTERNAL_SYSTEM_NAME + " test at " + path);
                    }
                }
                else
                {
                    //We have an embedded script which we need to send to the test execution engine
                    //If the automation engine doesn't support embedded/attached scripts, throw the following exception:

                    /*
                     * throw new InvalidOperationException("The " + Constants.EXTERNAL_SYSTEM_NAME + " automation engine only supports linked test scripts");
                     * 
                     */

                    //First we need to get the test script
                    if (automatedTestRun.TestScript == null || automatedTestRun.TestScript.Length == 0)
                    {
                        throw new ApplicationException("The provided " + Constants.EXTERNAL_SYSTEM_NAME + " test script is empty, aborting test execution");
                    }
                    string testScript = Encoding.UTF8.GetString(automatedTestRun.TestScript);

                    /*
                     * TODO: Add the external-tool specific code to actually run the test script stored in 'testScript'
                     */
                }

                //Track the time that it took to run the test
                DateTime endDate = DateTime.Now;

                //TODO: Continue from here

                /*
                //Now extract the test results and populate the test run object
                if (String.IsNullOrEmpty(automatedTestRun.RunnerName))
                {
                    automatedTestRun.RunnerName = this.ExtensionName;
                }
                automatedTestRun.RunnerTestName = Path.GetFileNameWithoutExtension(automatedTestRun.FilenameOrUrl);

                //Convert the status for use in SpiraTest
                TestRun.TestStatusEnum executionStatus;
                if (status == SeleniumCommand.CommandExecutionStatus.OK)
                {
                    executionStatus = TestRun.TestStatusEnum.Passed;
                }
                else
                {
                    executionStatus = TestRun.TestStatusEnum.Failed;
                }
                //Now build the results message
                StringBuilder resultsLog = new StringBuilder();
                int successCount = 0;
                int failureCount = 0;
                foreach (SeleniumCommand command in commands)
                {
                    resultsLog.AppendLine(command.ToString());
                    if (command.Status == SeleniumCommand.CommandExecutionStatus.OK)
                    {
                        successCount++;
                    }
                    else
                    {
                        failureCount++;
                    }
                }

                //Specify the start/end dates
                automatedTestRun.StartDate = startDate;
                automatedTestRun.EndDate = endDate;

                //The result log
                automatedTestRun.ExecutionStatus = executionStatus;
                automatedTestRun.RunnerMessage = "Tests completed with " + successCount + " successful commands and " + failureCount + " failures.";
                automatedTestRun.RunnerStackTrace = resultsLog.ToString();*/

                //Report as complete               
                base.status = EngineStatus.OK;
                return automatedTestRun;
            }
            catch (Exception exception)
            {
                //Log the error and denote failure
                LogEvent(exception.Message + " (" + exception.StackTrace + ")", EventLogEntryType.Error);

                //Report as completed with error
                base.status = EngineStatus.Error;
                throw exception;
            }
        }
    }
}

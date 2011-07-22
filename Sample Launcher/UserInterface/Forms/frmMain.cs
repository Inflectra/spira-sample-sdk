using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Inflectra.Rapise.RapiseLauncher.Business;
using System.Threading;
using System.Xml.Serialization;
using System.IO;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface.Forms
{
    public partial class frmMain : Form
    {
        private const string CLASS = "Inflectra.Rapise.RapiseLauncher.UserInterface.Forms.frmMain::";

        private const int MILISECS_SECOND = 1000;
        private const int MILISECS_MINUTE = 60000;
        private const int MILISECS_HOUR = 3600000;

        private bool isAboutDialogOpen = false;

        #region Notify Icon Indexes
        private int _intMnuStatusIdx = -1;
        private int _intMnuPauseIdx = -1;
        private int _intMnuResumeIdx = -1;
        #endregion

        #region Internal Parameters
        private bool _isRunning;
        private bool _isExiting;
        private List<PendingSet> _pendingSets;
        private PendingSet _executingTest;
        private bool _isCurrentlyTesting;
        private CLI _Arguments;
        private bool _isInLoading;
        #endregion

        #region Thread Stuffs
        private Thread _testThread;
        private RunTestThread _testThreadClass;
        #endregion

        #region Timer Items
        //private DateTime _dateNextPoll;
        private PauseTimer _timerPoll;
        private PauseTimer _timerMenuUpdate;
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public frmMain()
        {
            InitializeComponent();
        }


        #region General Setting Properties

        /// <summary>
        /// The list of command-line arguments
        /// </summary>
        public System.Collections.ObjectModel.ReadOnlyCollection<string> Arguments
        {
            get;
            set;
        }

        /// <summary>Whether or not the application's timer is running. True means it is, false means the application is paused.</summary>
        public bool IsTimerRunning
        {
            get
            {
                return this._isRunning;
            }
            set
            {
                this._isRunning = value;
                //Set the menu items...
                this.contextMenuStrip.Items[this._intMnuResumeIdx].Visible = !value;
                this.contextMenuStrip.Items[this._intMnuPauseIdx].Visible = value;
                //Set status page & menu label
                StatusImageEnum imageType = ((value) ? StatusImageEnum.Playing : StatusImageEnum.Paused);
                string imageMsg = ((value) ? string.Format(Properties.Resources.App_NotifyIconStatusNextUpdate, "...") : Properties.Resources.App_NotifyIconStatusPaused);
                this.UpdateStatuses(imageMsg, imageType);
                this.contextMenuStrip.Items[this._intMnuStatusIdx].Text = imageMsg;
            }
        }

        /// <summary>Whether or not an active poll is occurring.</summary>
        public bool IsCurrentlyPolling
        {
            get;
            set;
        }

        /// <summary>Whether or not a test is currently running.</summary>
        public bool IsCurrentlyTesting
        {
            get
            {
                return this._isCurrentlyTesting;
            }
            set
            {
                this._isCurrentlyTesting = value;
                Invoke(new Action<bool>(this.SetForceExecuteButton), new object[] { !value });
            }
        }
        #endregion


        #region Event Handlers

        /// <summary>
        /// Called when the form first loads
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void frmMain_Load(object sender, EventArgs e)
        {
            //Set the various icons from Branding Resources
            this.Icon = Branding.Resources.Main.App;
            this.trayIcon.Icon = Branding.Resources.Main.App;
            this.imgServer.Image = Branding.Resources.Main.Server.ToBitmap();

            //Display the title bar and group title
            this.Text = Branding.Resources.Main.App_FullName;
            this.trayIcon.Text = Branding.Resources.Main.App_FullName;
            this.groupServerConnection.Text = Branding.Resources.Main.Server_ProductName + " Server Connection";

            //Parse the command-line arguments
            this._Arguments = new CLI(Arguments);
            this._Arguments.FilenamePassed += new EventHandler(CLI_FilenamePassed);
            this._Arguments.TestSetPassed += new EventHandler(CLI_TestSetPassed);

            //Set the logging settings
            if (this._Arguments.TraceLogging)
            {
                Logger.TraceLogging = true;
            }
            if (this._Arguments.LogToFile)
            {
                Logger.LoggingToFile = true;
            }

            //Specify the list that contains the pending sets
            this._pendingSets = new List<PendingSet>();

            //Load our initial settings, create icon and timers.
            this.load_LoadSettings();
            this.load_LoadContextMenu();
            this.load_LoadTimers();

            //Check that the required fields are entered in.
            if (!this.CheckSettings())
            {
                //Make the form show.
                List<string> args2 = new List<string>();
                args2.Add("-status");
                args2.AddRange(Arguments);
                this._Arguments = new CLI(args2);
            }

            //Poll if the user wanted to.
            if (this._Arguments.PollOnStartup)
                this.PollServer();

            //Start timers.
            if (!this._Arguments.StartupPaused)
                this.StartTimers();

            //If we have a filename, load it in.
            if (!string.IsNullOrEmpty(this._Arguments.Filename))
                this.load_LoadFile();

            //If we have a test set, load it in.
            if (this._Arguments.TestSetId.HasValue)
                this.load_LoadTestSet();

            //Add event handlers to the text boxes on the client config form
            this.txtServerUrl.TextChanged += new EventHandler(txt_TextChanged);
            this.txtServerUser.TextChanged += new EventHandler(txt_TextChanged);
            this.txtServerPassword.TextChanged += new EventHandler(txt_TextChanged);
            this.txtAutomationHostName.TextChanged += new EventHandler(txt_TextChanged);
            this.txtPollingFrequency.TextChanged += new EventHandler(txt_TextChanged);
            this.txtReadAhead.TextChanged += new EventHandler(txt_TextChanged);
            this.chkRunOverdue.CheckedChanged += new EventHandler(txt_TextChanged);

            //Add event handlers on the pending test set list box
            this.lstPendingRuns.SelectedIndexChanged +=new EventHandler(lstPendingRuns_SelectedIndexChanged);
        }

        /// <summary>
        /// Called when the test connection button is clicked
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnTest_Click(object sender, EventArgs e)
        {
            //First make sure the URL is valid
            try
            {
                Uri testUri = new Uri(this.txtServerUrl.Text);
            }
            catch (UriFormatException)
            {
                MessageBox.Show(Properties.Resources.Config_ServerSettings_InvalidUrl, Branding.Resources.Main.App_FullName, MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            //Make sure the URL doesn't have any .aspx extensions listed
            if (this.txtServerUrl.Text.Contains(".aspx"))
            {
                MessageBox.Show(Properties.Resources.Config_ServerSettings_UrlNotIncludeLogin, Branding.Resources.Main.App_FullName, MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            SpiraConnect spiraConnect = null;
            try
            {
                //Try and connect
                spiraConnect = new SpiraConnect(this.txtServerUrl.Text.Trim(), this.txtServerUser.Text.Trim(), this.txtServerPassword.Text.Trim(), Branding.Resources.Main.App_FullName, "");
            }
            catch (Exception exception)
            {
                //If we get an exception, display that we could not connect, together with the message
                MessageBox.Show(String.Format(Properties.Resources.Config_ServerSettings_ConnectionFailed, exception.Message), Branding.Resources.Main.App_FullName, MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            //Display success
            MessageBox.Show(Properties.Resources.Config_ServerSettings_ConnectedSucceeded, Branding.Resources.Main.App_FullName, MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// Called when the Save button is clicked
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnSave_Click(object sender, EventArgs e)
        {
            const string METHOD = "btnSave_Click()";
            Logger.LogTrace(CLASS + METHOD + " Enter");
            Logger.LogTrace(CLASS + METHOD + " User clicked btnSave");

            //Save all of it.
            this.SaveSettings();
            //In case settings were invalid, reload them.
            this.load_LoadSettings();

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Hit whenever the selection changes in the Pending Runs.</summary>
        /// <param name="sender">lstPendingRuns</param>
        /// <param name="e">SelectionChangedEventArgs</param>
        private void lstPendingRuns_SelectedIndexChanged(object sender, EventArgs e)
        {
            const string METHOD = "lstPendingRuns_SelectedIndexChanged()";
            Logger.LogTrace(CLASS + METHOD + " Enter");
            Logger.LogTrace(CLASS + METHOD + " Selection in lstPendingRuns changed");

            if (this.lstPendingRuns.SelectedItems.Count == 1)
            {
                this.btnForceExecute.Enabled = (((PendingSet)this.lstPendingRuns.SelectedItem).Status == PendingSet.PendingSetRunStatusEnum.NotStarted);
            }
        }

        /// <summary>
        /// Called when the Close button is clicked
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnClose_Click(object sender, EventArgs e)
        {
            const string METHOD = "btnClose_Click()";
            Logger.LogTrace(CLASS + METHOD + " User clicked btnClose");

            this.Hide();
        }

        /// <summary>
        /// Called when you want to force execution of a test set
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnForceExecute_Click(object sender, EventArgs e)
        {
            const string METHOD = "btnRunNow_Click()";
            Logger.LogTrace(CLASS + METHOD + " Enter");
            Logger.LogTrace(CLASS + METHOD + " User clicked btnRunNow");

            if (((PendingSet)this.lstPendingRuns.SelectedItem).Status == PendingSet.PendingSetRunStatusEnum.NotStarted)
            {
                ((PendingSet)this.lstPendingRuns.SelectedItem).ScheduledDate = DateTime.Now;
                this._pendingSets.Sort();
            }
            else
                this.btnForceExecute.Enabled = false;

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>
        /// Forces the system to Poll for new test sets
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnForcePoll_Click(object sender, EventArgs e)
        {
            const string METHOD = "btnPollNow_Click()";
            Logger.LogTrace(CLASS + METHOD + " Enter");
            Logger.LogTrace(CLASS + METHOD + " User clicked btnPollNow");

            if (!this.IsCurrentlyPolling || !this.IsCurrentlyTesting)
            {
                //Pause timer.
                bool isRunning = this._timerPoll.Active;
                this._timerPoll.Active = false;
                //Force a poll.
                this.PollServer();
                //Reset timer.
                this._timerPoll.Reset();
                if (isRunning) this._timerPoll.Active = true;
            }
            //Reset button status..
            this.btnForcePoll.Enabled = !this.IsCurrentlyTesting;

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Hit when the user tries to pass a filename argument after program has launched.</summary>
        /// <param name="sender">CLI</param>
        /// <param name="e">EventArgs</param>
        private void CLI_FilenamePassed(object sender, EventArgs e)
        {
            if (this.IsCurrentlyTesting)
            {
                MessageBox.Show(Properties.Resources.App_CannotRunManualTest, Properties.Resources.App_Error, MessageBoxButtons.OK);
            }
            else
            {
                //Stop timers.
                this.PauseTimers(true);
                //Load our XML file.
                this.load_LoadFile();
                //Program will exit after this, because we're no longer automated.
            }
        }

        /// <summary>Hit when the user tries to pass a test set argument after program has launched.</summary>
        /// <param name="sender">CLI</param>
        /// <param name="e">EventArgs</param>
        void CLI_TestSetPassed(object sender, EventArgs e)
        {
            if (this.IsCurrentlyTesting)
            {
                MessageBox.Show(Properties.Resources.App_CannotRunManualTest, Properties.Resources.App_Error, MessageBoxButtons.OK);
            }
            else
            {
                //Stop timers.
                this.PauseTimers(true);
                //Load our test set
                this.load_LoadTestSet();
                //Program will exit after this, because we're no longer automated.
            }
        }

        #endregion

        #region Internal Functions

        /// <summary>
        /// Loads the specified test set from the command-line
        /// </summary>
        private void load_LoadTestSet()
        {
            const string METHOD = "load_LoadTestSet()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Set screen.
            this.UpdateStatuses(Properties.Resources.App_RunningTest, StatusImageEnum.Testing);

            if (!this._Arguments.TestSetId.HasValue)
            {
                Logger.LogMessage(CLASS + METHOD + "Test Set ID is null!");
                Logger.LogTrace(CLASS + METHOD + " Exit");
                return;
            }

            int testSetId = this._Arguments.TestSetId.Value;
            Logger.LogTrace(CLASS + METHOD + "Running specified testset #" + testSetId.ToString() + " specified from command-line.");

            try
            {
                //Get the pending run..
                SpiraConnect client = new SpiraConnect(Properties.Settings.Default.Spira_Server, Properties.Settings.Default.Spira_User, Password.UnHashPassword(Properties.Settings.Default.Spira_Password), Branding.Resources.Main.App_FullName, Properties.Settings.Default.App_HostName);
                PendingSet pendingSet = client.GetSpecifiedTests(testSetId);
                this.UpdateStatuses(Properties.Resources.App_RunningTest + " " + pendingSet.Name);
                client.Disconnect();
                client = null;

                //Add it to the list.
                if (pendingSet != null)
                {
                    this._pendingSets = new List<PendingSet>();
                    this._pendingSets.Add(pendingSet);
                    this.lstPendingRuns.DataSource = this._pendingSets;
                    this.lstPendingRuns.Refresh();

                    //Now execute the test.
                    this.RunNextTest();
                    this._isExiting = true;
                }
                else
                {
                    Logger.LogMessage(CLASS + METHOD + " Could not retrieve specified test runs from server. NULL returned.", System.Diagnostics.EventLogEntryType.Error);
                    //Inform user, take them to settings page.
                    MessageBox.Show(Properties.Resources.App_ConnectToServerError, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                    this.Show();
                    this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                    this.tabMain.SelectedIndex = 1;
                    this._isExiting = false;
                }
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, CLASS + METHOD + " Could not retrieve specified test runs from server.");
                //Inform user, take them to settings page.
                MessageBox.Show(Properties.Resources.App_ConnectToServerError, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                this.Show();
                this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                this.tabMain.SelectedIndex = 1;
                this._isExiting = false;
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        private void load_LoadSettings()
        {
            const string METHOD = "load_LoadSettings()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            try
            {
                this.txtAutomationHostName.Text = Properties.Settings.Default.App_HostName;
                this.txtPollingFrequency.Text = Properties.Settings.Default.Spira_PollingFreq.ToString();
                this.txtReadAhead.Text = Properties.Settings.Default.Spira_PollAhead.ToString();
                this.txtServerPassword.Text = Password.UnHashPassword(Properties.Settings.Default.Spira_Password);
                this.txtServerUrl.Text = Properties.Settings.Default.Spira_Server;
                this.txtServerUser.Text = Properties.Settings.Default.Spira_User;
                this.chkRunOverdue.Checked = Properties.Settings.Default.App_RunOverdue;

                if (!Logger.TraceLogging)
                {
                    //If not set by command args then read from settings file.
                    Logger.TraceLogging = Properties.Settings.Default.App_TraceLog;
                }

                //Let's write out our settings..
                Logger.LogTrace(CLASS + METHOD + " Setting 'AutomationHostName': " + Properties.Settings.Default.App_HostName);
                Logger.LogTrace(CLASS + METHOD + " Setting 'PollingFrequency': " + Properties.Settings.Default.Spira_PollingFreq.ToString());
                Logger.LogTrace(CLASS + METHOD + " Setting 'ReadAhead': " + Properties.Settings.Default.Spira_PollAhead.ToString());
                Logger.LogTrace(CLASS + METHOD + " Setting 'ServerUrl': " + Properties.Settings.Default.Spira_Server);
                Logger.LogTrace(CLASS + METHOD + " Setting 'ServerUser': " + Properties.Settings.Default.Spira_User);
                Logger.LogTrace(CLASS + METHOD + " Setting 'ServerPassword': " + Properties.Settings.Default.Spira_Password);
                Logger.LogTrace(CLASS + METHOD + " Setting 'RunOverdue': " + Properties.Settings.Default.App_RunOverdue.ToString());
                Logger.LogTrace(CLASS + METHOD + " Setting 'TraceLogging': " + Properties.Settings.Default.App_TraceLog.ToString());

                if (this.CheckSettings())
                {
                    this.btnSave.Enabled = false;
                }
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex);
            }
            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Save settings from the form into the Settings Class.</summary>
        private void SaveSettings()
        {
            const string METHOD = "SaveSettings()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Reset polling timer if the value changed.
            if (this.txtPollingFrequency.Text.Trim().All(Char.IsNumber))
            {
                int numSecs = int.Parse(this.txtPollingFrequency.Text.Trim());
                if (numSecs != Properties.Settings.Default.Spira_PollingFreq)
                {
                    bool wasActive = this._timerPoll.Active;
                    this._timerPoll.Interval = numSecs * MILISECS_MINUTE;
                    if (wasActive) this._timerPoll.Active = true;
                }
            }
            //Save settings.
            Properties.Settings.Default.Spira_Server = this.txtServerUrl.Text.Trim();
            Properties.Settings.Default.Spira_User = this.txtServerUser.Text.Trim();
            Properties.Settings.Default.Spira_Password = Password.HashPassword(this.txtServerPassword.Text);
            Properties.Settings.Default.Spira_PollingFreq = int.Parse(this.txtPollingFrequency.Text);
            Properties.Settings.Default.Spira_PollAhead = int.Parse(this.txtReadAhead.Text);
            Properties.Settings.Default.App_HostName = this.txtAutomationHostName.Text.Trim();
            Properties.Settings.Default.App_RunOverdue = this.chkRunOverdue.Checked;
            Properties.Settings.Default.Save();

            //Set the flag.
            bool setNew = this.CheckSettings();
            if (!setNew)
            {
                this.PauseTimers(true);
                this.UpdateStatuses(Properties.Resources.App_InvalidConfiguration, StatusImageEnum.Error);
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Initially loads the timers and optionally starts them.o</summary>
        /// <param name="Start">Whether to start them. Default - no.</param>
        private void load_LoadTimers()
        {
            const string METHOD = "load_LoadTimers()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Set the polling timer.
            this._timerPoll = new PauseTimer(Properties.Settings.Default.Spira_PollingFreq * frmMain.MILISECS_MINUTE);
            this._timerPoll.Elapsed += new EventHandler(this._timerPoll_Elapsed);

            //Set the menu update timer.
            this._timerMenuUpdate = new PauseTimer(frmMain.MILISECS_SECOND);
            this._timerMenuUpdate.Elapsed += new EventHandler(this._timerMenuUpdate_Elapsed);

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Loads the context menu onto the Notify icon</summary>
        private void load_LoadContextMenu()
        {
            const string METHOD = "load_LoadContextMenu()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            // - Program Label
            ToolStripLabel mnuName = new ToolStripLabel();
            mnuName.Text = Branding.Resources.Main.App_FullName;
            mnuName.Margin = new Padding(-32, mnuName.Margin.Top, mnuName.Margin.Right, mnuName.Margin.Bottom);
            mnuName.Font = new Font(mnuName.Font.Name, mnuName.Font.Size, System.Drawing.FontStyle.Bold);
            // - Status Message
            ToolStripLabel mnuStatus = new ToolStripLabel();
            mnuStatus.Text = Properties.Resources.App_NotifyIconStatusPaused;
            mnuStatus.Enabled = false;
            // - Go/Pause buttons.
            ToolStripMenuItem mnuGo = new ToolStripMenuItem();
            mnuGo.Text = Properties.Resources.App_NotifyIconMenuResume;
            mnuGo.Click += new EventHandler(mnuPauseGo_Click);
            mnuGo.Image = Branding.Resources.Main.icoResume;
            mnuGo.ToolTipText = Properties.Resources.App_NotifyIconMenuResumeTooltip;
            mnuGo.Visible = !this.IsTimerRunning;
            ToolStripMenuItem mnuPause = new ToolStripMenuItem();
            mnuPause.Text = Properties.Resources.App_NotifyIconMenuPause;
            mnuPause.Click += new EventHandler(mnuPauseGo_Click);
            mnuPause.Image = Branding.Resources.Main.icoPause;
            mnuPause.ToolTipText = Properties.Resources.App_NotifyIconMenuPauseTooltip;
            mnuGo.Visible = this.IsTimerRunning;
            // - Poll Now
            ToolStripMenuItem mnuForce = new ToolStripMenuItem();
            mnuForce.Text = Properties.Resources.App_NotifyIconMenuPollnow;
            mnuForce.Click += new EventHandler(mnuForce_Click);
            mnuForce.Image = Branding.Resources.Main.icoForce;
            mnuForce.ToolTipText = Properties.Resources.App_NotifyIconMenuPollnowTooltip;
            mnuForce.Visible = !this.IsTimerRunning;
            // - Option button.
            ToolStripMenuItem mnuConfig = new ToolStripMenuItem();
            mnuConfig.Text = Properties.Resources.App_NotifyIconMenuConfig;
            mnuConfig.Click += new EventHandler(mnuConfig_Click);
            mnuConfig.Image = Branding.Resources.Main.icoSettings;
            mnuConfig.ToolTipText = Properties.Resources.App_NotifyIconMenuConfigTooltip;
            // - Exit button.
            ToolStripMenuItem mnuExit = new ToolStripMenuItem();
            mnuExit.Text = Properties.Resources.App_NotifyIconMenuExit;
            mnuExit.Click += new EventHandler(mnuExit_Click);
            mnuExit.Image = Branding.Resources.Main.icoExit;
            mnuExit.ToolTipText = Properties.Resources.App_NotifyIconMenuExitTooltip;
            // - Help button:
            // - - Help Content button
            ToolStripMenuItem mnuHelpContent = new ToolStripMenuItem();
            mnuHelpContent.Text = Properties.Resources.App_NotifyIconMenuHelpView;
            mnuHelpContent.Click += new EventHandler(mnuHelpContent_Click);
            mnuHelpContent.ToolTipText = Properties.Resources.App_NotifyIconMenuHelpViewTooltip;
            mnuHelpContent.DisplayStyle = ToolStripItemDisplayStyle.Text;
            // - - About button
            ToolStripMenuItem mnuHelpAbout = new ToolStripMenuItem();
            mnuHelpAbout.Text = Properties.Resources.App_NotifyIconMenuHelpAbout;
            mnuHelpAbout.Click += new EventHandler(mnuHelpAbout_Click);
            mnuHelpAbout.ToolTipText = Properties.Resources.App_NotifyIconMenuHelpViewTooltip;
            mnuHelpAbout.DisplayStyle = ToolStripItemDisplayStyle.Text;
            // - Help button
            ToolStripMenuItem mnuHelp = new ToolStripMenuItem();
            mnuHelp.Text = Properties.Resources.App_NotifyIconMenuHelp;
            mnuHelp.Image = Branding.Resources.Main.icoHelp;
            mnuHelp.ToolTipText = Properties.Resources.App_NotifyIconMenuHelpTooltip;
            mnuHelp.DropDownItems.Add(mnuHelpContent);
            mnuHelp.DropDownItems.Add(mnuHelpAbout);

            //Add menu items to the context menu.
            this.contextMenuStrip.Items.Add(mnuName);
            this._intMnuStatusIdx = this.contextMenuStrip.Items.Add(mnuStatus);
            this.contextMenuStrip.Items.Add(new ToolStripSeparator());
            this._intMnuResumeIdx = this.contextMenuStrip.Items.Add(mnuGo);
            this._intMnuPauseIdx = this.contextMenuStrip.Items.Add(mnuPause);
            this.contextMenuStrip.Items.Add(mnuForce);
            this.contextMenuStrip.Items.Add(new ToolStripSeparator());
            this.contextMenuStrip.Items.Add(mnuConfig);
            this.contextMenuStrip.Items.Add(new ToolStripSeparator());
            this.contextMenuStrip.Items.Add(mnuHelp);
            this.contextMenuStrip.Items.Add(new ToolStripSeparator());
            this.contextMenuStrip.Items.Add(mnuExit);

            //Add the context menu to the strip
            this.trayIcon.ContextMenuStrip = this.contextMenuStrip;

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Loads the specified file from the command-line.</summary>
        private void load_LoadFile()
        {
            const string METHOD = "load_LoadFile()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Set screen.
            this.UpdateStatuses(Properties.Resources.App_RunningTest, StatusImageEnum.Testing);

            TestSetLaunch specifiedLaunch = null;
            try
            {
                XmlSerializer dataSet = new XmlSerializer(typeof(TestSetLaunch));
                specifiedLaunch = (TestSetLaunch)dataSet.Deserialize(new StreamReader(this._Arguments.Filename));
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, CLASS + METHOD + "Could not load specified file: " + this._Arguments.Filename);
                Logger.LogTrace(CLASS + METHOD + " Exit");
                return;
            }

            //If it's not null, execute it.
            if (specifiedLaunch != null)
            {
                Logger.LogTrace(CLASS + METHOD + "Running specified testset #" + specifiedLaunch.TestSetId.ToString() + " specified from file.");

                try
                {
                    //Check r names match.
                    if (Properties.Settings.Default.Spira_Server.ToLowerInvariant().Trim().Trim(new char[] { '/', '\\' }) == specifiedLaunch.ServerUrl.ToLowerInvariant().Trim().Trim(new char[] { '/', '\\' }))
                    {
                        //Get the pending run..
                        SpiraConnect client = new SpiraConnect(Properties.Settings.Default.Spira_Server, Properties.Settings.Default.Spira_User, Password.UnHashPassword(Properties.Settings.Default.Spira_Password), Branding.Resources.Main.App_FullName, Properties.Settings.Default.App_HostName);
                        PendingSet pendingSet = client.GetSpecifiedTests(specifiedLaunch);
                        this.UpdateStatuses(Properties.Resources.App_RunningTest + " " + pendingSet.Name);
                        client.Disconnect();
                        client = null;

                        //Add it to the list.
                        if (pendingSet != null)
                        {
                            this._pendingSets = new List<PendingSet>();
                            this._pendingSets.Add(pendingSet);
                            this.lstPendingRuns.DataSource = this._pendingSets;
                            this.lstPendingRuns.Refresh();

                            //Now execute the test.
                            this.RunNextTest();
                            this._isExiting = true;
                        }
                        else
                        {
                            Logger.LogMessage(CLASS + METHOD + " Could not retrieve specified test runs from server. NULL returned.", System.Diagnostics.EventLogEntryType.Error);
                            //Inform user, take them to settings page.
                            MessageBox.Show(Properties.Resources.App_ConnectToServerError, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                            this.Show();
                            this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                            this.tabMain.SelectedIndex = 1;
                            this._isExiting = false;
                        }
                    }
                    else
                    {
                        Logger.LogMessage(CLASS + METHOD + " File did not come from configured host.", System.Diagnostics.EventLogEntryType.Error);
                        //Inform user, take them to settings page.
                        MessageBox.Show(Properties.Resources.App_ServerErrorMessage, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                        this.Show();
                        this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                        this.tabMain.SelectedIndex = 1;
                        this._isExiting = false;
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogMessage(ex, CLASS + METHOD + " Could not retrieve specified test runs from server.");
                    //Inform user, take them to settings page.
                    MessageBox.Show(Properties.Resources.App_ConnectToServerError, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                    this.Show();
                    this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                    this.tabMain.SelectedIndex = 1;
                    this._isExiting = false;
                }
            }
            else
            {
                Logger.LogMessage(CLASS + METHOD + " Specified file corrupt: " + this._Arguments.Filename, System.Diagnostics.EventLogEntryType.Error);
                //Inform user, take them to settings page.
                MessageBox.Show(Properties.Resources.App_ConnectToServerError, Properties.Resources.App_CannotRunTest, MessageBoxButtons.OK, MessageBoxIcon.Error);
                this.Show();
                this.UpdateStatuses("Could not connect to server.", StatusImageEnum.Error);
                this.tabMain.SelectedIndex = 1;
                this._isExiting = false;
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Polls the server for a new list of TestSets to execute.</summary>
        private void PollServer()
        {
            const string METHOD = "PollServer()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            try
            {
                bool isRunning = this.IsTimerRunning;

                if (!this.IsCurrentlyPolling && this.CheckSettings())
                {
                    Logger.LogTrace(CLASS + METHOD + " Settings & License OK, initiating poll.");

                    //Pause timers so we don't step over each other.
                    this.PauseTimers(true);

                    //Set flag.
                    this.IsCurrentlyPolling = true;

                    //Let's try to connect..
                    Logger.LogTrace(CLASS + METHOD + " Getting tests for host '" + Properties.Settings.Default.App_HostName + "' for the next " + Properties.Settings.Default.Spira_PollAhead.ToString() + " minutes.");
                    SpiraConnect client = new SpiraConnect(Properties.Settings.Default.Spira_Server, Properties.Settings.Default.Spira_User, Password.UnHashPassword(Properties.Settings.Default.Spira_Password), Branding.Resources.Main.App_FullName, Properties.Settings.Default.App_HostName);
                    this._pendingSets = client.GetUpcomingTests(DateTime.Now.AddMinutes(Properties.Settings.Default.Spira_PollAhead));
                    Logger.LogTrace(CLASS + METHOD + " Got " + this._pendingSets.Count.ToString() + " tests.");

                    //Now add our existing, running test to the list..
                    if (this._executingTest != null)
                        this._pendingSets.Add(this._executingTest);
                    this._pendingSets.Sort();

                    //Update form.
                    this.lstPendingRuns.DataSource = this._pendingSets;
                    this.lstPendingRuns.Refresh();

                    //Reset polling flag, restart timers.
                    this.IsCurrentlyPolling = false;
                    if (isRunning) this.PauseTimers(false);
                }
                else
                    Logger.LogTrace(CLASS + METHOD + " Settings & License Error, not polling.");

            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, "Could not connect to server.");
                this.UpdateStatuses(Branding.Resources.Main.App_ErrorConnecting, StatusImageEnum.Error);
                this.IsCurrentlyPolling = false;
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Called from the Polling timer to update the status to polling.</summary>
        /// <param name="Message">The message to display.</param>
        private void UpdateStatus_Polling(string Message)
        {
            if (this.IsTimerRunning)
            {
                try
                {
                    this.imgStatus.Image = Branding.Resources.Main.icoPoll_Lg;
                }
                catch (Exception)
                {
                    //Do nothing - sometimes Windows XP will throw a null exception
                }
                this.UpdateStatuses(Message);
            }
        }

        /// <summary>Called from the MenuUpdate timer, to update the menu and status to the timer.</summary>
        /// <param name="Message">The message to display.</param>
        private void UpdateStatus_TimerCount(string Message)
        {
            //Set the form status message.
            if (!this.IsCurrentlyPolling)
                this.UpdateStatuses(Message, StatusImageEnum.Playing);
        }

        /// <summary>Called from one up the UpdateStatus_ methods to change text in the menu/status.</summary>
        /// <param name="Message">The message string to populate.</param>
        private void UpdateStatuses(string Message, StatusImageEnum image = StatusImageEnum.None)
        {
            if (!this._isExiting)
            {
                this.lblStatus.Text = Message;
                this.contextMenuStrip.Items[this._intMnuStatusIdx].Text = Message;
                this.UpdateStatusesImage(image);
            }
        }

        /// <summary>Update the status image/background color to the onoe specified.</summary>
        /// <param name="imageType">StatusImageEnum</param>
        private void UpdateStatusesImage(StatusImageEnum imageType)
        {
            Image statusImage = null;

            switch (imageType)
            {
                case StatusImageEnum.Paused:
                    statusImage = Branding.Resources.Main.icoPaused_Lg;
                    this.lblStatus.BeginColor = Color.LightYellow;
                    this.lblStatus.EndColor = Color.Yellow;
                    break;
                case StatusImageEnum.Playing:
                    statusImage = Branding.Resources.Main.icoRunning_Lg;
                    this.lblStatus.BeginColor = Color.LightGreen;
                    this.lblStatus.EndColor = Color.Green;
                    break;
                case StatusImageEnum.Polling:
                    statusImage = Branding.Resources.Main.icoPoll_Lg;
                    this.lblStatus.BeginColor = Color.LightGreen;
                    this.lblStatus.EndColor = Color.Green;
                    break;
                case StatusImageEnum.Testing:
                    statusImage = Branding.Resources.Main.icoTesting_Lg;
                    this.lblStatus.BeginColor = Color.Lavender;
                    this.lblStatus.EndColor = Color.DarkBlue;
                    break;
                case StatusImageEnum.Error:
                    statusImage = Branding.Resources.Main.icoError_Lg;
                    this.lblStatus.BeginColor = Color.LightCoral;
                    this.lblStatus.EndColor = Color.Firebrick;
                    break;
                default:
                    break;
            }

            if (statusImage != null)
            {
                try
                {
                    this.imgStatus.Image = statusImage;
                }
                catch (Exception)
                {
                    //Do nothing - sometimes Windows XP will throw a null exception
                }
            }
        }

        /// <summary>Checks current settings to verify they're proper.</summary>
        /// <returns>Bool representing status.</returns>
        private bool CheckSettings()
        {
            const string METHOD = "CheckSettings()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            bool retValue = false;

            if (!string.IsNullOrEmpty(Properties.Settings.Default.App_HostName) &&
                !string.IsNullOrEmpty(Password.UnHashPassword(Properties.Settings.Default.Spira_Password)) &&
                !string.IsNullOrEmpty(Properties.Settings.Default.Spira_Server) &&
                !string.IsNullOrEmpty(Properties.Settings.Default.Spira_User) &&
                Properties.Settings.Default.Spira_PollAhead > 0 &&
                Properties.Settings.Default.Spira_PollingFreq > 0)
            {
                retValue = true;
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
            return retValue;
        }

        /// <summary>Starts the timers.</summary>
        private void StartTimers()
        {
            const string METHOD = "StartTimers()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            if (this.CheckSettings())
            {
                //Start Timers
                this._timerMenuUpdate.Active = true;
                this._timerPoll.Active = true;
                //Set form flag.
                this.IsTimerRunning = true;
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Runs the next test in the list.</summary>
        /// <param name="runOverdue">Run overdue tests</param>
        private void RunNextTest()
        {
            const string METHOD = "RunNextTest()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Save time, we can see if a poll is even needed.
            if (this._pendingSets.Count > 0)
            {
                if (!this.IsCurrentlyTesting && this.CheckSettings())
                {
                    Logger.LogTrace(CLASS + METHOD + " Settings & License OK, starting search for next test..");

                    //Check for any pending test runs if we're not running a test already..
                    for (int i = 0; i < this._pendingSets.Count; i++)
                    {
                        //Check if the index item is NotStarted and it's execution datetime has passed.
                        if (this._pendingSets.ElementAt(i).Status == PendingSet.PendingSetRunStatusEnum.NotStarted && (this._pendingSets.ElementAt(i).ScheduledDate < DateTime.Now))
                        {
                            //If it was marked as overdue, see if we have the OverDue flag set.
                            if (!this._pendingSets.ElementAt(i).OverDue || Properties.Settings.Default.App_RunOverdue)
                            {
                                Logger.LogTrace(CLASS + METHOD + "Testset #" + this._pendingSets.ElementAt(i).TestSetId + " being started.");

                                //Set it to our current executing..
                                this._executingTest = this._pendingSets.ElementAt(i);
                                this._pendingSets.ElementAt(i).Status = PendingSet.PendingSetRunStatusEnum.InProgress;
                                Invoke(new Action(this.lstPendingRuns.Refresh));

                                //Create the client for the ThreadClass and to update status.
                                SpiraConnect client = new SpiraConnect(Properties.Settings.Default.Spira_Server, Properties.Settings.Default.Spira_User, Password.UnHashPassword(Properties.Settings.Default.Spira_Password), Branding.Resources.Main.App_FullName, Properties.Settings.Default.App_HostName);
                                client.UpdateTestSetStatus(this._pendingSets.ElementAt(i).TestSetId, this._pendingSets.ElementAt(i).ProjectId, PendingSet.PendingSetRunStatusEnum.InProgress);

                                //Create a new thread class.
                                this._testThreadClass = new RunTestThread(client, this._pendingSets.ElementAt(i));
                                this._testThreadClass.WorkCompleted += new EventHandler<RunTestThread.WorkCompletedArgs>(_testThreadClass_WorkCompleted);
                                this._testThreadClass.WorkProgress += new EventHandler<RunTestThread.WorkProgressArgs>(_testThreadClass_WorkProgress);

                                //Create and fire off the Thread.
                                this._testThread = new Thread(this._testThreadClass.Execute);
                                this._testThread.Name = Branding.Resources.Main.App_FullName + " - " + "Test: " + this._pendingSets.ElementAt(i).Name;
                                this._testThread.Priority = ThreadPriority.Normal;
                                this.IsCurrentlyTesting = true;
                                this._testThread.Start();
                                //DEBUG: Only use the next line if we need to NOT use a background thread for debugging purposes
                                //this._testThreadClass.Execute();

                                //Fast forward to jump out of loop.
                                i = this._pendingSets.Count;
                            }
                            else
                            {
                                //Log it.
                                string strMsg = CLASS + METHOD + "Testset #" + this._pendingSets.ElementAt(i).TestSetId + " not run because it was overdue and configuration option not set.";
                                Logger.LogMessage(strMsg, System.Diagnostics.EventLogEntryType.Warning);
                            }
                        }
                    }
                }
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Single function to set the status of the ForceExecute button. (So can be called from Dispatcher.)</summary>
        /// <param name="value">Enabled or not.</param>
        private void SetForceExecuteButton(bool value)
        {
            this.btnForceExecute.Enabled = value;
        }

        /// <summary>Pauses or resumes timers and tracking dates.</summary>
        /// <param name="Paused">Whether to pause or resume the timers. Default = true to pause.</param>
        private void PauseTimers(bool Paused = true)
        {
            const string METHOD = "PauseTimers()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            if (this.CheckSettings())
            {
                switch (Paused)
                {
                    case true:
                        //Pausing the timers. Want to pause all of them.
                        this._timerMenuUpdate.Active = false;
                        this._timerPoll.Active = false;
                        //Set form flags.
                        this.IsTimerRunning = false;
                        break;

                    case false:
                        //Resuming timers. Need to reset the polling time as well.
                        this._timerMenuUpdate.Active = true;
                        //this._dateNextPoll = DateTime.Now.AddMilliseconds(this._timerPoll.TimeLeft);
                        this._timerPoll.Active = true;
                        //Set form flag.
                        this.IsTimerRunning = true;
                        break;
                }
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        #endregion

        #region Thread Events
        /// <summary>Called whenever the running test wants to update progress in the middle of a test. Currently not used.</summary>
        /// <param name="sender">RunTestThread</param>
        /// <param name="e">RunTestThread.WorkProgressArgs</param>
        private void _testThreadClass_WorkProgress(object sender, RunTestThread.WorkProgressArgs e)
        {
            //Currently no work done here.
        }

        /// <summary>Called whenever a test is finished, ready to launch the next.</summary>
        /// <param name="sender">RunTestThread</param>
        /// <param name="e">RunTestThread.WorkCompletedArgs</param>
        private void _testThreadClass_WorkCompleted(object sender, RunTestThread.WorkCompletedArgs e)
        {
            const string METHOD = "_testThreadClass_WorkCompleted()";
            Logger.LogTrace(CLASS + METHOD + " Enter");

            //Update the test set status.
            try
            {
                Logger.LogTrace(CLASS + METHOD + " Contacting server...");
                SpiraConnect client = new SpiraConnect(Properties.Settings.Default.Spira_Server, Properties.Settings.Default.Spira_User, Password.UnHashPassword(Properties.Settings.Default.Spira_Password), Branding.Resources.Main.App_FullName, Properties.Settings.Default.App_HostName);
                PendingSet.PendingSetRunStatusEnum finStatus = ((e.Exception != null && e.Exception.GetType() == typeof(RunTestThread.ExtensionNotLoadedException)) ? PendingSet.PendingSetRunStatusEnum.Blocked : PendingSet.PendingSetRunStatusEnum.Completed);
                Logger.LogTrace(CLASS + METHOD + " Recording result. TX:" + e.TestSet.TestSetId.ToString() + ", PR:" + e.TestSet.ProjectId.ToString() + ", " + finStatus.ToString());
                client.UpdateTestSetStatus(e.TestSet.TestSetId, e.TestSet.ProjectId, finStatus);
                client.Disconnect();
                client = null;
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex, CLASS + METHOD + " Could not connect to server. Test result could not be recorded.");
                e.Status = RunTestThread.WorkCompletedArgs.WorkCompletedStatusEnum.Error;
            }

            //Remove set that was just run.
            int selNum = this._pendingSets.IndexOf(this._executingTest);
            this._pendingSets[selNum].Status = (PendingSet.PendingSetRunStatusEnum)e.Status;

            //Un-set flags.
            this._executingTest = null;
            this.IsCurrentlyTesting = false;

            //See if we need to exit.
            if (!string.IsNullOrEmpty(this._Arguments.Filename))
            {
                this._isExiting = true;
                Invoke(new Action(this.Close));
            }
            if (this._Arguments.TestSetId.HasValue)
            {
                this._isExiting = true;
                Invoke(new Action(this.Close));
            }

            Logger.LogTrace(CLASS + METHOD + " Exit");
        }

        /// <summary>Hit when any one of the textboxes is changed, to set the flag.</summary>
        /// <param name="sender">txtServerUrl,txtServerUsertxtServerPassword,txtAutomationHostName,txtPollingFrequency,txtReadAhead</param>
        /// <param name="e"></param>
        private void txt_TextChanged(object sender, EventArgs e)
        {
            this.btnSave.Enabled = true;
        }

        #endregion

        #region Timer Events
        /// <summary>Hit once a second or so to update the pop-up menu.</summary>
        /// <param name="sender">Timer</param>
        /// <param name="e">ElapsedEventArgs</param>
        private void _timerMenuUpdate_Elapsed(object sender, EventArgs e)
        {
            if (!this._isExiting)
            {
                //We don't actually need the timer information, just find out how long until the next query.
                TimeSpan tspDiff = new TimeSpan(0, 0, 0, 0, this._timerPoll.TimeLeft);
                //Set the status label..
                string timeSpanDisplay = tspDiff.Days.ToString("00") + ":" + tspDiff.Hours.ToString("00") + ":" + tspDiff.Minutes.ToString("00") + ":" + tspDiff.Seconds.ToString("00");
                Invoke(new Action<string>(this.UpdateStatus_TimerCount),
                    new object[] 
				    {
					    string.Format(Properties.Resources.App_NotifyIconStatusNextUpdate, timeSpanDisplay),
				    });
            }
            if (!this._isExiting)
            {
                //Update the Test List..
                Invoke(new Action(this.lstPendingRuns.Refresh));

                //Check to see if any tests need to be launched.
                if (!this.IsCurrentlyTesting)
                    this.RunNextTest();
            }
        }

        /// <summary>Handles the event for when the timer has expired, and needs to call to get an updated list of events.</summary>
        /// <param name="sender">Timer</param>
        /// <param name="e">ElapsedEventArgs</param>
        private void _timerPoll_Elapsed(object sender, EventArgs e)
        {
            Invoke(new Action<string>(this.UpdateStatus_Polling), Properties.Resources.App_NotifyIconStatusPolling);

            Invoke(new Action(this.PollServer));
            //this.ShowBaloon("DEBUG: Polling SpiraTeam Server...", System.Windows.Forms.ToolTipIcon.Info, 6);

        }
        #endregion

        #region Tray Menu Events
        /// <summary>Pauses/Restarts the timer.</summary>
        /// <param name="sender">ToolStripMenuItem</param>
        /// <param name="e">EventArgs</param>
        private void mnuPauseGo_Click(object sender, EventArgs e)
        {
            if (this.IsTimerRunning)
                this.PauseTimers(true);
            else
                this.PauseTimers(false);
        }

        /// <summary>Opens the Configuration window.</summary>
        /// <param name="sender">ToolStripMenuItem</param>
        /// <param name="e">EventArgs</param>
        private void mnuConfig_Click(object sender, EventArgs e)
        {
            //Show the window if it's not shown.
            if (!this.Visible)
                this.Show();
            else
                this.Activate();

            this.tabMain.SelectedIndex = 1;
        }

        /// <summary>Hit when the user double0-clicks the Notification Icon</summary>
        /// <param name="sender">NotifyIcon</param>
        /// <param name="e">EventArgs</param>
        private void trayIcon_MouseDoubleClick(object sender, MouseEventArgs e)
        {
            //Show the window if it's not shown.
            if (!this.Visible)
                this.Show();
            else
                this.Activate();

            this.tabMain.SelectedIndex = 0;
        }

        /// <summary>Offers to exit the application.</summary>
        /// <param name="sender">ToolStripMenuItem</param>
        /// <param name="e">EventArgs</param>
        private void mnuExit_Click(object sender, EventArgs e)
        {
            if (MessageBox.Show(Properties.Resources.App_AreYouSureToExit, Properties.Resources.App_AreYouSureToExitHeader, MessageBoxButtons.YesNo, MessageBoxIcon.Stop, MessageBoxDefaultButton.Button2, MessageBoxOptions.ServiceNotification) == DialogResult.Yes)
            {
                this._isExiting = true;
                Application.Exit();
            }
        }

        /// <summary>Displays the About dialog.</summary>
        /// <param name="sender">ToolStripMenuItem</param>
        /// <param name="e">EventArgs</param>
        private void mnuHelpAbout_Click(object sender, EventArgs e)
        {
            //Only display one instance
            if (!this.isAboutDialogOpen)
            {
                frmAbout formAbout = new frmAbout();
                formAbout.Owner = this;
                formAbout.Closed += new EventHandler(formAbout_Closed);
                this.isAboutDialogOpen = true;
                formAbout.ShowDialog();
            }
        }

        /// <summary>Clears the dialog open flag once it is closed (so that it can be opened again)</summary>
        /// <param name="sender">frmAbout</param>
        /// <param name="e">EventArgs</param>
        void formAbout_Closed(object sender, EventArgs e)
        {
            this.isAboutDialogOpen = false;
        }

        /// <summary>Hit when the form is finished loading, will set form display based on command line argument.</summary>
        /// <param name="sender">frmConfig</param>
        /// <param name="e">EventArgs</param>
        private void frmMain_Loaded(object sender, EventArgs e)
        {
            if (!this._isExiting && !this.CheckSettings())
            {
                this.UpdateStatuses(Properties.Resources.App_InvalidConfiguration, StatusImageEnum.Error);
                MessageBox.Show(Properties.Resources.App_InvalidConfigurationMessage, Properties.Resources.App_InvalidConfiguration, MessageBoxButtons.OK, MessageBoxIcon.Error);
                this.PauseTimers(true);
            }

            //At end of initialization, see if we need to hide it or close.
            if (!this._Arguments.ShowStatus || this._isExiting)
                this.Close();
            else
                this.trayIcon_MouseDoubleClick(null, new MouseEventArgs(System.Windows.Forms.MouseButtons.Left, 2, 0, 0, 0));
        }

        /// <summary>Takes the user to the help.</summary>
        /// <param name="sender">ToolStripMenuItem</param>
        /// <param name="e">EventArgs</param>
        private void mnuHelpContent_Click(object sender, EventArgs e)
        {
            //Launch browser to documentation URL.
            try
            {
                System.Diagnostics.Process.Start(Branding.Resources.Main.App_HelpPDFURL);
            }
            catch (Exception ex)
            {
                // System.ComponentModel.Win32Exception is a known exception that occurs when Firefox is default browser.  
                // It actually opens the browser but STILL throws this exception so we can just ignore it.  If not this exception,
                // then attempt to open the URL in IE instead.
                if (ex.GetType().ToString().ToLowerInvariant() != "system.componentmodel.win32exception")
                {
                    // sometimes throws exception so we have to just ignore
                    // this is a common .NET bug that no one online really has a great reason for so now we just need to try to open
                    // the URL using IE if we can.
                    // DEBUG: Going to just re-try Firefox.
                    try
                    {
                        System.Diagnostics.Process.Start(Branding.Resources.Main.App_HelpPDFURL);
                        //System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo("IExplore.exe", sUrl);
                        //System.Diagnostics.Process.Start(startInfo);
                        //startInfo = null;
                    }
                    catch (Exception ex2)
                    {
                        Logger.LogMessage(ex2, "Could not open browser to Help PDF.");
                        MessageBox.Show(string.Format(Properties.Resources.App_ErrorLaunchingBrowserMessage, Branding.Resources.Main.App_HelpPDFURL), Properties.Resources.App_ErrorLaunchingBrowser, MessageBoxButtons.OK, MessageBoxIcon.Information);
                    }

                }
            }
        }

        /// <summary>Hit when the user wants to force a poll.</summary>
        /// <param name="sender">mnuForce</param>
        /// <param name="e">EventArgs</param>
        private void mnuForce_Click(object sender, EventArgs e)
        {
            this.PollServer();
        }
        #endregion

        #region Enumerations

        /// <summary>Specifies which image to display on the status bar.</summary>
        private enum StatusImageEnum : int
        {
            None = -1,
            Playing = 0,
            Paused = 1,
            Polling = 2,
            Error = 3,
            Testing = 4
        }

        #endregion

        /// <summary>
        /// Called when the form is trying to close
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void frmMain_FormClosing(object sender, FormClosingEventArgs e)
        {
            //Don't close the form, just hide unless this was sent from the context menu
            if (e.CloseReason == CloseReason.ApplicationExitCall)
            {
                //Stop and clear timers.
                this.PauseTimers(true);
                this._timerMenuUpdate = null;
                this._timerPoll = null;
            }
            else
            {
                //Cancel the default behavior
                e.Cancel = true;

                //Check for unsaved changes
                if (this.btnSave.Enabled)
				{
					DialogResult res = MessageBox.Show("You have unsaved changes! Save now?", "Unsaved Changes", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button3);

					if (res == DialogResult.Cancel)
						return;
                    else if (res == DialogResult.Yes)
						this.SaveSettings();
					else
						this.load_LoadSettings();
				}

                //Hide the main form
                this.Hide();
            }
        }
    }
}

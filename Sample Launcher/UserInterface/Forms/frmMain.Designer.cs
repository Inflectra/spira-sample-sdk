namespace Inflectra.Rapise.RapiseLauncher.UserInterface.Forms
{
    partial class frmMain
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.tabMain = new System.Windows.Forms.TabControl();
            this.tabStatus = new System.Windows.Forms.TabPage();
            this.label9 = new System.Windows.Forms.Label();
            this.btnForcePoll = new System.Windows.Forms.Button();
            this.btnForceExecute = new System.Windows.Forms.Button();
            this.lstPendingRuns = new System.Windows.Forms.ListBox();
            this.tabClientSetup = new System.Windows.Forms.TabPage();
            this.btnSave = new System.Windows.Forms.Button();
            this.groupServerPolling = new System.Windows.Forms.GroupBox();
            this.chkRunOverdue = new System.Windows.Forms.CheckBox();
            this.label8 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.txtReadAhead = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.txtPollingFrequency = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.txtAutomationHostName = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.groupServerConnection = new System.Windows.Forms.GroupBox();
            this.imgServer = new System.Windows.Forms.PictureBox();
            this.btnTest = new System.Windows.Forms.Button();
            this.txtServerPassword = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.txtServerUser = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.txtServerUrl = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.btnClose = new System.Windows.Forms.Button();
            this.trayIcon = new System.Windows.Forms.NotifyIcon(this.components);
            this.contextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.imgStatus = new Inflectra.Rapise.RapiseLauncher.UserInterface.Classes.TransparentControl();
            this.lblStatus = new Inflectra.Rapise.RapiseLauncher.UserInterface.Classes.GradientLabel();
            this.tabMain.SuspendLayout();
            this.tabStatus.SuspendLayout();
            this.tabClientSetup.SuspendLayout();
            this.groupServerPolling.SuspendLayout();
            this.groupServerConnection.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.imgServer)).BeginInit();
            this.SuspendLayout();
            // 
            // tabMain
            // 
            this.tabMain.Controls.Add(this.tabStatus);
            this.tabMain.Controls.Add(this.tabClientSetup);
            this.tabMain.Location = new System.Drawing.Point(12, 12);
            this.tabMain.Name = "tabMain";
            this.tabMain.SelectedIndex = 0;
            this.tabMain.Size = new System.Drawing.Size(460, 405);
            this.tabMain.TabIndex = 0;
            // 
            // tabStatus
            // 
            this.tabStatus.BackColor = System.Drawing.SystemColors.Control;
            this.tabStatus.Controls.Add(this.imgStatus);
            this.tabStatus.Controls.Add(this.label9);
            this.tabStatus.Controls.Add(this.btnForcePoll);
            this.tabStatus.Controls.Add(this.btnForceExecute);
            this.tabStatus.Controls.Add(this.lstPendingRuns);
            this.tabStatus.Controls.Add(this.lblStatus);
            this.tabStatus.Location = new System.Drawing.Point(4, 22);
            this.tabStatus.Name = "tabStatus";
            this.tabStatus.Padding = new System.Windows.Forms.Padding(3);
            this.tabStatus.Size = new System.Drawing.Size(452, 379);
            this.tabStatus.TabIndex = 0;
            this.tabStatus.Text = "Status";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label9.Location = new System.Drawing.Point(7, 54);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(125, 13);
            this.label9.TabIndex = 3;
            this.label9.Text = "Upcoming Test Sets:";
            // 
            // btnForcePoll
            // 
            this.btnForcePoll.Location = new System.Drawing.Point(108, 341);
            this.btnForcePoll.Name = "btnForcePoll";
            this.btnForcePoll.Size = new System.Drawing.Size(85, 23);
            this.btnForcePoll.TabIndex = 2;
            this.btnForcePoll.Text = "Force Poll";
            this.btnForcePoll.UseVisualStyleBackColor = true;
            this.btnForcePoll.Click += new System.EventHandler(this.btnForcePoll_Click);
            // 
            // btnForceExecute
            // 
            this.btnForceExecute.Enabled = false;
            this.btnForceExecute.Location = new System.Drawing.Point(7, 341);
            this.btnForceExecute.Name = "btnForceExecute";
            this.btnForceExecute.Size = new System.Drawing.Size(95, 23);
            this.btnForceExecute.TabIndex = 1;
            this.btnForceExecute.Text = "Force Execute";
            this.btnForceExecute.UseVisualStyleBackColor = true;
            this.btnForceExecute.Click += new System.EventHandler(this.btnForceExecute_Click);
            // 
            // lstPendingRuns
            // 
            this.lstPendingRuns.FormattingEnabled = true;
            this.lstPendingRuns.Location = new System.Drawing.Point(6, 70);
            this.lstPendingRuns.Name = "lstPendingRuns";
            this.lstPendingRuns.Size = new System.Drawing.Size(440, 264);
            this.lstPendingRuns.TabIndex = 0;
            // 
            // tabClientSetup
            // 
            this.tabClientSetup.BackColor = System.Drawing.SystemColors.Control;
            this.tabClientSetup.Controls.Add(this.btnSave);
            this.tabClientSetup.Controls.Add(this.groupServerPolling);
            this.tabClientSetup.Controls.Add(this.groupServerConnection);
            this.tabClientSetup.Location = new System.Drawing.Point(4, 22);
            this.tabClientSetup.Name = "tabClientSetup";
            this.tabClientSetup.Padding = new System.Windows.Forms.Padding(3);
            this.tabClientSetup.Size = new System.Drawing.Size(452, 379);
            this.tabClientSetup.TabIndex = 1;
            this.tabClientSetup.Text = "Client Setup";
            // 
            // btnSave
            // 
            this.btnSave.Location = new System.Drawing.Point(7, 320);
            this.btnSave.Name = "btnSave";
            this.btnSave.Size = new System.Drawing.Size(56, 23);
            this.btnSave.TabIndex = 2;
            this.btnSave.Text = "Save";
            this.btnSave.UseVisualStyleBackColor = true;
            this.btnSave.Click += new System.EventHandler(this.btnSave_Click);
            // 
            // groupServerPolling
            // 
            this.groupServerPolling.Controls.Add(this.chkRunOverdue);
            this.groupServerPolling.Controls.Add(this.label8);
            this.groupServerPolling.Controls.Add(this.label7);
            this.groupServerPolling.Controls.Add(this.txtReadAhead);
            this.groupServerPolling.Controls.Add(this.label6);
            this.groupServerPolling.Controls.Add(this.txtPollingFrequency);
            this.groupServerPolling.Controls.Add(this.label5);
            this.groupServerPolling.Controls.Add(this.txtAutomationHostName);
            this.groupServerPolling.Controls.Add(this.label4);
            this.groupServerPolling.Location = new System.Drawing.Point(6, 173);
            this.groupServerPolling.Name = "groupServerPolling";
            this.groupServerPolling.Size = new System.Drawing.Size(440, 135);
            this.groupServerPolling.TabIndex = 1;
            this.groupServerPolling.TabStop = false;
            this.groupServerPolling.Text = "Server Polling";
            // 
            // chkRunOverdue
            // 
            this.chkRunOverdue.AutoSize = true;
            this.chkRunOverdue.Location = new System.Drawing.Point(135, 93);
            this.chkRunOverdue.Name = "chkRunOverdue";
            this.chkRunOverdue.Size = new System.Drawing.Size(215, 17);
            this.chkRunOverdue.TabIndex = 14;
            this.chkRunOverdue.Text = "Automatically run tests that are overdue.";
            this.chkRunOverdue.UseVisualStyleBackColor = true;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(215, 73);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(34, 13);
            this.label8.TabIndex = 13;
            this.label8.Text = "(mins)";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(215, 46);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(34, 13);
            this.label7.TabIndex = 12;
            this.label7.Text = "(mins)";
            // 
            // txtReadAhead
            // 
            this.txtReadAhead.Location = new System.Drawing.Point(135, 66);
            this.txtReadAhead.Name = "txtReadAhead";
            this.txtReadAhead.Size = new System.Drawing.Size(76, 20);
            this.txtReadAhead.TabIndex = 11;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(10, 73);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(70, 13);
            this.label6.TabIndex = 10;
            this.label6.Text = "Read Ahead:";
            // 
            // txtPollingFrequency
            // 
            this.txtPollingFrequency.Location = new System.Drawing.Point(135, 40);
            this.txtPollingFrequency.Name = "txtPollingFrequency";
            this.txtPollingFrequency.Size = new System.Drawing.Size(76, 20);
            this.txtPollingFrequency.TabIndex = 9;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(10, 47);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(94, 13);
            this.label5.TabIndex = 8;
            this.label5.Text = "Polling Frequency:";
            // 
            // txtAutomationHostName
            // 
            this.txtAutomationHostName.Location = new System.Drawing.Point(135, 13);
            this.txtAutomationHostName.Name = "txtAutomationHostName";
            this.txtAutomationHostName.Size = new System.Drawing.Size(171, 20);
            this.txtAutomationHostName.TabIndex = 7;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(10, 20);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(119, 13);
            this.label4.TabIndex = 0;
            this.label4.Text = "Automation Host Name:";
            // 
            // groupServerConnection
            // 
            this.groupServerConnection.Controls.Add(this.imgServer);
            this.groupServerConnection.Controls.Add(this.btnTest);
            this.groupServerConnection.Controls.Add(this.txtServerPassword);
            this.groupServerConnection.Controls.Add(this.label3);
            this.groupServerConnection.Controls.Add(this.txtServerUser);
            this.groupServerConnection.Controls.Add(this.label2);
            this.groupServerConnection.Controls.Add(this.txtServerUrl);
            this.groupServerConnection.Controls.Add(this.label1);
            this.groupServerConnection.Location = new System.Drawing.Point(6, 17);
            this.groupServerConnection.Name = "groupServerConnection";
            this.groupServerConnection.Size = new System.Drawing.Size(440, 137);
            this.groupServerConnection.TabIndex = 0;
            this.groupServerConnection.TabStop = false;
            this.groupServerConnection.Text = "(Product) Server Connection";
            // 
            // imgServer
            // 
            this.imgServer.Location = new System.Drawing.Point(410, 21);
            this.imgServer.Name = "imgServer";
            this.imgServer.Size = new System.Drawing.Size(16, 16);
            this.imgServer.TabIndex = 7;
            this.imgServer.TabStop = false;
            // 
            // btnTest
            // 
            this.btnTest.Location = new System.Drawing.Point(80, 99);
            this.btnTest.Name = "btnTest";
            this.btnTest.Size = new System.Drawing.Size(75, 23);
            this.btnTest.TabIndex = 6;
            this.btnTest.Text = "Test";
            this.btnTest.UseVisualStyleBackColor = true;
            this.btnTest.Click += new System.EventHandler(this.btnTest_Click);
            // 
            // txtServerPassword
            // 
            this.txtServerPassword.Location = new System.Drawing.Point(80, 72);
            this.txtServerPassword.Name = "txtServerPassword";
            this.txtServerPassword.Size = new System.Drawing.Size(171, 20);
            this.txtServerPassword.TabIndex = 5;
            this.txtServerPassword.UseSystemPasswordChar = true;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(7, 75);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(56, 13);
            this.label3.TabIndex = 4;
            this.label3.Text = "Password:";
            // 
            // txtServerUser
            // 
            this.txtServerUser.Location = new System.Drawing.Point(80, 46);
            this.txtServerUser.Name = "txtServerUser";
            this.txtServerUser.Size = new System.Drawing.Size(171, 20);
            this.txtServerUser.TabIndex = 3;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(7, 49);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(36, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "Login:";
            // 
            // txtServerUrl
            // 
            this.txtServerUrl.Location = new System.Drawing.Point(80, 20);
            this.txtServerUrl.Name = "txtServerUrl";
            this.txtServerUrl.Size = new System.Drawing.Size(323, 20);
            this.txtServerUrl.TabIndex = 1;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(7, 23);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(66, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Server URL:";
            // 
            // btnClose
            // 
            this.btnClose.Location = new System.Drawing.Point(397, 423);
            this.btnClose.Name = "btnClose";
            this.btnClose.Size = new System.Drawing.Size(75, 23);
            this.btnClose.TabIndex = 1;
            this.btnClose.Text = "Close";
            this.btnClose.UseVisualStyleBackColor = true;
            this.btnClose.Click += new System.EventHandler(this.btnClose_Click);
            // 
            // trayIcon
            // 
            this.trayIcon.Text = "trayIcon";
            this.trayIcon.Visible = true;
            this.trayIcon.MouseDoubleClick += new System.Windows.Forms.MouseEventHandler(this.trayIcon_MouseDoubleClick);
            // 
            // contextMenuStrip
            // 
            this.contextMenuStrip.Name = "contextMenuStrip";
            this.contextMenuStrip.Size = new System.Drawing.Size(61, 4);
            // 
            // imgStatus
            // 
            this.imgStatus.BackColor = System.Drawing.Color.Transparent;
            this.imgStatus.Image = null;
            this.imgStatus.Location = new System.Drawing.Point(10, 9);
            this.imgStatus.Margin = new System.Windows.Forms.Padding(0);
            this.imgStatus.Name = "imgStatus";
            this.imgStatus.Size = new System.Drawing.Size(32, 32);
            this.imgStatus.TabIndex = 6;
            // 
            // lblStatus
            // 
            this.lblStatus.BeginColor = System.Drawing.SystemColors.ActiveCaption;
            this.lblStatus.EndColor = System.Drawing.SystemColors.Control;
            this.lblStatus.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblStatus.Location = new System.Drawing.Point(7, 9);
            this.lblStatus.Margin = new System.Windows.Forms.Padding(0);
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Padding = new System.Windows.Forms.Padding(40, 0, 0, 0);
            this.lblStatus.Size = new System.Drawing.Size(439, 32);
            this.lblStatus.TabIndex = 5;
            this.lblStatus.Text = "Status Message Text";
            this.lblStatus.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // frmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.ControlLightLight;
            this.ClientSize = new System.Drawing.Size(484, 462);
            this.Controls.Add(this.btnClose);
            this.Controls.Add(this.tabMain);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "frmMain";
            this.ShowInTaskbar = false;
            this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.frmMain_FormClosing);
            this.Load += new System.EventHandler(this.frmMain_Load);
            this.tabMain.ResumeLayout(false);
            this.tabStatus.ResumeLayout(false);
            this.tabStatus.PerformLayout();
            this.tabClientSetup.ResumeLayout(false);
            this.groupServerPolling.ResumeLayout(false);
            this.groupServerPolling.PerformLayout();
            this.groupServerConnection.ResumeLayout(false);
            this.groupServerConnection.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.imgServer)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabControl tabMain;
        private System.Windows.Forms.TabPage tabStatus;
        private System.Windows.Forms.TabPage tabClientSetup;
        private System.Windows.Forms.Button btnClose;
        private System.Windows.Forms.Button btnSave;
        private System.Windows.Forms.GroupBox groupServerPolling;
        private System.Windows.Forms.GroupBox groupServerConnection;
        private System.Windows.Forms.TextBox txtServerPassword;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtServerUser;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox txtServerUrl;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnTest;
        private System.Windows.Forms.TextBox txtPollingFrequency;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox txtAutomationHostName;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.TextBox txtReadAhead;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.CheckBox chkRunOverdue;
        private System.Windows.Forms.NotifyIcon trayIcon;
        private System.Windows.Forms.PictureBox imgServer;
        private System.Windows.Forms.Button btnForcePoll;
        private System.Windows.Forms.Button btnForceExecute;
        private System.Windows.Forms.ListBox lstPendingRuns;
        private System.Windows.Forms.Label label9;
        private Classes.GradientLabel lblStatus;
        private Classes.TransparentControl imgStatus;
        private System.Windows.Forms.ContextMenuStrip contextMenuStrip;
    }
}
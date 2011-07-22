using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	public class Logger
	{
		private static EventLog Eventlog;

		#region Properties
		/// <summary>Whether or not to log Informational events to the Application Event Log.</summary>
		public static bool TraceLogging
		{
			get;
			set;
		}

		/// <summary>Whether we're logging to an output file instead of the EventLog.</summary>
		public static bool LoggingToFile
		{
			get;
			set;
		}

		/// <summary>The filename that stores the file we're writing to.</summary>
		private static string _logFile
		{
			get;
			set;
		}
		#endregion

		/// <summary>Creates the log if needed.</summary>
		/// <param name="applicationname">The name of the application.</param>
		public Logger(string applicationName)
		{
			Exception createEx = null;
			Exception createFile = null;

			if (Logger.Eventlog == null)
			{
				if (!Logger.LoggingToFile)
				{
					try
					{
						//Create event source if needed.
						if (!System.Diagnostics.EventLog.SourceExists(applicationName))
							System.Diagnostics.EventLog.CreateEventSource(applicationName, "Application");
						if (Logger.Eventlog == null)
							Logger.Eventlog = new System.Diagnostics.EventLog("Application", ".", applicationName);
					}
					catch (Exception ex)
					{
						Logger.LoggingToFile = true;
						//Write message saying that we couldn't write to eventlog.
						createEx = ex;
					}

					if (Logger.Eventlog == null)
						Logger.LoggingToFile = true;
				}
			}

			if (LoggingToFile)
			{
				//Let's create the file if it doesn't exist, and write our launch.
				string LogPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData) + "\\" + applicationName.Replace(" ", "") + ".log";
				try
				{
					StreamWriter logFile = File.CreateText(LogPath);
					logFile.Close();

					Logger._logFile = LogPath;
				}
				catch (Exception ex)
				{
					try
					{
						//Error creating user's profile, let's try the temp directory.
						LogPath = Path.GetTempPath() + applicationName.Replace(" ", "") + ".log";
						StreamWriter logFile = File.CreateText(LogPath);
						logFile.Close();

						Logger._logFile = LogPath;
						createFile = ex;
					}
					catch (Exception ex2)
					{
						//We can't even log to file. Darnit.
					}
				}
			}

			//Write intro and any prevailing error messages.
			Logger.LogMessage("**********" + Environment.NewLine + "* RemoteLaunch (" + Assembly.GetExecutingAssembly().GetName().Version.ToString() + ")" + Environment.NewLine + "**********", EventLogEntryType.Information);
			if (createEx != null)
			{
				Logger.LogMessage(createEx, "Could not create EventLog");
			}
			if (createFile != null)
			{
				Logger.LogMessage(createFile, "Could not create initial log file");
			}
		}

		#region Static Log Methods
		/// <summary>Logs a message to the EventLog using the specified type.</summary>
		/// <param name="Message">The message to log.</param>
		/// <param name="messageType">The type of the message.</param>
		public static void LogMessage(string Message, EventLogEntryType messageType = EventLogEntryType.Information)
		{
			//Initialize if necessary.
			if (Logger.Eventlog == null && String.IsNullOrEmpty(Logger._logFile))
			{
				Logger log = new Logger(Branding.Resources.Main.App_FullName);
			}


			if (!Logger.LoggingToFile)
			{

				if ((messageType != EventLogEntryType.Information && messageType != EventLogEntryType.SuccessAudit) || Logger.TraceLogging)
					if (Logger.Eventlog != null)
						Logger.Eventlog.WriteEntry(Message, messageType);
			}
			else
			{
                if (!string.IsNullOrEmpty(Logger._logFile))
				{
					//Log the message to a file.
					string timestring = DateTime.Now.ToString("HH:mm:ss.fffffff") + ": ";
					try
					{
						string strMsg = timestring + Message.Replace(Environment.NewLine, Environment.NewLine + "\t");
						File.AppendAllText(Logger._logFile, strMsg + Environment.NewLine);
					}
					catch
					{ }
				}
			}
		}

		/// <summary>Logs an exception to the EventLog as an Error type message.</summary>
		/// <param name="ex">The exception to log.</param>
		public static void LogMessage(Exception ex, string message = null)
		{
			//Generate message.
			string strTrace = ex.StackTrace;
			string strMsg = ex.Message;
			while (ex.InnerException != null)
			{
				strMsg += Environment.NewLine + ex.InnerException.Message;
				ex = ex.InnerException;
			}
			strMsg += Environment.NewLine + Environment.NewLine + strTrace;
			if (message != null) strMsg = message + Environment.NewLine + strMsg;

			Logger.LogMessage(strMsg, EventLogEntryType.Error);
		}

		public static EventLog EventLog
		{
			get
			{
				return Logger.Eventlog;
			}
		}

		/// <summary>Logs a debug message to the debug console and EventLog if enabled.</summary>
		/// <param name="Message">The string to log.</param>
		public static void LogTrace(string Message)
		{
			//Log it to the debug console.
			Debug.WriteLine(DateTime.Now.ToString("HH:mm:ss.FFFFFFF") + ": " + Message);
			//Log it to the event log if enabled.
			if (Logger.TraceLogging || Logger.LoggingToFile)
				Logger.LogMessage(Message, EventLogEntryType.Information);
		}
		#endregion
	}
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

using Inflectra.Rapise.RapiseLauncher.Business.DataObjects;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	/// <summary>
	/// This class is responsible to actually communicating with SmarteStudio/Rapise's API for executing the tests
    /// and receiving the results
	/// </summary>
	public class AutomationEngine
    {
        #region Enumerations

        /// <summary>
        /// Defines the list of engine statuses
        /// </summary>
        public enum EngineStatus
        {
            OK = 1,
            Error = 2,
        }

        #endregion

        #region Public Members

        protected EngineStatus status;
		protected EventLog applicationLog;

		/// <summary>
		/// Returns the current status of the engine
		/// </summary>
		public EngineStatus Status
		{
			get
			{
				return this.status;
			}
		}

		/// <summary>
		/// Used to set a reference to the application Log
		/// </summary>
		public EventLog ApplicationLog
		{
			set
			{
				this.applicationLog = value;
			}
		}

        /// <summary>
        /// Actually runs the SmarteStudio/Rapise test run and extracts the results
        /// </summary>
        /// <param name="automatedTestRun">The automated test run</param>
        /// <returns>The test run with populated results</returns>
        /// <param name="documents">Any documents to the test case</param>
        /// <param name="projectId">The id of the Spira project</param>
        public AutomatedTestRun StartExecution(AutomatedTestRun automatedTestRun, int projectId, List<AttachedDocument> documents)
        {
            //TODO: SmarteSoft needs to implement this method
            throw new NotImplementedException();
        }

        #endregion

        #region Private/Protected Members

        /// <summary>
		/// Writes an entry to the application log
		/// </summary>
		/// <param name="message">The message to write</param>
		/// <param name="type">The type of message</param>
		protected void LogEvent(string message, EventLogEntryType type)
		{
			//Make sure we have an instantiated event log and write an entry to it
			if (this.applicationLog != null)
			{
				applicationLog.WriteEntry(message, type);
			}
		}

		#endregion
	}
}

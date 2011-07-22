using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.Rapise.RapiseLauncher.Business.SpiraImportExport;
using Inflectra.Rapise.RapiseLauncher.Business.DataObjects;
using System.Diagnostics;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	public class RunTestThread
	{
		private const string CLASS = "Inflectra.Rapise.RapiseLauncher.RunTestThread:";

		private PendingSet _testsToRun;
		private SpiraConnect _client;

		#region Events
		public event EventHandler<WorkCompletedArgs> WorkCompleted;
		public event EventHandler<WorkProgressArgs> WorkProgress;
		#endregion

		public RunTestThread(SpiraConnect client, PendingSet testToRun)
		{
			this._testsToRun = testToRun;
			this._client = client;
		}

		public void Execute()
		{
			const string METHOD = "Execute()";

			Logger.LogTrace(CLASS + METHOD + " Enter");

			//Loop through each test, execute it and then finally raise the event.
			bool isBlocked = false;
			for (int i = 0; i < this._testsToRun.TestRuns.Count() && !isBlocked; i++)
			{
				//DEBUG: Is it contained?
                RemoteAutomatedTestRun automatedTestRun = this._testsToRun.TestRuns.ElementAt(i);
                int testCaseId = automatedTestRun.TestCaseId;
                string strLog2 = " Checking TC:" + testCaseId + ", RunnerName: '" + automatedTestRun.AutomationEngineToken;
				Logger.LogTrace(CLASS + METHOD + strLog2);

				//Get the test details. Copy to the return set in case there's an error.
                int projectId = this._testsToRun.TestRuns.ElementAt(i).ProjectId.Value;
                AutomatedTestRun testToExecute = this._client.PopulateAutomatedTestScript(automatedTestRun, projectId);
				AutomatedTestRun testComplete = testToExecute;

				//Execute the test.
				try
				{
					if (testToExecute != null)
					{
						try
						{
							Logger.LogTrace(CLASS + METHOD + " Executing test TC:" + testCaseId);

                            //Create an instance of the automation engine
                            AutomationEngine engine = new AutomationEngine();
                            engine.ApplicationLog = Logger.EventLog;

                            //Download any documents attached to the test case
                            //Since they need to be passed to the test engine
                            List<AttachedDocument> attachedDocs = this._client.GetAttachedDocuments(projectId, testCaseId);

                            //Execute test.
                            testComplete.StartDate = DateTime.Now;
                            testComplete = engine.StartExecution(testToExecute, projectId, attachedDocs);
						}
						catch (Exception ex)
						{
							//Log it first.
							Logger.LogMessage(ex, CLASS + METHOD + " Error running test.");

							//Mark test as blocked, insert data.
							testComplete.ExecutionStatus = TestRun.TestStatusEnum.Blocked;
                            testComplete.RunnerName = automatedTestRun.AutomationEngineToken;
							testComplete.EndDate = testComplete.StartDate;
							//Get exception message.
							string strStack = "Stack Trace:" + Environment.NewLine + ex.StackTrace;
							string strMsg = ex.Message;
							while (ex.InnerException != null)
							{
								strMsg += Environment.NewLine + ex.InnerException.Message;
								ex = ex.InnerException;
							}
							testComplete.RunnerMessage = strMsg;
							testComplete.RunnerStackTrace = "Exception:" + Environment.NewLine + strMsg + Environment.NewLine + Environment.NewLine + strStack;
							Logger.LogTrace(CLASS + METHOD + " Sending result to server.");
							try
							{
								this._client.UpdateAutomatedTestRun(testComplete, this._testsToRun.ProjectId);
							}
							catch (Exception ex2)
							{
								Logger.LogMessage(ex2, CLASS + METHOD + " Could not send result to server.");
							}
						}
					}
					else
					{
                        Logger.LogMessage(CLASS + METHOD + " Retrieved test was null. Cannot execute." + Environment.NewLine + "Skipping TC:" + testCaseId + " in TX:" + automatedTestRun.TestSetId, System.Diagnostics.EventLogEntryType.Error);
					}
					//Record test.
					if (testComplete != null)
					{
						Logger.LogMessage(CLASS + METHOD + " Test finished. Recording results.");
						try
						{
							this._client.UpdateAutomatedTestRun(testComplete, this._testsToRun.ProjectId);
						}
						catch (Exception ex)
						{
							Logger.LogMessage(ex, CLASS + METHOD + " Error saving results.");
						}
					}
					else
					{
						Logger.LogMessage(CLASS + METHOD + " Returned tstComplete variable null! Not recording.", EventLogEntryType.Error);
					}
				}
				catch (Exception ex)
				{
					Logger.LogMessage(ex, CLASS + METHOD + " Uncaught error occurred when running test! Cancelling rest of pending tests.");
					this.WorkCompleted(this, new WorkCompletedArgs(this._testsToRun, ex, WorkCompletedArgs.WorkCompletedStatusEnum.Error));
					isBlocked = true;
				}

				//Now, report on specific test.
				this.WorkProgress(this, new WorkProgressArgs());
			}

            if (!isBlocked)
            {
                this.WorkCompleted(this, new WorkCompletedArgs(this._testsToRun, null, WorkCompletedArgs.WorkCompletedStatusEnum.OK));
            }
		}

		#region Event Classes
		/// <summary>Class to handle work being complete on a test case.</summary>
		public class WorkCompletedArgs : EventArgs
		{
			#region Internal Vars
			private PendingSet _testSet;
			private Exception _exception;
			#endregion

			#region Properties
			/// <summary>The test set that was completed.</summary>
			public PendingSet TestSet
			{
				get
				{
					return this._testSet;
				}
			}

			/// <summary>The status of the specified test set.</summary>
			public WorkCompletedStatusEnum Status
			{
				get;
				set;
			}

			/// <summary>Any exception that was thrown during execution.</summary>
			public Exception Exception
			{
				get
				{
					return this._exception;
				}
			}
			#endregion

			public WorkCompletedArgs(PendingSet TestSet, Exception ex, WorkCompletedStatusEnum FinishStatus)
			{
				this._testSet = TestSet;
				this._exception = ex;
				this.Status = FinishStatus;
			}

			/// <summary>The status of the final test run.</summary>
			public enum WorkCompletedStatusEnum : int
			{
				OK = 3,
				Error = 4
			}
		}
		/// <summary>Class to handle progress being reported on a test case.</summary>
		public class WorkProgressArgs : EventArgs
		{
		}

		/// <summary>The exception class when we don't have an extension loaded, or the extension is in error.</summary>
		public class ExtensionNotLoadedException : Exception
		{
			public ExtensionNotLoadedException(string Message)
				: base(Message)
			{ }

			public ExtensionNotLoadedException()
				: base("Extension was not loaded or was in error condition.")
			{ }
		}
		#endregion
	}
}

using System;
using System.Collections.Generic;
using System.ServiceModel;

using Inflectra.Rapise.RapiseLauncher.Business.DataObjects;
using Inflectra.Rapise.RapiseLauncher.Business.SpiraImportExport;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	public partial class SpiraConnect
	{
		public const string CLASS = "Inflectra.Rapise.RapiseLauncher.SpiraConnect:";

        #region Private Vars
		
        SpiraImportExport.ImportExportClient _client;
		private const string IMPORT_WEB_SERVICES_URL = "/Services/v3_0/ImportExport.svc";

		#endregion

		/// <summary>Creates a new instance of the class, and logs into the server.</summary>
		/// <param name="serverUrl">The Server's URL</param>
		/// <param name="userName">The username to log in as.</param>
		/// <param name="userPassword">The password for the user to log in as.</param>
		/// <param name="appName">The application name.</param>
		/// <param name="hostName">The host name of the machine to pull tests for.</param>
		public SpiraConnect(string serverUrl, string userName, string userPassword, string appName, string hostName)
		{
			const string METHOD = ".ctor()";
			Logger.LogTrace(CLASS + METHOD + " Enter");

			this.ServerUrl = serverUrl;
			this.UserName = userName;
			this.UserPassword = userPassword;
			this.AppName = AppName;
			this.HostName = hostName;

			//Create the client.
			if (_client == null)
			{
				this._client = new SpiraImportExport.ImportExportClient();
				this._client.Endpoint.Address = new EndpointAddress(this.ServerUrl + IMPORT_WEB_SERVICES_URL);
				ConfigureBinding((BasicHttpBinding)this._client.Endpoint.Binding, this._client.Endpoint.Address.Uri);

				//Try logging in.
				if (!this._client.Connection_Authenticate2(this.UserName, this.UserPassword, this.AppName))
				{
					Logger.LogTrace(CLASS + METHOD + " Couldn't log into SpiraTeam. Incorrect username / password.");
					Logger.LogTrace(CLASS + METHOD + " Exit");
					throw new Exception("Invalid username / password specified.");
				}
			}

			Logger.LogTrace(CLASS + METHOD + " Exit");
		}

		/// <summary>
		/// Retrieves the automated test script from the server for the provided automated test run and populates
		/// the test run info as well as the test script attachment info
		/// </summary>
		/// <param name="remoteAutomatedTestRun">The api data object from the server</param>
		/// <param name="projectId">The id of the current project</param>
		/// <returns>The automated test run object that will be passed to the plugins</returns>
		public AutomatedTestRun PopulateAutomatedTestScript(RemoteAutomatedTestRun remoteAutomatedTestRun, int projectId)
		{
			try
			{
				//Create a new automated test run object
				AutomatedTestRun automatedTestRun = new AutomatedTestRun();

				if (this._client.Connection_ConnectToProject(projectId))
				{
					if (remoteAutomatedTestRun.AutomationAttachmentId.HasValue)
					{
						RemoteDocument remoteDocument = this._client.Document_RetrieveById(remoteAutomatedTestRun.AutomationAttachmentId.Value);
						if (remoteDocument == null)
						{
							string strMessage = "Could not retrieve attachment for TC:" + remoteAutomatedTestRun.TestCaseId + ".";
							Logger.LogMessage(strMessage, System.Diagnostics.EventLogEntryType.Warning);
							return null;
						}
						else
						{
							//Populate the automated test run object from api test run object
							automatedTestRun.TestRunId = new Guid(); //Generates new random GUID

							//Standard Fields
							automatedTestRun.Name = remoteAutomatedTestRun.Name;
							automatedTestRun.AutomationEngineId = remoteAutomatedTestRun.AutomationEngineId;
							automatedTestRun.AutomationHostId = remoteAutomatedTestRun.AutomationHostId;
							automatedTestRun.AutomationEngineToken = remoteAutomatedTestRun.AutomationEngineToken;
							automatedTestRun.ReleaseId = remoteAutomatedTestRun.ReleaseId;
							automatedTestRun.TestCaseId = remoteAutomatedTestRun.TestCaseId;
							automatedTestRun.TestSetId = remoteAutomatedTestRun.TestSetId;
							automatedTestRun.TestSetTestCaseId = remoteAutomatedTestRun.TestSetTestCaseId;
							automatedTestRun.TestRunTypeId = remoteAutomatedTestRun.TestRunTypeId;
							automatedTestRun.TestSetTestCaseId = remoteAutomatedTestRun.TestSetTestCaseId;

							//Parameters
							if (remoteAutomatedTestRun.Parameters != null)
							{
								automatedTestRun.Parameters = new List<TestRunParameter>();
								foreach (RemoteTestSetTestCaseParameter parameter in remoteAutomatedTestRun.Parameters)
								{
									TestRunParameter testRunParameter = new TestRunParameter();
									testRunParameter.Name = parameter.Name;
									testRunParameter.Value = parameter.Value;
									automatedTestRun.Parameters.Add(testRunParameter);
								}
							}

							//Custom Properties
							automatedTestRun.Text01 = remoteAutomatedTestRun.Text01;
							automatedTestRun.Text02 = remoteAutomatedTestRun.Text02;
							automatedTestRun.Text03 = remoteAutomatedTestRun.Text03;
							automatedTestRun.Text04 = remoteAutomatedTestRun.Text04;
							automatedTestRun.Text05 = remoteAutomatedTestRun.Text05;
							automatedTestRun.Text06 = remoteAutomatedTestRun.Text06;
							automatedTestRun.Text07 = remoteAutomatedTestRun.Text07;
							automatedTestRun.Text08 = remoteAutomatedTestRun.Text08;
							automatedTestRun.Text09 = remoteAutomatedTestRun.Text09;
							automatedTestRun.Text10 = remoteAutomatedTestRun.Text10;
							automatedTestRun.List01 = remoteAutomatedTestRun.List01;
							automatedTestRun.List02 = remoteAutomatedTestRun.List02;
							automatedTestRun.List03 = remoteAutomatedTestRun.List03;
							automatedTestRun.List04 = remoteAutomatedTestRun.List04;
							automatedTestRun.List05 = remoteAutomatedTestRun.List05;
							automatedTestRun.List06 = remoteAutomatedTestRun.List06;
							automatedTestRun.List07 = remoteAutomatedTestRun.List07;
							automatedTestRun.List08 = remoteAutomatedTestRun.List08;
							automatedTestRun.List09 = remoteAutomatedTestRun.List09;
							automatedTestRun.List10 = remoteAutomatedTestRun.List10;

							//Populate the script info
							automatedTestRun.FilenameOrUrl = remoteDocument.FilenameOrUrl;
							automatedTestRun.Type = (AutomatedTestRun.AttachmentType)remoteDocument.AttachmentTypeId;
							automatedTestRun.Size = remoteDocument.Size;
							if (automatedTestRun.Type == AutomatedTestRun.AttachmentType.File)
							{
								//Need to actually download the test script
								automatedTestRun.TestScript = this._client.Document_OpenFile(remoteDocument.AttachmentId.Value);
							}

							//Return the populated test run
							return automatedTestRun;
						}
					}
					else
					{
						string strMessage = "Could not find attachment for TC:" + remoteAutomatedTestRun.TestCaseId + ", ID #" + remoteAutomatedTestRun.AutomationAttachmentId + ".";
						Logger.LogMessage(strMessage, System.Diagnostics.EventLogEntryType.Warning);
						return null;
					}
				}
				else
				{
					string strMessage = "Could not log on to system.";
					Logger.LogMessage(strMessage, System.Diagnostics.EventLogEntryType.Warning);
					return null;
				}
			}
			catch (Exception ex)
			{
				Logger.LogMessage(ex);
				return null;
			}
		}

		/// <summary>Takes an automated test run and creates a test run in the application.</summary>
		/// <param name="automatedTestRun">An automated test run.</param>
		/// <param name="projectId">The projectId of the test run.</param>
		/// <returns>Status on updating.</returns>
		public bool UpdateAutomatedTestRun(AutomatedTestRun automatedTestRun, int projectId)
		{
			try
			{
				if (this._client.Connection_ConnectToProject(projectId))
				{
					//Create the RemoteTestRun.
					RemoteAutomatedTestRun updTestRun = new RemoteAutomatedTestRun();
					updTestRun.AutomationEngineId = updTestRun.AutomationEngineId;
					updTestRun.AutomationHostId = automatedTestRun.AutomationHostId;
					updTestRun.AutomationEngineToken = automatedTestRun.AutomationEngineToken;
					updTestRun.EndDate = automatedTestRun.EndDate;
					updTestRun.ExecutionStatusId = (int)automatedTestRun.ExecutionStatus;
					updTestRun.List01 = automatedTestRun.List01;
					updTestRun.List02 = automatedTestRun.List02;
					updTestRun.List03 = automatedTestRun.List03;
					updTestRun.List04 = automatedTestRun.List04;
					updTestRun.List05 = automatedTestRun.List05;
					updTestRun.List06 = automatedTestRun.List06;
					updTestRun.List07 = automatedTestRun.List07;
					updTestRun.List08 = automatedTestRun.List08;
					updTestRun.List09 = automatedTestRun.List09;
					updTestRun.List10 = automatedTestRun.List10;
					updTestRun.Name = automatedTestRun.Name;
					//Copy parameters..
					if (automatedTestRun.Parameters != null)
					{
						List<RemoteTestSetTestCaseParameter> trParms = new List<RemoteTestSetTestCaseParameter>();
						foreach (TestRunParameter parameter in automatedTestRun.Parameters)
						{
							RemoteTestSetTestCaseParameter testRunParam = new RemoteTestSetTestCaseParameter();
							testRunParam.Name = parameter.Name;
							testRunParam.Value = parameter.Value;
							trParms.Add(testRunParam);
						}
						updTestRun.Parameters = trParms.ToArray();
					}
					updTestRun.ProjectId = projectId;
					updTestRun.ReleaseId = automatedTestRun.ReleaseId;
					updTestRun.RunnerAssertCount = automatedTestRun.RunnerAssertCount;
					updTestRun.RunnerMessage = automatedTestRun.RunnerMessage;
					updTestRun.RunnerName = automatedTestRun.RunnerName;
					updTestRun.RunnerStackTrace = automatedTestRun.RunnerStackTrace;
					updTestRun.RunnerTestName = automatedTestRun.RunnerTestName;
					updTestRun.StartDate = automatedTestRun.StartDate;
					updTestRun.TestCaseId = automatedTestRun.TestCaseId;
					updTestRun.TesterId = automatedTestRun.TesterId;
					//updTestRun.TestRunId = automatedTestRun.TestRunId;
					updTestRun.TestRunTypeId = automatedTestRun.TestRunTypeId;
					updTestRun.TestSetId = automatedTestRun.TestSetId;
					updTestRun.TestSetTestCaseId = automatedTestRun.TestSetTestCaseId;
					updTestRun.Text01 = automatedTestRun.Text01;
					updTestRun.Text02 = automatedTestRun.Text02;
					updTestRun.Text03 = automatedTestRun.Text03;
					updTestRun.Text04 = automatedTestRun.Text04;
					updTestRun.Text05 = automatedTestRun.Text05;
					updTestRun.Text06 = automatedTestRun.Text06;
					updTestRun.Text07 = automatedTestRun.Text07;
					updTestRun.Text08 = automatedTestRun.Text08;
					updTestRun.Text09 = automatedTestRun.Text09;
					updTestRun.Text10 = automatedTestRun.Text10;

					//That's it, go create the test run..
					this._client.TestRun_RecordAutomated1(updTestRun);

					//Return true.
					return true;
				}
				else
				{
					Logger.LogMessage("Could not connect to ProjectId " + projectId + ".", System.Diagnostics.EventLogEntryType.Warning);
					return false;
				}
			}
			catch (Exception ex)
			{
				Logger.LogMessage(ex);
				return false;
			}
		}

		/// <summary>Retrieves the upcoming tests that are scheduled up to the toDate.</summary>
		/// <param name="toDate">The date for the cutoff of upcoming tests.</param>
		/// <returns>A list of upcoming test runs.</returns>
		public List<PendingSet> GetUpcomingTests(DateTime toDate)
		{
			DateRange dteRange = new DateRange() { StartDate = DateTime.Now, EndDate = toDate };

			//Create a new dictionary..
			Dictionary<int, PendingSet> setRuns = new Dictionary<int, PendingSet>();

			//Get a list of projects and loop through each one.
			RemoteProject[] remoteProjects = this._client.Project_Retrieve();
			foreach (RemoteProject remoteProject in remoteProjects)
			{
				try
				{
					if (this._client.Connection_ConnectToProject(remoteProject.ProjectId.Value))
					{
						RemoteAutomatedTestRun[] testRuns = this._client.TestRun_CreateForAutomationHost(this.HostName, dteRange);
						//Loop through each TestRun..
						foreach (RemoteAutomatedTestRun testRun in testRuns)
						{
							//If the test set already exists..
							if (setRuns.ContainsKey(testRun.TestSetId.Value))
							{
								//Add it.
								setRuns[testRun.TestSetId.Value].AddPendingTestRun(testRun);
							}
							else
							{
								//Create a new one and add it.
								//Get the test set name..
								string strTestSetName = "Test Set #" + testRun.TestSetId.Value.ToString();
								try
								{
									RemoteTestSet specifiedTestSet = this._client.TestSet_RetrieveById(testRun.TestSetId.Value);
									strTestSetName = specifiedTestSet.Name;
								}
								catch { }

								//Create and add the pending set.
								PendingSet newSet = new PendingSet(strTestSetName, testRun);
								newSet.OverDue = (testRun.ScheduledDate < DateTime.Now);
								setRuns.Add(testRun.TestSetId.Value, newSet);
							}
						}
					}
					else
					{
						Logger.LogMessage("Error retrieving upcoming tests. Could not connect to project #" + remoteProject.ProjectId, System.Diagnostics.EventLogEntryType.Warning);
					}
				}
				catch
				{
					//No reason to log an error here.
				}
			}

			//Copy over the pending sets into a new date-ordered list.
			List<PendingSet> retDict = new List<PendingSet>();
			foreach (KeyValuePair<int, PendingSet> pendingSet in setRuns)
				retDict.Add(pendingSet.Value);

			return retDict;
		}

        /// <summary>Gets automated test runs for the specified test set id.</summary>
        /// <param name="testSetId">The specified test set from the command-line.</param>
        /// <returns>PendingSet</returns>
        public PendingSet GetSpecifiedTests(int testSetId)
        {
            try
            {
                PendingSet retSet = null;

                //Have to find the projectId that contains the test set
                RemoteTestSet remoteTestSet = null;
                RemoteProject[] remoteProjects = this._client.Project_Retrieve();
                for (int i = 0; i < remoteProjects.Length && remoteTestSet == null; i++)
                {
                    //Connect to the project.
                    if (this._client.Connection_ConnectToProject(remoteProjects[i].ProjectId.Value))
                    {
                        remoteTestSet = this._client.TestSet_RetrieveById(testSetId);
                    }
                }

                //Now get the newly created test runs..
                if (remoteTestSet != null)
                {
                    //Get the tests for the testset.
                    List<RemoteAutomatedTestRun> testRuns = new List<RemoteAutomatedTestRun>();
                    testRuns.AddRange(this._client.TestRun_CreateForAutomatedTestSet(testSetId, this.HostName));

                    //Create and add the pending set.
                    retSet = new PendingSet(remoteTestSet.Name);
                    foreach (RemoteAutomatedTestRun testRun in testRuns)
                    {
                        testRun.ScheduledDate = DateTime.Now;
                        retSet.AddPendingTestRun(testRun);
                    }
                }

                return retSet;

            }
            catch
            {
                return null;
            }
        }

		/// <summary>Returns the specified test set information.</summary>
		/// <param name="ProjectId">The project id of the test set.</param>
		/// <param name="TestSetId">The test set id to get information for.</param>
		/// <returns>A RemoteTestSet of the specified ID, null if none or error.</returns>
		public RemoteTestSet GetTestSetById(int ProjectId, int TestSetId)
		{
			try
			{
				//Switch to specified project.
				if (this._client.Connection_ConnectToProject(ProjectId))
					return this._client.TestSet_RetrieveById(TestSetId);
				else
					return null;
			}
			catch
			{
				return null;
			}
		}

		/// <summary>Updates the specified TestSet with the given status.</summary>
		/// <param name="TestSetId">The TestSet to update.</param>
		/// <param name="ProjectId">The ProjectId that contains the TestSet.</param>
		/// <param name="Status">The status to update the TestSet to.</param>
		public void UpdateTestSetStatus(int TestSetId, int ProjectId, PendingSet.PendingSetRunStatusEnum Status)
		{
			try
			{
				RemoteTestSet testSet = this.GetTestSetById(ProjectId, TestSetId);
				testSet.TestSetStatusId = (int)Status;
				this._client.TestSet_Update(testSet);
			}
			catch
			{ }
		}

        /// <summary>
        /// Returns the list of documents attached to a test case
        /// </summary>
        /// <param name="projectId">The id of the project</param>
        /// <param name="testCaseId">The id of the test case</param>
        /// <returns>The list of documents</returns>
        public List<AttachedDocument> GetAttachedDocuments(int projectId, int testCaseId)
        {
            List<AttachedDocument> attachedDocs = new List<AttachedDocument>();

            //Connect to the project.
            if (this._client.Connection_ConnectToProject(projectId))
            {
                try
                {
                    RemoteSort remoteSort = new RemoteSort();
                    remoteSort.PropertyName = "Filename";
                    remoteSort.SortAscending = true;
                    RemoteDocument[] remoteDocuments = this._client.Document_RetrieveForArtifact((int)ArtifactType.TestCase, testCaseId, null, remoteSort);

                    //Iterate through each document and download the physical file if it is a file
                    foreach (RemoteDocument remoteDocument in remoteDocuments)
                    {
                        //Populate the internal object class
                        AttachedDocument attachedDoc = new AttachedDocument();
                        attachedDocs.Add(attachedDoc);
                        attachedDoc.Type = (AttachedDocument.AttachmentType)remoteDocument.AttachmentTypeId;
                        attachedDoc.FilenameOrUrl = remoteDocument.FilenameOrUrl;
                        attachedDoc.TestCaseId = remoteDocument.ArtifactId.Value;
                        attachedDoc.ProjectAttachmentTypeId = remoteDocument.ProjectAttachmentTypeId.Value;
                        attachedDoc.ProjectAttachmentFolderId = remoteDocument.ProjectAttachmentFolderId.Value;
                        attachedDoc.Size = remoteDocument.Size;
                        attachedDoc.Tags = remoteDocument.Tags;
                        attachedDoc.UploadDate = remoteDocument.UploadDate;
                        attachedDoc.EditedDate = remoteDocument.EditedDate;
                        attachedDoc.CurrentVersion = remoteDocument.CurrentVersion;

                        if (remoteDocument.AttachmentTypeId == (int)AttachedDocument.AttachmentType.File)
                        {
                            byte[] binaryData = this._client.Document_OpenFile(remoteDocument.AttachmentId.Value);
                            attachedDoc.BinaryData = binaryData;
                        }
                    }
                }
                catch (Exception exception)
                {
                    string strMessage = "Error getting attachments for test case TC:" + testCaseId + " in project PR:" + projectId + " (" + exception.Message + ")";
                    Logger.LogMessage(strMessage, System.Diagnostics.EventLogEntryType.Error);
                    return null;
                }
            }
            else
            {
                string strMessage = "Could not connect to project PR:" + projectId + ".";
                Logger.LogMessage(strMessage, System.Diagnostics.EventLogEntryType.Error);
                return null;
            }

            return attachedDocs;
        }

		/// <summary>Gets automated test runs for the specified TexstSetLaunch class.</summary>
		/// <param name="testSetLaunch">The specified data from a TST file.</param>
		/// <returns>PendingSet</returns>
		public PendingSet GetSpecifiedTests(TestSetLaunch testSetLaunch)
		{
			PendingSet retSet = null;

			//The testset.
			RemoteTestSet testSet = null;

			//Get the test set, first.
			if (testSetLaunch.ProjectId == null)
			{

				//Have to find the projectId.
				RemoteProject[] remoteProjects = this._client.Project_Retrieve();
				for (int i = 0; i < remoteProjects.Length && testSet == null; i++)
				{
					//Connect to the project.
					if (this._client.Connection_ConnectToProject(remoteProjects[i].ProjectId.Value))
					{
						testSet = this._client.TestSet_RetrieveById(testSetLaunch.TestSetId);
					}
				}
			}
			else
			{
				if (this._client.Connection_ConnectToProject(testSetLaunch.ProjectId.Value))
				{
					testSet = this._client.TestSet_RetrieveById(testSetLaunch.TestSetId);
				}
			}

			//Now get the newly created test runs..
			if (testSet != null)
			{
				//Get the tests for the testset.
				List<RemoteAutomatedTestRun> testRuns = new List<RemoteAutomatedTestRun>();
				testRuns.AddRange(this._client.TestRun_CreateForAutomatedTestSet(testSetLaunch.TestSetId, this.HostName));

				//Create and add the pending set.
				retSet = new PendingSet(testSet.Name);
				foreach (RemoteAutomatedTestRun testRun in testRuns)
				{
					testRun.ScheduledDate = DateTime.Now;
					retSet.AddPendingTestRun(testRun);
				}
			}

			return retSet;
		}

		/// <summary>Disconnects the client.</summary>
		public void Disconnect()
		{
			this._client.Connection_Disconnect();
		}

		#region Internal Funcs
		/// <summary>Configure the SOAP connection for HTTP or HTTPS depending on what was specified</summary>
		/// <param name="httpBinding"></param>
		/// <param name="uri"></param>
		/// <remarks>Allows self-signed certs to be used</remarks>
		private void ConfigureBinding(BasicHttpBinding httpBinding, Uri uri)
		{
			//Handle SSL if necessary
			if (uri.Scheme == "https")
			{
				httpBinding.Security.Mode = BasicHttpSecurityMode.Transport;
				httpBinding.Security.Transport.ClientCredentialType = HttpClientCredentialType.None;

				//Allow self-signed certificates
				PermissiveCertificatePolicy.Enact("");
			}
			else
			{
				httpBinding.Security.Mode = BasicHttpSecurityMode.None;
			}
            httpBinding.AllowCookies = true;
		}
		#endregion

		#region Class Properties
		private string ServerUrl
		{
			get;
			set;
		}
		private string UserName
		{
			get;
			set;
		}
		private string UserPassword
		{
			get;
			set;
		}
		private string AppName
		{
			get;
			set;
		}
		private string HostName
		{
			get;
			set;
		}
		#endregion

		#region Enumeration
		public enum TestCaseStatusEnum : int
		{
			_Waiting = -2,
			_Running = -1,
			Failed = 1,
			Passed = 2,
			NotRun = 3,
			NotApplicable = 4,
			Blocked = 5,
			Caution = 6
		}

        /// <summary>
        /// The ids of artifact types used in SpiraTest
        /// </summary>
        public enum ArtifactType
        {
            TestCase = 2
        }

		#endregion
	}
}

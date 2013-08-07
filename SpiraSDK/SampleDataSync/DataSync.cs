using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.SpiraTest.PlugIns;
using System.Diagnostics;
using SampleDataSync.SpiraImportExport;
using System.Globalization;

namespace SampleDataSync
{
    /// <summary>
    /// Sample data-synchronization provider that synchronizes incidents between SpiraTest/Plan/Team and an external system
    /// </summary>
    /// <remarks>
    /// Requires Spira v4.0 or newer since it uses the v4.0+ compatible web service API
    /// </remarks>
    public class DataSync : IDataSyncPlugIn
    {
        //Constant containing data-sync name and internal API URL suffix to access
        private const string DATA_SYNC_NAME = "SampleDataSync"; //The name of the data-synchronization plugin
        private const string EXTERNAL_SYSTEM_NAME = "External System";  //The name of the external system we're integrating with
        private const string EXTERNAL_BUG_URL = "http://mybugtracker/{0}.htm";  //The URL format to use to link incidents to (leave null to not add a link)

        // Track whether Dispose has been called.
        private bool disposed = false;

        //Configuration data passed through from calling service
        private EventLog eventLog;
        private bool traceLogging;
        private int dataSyncSystemId;
        private string webServiceBaseUrl;
        private string internalLogin;
        private string internalPassword;
        private string connectionString;
        private string externalLogin;
        private string externalPassword;
        private int timeOffsetHours;
        private bool autoMapUsers;

        //Custom configuration properties
        private string custom01;
        private string custom02;
        private string custom03;
        private string custom04;
        private string custom05;

        /// <summary>
        /// Constructor, does nothing - all setup in the Setup() method instead
        /// </summary>
        public DataSync()
        {
            //Does Nothing - all setup in the Setup() method instead
        }

        /// <summary>
        /// Loads in all the configuration information passed from the calling service
        /// </summary>
        /// <param name="eventLog">Handle to the event log to use</param>
        /// <param name="dataSyncSystemId">The id of the plug-in used when accessing the mapping repository</param>
        /// <param name="webServiceBaseUrl">The base URL of the Spira web service</param>
        /// <param name="internalLogin">The login to Spira</param>
        /// <param name="internalPassword">The password used for the Spira login</param>
        /// <param name="connectionString">The URL for accessing the external system</param>
        /// <param name="externalLogin">The login used for accessing the external system</param>
        /// <param name="externalPassword">The password for the external system</param>
        /// <param name="timeOffsetHours">Any time offset to apply between Spira and the external system</param>
        /// <param name="autoMapUsers">Should we auto-map users</param>
        /// <param name="custom01">Custom configuration 01</param>
        /// <param name="custom02">Custom configuration 02</param>
        /// <param name="custom03">Custom configuration 03</param>
        /// <param name="custom04">Custom configuration 04</param>
        /// <param name="custom05">Custom configuration 05</param>
        public void Setup(
            EventLog eventLog,
            bool traceLogging,
            int dataSyncSystemId,
            string webServiceBaseUrl,
            string internalLogin,
            string internalPassword,
            string connectionString,
            string externalLogin,
            string externalPassword,
            int timeOffsetHours,
            bool autoMapUsers,
            string custom01,
            string custom02,
            string custom03,
            string custom04,
            string custom05
            )
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                //Set the member variables from the passed-in values
                this.eventLog = eventLog;
                this.traceLogging = traceLogging;
                this.dataSyncSystemId = dataSyncSystemId;
                this.webServiceBaseUrl = webServiceBaseUrl;
                this.internalLogin = internalLogin;
                this.internalPassword = internalPassword;
                this.connectionString = connectionString;
                this.externalLogin = externalLogin;
                this.externalPassword = externalPassword;
                this.timeOffsetHours = timeOffsetHours;
                this.autoMapUsers = autoMapUsers;
                this.custom01 = custom01;
                this.custom02 = custom02;
                this.custom03 = custom03;
                this.custom04 = custom04;
                this.custom05 = custom05;
            }
            catch (Exception exception)
            {
                //Log and rethrow the exception
                eventLog.WriteEntry("Unable to setup the " + DATA_SYNC_NAME + " plug-in ('" + exception.Message + "')\n" + exception.StackTrace, EventLogEntryType.Error);
                throw exception;
            }
        }

        /// <summary>
        /// Executes the data-sync functionality between the two systems
        /// </summary>
        /// <param name="LastSyncDate">The last date/time the plug-in was successfully executed (in UTC)</param>
        /// <param name="serverDateTime">The current date/time on the server (in UTC)</param>
        /// <returns>Code denoting success, failure or warning</returns>
        public ServiceReturnType Execute(DateTime? lastSyncDate, DateTime serverDateTime)
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                LogTraceEvent(eventLog, "Starting " + DATA_SYNC_NAME + " data synchronization", EventLogEntryType.Information);

                //Instantiate the SpiraTest web-service proxy class
                Uri spiraUri = new Uri(this.webServiceBaseUrl + Constants.WEB_SERVICE_URL_SUFFIX);
                SpiraImportExport.ImportExportClient spiraImportExport = SpiraClientFactory.CreateClient(spiraUri);

                /*
                 * TODO: Add the code to connect and authenticate to the external system
                 * Connect using the following variables:
                 *  this.connectionString
                 *  this.externalLogin
                 *  this.externalPassword
                 */

                //Now lets get the product name we should be referring to
                string productName = spiraImportExport.System_GetProductName();

                //**** Next lets load in the project and user mappings ****
                bool success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                if (!success)
                {
                    //We can't authenticate so end
                    LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                RemoteDataMapping[] projectMappings = spiraImportExport.DataMapping_RetrieveProjectMappings(dataSyncSystemId);
                RemoteDataMapping[] userMappings = spiraImportExport.DataMapping_RetrieveUserMappings(dataSyncSystemId);

                //Loop for each of the projects in the project mapping
                foreach (RemoteDataMapping projectMapping in projectMappings)
                {
                    //Get the SpiraTest project id equivalent external system project identifier
                    int projectId = projectMapping.InternalId;
                    string externalProjectId = projectMapping.ExternalKey;

                    //Connect to the SpiraTest project
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        LogErrorEvent(String.Format("Unable to connect to {0} project PR{1}, please check that the {0} login has the appropriate permissions", productName, projectId), EventLogEntryType.Error);
                        continue;
                    }

                    //Get the list of project-specific mappings from the data-mapping repository
                    //We need to get severity, priority, status and type mappings
                    RemoteDataMapping[] severityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Severity);
                    RemoteDataMapping[] priorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Priority);
                    RemoteDataMapping[] statusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Status);
                    RemoteDataMapping[] typeMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Type);

                    //Get the list of custom properties configured for this project and the corresponding data mappings
                    RemoteCustomProperty[] incidentCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Incident, false);
                    Dictionary<int, RemoteDataMapping> customPropertyMappingList = new Dictionary<int, RemoteDataMapping>();
                    Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList = new Dictionary<int, RemoteDataMapping[]>();
                    foreach (RemoteCustomProperty customProperty in incidentCustomProperties)
                    {
                        //Get the mapping for this custom property
                        if (customProperty.CustomPropertyId.HasValue)
                        {
                            RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                            customPropertyMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyMapping);

                            //For list types need to also get the property value mappings
                            if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List || customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                            {
                                RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                                customPropertyValueMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyValueMappings);
                            }
                        }
                    }

                    //Now get the list of releases and incidents that have already been mapped
                    RemoteDataMapping[] incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                    RemoteDataMapping[] releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release);

                    /*
                     * TODO: Next add the code to connect to the project in the external system if necessary
                     * The following variables can be used
                     *  this.externalLogin
                     *  this.externalPassword
                     *  externalProjectId
                     */

                    //**** First we need to get the list of recently created incidents in SpiraTest ****

                    //If we don't have a last-sync data, default to 1/1/1950
                    if (!lastSyncDate.HasValue)
                    {
                        lastSyncDate = DateTime.ParseExact("1/1/1950", "M/d/yyyy", CultureInfo.InvariantCulture);
                    }

                    //Get the incidents in batches of 100
                    List<RemoteIncident> incidentList = new List<RemoteIncident>();
                    long incidentCount = spiraImportExport.Incident_Count(null);
                    for (int startRow = 1; startRow <= incidentCount; startRow += Constants.INCIDENT_PAGE_SIZE)
                    {
                        RemoteIncident[] incidentBatch = spiraImportExport.Incident_RetrieveNew(lastSyncDate.Value, startRow, Constants.INCIDENT_PAGE_SIZE);
                        incidentList.AddRange(incidentBatch);
                    }
                    LogTraceEvent(eventLog, "Found " + incidentList.Count + " new incidents in " + productName, EventLogEntryType.Information);

                    //Create the mapping collections to hold any new items that need to get added to the mappings
                    //or any old items that need to get removed from the mappings
                    List<RemoteDataMapping> newIncidentMappings = new List<RemoteDataMapping>();
                    List<RemoteDataMapping> newReleaseMappings = new List<RemoteDataMapping>();
                    List<RemoteDataMapping> oldReleaseMappings = new List<RemoteDataMapping>();

                    //Iterate through each new Spira incident record and add to the external system
                    foreach (RemoteIncident remoteIncident in incidentList)
                    {
                        try
                        {
                            ProcessIncident(projectId, spiraImportExport, remoteIncident, newIncidentMappings, newReleaseMappings, oldReleaseMappings, customPropertyMappingList, customPropertyValueMappingList, incidentCustomProperties, incidentMappings, externalProjectId, productName, severityMappings, priorityMappings, statusMappings, typeMappings, userMappings, releaseMappings);
                        }
                        catch (Exception exception)
                        {
                            //Log and continue execution
                            LogErrorEvent("Error Adding " + productName + " Incident to " + EXTERNAL_SYSTEM_NAME +": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                        }
                    }

                    //Finally we need to update the mapping data on the server before starting the second phase
                    //of the data-synchronization
                    //At this point we have potentially added incidents, added releases and removed releases
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                    spiraImportExport.DataMapping_RemoveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, oldReleaseMappings.ToArray());

                    //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                    success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                    if (!success)
                    {
                        //We can't authenticate so end
                        LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                        return ServiceReturnType.Error;
                    }
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                        return ServiceReturnType.Error;
                    }

                    //**** Next we need to see if any of the previously mapped incidents has changed or any new items added to the external system ****
                    incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);

                    //Need to create a list to hold any new releases and new incidents
                    newIncidentMappings = new List<RemoteDataMapping>();
                    newReleaseMappings = new List<RemoteDataMapping>();

                    /*
                     * TODO: Call the External System API to get the list of recently added/changed bugs
                     * - i.e. bugs that have a last updated date >= filterDate and are in the appropriate project
                     * We shall simply use a generic list of objects to simulate this
                     */

                    DateTime filterDate = lastSyncDate.Value.AddHours(-timeOffsetHours);
                    if (filterDate < DateTime.Parse("1/1/1990"))
                    {
                        filterDate = DateTime.Parse("1/1/1990");
                    }
                    List<object> externalSystemBugs = null; //TODO: Replace with real code to get bugs created since filterDate

                    //Iterate through these items
                    foreach (object externalSystemBug in externalSystemBugs)
                    {
                        try
                        {
                            //Extract the data from the external bug object and load into Spira as a new incident
                            ProcessExternalBug(projectId, spiraImportExport, externalSystemBug, newIncidentMappings, newReleaseMappings, oldReleaseMappings, customPropertyMappingList, customPropertyValueMappingList, incidentCustomProperties, incidentMappings, externalProjectId, productName, severityMappings, priorityMappings, statusMappings, typeMappings, userMappings, releaseMappings);
                        }
                        catch (Exception exception)
                        {
                            //Log and continue execution
                            LogErrorEvent("Error Inserting/Updating " + EXTERNAL_SYSTEM_NAME + " Bug in " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                        }
                    }

                    //Finally we need to update the mapping data on the server
                    //At this point we have potentially added releases and incidents
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                }

                //The following code is only needed during debugging
                LogTraceEvent(eventLog, "Import Completed", EventLogEntryType.Warning);

                //Mark objects ready for garbage collection
                spiraImportExport = null;
                
                /*
                 * TODO: Set to null any objects releated to the external system, call Dispose() if they implement IDisposable
                 */

                //Let the service know that we ran correctly
                return ServiceReturnType.Success;
            }
            catch (Exception exception)
            {
                //Log the exception and return as a failure
                LogErrorEvent("General Error: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                return ServiceReturnType.Error;
            }
        }

        /// <summary>
        /// Processes a single SpiraTest incident record and adds to the external system
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteIncident">The Spira incident</param>
        /// <param name="newIncidentMappings">The list of any new incidents to be mapped</param>
        /// <param name="newReleaseMappings">The list of any new releases to be mapped</param>
        /// <param name="oldReleaseMappings">The list list of old releases to be un-mapped</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="incidentCustomProperties">The list of incident custom properties defined for this project</param>
        /// <param name="incidentMappings">The list of existing mapped incidents</param>
        /// <param name="externalProjectId">The id of the project in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="severityMappings">The incident severity mappings</param>
        /// <param name="priorityMappings">The incident priority mappings</param>
        /// <param name="statusMappings">The incident status mappings</param>
        /// <param name="typeMappings">The incident type mappings</param>
        /// <param name="userMappings">The incident user mappings</param>
        /// <param name="releaseMappings">The release mappings</param>
        private void ProcessIncident(int projectId, ImportExportClient spiraImportExport, RemoteIncident remoteIncident, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] incidentMappings, string externalProjectId, string productName, RemoteDataMapping[] severityMappings, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] typeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Get certain incident fields into local variables (if used more than once)
            int incidentId = remoteIncident.IncidentId.Value;
            int incidentStatusId = remoteIncident.IncidentStatusId.Value;

            //Make sure we've not already loaded this issue
            if (InternalFunctions.FindMappingByInternalId(projectId, incidentId, incidentMappings) == null)
            {
                //We need to add the incident number and owner in the description to make it easier
                //to track issues between the two systems. Also include a link
                string baseUrl = spiraImportExport.System_GetWebServerUrl();
                string incidentUrl = spiraImportExport.System_GetArtifactUrl((int)Constants.ArtifactType.Incident, projectId, incidentId, "").Replace("~", baseUrl);
                string externalName = remoteIncident.Name;
                string externalDescription = "Incident [" + Constants.INCIDENT_PREFIX + remoteIncident.IncidentId.ToString() + "|" + incidentUrl + "] detected by " + remoteIncident.OpenerName + " in " + productName + ".\n" + InternalFunctions.HtmlRenderAsPlainText(remoteIncident.Description);

                //Now get the external system's equivalent incident status from the mapping
                RemoteDataMapping dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId.Value, statusMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    LogErrorEvent("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string externalStatus = dataMapping.ExternalKey;

                //Now get the external system's equivalent incident type from the mapping
                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId.Value, typeMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    LogErrorEvent("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string externalType = dataMapping.ExternalKey;

                //Now get the external system's equivalent priority from the mapping (if priority is set)
                string externalPriority = "";
                if (remoteIncident.PriorityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, priorityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        LogErrorEvent("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalPriority = dataMapping.ExternalKey;
                    }
                }

                //Now get the external system's equivalent severity from the mapping (if severity is set)
                string externalSeverity = "";
                if (remoteIncident.SeverityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.SeverityId.Value, severityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the severity
                        LogErrorEvent("Unable to locate mapping entry for incident severity " + remoteIncident.SeverityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalSeverity = dataMapping.ExternalKey;
                    }
                }

                //Now get the external system's ID for the Opener/Detector of the incident (reporter)
                string externalReporter = "";
                dataMapping = FindUserMappingByInternalId(remoteIncident.OpenerId.Value, userMappings, spiraImportExport);
                //If we can't find the user, just log a warning
                if (dataMapping == null)
                {
                    LogErrorEvent("Unable to locate mapping entry for user id " + remoteIncident.OpenerId.Value + " so using synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    externalReporter = dataMapping.ExternalKey;
                }

                //Now get the external system's ID for the Owner of the incident (assignee)
                string externalAssignee = "";
                if (remoteIncident.OwnerId.HasValue)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OwnerId.Value, userMappings, spiraImportExport);
                    //If we can't find the user, just log a warning
                    if (dataMapping == null)
                    {
                        LogErrorEvent("Unable to locate mapping entry for user id " + remoteIncident.OwnerId.Value + " so leaving assignee empty", EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalAssignee = dataMapping.ExternalKey;
                    }
                }

                //Specify the detected-in version/release if applicable
                string externalDetectedRelease = "";
                if (remoteIncident.DetectedReleaseId.HasValue)
                {
                    int detectedReleaseId = remoteIncident.DetectedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new version in the external system and add to mappings
                        //Since version numbers are now unique in both systems, we can simply use that
                        LogTraceEvent(eventLog, "Adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + detectedReleaseId + "\n", EventLogEntryType.Information);

                        //Get the Spira release
                        RemoteRelease remoteRelease = spiraImportExport.Release_RetrieveById(detectedReleaseId);
                        if (remoteRelease != null)
                        {
                            /*
                             * TODO: Add the code to actually insert the new Release/Version in the external System
                             * using the values from the remoteRelease object.
                             * Need to get the ID of the new release from the external system and then
                             * populate the externalDetectedRelease variable with the value
                             */

                            //Add a new mapping entry
                            RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = detectedReleaseId;
                            newReleaseMapping.ExternalKey = externalDetectedRelease;
                            newReleaseMappings.Add(newReleaseMapping);
                        }
                    }
                    else
                    {
                        externalDetectedRelease = dataMapping.ExternalKey;
                    }

                    //Verify that this release still exists in the external system
                    LogTraceEvent(eventLog, "Looking for " + EXTERNAL_SYSTEM_NAME + " detected release: " + externalDetectedRelease + "\n", EventLogEntryType.Information);

                    /*
                     * TODO: Set the value of the matchFound flag based on whether the external release exists
                     */

                    bool matchFound = false;
                    if (matchFound)
                    {
                        //TODO: Set the externalRelease value on the external incident
                    }
                    else
                    {
                        //We can't find the matching item so log and just don't set the release
                        LogErrorEvent("Unable to locate " + EXTERNAL_SYSTEM_NAME + " detected release " + externalDetectedRelease + " in project " + externalProjectId, EventLogEntryType.Warning);

                        //Add this to the list of mappings to remove
                        RemoteDataMapping oldReleaseMapping = new RemoteDataMapping();
                        oldReleaseMapping.ProjectId = projectId;
                        oldReleaseMapping.InternalId = detectedReleaseId;
                        oldReleaseMapping.ExternalKey = externalDetectedRelease;
                        oldReleaseMappings.Add(oldReleaseMapping);
                    }
                }
                LogTraceEvent(eventLog, "Set " + EXTERNAL_SYSTEM_NAME + " detected release\n", EventLogEntryType.Information);

                //Specify the resolved-in version/release if applicable
                string externalResolvedRelease = "";
                if (remoteIncident.ResolvedReleaseId.HasValue)
                {
                    int resolvedReleaseId = remoteIncident.ResolvedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, resolvedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new version in the external system and add to mappings
                        //Since version numbers are now unique in both systems, we can simply use that
                        LogTraceEvent(eventLog, "Adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + resolvedReleaseId + "\n", EventLogEntryType.Information);

                        //Get the Spira release
                        RemoteRelease remoteRelease = spiraImportExport.Release_RetrieveById(resolvedReleaseId);
                        if (remoteRelease != null)
                        {
                            /*
                             * TODO: Add the code to actually insert the new Release/Version in the external System
                             * using the values from the remoteRelease object.
                             * Need to get the ID of the new release from the external system and then
                             * populate the externalResolvedRelease variable with the value
                             */

                            //Add a new mapping entry
                            RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                            newReleaseMapping.ProjectId = projectId;
                            newReleaseMapping.InternalId = resolvedReleaseId;
                            newReleaseMapping.ExternalKey = externalResolvedRelease;
                            newReleaseMappings.Add(newReleaseMapping);
                        }
                    }
                    else
                    {
                        externalResolvedRelease = dataMapping.ExternalKey;
                    }

                    //Verify that this release still exists in the external system
                    LogTraceEvent(eventLog, "Looking for " + EXTERNAL_SYSTEM_NAME + " resolved release: " + externalResolvedRelease + "\n", EventLogEntryType.Information);

                    /*
                     * TODO: Set the value of the matchFound flag based on whether the external release exists
                     */

                    bool matchFound = false;
                    if (matchFound)
                    {
                        //TODO: Set the externalRelease value on the external incident
                    }
                    else
                    {
                        //We can't find the matching item so log and just don't set the release
                        LogErrorEvent("Unable to locate " + EXTERNAL_SYSTEM_NAME + " resolved release " + externalResolvedRelease + " in project " + externalProjectId, EventLogEntryType.Warning);

                        //Add this to the list of mappings to remove
                        RemoteDataMapping oldReleaseMapping = new RemoteDataMapping();
                        oldReleaseMapping.ProjectId = projectId;
                        oldReleaseMapping.InternalId = resolvedReleaseId;
                        oldReleaseMapping.ExternalKey = externalResolvedRelease;
                        oldReleaseMappings.Add(oldReleaseMapping);
                    }
                }
                LogTraceEvent(eventLog, "Set " + EXTERNAL_SYSTEM_NAME + " resolved release\n", EventLogEntryType.Information);

                //Setup the dictionary to hold the various custom properties to set on the external bug system
                //TODO: Replace with the real custom property collection for the external system
                Dictionary<string, object> externalSystemCustomFieldValues = new Dictionary<string, object>();

                //Now we need to see if any of the custom properties have changed
                if (remoteIncident.CustomProperties != null && remoteIncident.CustomProperties.Length > 0)
                {
                    ProcessCustomProperties(productName, projectId, remoteIncident, externalSystemCustomFieldValues, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);
                }
                LogTraceEvent(eventLog, "Captured incident custom values\n", EventLogEntryType.Information);

                /*
                 * TODO: Create the incident in the external system using the following values
                 *  - externalName
                 *  - externalDescription
                 *  - externalProjectId
                 *  - externalStatus
                 *  - externalType
                 *  - externalPriority
                 *  - externalSeverity
                 *  - externalReporter
                 *  - externalAssignee
                 *  - externalDetectedRelease
                 *  - externalResolvedRelease
                 *  
                 * We assume that the ID of the new bug generated is stored in externalBugId
                 */
                string externalBugId = "";  //TODO: Replace with the code to get the real external bug id

                //Add the external bug id to mappings table
                RemoteDataMapping newIncidentMapping = new RemoteDataMapping();
                newIncidentMapping.ProjectId = projectId;
                newIncidentMapping.InternalId = incidentId;
                newIncidentMapping.ExternalKey = externalBugId;
                newIncidentMappings.Add(newIncidentMapping);

                //We also add a link to the external issue from the Spira incident

                /*
                * TODO: Need to add the base URL onto the URL that we use to link the Spira incident to the external system
                */
                if (!String.IsNullOrEmpty(EXTERNAL_BUG_URL))
                {
                    string externalUrl = String.Format(EXTERNAL_BUG_URL, externalBugId);
                    RemoteDocument remoteUrl = new RemoteDocument();
                    remoteUrl.ArtifactId = incidentId;
                    remoteUrl.ArtifactTypeId = (int)Constants.ArtifactType.Incident;
                    remoteUrl.Description = "Link to issue in " + EXTERNAL_SYSTEM_NAME;
                    remoteUrl.FilenameOrUrl = externalUrl;
                    spiraImportExport.Document_AddUrl(remoteUrl);
                }

                //See if we have any comments to add to the external system
                RemoteComment[] incidentComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                if (incidentComments != null)
                {
                    foreach (RemoteComment incidentComment in incidentComments)
                    {
                        string externalResolutionText = incidentComment.Text;
                        DateTime creationDate = incidentComment.CreationDate.Value;

                        //Get the id of the corresponding external user that added the comments
                        string externalCommentAuthor = "";
                        dataMapping = InternalFunctions.FindMappingByInternalId(incidentComment.UserId.Value, userMappings);
                        //If we can't find the user, just log a warning
                        if (dataMapping == null)
                        {
                            LogErrorEvent("Unable to locate mapping entry for user id " + incidentComment.UserId.Value + " so using synchronization user", EventLogEntryType.Warning);
                        }
                        else
                        {
                            externalCommentAuthor = dataMapping.ExternalKey;
                        }

                        /*
                         * TODO: Add a comment to the external bug-tracking system using the following values
                         *  - externalResolutionText
                         *  - creationDate
                         *  - externalCommentAuthor
                         */
                    }
                }
            }
        }

        /// <summary>
        /// Processes a single external bug record and either adds or updates it in SpiraTest
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="externalSystemBug">The external bug object</param>
        /// <param name="newIncidentMappings">The list of any new incidents to be mapped</param>
        /// <param name="newReleaseMappings">The list of any new releases to be mapped</param>
        /// <param name="oldReleaseMappings">The list list of old releases to be un-mapped</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="incidentCustomProperties">The list of incident custom properties defined for this project</param>
        /// <param name="incidentMappings">The list of existing mapped incidents</param>
        /// <param name="externalProjectId">The id of the project in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="severityMappings">The incident severity mappings</param>
        /// <param name="priorityMappings">The incident priority mappings</param>
        /// <param name="statusMappings">The incident status mappings</param>
        /// <param name="typeMappings">The incident type mappings</param>
        /// <param name="userMappings">The incident user mappings</param>
        /// <param name="releaseMappings">The release mappings</param>
        private void ProcessExternalBug(int projectId, ImportExportClient spiraImportExport, object externalSystemBug, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] incidentMappings, string externalProjectId, string productName, RemoteDataMapping[] severityMappings, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] typeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            /*
             * TODO: Need to add the code that actually gets the data from the external bug object
             */
            string externalBugId = "";
            string externalBugName = "";
            string externalBugDescription = "";
            string externalBugProjectId = "";
            string externalBugCreator = "";
            string externalBugPriority = "";
            string externalBugSeverity = "";
            string externalBugStatus = "";
            string externalBugType = "";
            string externalBugAssignee = "";
            string externalBugDetectedRelease = "";
            string externalBugResolvedRelease = "";
            DateTime? externalBugStartDate = null;
            DateTime? externalBugClosedDate = null;

            //Make sure the projects match (i.e. the external bug is in the project being synced)
            //It should be handled previously in the filter sent to external system, but use this as an extra check
            if (externalBugProjectId == externalProjectId)
            {
                //See if we have an existing mapping or not
                RemoteDataMapping incidentMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugId, incidentMappings, false);

                int incidentId = -1;
                RemoteIncident remoteIncident = null;
                if (incidentMapping == null)
                {
                    //This bug needs to be inserted into SpiraTest
                    remoteIncident = new RemoteIncident();
                    remoteIncident.ProjectId = projectId;

                    //Set the name for new incidents
                    if (String.IsNullOrEmpty(externalBugName))
                    {
                        remoteIncident.Name = "Name Not Specified";
                    }
                    else
                    {
                        remoteIncident.Description = externalBugName;
                    }

                    //Set the description for new incidents
                    if (String.IsNullOrEmpty(externalBugDescription))
                    {
                        remoteIncident.Description = "Description Not Specified";
                    }
                    else
                    {
                        remoteIncident.Description = externalBugDescription;
                    }

                    //Set the dectector for new incidents
                    if (!String.IsNullOrEmpty(externalBugCreator))
                    {
                        RemoteDataMapping dataMapping = FindUserMappingByExternalKey(externalBugCreator, userMappings, spiraImportExport);
                        if (dataMapping == null)
                        {
                            //We can't find the matching user so log and ignore
                            LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " user " + externalBugCreator + " so using synchronization user as detector.", EventLogEntryType.Error);
                        }
                        else
                        {
                            remoteIncident.OpenerId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the detector " + remoteIncident.OpenerId.ToString() + "\n", EventLogEntryType.Information);
                        }
                    }
                }
                else
                {
                    //We need to load the matching SpiraTest incident and update
                    incidentId = incidentMapping.InternalId;

                    //Now retrieve the SpiraTest incident using the Import APIs
                    try
                    {
                        remoteIncident = spiraImportExport.Incident_RetrieveById(incidentId);


                        //Update the name for existing incidents
                        if (!String.IsNullOrEmpty(externalBugName))
                        {
                            remoteIncident.Description = externalBugName;
                        }

                        //Update the description for existing incidents
                        if (!String.IsNullOrEmpty(externalBugDescription))
                        {
                            remoteIncident.Description = externalBugDescription;
                        }
                    }
                    catch (Exception)
                    {
                        //Ignore as it will leave the remoteIncident as null
                    }
                }

                try
                {
                    //Make sure we have retrieved or created the incident
                    if (remoteIncident != null)
                    {
                        RemoteDataMapping dataMapping;
                        LogTraceEvent(eventLog, "Retrieved incident in " + productName + "\n", EventLogEntryType.Information);

                        //Now get the bug priority from the mapping (if priority is set)
                        if (String.IsNullOrEmpty(externalBugPriority))
                        {
                            remoteIncident.PriorityId = null;
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugPriority, priorityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the priority
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug priority " + externalBugPriority + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                remoteIncident.PriorityId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the priority\n", EventLogEntryType.Information);

                        //Now get the bug severity from the mapping (if severity is set)
                        if (String.IsNullOrEmpty(externalBugSeverity))
                        {
                            remoteIncident.SeverityId = null;
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugSeverity, severityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the severity
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug severity " + externalBugSeverity + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                remoteIncident.SeverityId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the severity\n", EventLogEntryType.Information);

                        //Now get the bug status from the mapping
                        if (!String.IsNullOrEmpty(externalBugStatus))
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugStatus, statusMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and ignore
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug status " + externalBugStatus + " in project " + projectId, EventLogEntryType.Error);
                            }
                            else
                            {
                                remoteIncident.IncidentStatusId = dataMapping.InternalId;
                            }
                        }

                        LogTraceEvent(eventLog, "Got the status\n", EventLogEntryType.Information);

                        //Now get the bug type from the mapping
                        if (!String.IsNullOrEmpty(externalBugType))
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugType, typeMappings, true);
                            if (dataMapping == null)
                            {
                                //If this is a new issue and we don't have the type mapped
                                //it means that they don't want them getting added to SpiraTest
                                if (incidentId == -1)
                                {
                                    return;
                                }
                                //We can't find the matching item so log and ignore
                                eventLog.WriteEntry("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " incident type " + externalBugType + " in project " + projectId, EventLogEntryType.Error);
                            }
                            else
                            {
                                remoteIncident.IncidentTypeId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the type\n", EventLogEntryType.Information);

                        //Now update the bug's owner/assignee in SpiraTest
                        dataMapping = FindUserMappingByExternalKey(externalBugAssignee, userMappings, spiraImportExport);
                        if (dataMapping == null)
                        {
                            //We can't find the matching user so log and ignore
                            LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " user " + externalBugAssignee + " so ignoring the assignee change", EventLogEntryType.Error);
                        }
                        else
                        {
                            remoteIncident.OwnerId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the assignee " + remoteIncident.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                        }

                        //Update the start-date if necessary
                        if (externalBugStartDate.HasValue)
                        {
                            remoteIncident.StartDate = externalBugStartDate.Value;
                        }

                        //Update the closed-date if necessary
                        if (externalBugClosedDate.HasValue)
                        {
                            remoteIncident.ClosedDate = externalBugClosedDate.Value;
                        }

                        //Now we need to get all the comments attached to the bug in the external system
                        /*
                         * TODO: Add the code to get all the comments associated with the external bug using:
                         *  - externalBugId
                         */

                        List<object> externalBugComments = null;    //TODO: Replace with real code

                        //Now get the list of comments attached to the SpiraTest incident
                        //If this is the new incident case, just leave as null
                        RemoteComment[] incidentComments = null;
                        if (incidentId != -1)
                        {
                            incidentComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                        }

                        //Iterate through all the comments and see if we need to add any to SpiraTest
                        List<RemoteComment> newIncidentComments = new List<RemoteComment>();
                        if (externalBugComments != null)
                        {
                            foreach (object externalBugComment in externalBugComments)
                            {
                                /*
                                 * TODO: Replace the following sample code with the code that will extract the information
                                 *       from the real externalBugComment object
                                 */

                                //Extract the resolution values from the external system
                                string externalCommentText = "";
                                string externalCommentCreator = "";
                                DateTime? externalCommentCreationDate = null;

                                //See if we already have this resolution inside SpiraTest
                                bool alreadyAdded = false;
                                if (incidentComments != null)
                                {
                                    foreach (RemoteComment incidentComment in incidentComments)
                                    {
                                        if (incidentComment.Text.Trim() == externalCommentText.Trim())
                                        {
                                            alreadyAdded = true;
                                        }
                                    }
                                }
                                if (!alreadyAdded)
                                {
                                    //Get the resolution author mapping
                                    LogTraceEvent(eventLog, "Looking for " + EXTERNAL_SYSTEM_NAME + " comments creator: '" + externalCommentCreator + "'\n", EventLogEntryType.Information);
                                    dataMapping = InternalFunctions.FindMappingByExternalKey(externalCommentCreator, userMappings);
                                    int? creatorId = null;
                                    if (dataMapping != null)
                                    {
                                        //Set the creator of the comment, otherwise leave null and SpiraTest will
                                        //simply use the synchronization user
                                        creatorId = dataMapping.InternalId;
                                    }

                                    //Add the comment to SpiraTest
                                    RemoteComment newIncidentComment = new RemoteComment();
                                    newIncidentComment.ArtifactId = incidentId;
                                    newIncidentComment.UserId = creatorId;
                                    newIncidentComment.CreationDate = (externalCommentCreationDate.HasValue) ? externalCommentCreationDate.Value : DateTime.Now;
                                    newIncidentComment.Text = externalCommentText;
                                    newIncidentComments.Add(newIncidentComment);
                                }
                            }
                        }
                        //The resolutions will actually get added later when we insert/update the incident record itself

                        //Debug logging - comment out for production code
                        LogTraceEvent(eventLog, "Got the comments/resolution\n", EventLogEntryType.Information);

                        //Specify the detected-in release if applicable
                        if (!String.IsNullOrEmpty(externalBugDetectedRelease))
                        {
                            //See if we have a mapped SpiraTest release in either the existing list of
                            //mapped releases or the list of newly added ones
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugDetectedRelease, releaseMappings, false);
                            if (dataMapping == null)
                            {
                                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugDetectedRelease, newReleaseMappings.ToArray(), false);
                            }
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                /*
                                 * TODO: Add code to retrieve the release/version in the external system (if necessary) and extract the properties
                                 *       into the following temporary variables
                                 */
                                string externalReleaseName = "";
                                string externalReleaseVersionNumber = "";
                                DateTime? externalReleaseStartDate = null;
                                DateTime? externalReleaseEndDate = null;

                                LogTraceEvent(eventLog, "Adding new release in " + productName + " for version " + externalBugDetectedRelease + "\n", EventLogEntryType.Information);
                                RemoteRelease remoteRelease = new RemoteRelease();
                                remoteRelease.Name = externalReleaseName;
                                if (externalReleaseVersionNumber.Length > 10)
                                {
                                    remoteRelease.VersionNumber = externalReleaseVersionNumber.Substring(0, 10);
                                }
                                else
                                {
                                    remoteRelease.VersionNumber = externalReleaseVersionNumber;
                                }
                                remoteRelease.Active = true;
                                //If no start-date specified, simply use now
                                remoteRelease.StartDate = (externalReleaseStartDate.HasValue) ? externalReleaseStartDate.Value : DateTime.Now;
                                //If no end-date specified, simply use 1-month from now
                                remoteRelease.EndDate = (externalReleaseEndDate.HasValue) ? externalReleaseEndDate.Value : DateTime.Now.AddMonths(1);
                                remoteRelease.CreatorId = remoteIncident.OpenerId;
                                remoteRelease.CreationDate = DateTime.Now;
                                remoteRelease.ResourceCount = 1;
                                remoteRelease.DaysNonWorking = 0;
                                remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                                //Add a new mapping entry
                                RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                                newReleaseMapping.ProjectId = projectId;
                                newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                                newReleaseMapping.ExternalKey = externalBugDetectedRelease;
                                newReleaseMappings.Add(newReleaseMapping);
                                remoteIncident.DetectedReleaseId = newReleaseMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting detected release id to  " + newReleaseMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                            else
                            {
                                remoteIncident.DetectedReleaseId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting detected release id to  " + dataMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                        }

                        //Specify the resolved-in release if applicable
                        if (!String.IsNullOrEmpty(externalBugResolvedRelease))
                        {
                            //See if we have a mapped SpiraTest release in either the existing list of
                            //mapped releases or the list of newly added ones
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugResolvedRelease, releaseMappings, false);
                            if (dataMapping == null)
                            {
                                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugResolvedRelease, newReleaseMappings.ToArray(), false);
                            }
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                                /*
                                 * TODO: Add code to retrieve the release/version in the external system (if necessary) and extract the properties
                                 *       into the following temporary variables
                                 */
                                string externalReleaseName = "";
                                string externalReleaseVersionNumber = "";
                                DateTime? externalReleaseStartDate = null;
                                DateTime? externalReleaseEndDate = null;

                                LogTraceEvent(eventLog, "Adding new release in " + productName + " for version " + externalBugResolvedRelease + "\n", EventLogEntryType.Information);
                                RemoteRelease remoteRelease = new RemoteRelease();
                                remoteRelease.Name = externalReleaseName;
                                if (externalReleaseVersionNumber.Length > 10)
                                {
                                    remoteRelease.VersionNumber = externalReleaseVersionNumber.Substring(0, 10);
                                }
                                else
                                {
                                    remoteRelease.VersionNumber = externalReleaseVersionNumber;
                                }
                                remoteRelease.Active = true;
                                //If no start-date specified, simply use now
                                remoteRelease.StartDate = (externalReleaseStartDate.HasValue) ? externalReleaseStartDate.Value : DateTime.Now;
                                //If no end-date specified, simply use 1-month from now
                                remoteRelease.EndDate = (externalReleaseEndDate.HasValue) ? externalReleaseEndDate.Value : DateTime.Now.AddMonths(1);
                                remoteRelease.CreatorId = remoteIncident.OpenerId;
                                remoteRelease.CreationDate = DateTime.Now;
                                remoteRelease.ResourceCount = 1;
                                remoteRelease.DaysNonWorking = 0;
                                remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                                //Add a new mapping entry
                                RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                                newReleaseMapping.ProjectId = projectId;
                                newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                                newReleaseMapping.ExternalKey = externalBugResolvedRelease;
                                newReleaseMappings.Add(newReleaseMapping);
                                remoteIncident.ResolvedReleaseId = newReleaseMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting resolved release id to  " + newReleaseMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                            else
                            {
                                remoteIncident.ResolvedReleaseId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting resolved release id to  " + dataMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                        }

                        /*
                         * TODO: Need to get the list of custom property values for the bug from the external system.
                         * The following sample code just stores them in a simple text dictionary
                         * where the Key=Field Name, Value=Field Value
                         */
                        Dictionary<string, object> externalSystemCustomFieldValues = null;  //TODO: Replace with real code

                        //Now we need to see if any of the custom fields have changed in the external system bug
                        if (remoteIncident.CustomProperties != null && remoteIncident.CustomProperties.Length > 0)
                        {
                            ProcessExternalSystemCustomFields(productName, projectId, remoteIncident, externalSystemCustomFieldValues, incidentCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);
                        }

                        //Finally add or update the incident in SpiraTest
                        if (incidentId == -1)
                        {
                            //Debug logging - comment out for production code
                            try
                            {
                                remoteIncident = spiraImportExport.Incident_Create(remoteIncident);
                            }
                            catch (Exception exception)
                            {
                                LogErrorEvent("Error Adding " + EXTERNAL_SYSTEM_NAME + " bug " + externalBugId + " to " + productName + " (" + exception.Message + ")\n" + exception.StackTrace, EventLogEntryType.Error);
                                return;
                            }
                            LogTraceEvent(eventLog, "Successfully added " + EXTERNAL_SYSTEM_NAME + " bug " + externalBugId + " to " + productName + "\n", EventLogEntryType.Information);

                            //Extract the SpiraTest incident and add to mappings table
                            RemoteDataMapping newIncidentMapping = new RemoteDataMapping();
                            newIncidentMapping.ProjectId = projectId;
                            newIncidentMapping.InternalId = remoteIncident.IncidentId.Value;
                            newIncidentMapping.ExternalKey = externalBugId;
                            newIncidentMappings.Add(newIncidentMapping);

                            //Now add any comments (need to set the ID)
                            foreach (RemoteComment newIncidentComment in newIncidentComments)
                            {
                                newIncidentComment.ArtifactId = remoteIncident.IncidentId.Value;
                            }
                            spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());
                        }
                        else
                        {
                            spiraImportExport.Incident_Update(remoteIncident);

                            //Now add any resolutions
                            spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());

                            //Debug logging - comment out for production code
                            LogTraceEvent(eventLog, "Successfully updated\n", EventLogEntryType.Information);
                        }
                    }
                }
                catch (Exception exception)
                {
                    //Log and continue execution
                    LogErrorEvent("Error Inserting/Updating " + EXTERNAL_SYSTEM_NAME + " Bug in " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                }
            }
        }

        /// <summary>
        /// Updates the Spira incident object's custom properties with any new/changed custom fields from the external bug object
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteArtifact">The Spira artifact</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="externalSystemCustomFieldValues">The list of custom fields in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="userMappings">The user mappings</param>
        private void ProcessExternalSystemCustomFields(string productName, int projectId, RemoteArtifact remoteArtifact, Dictionary<string, object> externalSystemCustomFieldValues, RemoteCustomProperty[] customProperties, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport)
        {
            //Loop through all the defined Spira custom properties
            foreach (SpiraImportExport.RemoteCustomProperty customProperty in customProperties)
            {
                //Get the external key of this custom property (if it has one)
                if (customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                {
                    SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                    if (customPropertyDataMapping != null)
                    {
                        LogTraceEvent(eventLog, "Found custom property mapping for " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + "\n", EventLogEntryType.Information);
                        string externalKey = customPropertyDataMapping.ExternalKey;
                        //See if we have a list, multi-list or user custom field as they need to be handled differently
                        if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a LIST property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest incident
                            if (externalSystemCustomFieldValues.ContainsKey(externalKey))
                            {
                                if (externalSystemCustomFieldValues[externalKey] == null)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property value
                                    string fieldValue = externalSystemCustomFieldValues[externalKey].ToString();
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                    SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, fieldValue, customPropertyValueMappings, false);
                                    if (customPropertyValueMapping != null)
                                    {
                                        InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId);
                                    }
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a USER property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest incident
                            if (externalSystemCustomFieldValues.ContainsKey(externalKey))
                            {
                                if (externalSystemCustomFieldValues[externalKey] == null)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property value
                                    string fieldValue = externalSystemCustomFieldValues[externalKey].ToString();
                                    RemoteDataMapping customPropertyValueMapping = FindUserMappingByExternalKey(fieldValue, userMappings, spiraImportExport);
                                    if (customPropertyValueMapping != null)
                                    {
                                        InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId);
                                    }
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a MULTILIST property\n", EventLogEntryType.Information);

                            //Next the multi-list fields
                            //Now we need to set the value on the SpiraTest incident
                            if (externalSystemCustomFieldValues.ContainsKey(externalKey))
                            {
                                if (externalSystemCustomFieldValues[externalKey] == null)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (List<int>)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property value
                                    List<string> externalCustomFieldValues = (List<string>)externalSystemCustomFieldValues[externalKey];
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];

                                    //Data-map each of the custom property values
                                    //We assume that the external system has a multiselect stored list of string values (List<string>)
                                    List<int> spiraCustomValueIds = new List<int>();
                                    foreach (string externalCustomFieldValue in externalCustomFieldValues)
                                    {
                                        RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalCustomFieldValue, customPropertyValueMappings, false);
                                        if (customPropertyValueMapping != null)
                                        {
                                            spiraCustomValueIds.Add(customPropertyValueMapping.InternalId);
                                        }
                                    }
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, spiraCustomValueIds);
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                        else
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a VALUE property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest artifact
                            if (externalSystemCustomFieldValues.ContainsKey(externalKey))
                            {
                                switch ((Constants.CustomPropertyType)customProperty.CustomPropertyTypeId)
                                {
                                    case Constants.CustomPropertyType.Boolean:
                                        {
                                            if (externalSystemCustomFieldValues[externalKey] == null || !(externalSystemCustomFieldValues[externalKey] is Boolean))
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (bool?)null);
                                            }
                                            else
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (bool)externalSystemCustomFieldValues[externalKey]);
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + externalSystemCustomFieldValues[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Date:
                                        {
                                            if (externalSystemCustomFieldValues[externalKey] == null || !(externalSystemCustomFieldValues[externalKey] is DateTime))
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (DateTime?)null);
                                            }
                                            else
                                            {
                                                //Need to convert to UTC for Spira
                                                DateTime localTime = (DateTime)externalSystemCustomFieldValues[externalKey];
                                                DateTime utcTime = localTime.ToUniversalTime();

                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, utcTime);
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + utcTime + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;


                                    case Constants.CustomPropertyType.Decimal:
                                        {
                                            if (externalSystemCustomFieldValues[externalKey] == null || !(externalSystemCustomFieldValues[externalKey] is Decimal))
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal?)null);
                                            }
                                            else
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal)externalSystemCustomFieldValues[externalKey]);
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + externalSystemCustomFieldValues[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Integer:
                                        {
                                            if (externalSystemCustomFieldValues[externalKey] == null || !(externalSystemCustomFieldValues[externalKey] is Int32))
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                            }
                                            else
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int)externalSystemCustomFieldValues[externalKey]);
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + externalSystemCustomFieldValues[externalKey] + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Text:
                                    default:
                                        {
                                            if (externalSystemCustomFieldValues[externalKey] == null)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (string)null);
                                            }
                                            else
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, externalSystemCustomFieldValues[externalKey].ToString());
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + externalSystemCustomFieldValues[externalKey].ToString() + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Updates the external bug object with any incident custom property values
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteArtifact">The Spira artifact</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="externalSystemCustomFieldValues">The list of custom fields in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="userMappings">The user mappings</param>
        private void ProcessCustomProperties(string productName, int projectId, RemoteArtifact remoteArtifact, Dictionary<string, object> externalSystemCustomFieldValues, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport)
        {
            foreach (RemoteArtifactCustomProperty artifactCustomProperty in remoteArtifact.CustomProperties)
            {
                //Handle user, list and non-list separately since only the list types need to have value mappings
                RemoteCustomProperty customProperty = artifactCustomProperty.Definition;
                if (customProperty != null && customProperty.CustomPropertyId.HasValue)
                {
                    if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                    {
                        //Single-Select List
                        LogTraceEvent(eventLog, "Checking list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                        //See if we have a custom property value set
                        //Get the corresponding external custom field (if there is one)
                        if (artifactCustomProperty.IntegerValue.HasValue && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                        {
                            LogTraceEvent(eventLog, "Got value for list custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                            SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                            if (customPropertyDataMapping != null)
                            {
                                string externalCustomField = customPropertyDataMapping.ExternalKey;

                                //Get the corresponding external custom field value (if there is one)
                                if (!String.IsNullOrEmpty(externalCustomField) && customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                {
                                    SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                    if (customPropertyValueMappings != null)
                                    {
                                        SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, artifactCustomProperty.IntegerValue.Value, customPropertyValueMappings);
                                        if (customPropertyValueMapping != null)
                                        {
                                            string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                            //Make sure we have a mapped custom field in the external system
                                            if (!String.IsNullOrEmpty(externalCustomFieldValue))
                                            {
                                                LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                externalSystemCustomFieldValues[externalCustomField] = externalCustomFieldValue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                    {
                        //Multi-Select List
                        LogTraceEvent(eventLog, "Checking multi-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                        //See if we have a custom property value set
                        //Get the corresponding external custom field (if there is one)
                        if (artifactCustomProperty.IntegerListValue != null && artifactCustomProperty.IntegerListValue.Length > 0 && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                        {
                            LogTraceEvent(eventLog, "Got values for multi-list custom property: " + customProperty.Name + " (Count=" + artifactCustomProperty.IntegerListValue.Length + ")\n", EventLogEntryType.Information);
                            SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                            if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                            {
                                string externalCustomField = customPropertyDataMapping.ExternalKey;
                                LogTraceEvent(eventLog, "Got external key for multi-list custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                //Loop through each value in the list
                                List<string> externalCustomFieldValues = new List<string>();
                                foreach (int customPropertyListValue in artifactCustomProperty.IntegerListValue)
                                {
                                    //Get the corresponding external custom field value (if there is one)
                                    if (customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                    {
                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                        if (customPropertyValueMappings != null)
                                        {
                                            SpiraImportExport.RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, customPropertyListValue, customPropertyValueMappings);
                                            if (customPropertyValueMapping != null)
                                            {
                                                LogTraceEvent(eventLog, "Added multi-list custom property field value: " + customProperty.Name + " (Value=" + customPropertyValueMapping.ExternalKey + ")\n", EventLogEntryType.Information);
                                                externalCustomFieldValues.Add(customPropertyValueMapping.ExternalKey);
                                            }
                                        }
                                    }
                                }

                                //Make sure that we have some values to set
                                LogTraceEvent(eventLog, "Got mapped values for multi-list custom property: " + customProperty.Name + " (Count=" + externalCustomFieldValues.Count + ")\n", EventLogEntryType.Information);
                                if (externalCustomFieldValues.Count > 0)
                                {
                                    externalSystemCustomFieldValues[externalCustomField] = externalCustomFieldValues;
                                }
                                else
                                {
                                    externalSystemCustomFieldValues[externalCustomField] = null;
                                }
                            }
                        }
                    }
                    else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                    {
                        //User
                        LogTraceEvent(eventLog, "Checking user custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                        //See if we have a custom property value set
                        if (artifactCustomProperty.IntegerValue.HasValue)
                        {
                            SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                            if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                            {
                                string externalCustomField = customPropertyDataMapping.ExternalKey;
                                LogTraceEvent(eventLog, "Got external key for user custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                LogTraceEvent(eventLog, "Got value for user custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                                //Get the corresponding external system user (if there is one)
                                RemoteDataMapping dataMapping = FindUserMappingByInternalId(artifactCustomProperty.IntegerValue.Value, userMappings, spiraImportExport);
                                if (dataMapping != null)
                                {
                                    string externalUserName = dataMapping.ExternalKey;
                                    LogTraceEvent(eventLog, "Adding user custom property field value: " + customProperty.Name + " (Value=" + externalUserName + ")\n", EventLogEntryType.Information);
                                    LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                    externalSystemCustomFieldValues[externalCustomField] = externalUserName;
                                }
                                else
                                {
                                    LogErrorEvent("Unable to find a matching " + EXTERNAL_SYSTEM_NAME + " user for " + productName + " user with ID=" + artifactCustomProperty.IntegerValue.Value + " so leaving property null.", EventLogEntryType.Warning);
                                }
                            }
                        }
                    }
                    else
                    {
                        //Other
                        LogTraceEvent(eventLog, "Checking non-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                        //See if we have a custom property value set
                        if (!String.IsNullOrEmpty(artifactCustomProperty.StringValue) || artifactCustomProperty.BooleanValue.HasValue
                            || artifactCustomProperty.DateTimeValue.HasValue || artifactCustomProperty.DecimalValue.HasValue
                            || artifactCustomProperty.IntegerValue.HasValue)
                        {
                            LogTraceEvent(eventLog, "Got value for non-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);
                            //Get the corresponding external custom field (if there is one)
                            if (customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                            {
                                SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null)
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;

                                    //Make sure we have a mapped custom field in the external system mapped
                                    if (!String.IsNullOrEmpty(externalCustomField))
                                    {
                                        LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                        object customFieldValue = InternalFunctions.GetCustomPropertyValue(artifactCustomProperty);
                                        externalSystemCustomFieldValues[externalCustomField] = customFieldValue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the user-id instead</remarks>
        protected RemoteDataMapping FindUserMappingByInternalId(int internalId, RemoteDataMapping[] dataMappings, ImportExportClient client)
        {
            if (this.autoMapUsers)
            {
                RemoteUser remoteUser = client.User_RetrieveById(internalId);
                if (remoteUser == null)
                {
                    return null;
                }
                RemoteDataMapping userMapping = new RemoteDataMapping();
                userMapping.InternalId = remoteUser.UserId.Value;
                userMapping.ExternalKey = remoteUser.UserName;
                return userMapping;
            }
            else
            {
                return InternalFunctions.FindMappingByInternalId(internalId, dataMappings);
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the username instead</remarks>
        protected RemoteDataMapping FindUserMappingByExternalKey(string externalKey, RemoteDataMapping[] dataMappings, ImportExportClient client)
        {
            if (this.autoMapUsers)
            {
                try
                {
                    RemoteUser remoteUser = client.User_RetrieveByUserName(externalKey);
                    if (remoteUser == null)
                    {
                        return null;
                    }
                    RemoteDataMapping userMapping = new RemoteDataMapping();
                    userMapping.InternalId = remoteUser.UserId.Value;
                    userMapping.ExternalKey = remoteUser.UserName;
                    return userMapping;
                }
                catch (Exception)
                {
                    //User could not be found so return null
                    return null;
                }
            }
            else
            {
                return InternalFunctions.FindMappingByExternalKey(externalKey, dataMappings);
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="eventLog">The event log handle</param>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        protected void LogTraceEvent(EventLog eventLog, string message, EventLogEntryType type = EventLogEntryType.Information)
        {
            if (traceLogging && eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        public void LogErrorEvent(string message, EventLogEntryType type = EventLogEntryType.Error)
        {
            if (this.eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
            Dispose(true);
            // Take yourself off the Finalization queue 
            // to prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize(this);
        }

        // Use C# destructor syntax for finalization code.
        // This destructor will run only if the Dispose method 
        // does not get called.
        // It gives your base class the opportunity to finalize.
        // Do not provide destructors in types derived from this class.
        ~DataSync()
        {
            // Do not re-create Dispose clean-up code here.
            // Calling Dispose(false) is optimal in terms of
            // readability and maintainability.
            Dispose(false);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the 
        // runtime from inside the finalizer and you should not reference 
        // other objects. Only unmanaged resources can be disposed.
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.disposed)
            {
                // If disposing equals true, dispose all managed 
                // and unmanaged resources.
                if (disposing)
                {
                    //Remove the event log reference
                    this.eventLog = null;
                }
                // Release unmanaged resources. If disposing is false, 
                // only the following code is executed.

                //This class doesn't have any unmanaged resources to worry about
            }
            disposed = true;
        }
    }
}

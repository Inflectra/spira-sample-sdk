using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.SpiraTest.PlugIns;
using System.Diagnostics;
using SampleDataSync.SpiraImportExport;

namespace SampleDataSync
{
    /// <summary>
    /// Sample data-synchronization provider that synchronizes incidents between SpiraTest/Plan/Team and an external system
    /// </summary>
    /// <remarks>
    /// Requires Spira v3.0 or newer since it uses the v3.0+ compatible web service API
    /// </remarks>
    public class DataSync : IServicePlugIn
    {
        //Constant containing data-sync name and internal API URL suffix to access
        private const string DATA_SYNC_NAME = "SampleDataSync";
        private const string EXTERNAL_SYSTEM_NAME = "External System";

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
        /// <param name="LastSyncDate">The last date/time the plug-in was successfully executed</param>
        /// <param name="serverDateTime">The current date/time on the server</param>
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
                 * Add the code to connect and authenticate to the external system
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
                    eventLog.WriteEntry("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                SpiraImportExport.RemoteDataMapping[] projectMappings = spiraImportExport.DataMapping_RetrieveProjectMappings(dataSyncSystemId);
                SpiraImportExport.RemoteDataMapping[] userMappings = spiraImportExport.DataMapping_RetrieveUserMappings(dataSyncSystemId);

                //Loop for each of the projects in the project mapping
                foreach (SpiraImportExport.RemoteDataMapping projectMapping in projectMappings)
                {
                    //Get the SpiraTest project id equivalent external system project identifier
                    int projectId = projectMapping.InternalId;
                    string externalProjectId = projectMapping.ExternalKey;

                    //Connect to the SpiraTest project
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        eventLog.WriteEntry("Unable to connect to " + productName + " project, please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                        continue;
                    }

                    //Get the list of project-specific mappings from the data-mapping repository
                    //We need to get severity, priority, status and type mappings
                    SpiraImportExport.RemoteDataMapping[] severityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Severity);
                    SpiraImportExport.RemoteDataMapping[] priorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Priority);
                    SpiraImportExport.RemoteDataMapping[] statusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Status);
                    SpiraImportExport.RemoteDataMapping[] typeMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Type);

                    //Get the list of custom properties configured for this project and the corresponding data mappings
                    SpiraImportExport.RemoteCustomProperty[] projectCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Incident);
                    Dictionary<int, SpiraImportExport.RemoteDataMapping> customPropertyMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping>();
                    Dictionary<int, SpiraImportExport.RemoteDataMapping[]> customPropertyValueMappingList = new Dictionary<int, SpiraImportExport.RemoteDataMapping[]>();
                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in projectCustomProperties)
                    {
                        //Get the mapping for this custom property
                        SpiraImportExport.RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId);
                        customPropertyMappingList.Add(customProperty.CustomPropertyId, customPropertyMapping);

                        //For list types need to also get the property value mappings
                        if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                        {
                            SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId);
                            customPropertyValueMappingList.Add(customProperty.CustomPropertyId, customPropertyValueMappings);
                        }
                    }

                    //Now get the list of releases and incidents that have already been mapped
                    SpiraImportExport.RemoteDataMapping[] incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                    SpiraImportExport.RemoteDataMapping[] releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release);

                    /*
                     * Next add the code to connect to the project in the external system if necessary
                     * The following variables can be used
                     *  this.externalLogin
                     *  this.externalPassword
                     *  externalProjectId
                     */

                    //**** First we need to get the list of recently created incidents in SpiraTest ****
                    if (!lastSyncDate.HasValue)
                    {
                        lastSyncDate = DateTime.Parse("1/1/1900");
                    }
                    SpiraImportExport.RemoteIncident[] incidentList = spiraImportExport.Incident_RetrieveNew(lastSyncDate.Value);

                    //Create the mapping collections to hold any new items that need to get added to the mappings
                    //or any old items that need to get removed from the mappings
                    List<SpiraImportExport.RemoteDataMapping> newIncidentMappings = new List<SpiraImportExport.RemoteDataMapping>();
                    List<SpiraImportExport.RemoteDataMapping> newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();
                    List<SpiraImportExport.RemoteDataMapping> oldReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();

                    //Iterate through each new Spira incident record
                    foreach (SpiraImportExport.RemoteIncident remoteIncident in incidentList)
                    {
                        try
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
                                SpiraImportExport.RemoteDataMapping dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId.Value, statusMappings);
                                if (dataMapping == null)
                                {
                                    //We can't find the matching item so log and move to the next incident
                                    eventLog.WriteEntry("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                                    continue;
                                }
                                string externalStatus = dataMapping.ExternalKey;

                                //Now get the external system's equivalent incident type from the mapping
                                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId.Value, typeMappings);
                                if (dataMapping == null)
                                {
                                    //We can't find the matching item so log and move to the next incident
                                    eventLog.WriteEntry("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                                    continue;
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
                                        eventLog.WriteEntry("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
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
                                        eventLog.WriteEntry("Unable to locate mapping entry for incident severity " + remoteIncident.SeverityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                                    }
                                    else
                                    {
                                        externalSeverity = dataMapping.ExternalKey;
                                    }
                                }

                                //Now get the external system's ID for the Opener/Detector of the incident (reporter)
                                string externalReporter = "";
                                dataMapping = InternalFunctions.FindMappingByInternalId(remoteIncident.OpenerId.Value, userMappings);
                                //If we can't find the user, just log a warning
                                if (dataMapping == null)
                                {
                                    eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OpenerId.Value + " so using synchronization user", EventLogEntryType.Warning);
                                }
                                else
                                {
                                    externalReporter = dataMapping.ExternalKey;
                                }

                                //Now get the external system's ID for the Owner of the incident (assignee)
                                string externalAssignee = "";
                                if (remoteIncident.OwnerId.HasValue)
                                {
                                    dataMapping = InternalFunctions.FindMappingByInternalId(remoteIncident.OwnerId.Value, userMappings);
                                    //If we can't find the user, just log a warning
                                    if (dataMapping == null)
                                    {
                                        eventLog.WriteEntry("Unable to locate mapping entry for user id " + remoteIncident.OwnerId.Value + " so leaving assignee empty", EventLogEntryType.Warning);
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
                                             * Add the code to actually insert the new Release/Version in the external System
                                             * using the values from the remoteRelease object.
                                             * Need to get the ID of the new release from the external system and then
                                             * populate the externalDetectedRelease variable with the value
                                             */

                                            //Add a new mapping entry
                                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
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

                                    //TODO: Continue from here

                                    //Get the list of versions from the server and find the one that corresponds to the SpiraTest Release
                                    JiraSoapService.RemoteVersion[] remoteVersions = jiraSoapService.getVersions(jiraToken, jiraProject);
                                    JiraSoapService.RemoteVersion[] detectedVersion = new JiraSoapService.RemoteVersion[1];
                                    LogTraceEvent(eventLog, "Looking for JIRA affected version: " + jiraVersionId + "\n", EventLogEntryType.Information);
                                    bool matchFound = false;
                                    for (int j = 0; j < remoteVersions.Length; j++)
                                    {
                                        //See if we have an match, if not remove
                                        if (remoteVersions[j].id == jiraVersionId)
                                        {
                                            detectedVersion[0] = remoteVersions[j];
                                            jiraIssue.affectsVersions = detectedVersion;
                                            LogTraceEvent(eventLog, "Found JIRA affected version: " + jiraVersionId + "\n", EventLogEntryType.Information);
                                            matchFound = true;
                                        }
                                    }
                                    if (!matchFound)
                                    {
                                        //We can't find the matching item so log and just don't set the release
                                        eventLog.WriteEntry("Unable to locate JIRA affected version " + jiraVersionId + " in project " + jiraProject, EventLogEntryType.Warning);

                                        //Add this to the list of mappings to remove
                                        SpiraImportExport.RemoteDataMapping oldReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                                        oldReleaseMapping.ProjectId = projectId;
                                        oldReleaseMapping.InternalId = detectedReleaseId;
                                        oldReleaseMapping.ExternalKey = jiraVersionId;
                                        oldReleaseMappings.Add(oldReleaseMapping);
                                    }
                                }
                                LogTraceEvent(eventLog, "Set issue affected version\n", EventLogEntryType.Information);

                                //Specify the resolved-in version/release if applicable
                                if (remoteIncident.ResolvedReleaseId.HasValue)
                                {
                                    int resolvedReleaseId = remoteIncident.ResolvedReleaseId.Value;
                                    dataMapping = FindMappingByInternalId(projectId, resolvedReleaseId, releaseMappings);
                                    string jiraVersionId = null;
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching item so need to create a new version in JIRA and add to mappings
                                        //Since version numbers are now unique in both systems, we can simply use that
                                        LogTraceEvent(eventLog, "Adding new version in jira for release " + resolvedReleaseId + "\n", EventLogEntryType.Information);
                                        JiraSoapService.RemoteVersion jiraVersion = new JiraSoapService.RemoteVersion();
                                        jiraVersion.name = remoteIncident.ResolvedReleaseVersionNumber;
                                        jiraVersion.archived = false;
                                        jiraVersion.released = false;
                                        jiraVersion = jiraSoapService.addVersion(jiraToken, jiraProject, jiraVersion);

                                        //Add a new mapping entry
                                        SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                                        newReleaseMapping.ProjectId = projectId;
                                        newReleaseMapping.InternalId = resolvedReleaseId;
                                        newReleaseMapping.ExternalKey = jiraVersion.id;
                                        newReleaseMappings.Add(newReleaseMapping);
                                        jiraVersionId = jiraVersion.id;
                                    }
                                    else
                                    {
                                        jiraVersionId = dataMapping.ExternalKey;
                                    }
                                    //Get the list of versions from the server and find the one that corresponds to the SpiraTest Release
                                    JiraSoapService.RemoteVersion[] remoteVersions = jiraSoapService.getVersions(jiraToken, jiraProject);
                                    JiraSoapService.RemoteVersion[] resolvedVersion = new JiraSoapService.RemoteVersion[1];
                                    LogTraceEvent(eventLog, "Looking for JIRA fix version: " + jiraVersionId + "\n", EventLogEntryType.Information);
                                    bool matchFound = false;
                                    for (int j = 0; j < remoteVersions.Length; j++)
                                    {
                                        //See if we have an match, if not remove
                                        if (remoteVersions[j].id == jiraVersionId)
                                        {
                                            resolvedVersion[0] = remoteVersions[j];
                                            jiraIssue.fixVersions = resolvedVersion;
                                            LogTraceEvent(eventLog, "Found JIRA fix version: " + jiraVersionId + "\n", EventLogEntryType.Information);
                                            matchFound = true;
                                        }
                                    }
                                    if (!matchFound)
                                    {
                                        //We can't find the matching item so log and just don't set the release
                                        eventLog.WriteEntry("Unable to locate JIRA fix version " + jiraVersionId + " in project " + jiraProject, EventLogEntryType.Warning);

                                        //Add this to the list of mappings to remove
                                        SpiraImportExport.RemoteDataMapping oldReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                                        oldReleaseMapping.ProjectId = projectId;
                                        oldReleaseMapping.InternalId = resolvedReleaseId;
                                        oldReleaseMapping.ExternalKey = jiraVersionId;
                                        oldReleaseMappings.Add(oldReleaseMapping);
                                    }
                                }
                                LogTraceEvent(eventLog, "Set issue fix version\n", EventLogEntryType.Information);

                                //Now iterate through the project custom properties to populate the special bugzilla fields
                                string component = "";
                                string operatingSystem = "";
                                string hardware = "";
                                if (projectCustomProperties != null)
                                {
                                    foreach (SpiraImportExport.RemoteCustomProperty customProperty in projectCustomProperties)
                                    {
                                        //For bugzilla we only care about the list ones
                                        if (customProperty.CustomPropertyTypeId.HasValue && customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_LIST)
                                        {
                                            //See if we have a custom property value set
                                            Nullable<int> customPropertyValue = GetCustomPropertyListValue(remoteIncident, customProperty.CustomPropertyName);

                                            //Get the corresponding external custom field (if there is one)
                                            if (customPropertyValue.HasValue && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId))
                                            {
                                                SpiraImportExport.RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId];
                                                if (customPropertyDataMapping != null)
                                                {
                                                    string externalCustomField = customPropertyDataMapping.ExternalKey;

                                                    //Get the corresponding external custom field value (if there is one)
                                                    if (!String.IsNullOrEmpty(externalCustomField) && customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId))
                                                    {
                                                        SpiraImportExport.RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId];
                                                        if (customPropertyValueMappings != null)
                                                        {
                                                            SpiraImportExport.RemoteDataMapping customPropertyValueMapping = FindMappingByInternalId(projectId, customPropertyValue.Value, customPropertyValueMappings);
                                                            if (customPropertyValueMapping != null)
                                                            {
                                                                string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;
                                                                if (!String.IsNullOrEmpty(externalCustomFieldValue))
                                                                {
                                                                    //See which of the special bugzilla fields we have to populate
                                                                    if (externalCustomField == BUGZILLA_SPECIAL_FIELD_COMPONENT)
                                                                    {
                                                                        //Now set the value of the component
                                                                        component = externalCustomFieldValue;
                                                                    }
                                                                    if (externalCustomField == BUGZILLA_SPECIAL_FIELD_HARDWARE)
                                                                    {
                                                                        //Now set the value of the hardware platform
                                                                        hardware = externalCustomFieldValue;
                                                                    }
                                                                    if (externalCustomField == BUGZILLA_SPECIAL_FIELD_OPERATING_SYSTEM)
                                                                    {
                                                                        //Now set the value of the operating system
                                                                        operatingSystem = externalCustomFieldValue;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                //Now actually create the new bugzilla bug
                                int bugzillaBugId = bugzilla.Create(
                                    bugzillaProject,
                                    component,
                                    summary,
                                    bugzillaVersion,
                                    description,
                                    operatingSystem,
                                    hardware,
                                    priority,
                                    severity,
                                    alias,
                                    assignedto,
                                    null,
                                    "",
                                    status,
                                    ""
                                    );

                                //Add the bugzilla bug id to mappings table
                                SpiraImportExport.RemoteDataMapping newIncidentMapping = new SpiraImportExport.RemoteDataMapping();
                                newIncidentMapping.ProjectId = projectId;
                                newIncidentMapping.InternalId = incidentId;
                                newIncidentMapping.ExternalKey = bugzillaBugId.ToString();
                                newIncidentMappings.Add(newIncidentMapping);

                                //See if we have any comments to add to bugzilla
                                RemoteIncidentResolution[] incidentResolutions = spiraImportExport.Incident_RetrieveResolutions(incidentId);
                                if (incidentResolutions != null)
                                {
                                    foreach (RemoteIncidentResolution incidentResolution in incidentResolutions)
                                    {
                                        bugzilla.AddComment(bugzillaBugId, incidentResolution.Resolution, false);
                                    }
                                }
                            }
                        }
                        catch (Exception exception)
                        {
                            //Log and continue execution
                            eventLog.WriteEntry("Error Adding " + productName + " Incident to Bugzilla: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                        }
                    }

                    //Finally we need to update the mapping data on the server before starting the second phase
                    //of the data-synchronization
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());

                    //**** Next we need to see if any of the previously mapped incidents has changed or any new items added to Bugzilla ****
                    incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);

                    //Need to create a list to hold any new releases and new incidents
                    newIncidentMappings = new List<SpiraImportExport.RemoteDataMapping>();
                    List<SpiraImportExport.RemoteDataMapping> newReleaseMappings = new List<SpiraImportExport.RemoteDataMapping>();

                    //Call the Bugzilla API to get the list of recently added/changed bugs
                    DateTime filterDate = lastSyncDate.Value.AddHours(-timeOffsetHours);
                    if (filterDate < DateTime.Parse("1/1/1990"))
                    {
                        filterDate = DateTime.Parse("1/1/1990");
                    }
                    List<Bug> bugzillaBugs = bugzilla.SearchByLastChangeTime(bugzillaProject, filterDate);

                    //Iterate through these items
                    foreach (Bug bugzillaBug in bugzillaBugs)
                    {
                        //Make sure the projects match
                        if (bugzillaBug.Product == bugzillaProject)
                        {
                            //See if we have an existing mapping or not
                            SpiraImportExport.RemoteDataMapping incidentMapping = FindMappingByExternalKey(projectId, bugzillaBug.Id.ToString(), incidentMappings, false);

                            int incidentId = -1;
                            SpiraImportExport.RemoteIncident remoteIncident = null;
                            if (incidentMapping == null)
                            {
                                //This case needs to be inserted into SpiraTest
                                remoteIncident = new SpiraImportExport.RemoteIncident();
                                remoteIncident.ProjectId = projectId;

                                //Set the description for new incidents
                                if (String.IsNullOrEmpty(bugzillaBug.Summary))
                                {
                                    remoteIncident.Name = "Summary Not Specified";
                                    remoteIncident.Description = "Summary Not Specified";
                                }
                                else
                                {
                                    remoteIncident.Description = bugzillaBug.Summary;
                                }

                                //Set the dectector for new incidents
                                if (!String.IsNullOrEmpty(bugzillaBug.Creator))
                                {
                                    SpiraImportExport.RemoteDataMapping dataMapping = FindMappingByExternalKey(bugzillaBug.Creator, userMappings);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching user so log and ignore
                                        eventLog.WriteEntry("Unable to locate mapping entry for Bugzilla user " + bugzillaBug.Creator + " so using synchronization user as detector.", EventLogEntryType.Error);
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

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Retrieved incident in " + productName + "\n", EventLogEntryType.Information);

                                    //Update the incident with the text fields
                                    if (!String.IsNullOrEmpty(bugzillaBug.Summary))
                                    {
                                        remoteIncident.Name = bugzillaBug.Summary;
                                    }
                                    LogTraceEvent(eventLog, "Got the name\n", EventLogEntryType.Information);

                                    //Now get the bug priority from the mapping (if priority is set)
                                    if (String.IsNullOrEmpty(bugzillaBug.Priority))
                                    {
                                        remoteIncident.PriorityId = null;
                                    }
                                    else
                                    {
                                        dataMapping = FindMappingByExternalKey(projectId, bugzillaBug.Priority, priorityMappings, true);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and just don't set the priority
                                            eventLog.WriteEntry("Unable to locate mapping entry for bug priority " + bugzillaBug.Priority + " in project " + projectId, EventLogEntryType.Warning);
                                        }
                                        else
                                        {
                                            remoteIncident.PriorityId = dataMapping.InternalId;
                                        }
                                    }
                                    LogTraceEvent(eventLog, "Got the priority\n", EventLogEntryType.Information);

                                    //Now get the bug severity from the mapping (if severity is set)
                                    if (String.IsNullOrEmpty(bugzillaBug.BugSeverity))
                                    {
                                        remoteIncident.SeverityId = null;
                                    }
                                    else
                                    {
                                        dataMapping = FindMappingByExternalKey(projectId, bugzillaBug.BugSeverity, severityMappings, true);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and just don't set the severity
                                            eventLog.WriteEntry("Unable to locate mapping entry for bug severity " + bugzillaBug.BugSeverity + " in project " + projectId, EventLogEntryType.Warning);
                                        }
                                        else
                                        {
                                            remoteIncident.SeverityId = dataMapping.InternalId;
                                        }
                                    }
                                    LogTraceEvent(eventLog, "Got the severity\n", EventLogEntryType.Information);

                                    //Now get the issue status from the mapping
                                    if (!String.IsNullOrEmpty(bugzillaBug.BugStatus))
                                    {
                                        dataMapping = FindMappingByExternalKey(projectId, bugzillaBug.BugStatus, statusMappings, true);
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so log and ignore
                                            eventLog.WriteEntry("Unable to locate mapping entry for bug status " + bugzillaBug.BugStatus + " in project " + projectId, EventLogEntryType.Error);
                                        }
                                        else
                                        {
                                            remoteIncident.IncidentStatusId = dataMapping.InternalId;
                                        }
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the status\n", EventLogEntryType.Information);

                                    //Now update the bug's owner in SpiraTest
                                    dataMapping = FindMappingByExternalKey(bugzillaBug.AssignedTo, userMappings);
                                    if (dataMapping == null)
                                    {
                                        //We can't find the matching user so log and ignore
                                        eventLog.WriteEntry("Unable to locate mapping entry for Bugzilla user " + bugzillaBug.AssignedTo + " so ignoring the assignee change", EventLogEntryType.Error);
                                    }
                                    else
                                    {
                                        remoteIncident.OwnerId = dataMapping.InternalId;
                                        LogTraceEvent(eventLog, "Got the assignee " + remoteIncident.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                                    }

                                    //Now we need to get all the comments attached to the bug in Bugzilla
                                    List<Comment> bugzillaComments = bugzilla.Comments(bugzillaBug.Id);

                                    //Now get the list of comments attached to the SpiraTest incident
                                    //If this is the new incident case, just leave as null
                                    SpiraImportExport.RemoteIncidentResolution[] incidentResolutions = null;
                                    if (incidentId != -1)
                                    {
                                        incidentResolutions = spiraImportExport.Incident_RetrieveResolutions(incidentId);
                                    }

                                    //Iterate through all the comments and see if we need to add any to SpiraTest
                                    List<SpiraImportExport.RemoteIncidentResolution> newIncidentResolutions = new List<SpiraImportExport.RemoteIncidentResolution>();
                                    if (bugzillaComments != null)
                                    {
                                        foreach (Comment bugzillaComment in bugzillaComments)
                                        {
                                            //See if we already have this resolution inside SpiraTest
                                            bool alreadyAdded = false;
                                            if (incidentResolutions != null)
                                            {
                                                foreach (SpiraImportExport.RemoteIncidentResolution incidentResolution in incidentResolutions)
                                                {
                                                    if (incidentResolution.Resolution == bugzillaComment.Text)
                                                    {
                                                        alreadyAdded = true;
                                                    }
                                                }
                                            }
                                            if (!alreadyAdded)
                                            {
                                                //Get the resolution author mapping
                                                LogTraceEvent(eventLog, "Looking for comments creator: '" + bugzillaComment.Creator + "'\n", EventLogEntryType.Information);
                                                dataMapping = FindMappingByExternalKey(bugzillaComment.Creator, userMappings);
                                                int? creatorId = null;
                                                if (dataMapping != null)
                                                {
                                                    //Set the creator of the comment, otherwise leave null and SpiraTest will
                                                    //simply use the synchronization user
                                                    creatorId = dataMapping.InternalId;
                                                }

                                                //Add the comment to SpiraTest
                                                SpiraImportExport.RemoteIncidentResolution newIncidentResolution = new SpiraImportExport.RemoteIncidentResolution();
                                                newIncidentResolution.IncidentId = incidentId;
                                                newIncidentResolution.CreatorId = creatorId;
                                                newIncidentResolution.CreationDate = bugzillaComment.Time;
                                                newIncidentResolution.Resolution = bugzillaComment.Text;
                                                newIncidentResolutions.Add(newIncidentResolution);
                                            }
                                        }
                                    }
                                    //The resolutions will actually get added later when we insert/update the incident record itself

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the comments/resolution\n", EventLogEntryType.Information);

                                    //Specify the detected-in release if applicable
                                    if (!String.IsNullOrWhiteSpace(bugzillaBug.Version))
                                    {
                                        //See if we have a mapped SpiraTest release in either the existing list of
                                        //mapped releases or the list of newly added ones
                                        dataMapping = FindMappingByExternalKey(projectId, bugzillaBug.Version, releaseMappings, false);
                                        if (dataMapping == null)
                                        {
                                            dataMapping = FindMappingByExternalKey(projectId, bugzillaBug.Version, newReleaseMappings.ToArray(), false);
                                        }
                                        if (dataMapping == null)
                                        {
                                            //We can't find the matching item so need to create a new release in SpiraTest and add to mappings
                                            LogTraceEvent(eventLog, "Adding new release in " + productName + " for version " + bugzillaBug.Version + "\n", EventLogEntryType.Information);
                                            SpiraImportExport.RemoteRelease remoteRelease = new SpiraImportExport.RemoteRelease();
                                            remoteRelease.Name = bugzillaBug.Version;
                                            if (bugzillaBug.Version.Length > 10)
                                            {
                                                remoteRelease.VersionNumber = bugzillaBug.Version.Substring(0, 10);
                                            }
                                            else
                                            {
                                                remoteRelease.VersionNumber = bugzillaBug.Version;
                                            }
                                            remoteRelease.Active = true;
                                            remoteRelease.StartDate = DateTime.Now.Date;
                                            remoteRelease.EndDate = DateTime.Now.Date.AddDays(5);
                                            remoteRelease.CreatorId = remoteIncident.OpenerId;
                                            remoteRelease.CreationDate = DateTime.Now;
                                            remoteRelease.ResourceCount = 1;
                                            remoteRelease.DaysNonWorking = 0;
                                            remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                                            //Add a new mapping entry
                                            SpiraImportExport.RemoteDataMapping newReleaseMapping = new SpiraImportExport.RemoteDataMapping();
                                            newReleaseMapping.ProjectId = projectId;
                                            newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                                            newReleaseMapping.ExternalKey = bugzillaBug.Version;
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

                                    //See if we have bugzilla resolution value
                                    if (!String.IsNullOrEmpty(bugzillaBug.Resolution))
                                    {
                                        //Now iterate through the project custom properties to populate the special bugzilla fields
                                        //Currently that's only the special Resolution field used to capture the Bugzilla resolution
                                        if (!String.IsNullOrEmpty(bugzillaBug.Resolution))
                                        {
                                            foreach (SpiraImportExport.RemoteCustomProperty customProperty in projectCustomProperties)
                                            {
                                                //This is a text field
                                                if (customProperty.CustomPropertyTypeId == CUSTOM_PROPERTY_TYPE_TEXT && customProperty.Alias == BUGZILLA_SPECIAL_FIELD_RESOLUTION)
                                                {
                                                    //Now we need to set the value on the SpiraTest incident
                                                    SetCustomPropertyTextValue(remoteIncident, customProperty.CustomPropertyName, bugzillaBug.Resolution);
                                                }
                                            }
                                        }
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the resolution\n", EventLogEntryType.Information);

                                    //See if we have bugzilla OS value
                                    if (!String.IsNullOrEmpty(bugzillaBug.OpSys))
                                    {
                                        SetListCustomPropertyMappedValue(projectCustomProperties, customPropertyMappingList, customPropertyValueMappingList, BUGZILLA_SPECIAL_FIELD_OPERATING_SYSTEM, remoteIncident, bugzillaBug.OpSys, projectId);
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the OS\n", EventLogEntryType.Information);

                                    //See if we have bugzilla Hardware/Platform value
                                    if (!String.IsNullOrEmpty(bugzillaBug.RepPlatform))
                                    {
                                        SetListCustomPropertyMappedValue(projectCustomProperties, customPropertyMappingList, customPropertyValueMappingList, BUGZILLA_SPECIAL_FIELD_HARDWARE, remoteIncident, bugzillaBug.RepPlatform, projectId);
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the Hardware\n", EventLogEntryType.Information);

                                    //See if we have a bugzilla Component value
                                    if (!String.IsNullOrEmpty(bugzillaBug.Component))
                                    {
                                        SetListCustomPropertyMappedValue(projectCustomProperties, customPropertyMappingList, customPropertyValueMappingList, BUGZILLA_SPECIAL_FIELD_COMPONENT, remoteIncident, bugzillaBug.Component, projectId);
                                    }

                                    //Debug logging - comment out for production code
                                    LogTraceEvent(eventLog, "Got the Component\n", EventLogEntryType.Information);

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
                                            eventLog.WriteEntry("Error Adding Bugzilla bug " + bugzillaBug.Id + " to " + productName + " (" + exception.Message + ")\n" + exception.StackTrace, EventLogEntryType.Error);
                                            continue;
                                        }
                                        LogTraceEvent(eventLog, "Successfully added Bugzilla bug " + bugzillaBug.Id + " to " + productName + "\n", EventLogEntryType.Information);

                                        //Extract the SpiraTest incident and add to mappings table
                                        SpiraImportExport.RemoteDataMapping newIncidentMapping = new SpiraImportExport.RemoteDataMapping();
                                        newIncidentMapping.ProjectId = projectId;
                                        newIncidentMapping.InternalId = remoteIncident.IncidentId.Value;
                                        newIncidentMapping.ExternalKey = bugzillaBug.Id.ToString();
                                        newIncidentMappings.Add(newIncidentMapping);

                                        //Now add any resolutions (need to set the ID)
                                        foreach (SpiraImportExport.RemoteIncidentResolution newResolution in newIncidentResolutions)
                                        {
                                            newResolution.IncidentId = remoteIncident.IncidentId.Value;
                                        }
                                        spiraImportExport.Incident_AddResolutions(newIncidentResolutions.ToArray());

                                    }
                                    else
                                    {
                                        spiraImportExport.Incident_Update(remoteIncident);

                                        //Now add any resolutions
                                        spiraImportExport.Incident_AddResolutions(newIncidentResolutions.ToArray());

                                        //Debug logging - comment out for production code
                                        LogTraceEvent(eventLog, "Successfully updated\n", EventLogEntryType.Information);
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                //Log and continue execution
                                eventLog.WriteEntry("Error Inserting/Updating Bugzilla Bug in " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            }
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
                bugzilla = null;

                //Let the service know that we ran correctly
                return ServiceReturnType.Success;
            }
            catch (Exception exception)
            {
                //Log the exception and return as a failure
                eventLog.WriteEntry("General Error: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                return ServiceReturnType.Error;
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="eventLog">The event log handle</param>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        protected void LogTraceEvent(EventLog eventLog, string message, EventLogEntryType type)
        {
            if (traceLogging)
            {
                eventLog.WriteEntry(message, type);
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

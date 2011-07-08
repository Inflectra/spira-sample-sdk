using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.SpiraTest.PlugIns;
using System.Diagnostics;
using System.Net;
using System.Collections;
using System.IO;
using System.Text.RegularExpressions;

namespace SampleVersionControlProvider
{
    /// <summary>
    /// Sample plug-in that illustrates how you can write a plugin for SpiraPlan/Team that allows you to connect
    /// to a third-party Version Control / Software Configuration Management (SCM) system
    /// </summary>
    public class SampleVersionControlPlugIn : IVersionControlPlugIn
    {
        protected static EventLog applicationEventLog = null;

        /// <summary>
        /// Initializes the provider - connects, authenticates and returns a session token
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="credentials"></param>
        /// <param name="parameters"></param>
        /// <returns>
        /// A provider-specific object that is passed on subsequent calls. Since this is a dummy provider, we'll
        /// just pass back the connection and credentials
        /// </returns>
        /// <param name="eventLog">Handle to an event log object</param>
        /// <param name="custom01">Custom parameters that are provider-specific</param>
        /// <param name="custom02">Custom parameters that are provider-specific</param>
        /// <param name="custom03">Custom parameters that are provider-specific</param>
        /// <param name="custom04">Custom parameters that are provider-specific</param>
        /// <param name="custom05">Custom parameters that are provider-specific</param>
        /// <remarks>Throws an exception if unable to connect or authenticate</remarks>
        public object Initialize(string connection, NetworkCredential credentials, Dictionary<string, string> parameters, EventLog eventLog, string custom01, string custom02, string custom03, string custom04, string custom05)
        {
            /*
             * TODO: Replace with real logic for checking the connection information and the username/password
             */

            if (credentials.UserName != "fredbloggs" && credentials.UserName != "joesmith")
            {
                //Need to thow an exception of this type so that SpiraPlan knows to display the appropriate message to the user
                throw new VersionControlAuthenticationException("Unable to login to version control provider");
            }
            if (connection.Length < 7 || connection.Substring(0, 7) != "test://")
            {
                throw new VersionControlGeneralException("Unable to access version control provider with provided connection information");
            }
            applicationEventLog = eventLog;
            
            //Create and return the token
            AuthenticationToken token = new AuthenticationToken();
            token.Connection = connection;
            token.UserName = credentials.UserName;
            token.Password = credentials.Password;

            return token;
        }

        /// <summary>
        /// Retrieves the parent folder of the passed-in file
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="fileKey">The file identifier</param>
        /// <returns>Single version control folder</returns>
        public VersionControlFolder RetrieveFolderByFile(object token, string fileKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //We just get the file path and remove the last part (the file node)
            Uri uri = new Uri(fileKey);
            VersionControlFolder versionControlFolder = new VersionControlFolder();
            if (uri.Segments.Length < 2)
            {
                versionControlFolder.Name = "Root Folder";
                versionControlFolder.FolderKey = "test://";
            }
            else
            {
                versionControlFolder.Name = uri.Segments[uri.Segments.Length - 2].Replace("/","");
                string folderKey = "";
                for (int i = 0; i < uri.Segments.Length - 1; i++)
                {
                    folderKey += uri.Segments[i];
                }
                versionControlFolder.FolderKey = folderKey;
            }
            return versionControlFolder;
        }

        /// <summary>
        /// Retrieves a single file by its unique key
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="fileKey">The file identifier</param>
        /// <returns>Single version control file</returns>
        public VersionControlFile RetrieveFile(object token, string fileKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */
            
            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //For this test provider, just get the list of files and then find the matching one
            List<VersionControlFile> versionControlFiles = this.RetrieveFilesByFolder(token, "", "", true, null);
            foreach (VersionControlFile versionControlFile in versionControlFiles)
            {
                if (versionControlFile.FileKey == fileKey)
                {
                    return versionControlFile;
                }
            }
            //Otherwise throw a not found exception
            throw new VersionControlArtifactNotFoundException("Could not find file '" + fileKey + "'");
        }

        /// <summary>
        /// Opens the contents of a single file by its key, if the revision is specified, need to return the
        /// details of the file for that specific revision, otherwise just return the most recent
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="fileKey">The file identifier</param>
        /// <param name="revisionKey">The revision identifier (optional)</param>
        /// <returns></returns>
        public VersionControlFileStream OpenFile(object token, string fileKey, string revisionKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //For this dummy provider we just need to create a new in-memory stream, one for latest revision
            //and one for a specific one
            string dummyText = "";
            if (revisionKey == "")
            {
                dummyText = "Latest Revision";
            }
            else
            {
                dummyText = "Specific Revision";
            }

            byte[] buffer = ASCIIEncoding.UTF8.GetBytes(dummyText);
            MemoryStream memoryStream = new MemoryStream(buffer);

            VersionControlFileStream versionControlFileStream = new VersionControlFileStream();
            versionControlFileStream.FileKey = fileKey;
            versionControlFileStream.RevisionKey = revisionKey;
            versionControlFileStream.LocalPath = "";    //Not used by this provider since memory stream
            versionControlFileStream.DataStream = memoryStream;
            return versionControlFileStream;
        }

        /// <summary>
        /// Closes the data stream provided by OpenFile. Clients must NOT CLOSE THE STREAM DIRECTLY
        /// </summary>
        /// <param name="versionControlFileStream">The stream to be closed</param>
        public void CloseFile(VersionControlFileStream versionControlFileStream)
        {
            /*
             * TODO: Make sure that any temporary resources associated with the stream are released.
             *  Also if this is a file stream from a temporary file location, should clean up the temporary files
             */
            versionControlFileStream.DataStream.Close();
        }

        /// <summary>
        /// Retrieves a single folder by its unique key
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="folderKey">The folder identifier</param>
        /// <returns>Single version control folder</returns>
        public VersionControlFolder RetrieveFolder(object token, string folderKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            try
            {
                //Just strip of the last part of the fake URI
                Uri uri = new Uri(folderKey);
                VersionControlFolder versionControlFolder = new VersionControlFolder();
                versionControlFolder.FolderKey = folderKey;
                versionControlFolder.Name = uri.Segments[uri.Segments.Length - 1];
                return versionControlFolder;
            }
            catch (Exception exception)
            {
                //Throw an unable to get artifact exception
                throw new VersionControlArtifactNotFoundException("Unable to retrieve folder '" + folderKey + "'", exception);
            }
        }

        /// <summary>
        /// Retrieves a list of folders under the passed in parent folder
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="parentFolderKey">The parent folder (or NullString if root folders requested)</param>
        /// <returns>List of version control folders</returns>
        public List<VersionControlFolder> RetrieveFolders(object token, string parentFolderKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //Create a list of folders based on what's passed in
            List<VersionControlFolder> versionControlFolders = new List<VersionControlFolder>();
            if (String.IsNullOrEmpty(parentFolderKey))
            {
                versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Design", "Design"));
                versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Development", "Development"));
                versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Test", "Test"));
                versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Documentation", "Documentation"));
                versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Training", "Training"));
            }
            else
            {
                if (parentFolderKey == "test://Server/Root/Design")
                {
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Design/Business", "Business Design"));
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Design/Technical", "Technical Design"));
                }
                if (parentFolderKey == "test://Server/Root/Documentation")
                {
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Documentation/EndUser", "End User"));
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Documentation/Technical", "Technical"));
                }
                if (parentFolderKey == "test://Server/Root/Documentation/EndUser")
                {
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Documentation/EndUser/Presentations", "Presentations"));
                    versionControlFolders.Add(new VersionControlFolder("test://Server/Root/Documentation/EndUser/Manuals", "Manuals"));
                }
            }
            return versionControlFolders;
        }

        /// <summary>
        /// Retrieves a list of revisions for a specific file
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="filters">Any filters to apply to the results</param>
        /// <param name="sortAscending">Do we want to sort the results ascending or descending</param>
        /// <param name="sortProperty">What fields do we want to sort the results on</param>
        /// <param name="fileKey">The key of the file</param>
        /// <returns>List of revisions</returns>
        /// <remarks>For this test provider, it always returns the same list, which is a subset of the total list of revisions</remarks>
        public List<VersionControlRevision> RetrieveRevisionsForFile(object token, string fileKey, string sortProperty, bool sortAscending, Hashtable filters)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            List<VersionControlRevision> revisions = this.RetrieveRevisions(token, sortProperty, sortAscending, filters);

            //Remove a couple of entries so we know that this was the method used
            if (revisions.Count >= 1)
            {
                revisions.Remove(revisions[0]);
            }
            if (revisions.Count >= 11)
            {
                revisions.Remove(revisions[10]);
            }
            return revisions;
        }

        /// <summary>
        /// Retrieves a list of source code revisions associated with a specific SpiraTeam artifact
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="artifactPrefix">The two-letter prefix for the artifact</param>
        /// <param name="artifactId">The id of the artifact</param>
        /// <returns>A list of associated revisions</returns>
        public List<VersionControlRevision> RetrieveRevisionsForArtifact(object token, string artifactPrefix, int artifactId)
        {
            /*
             * TODO: This sample code will work but does not offer particularly good performance since it needs to retrieve all the
             *       revisions in the system and then examine each one for matching artifacts. If you can use the SCM system to do
             *       the matching, it will generally be much faster.
             */

            //We need to get the complete list of revisions in the repository first
            List<VersionControlRevision> revisions = this.RetrieveRevisions(token, "", true, null);

            //Now we need to find ones that match the artifact
            Regex regex = new Regex(@"\[" + artifactPrefix + ":[0]*" + artifactId.ToString() + @"\]");
            List<VersionControlRevision> matchedRevisions = new List<VersionControlRevision>();
            foreach (VersionControlRevision revision in revisions)
            {
                if (!String.IsNullOrEmpty(revision.Message) && regex.Match(revision.Message).Success)
                {
                    matchedRevisions.Add(revision);
                }
            }

            return matchedRevisions;
        }

        /// <summary>
        /// Retrieves a list of artifact associations for the specified revision
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="revisionKey">The key of the revision</param>
        /// <returns>A list of artifact associations</returns>
        public List<VersionControlRevisionAssociation> RetrieveAssociationsForRevision(object token, string revisionKey)
        {
            /*
             * TODO: The implementation is provider-dependent, however in most cases you will need to parse the text in the revision
             *      for the list of tokens that signify SpiraPlan/Team artifacts. An example is provided below that will work in most cases
             */

            //First we need to actually retrieve the revision record
            try
            {
                VersionControlRevision versionControlRevision = this.RetrieveRevision(token, revisionKey);
                if (versionControlRevision == null)
                {
                    //If the revision can't be found, just return a null to indicate no associations
                    return null;
                }

                //Now get the revision string and parse out the associations from the included tokens
                string revisionMessage = versionControlRevision.Message;
                List<VersionControlRevisionAssociation> versionControlRevisionAssociations = new List<VersionControlRevisionAssociation>();
                if (!String.IsNullOrEmpty(revisionMessage))
                {
                    Regex regExp = new Regex(@"\[([A-Z]{2,}):([0-9]+)\]", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                    MatchCollection artMatches = regExp.Matches(revisionMessage);
                    foreach (Match artMatch in artMatches)
                    {
                        VersionControlRevisionAssociation versionControlRevisionAssociation = new VersionControlRevisionAssociation();

                        versionControlRevisionAssociation.ArtifactId = Int32.Parse(artMatch.Groups[2].Value);
                        versionControlRevisionAssociation.ArtifactTypePrefix = artMatch.Groups[1].Value;
                        versionControlRevisionAssociation.AssociationDate = versionControlRevision.UpdateDate;
                        versionControlRevisionAssociation.Comment = "";
                        versionControlRevisionAssociation.RevisionKey = revisionKey;
                        versionControlRevisionAssociations.Add(versionControlRevisionAssociation);
                    }
                }

                return versionControlRevisionAssociations;
            }
            catch (VersionControlArtifactNotFoundException)
            {
                //If the revision can't be found, just return a null to indicate no associations
                return null;
            }
        }

        /// <summary>
        /// Retrieves a single revision by its key
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="revisionKey">The revision identifier</param>
        /// <returns>The revision requested</returns>
        public VersionControlRevision RetrieveRevision(object token, string revisionKey)
        {
            /*
             * TODO: This implementation does not provide very good performanace, ideally should use a built-in 
             *      method of the SCM system to actually get the revision by its ID/key rather that getting all
             *      revisions and looping (very slow)
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //For this test provider, just get the list of revisions and then find the matching one
            List<VersionControlRevision> versionControlRevisions = this.RetrieveRevisions(token, "", true, null);
            foreach (VersionControlRevision versionControlRevision in versionControlRevisions)
            {
                if (versionControlRevision.RevisionKey == revisionKey)
                {
                    return versionControlRevision;
                }
            }
            //Otherwise throw a not found exception
            throw new VersionControlArtifactNotFoundException("Could not find revision '" + revisionKey + "'");
        }


        /// <summary>
        /// Retrieves a list of revisions for the current repository
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="filters">Any filters to apply to the results</param>
        /// <param name="sortAscending">Do we want to sort the results ascending or descending</param>
        /// <param name="sortProperty">What fields do we want to sort the results on</param>
        /// <returns>List of revisions</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlRevision> RetrieveRevisions(object token, string sortProperty, bool sortAscending, Hashtable filters)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider, should use the SCM system to do the sorting
             *          and filtering if that is possible
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //First create the list
            List<VersionControlRevision> versionControlRevisions = new List<VersionControlRevision>();
            versionControlRevisions.Add(new VersionControlRevision("0001", "rev0001", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, true));
            versionControlRevisions.Add(new VersionControlRevision("0002", "rev0002", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0003", "rev0003", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, false, false));
            versionControlRevisions.Add(new VersionControlRevision("0004", "rev0004", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0005", "rev0005", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, false, false));
            versionControlRevisions.Add(new VersionControlRevision("0006", "rev0006", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0007", "rev0007", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, true));
            versionControlRevisions.Add(new VersionControlRevision("0008", "rev0008", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, true));
            versionControlRevisions.Add(new VersionControlRevision("0009", "rev0009", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, true));
            versionControlRevisions.Add(new VersionControlRevision("0010", "rev0010", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, true));
            versionControlRevisions.Add(new VersionControlRevision("0011", "rev0011", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0012", "rev0012", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0013", "rev0013", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, false, true));
            versionControlRevisions.Add(new VersionControlRevision("0014", "rev0014", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, false, true));
            versionControlRevisions.Add(new VersionControlRevision("0015", "rev0015", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));
            versionControlRevisions.Add(new VersionControlRevision("0016", "rev0016", "Fred Bloggs", "The artifact was changed in this version to fix the issue with the data access component", DateTime.Now, true, false));

            //Now see if we need to filter the results
            if (filters != null)
            {
                List<VersionControlRevision> filteredVersionControlRevisions = new List<VersionControlRevision>();
                foreach (VersionControlRevision versionControlRevision in versionControlRevisions)
                {
                    bool match = true;
                    //Name filtering
                    if (filters["Name"] != null)
                    {
                        string filterText = (string)filters["Name"];
                        if (!versionControlRevision.Name.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //Author filtering
                    if (filters["Author"] != null)
                    {
                        string filterText = (string)filters["Author"];
                        if (!versionControlRevision.Author.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //Message filtering
                    if (filters["Message"] != null)
                    {
                        string filterText = (string)filters["Message"];
                        if (!versionControlRevision.Message.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //UpdateDate filtering
                    if (filters["UpdateDate"] != null)
                    {
                        DateRange filterRange = (DateRange)filters["UpdateDate"];
                        if (filterRange.StartDate.HasValue && versionControlRevision.UpdateDate < filterRange.StartDate.Value.Date)
                        {
                            match = false;
                        }
                        if (filterRange.EndDate.HasValue && versionControlRevision.UpdateDate > filterRange.EndDate.Value.Date)
                        {
                            match = false;
                        }
                    }
                    //ContentChanged filtering
                    if (filters["ContentChanged"] != null)
                    {
                        string flagYn = (string)filters["ContentChanged"];
                        if ((versionControlRevision.ContentChanged && flagYn == "N") || (!versionControlRevision.ContentChanged && flagYn == "Y"))
                        {
                            match = false;
                        }
                    }
                    //PropertiesChanged filtering
                    if (filters["PropertiesChanged"] != null)
                    {
                        string flagYn = (string)filters["PropertiesChanged"];
                        if ((versionControlRevision.PropertiesChanged && flagYn == "N") || (!versionControlRevision.PropertiesChanged && flagYn == "Y"))
                        {
                            match = false;
                        }
                    }

                    //Add the item if we have a match
                    if (match)
                    {
                        filteredVersionControlRevisions.Add(versionControlRevision);
                    }
                }
                versionControlRevisions = filteredVersionControlRevisions;
            }

            //Now see if we need to sort it
            if (String.IsNullOrEmpty(sortProperty))
            {
                //Use the default sort
                versionControlRevisions.Sort();
            }
            else
            {
                if (sortProperty == "Name")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.NameAscComparison : VersionControlRevision.NameDescComparison);
                }
                if (sortProperty == "Author")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.AuthorAscComparison : VersionControlRevision.AuthorDescComparison);
                }
                if (sortProperty == "Message")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.MessageAscComparison : VersionControlRevision.MessageDescComparison);
                }
                if (sortProperty == "UpdateDate")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.UpdateDateAscComparison : VersionControlRevision.UpdateDateDescComparison);
                }
                if (sortProperty == "ContentChanged")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.ContentChangedAscComparison : VersionControlRevision.ContentChangedDescComparison);
                }
                if (sortProperty == "PropertiesChanged")
                {
                    versionControlRevisions.Sort((sortAscending) ? VersionControlRevision.PropertiesChangedAscComparison : VersionControlRevision.PropertiesChangedDescComparison);
                }
            }

            return versionControlRevisions;
        }

        /// <summary>
        /// Retrieves a list of files for a specific revision
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="revisionKey">The revision we want the files for</param>
        /// <param name="filters">Any filters to apply to the results</param>
        /// <param name="sortAscending">Do we want to sort the results ascending or descending</param>
        /// <param name="sortProperty">What fields do we want to sort the results on</param>
        /// <returns>List of files</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlFile> RetrieveFilesForRevision(object token, string revisionKey, string sortProperty, bool sortAscending, Hashtable filters)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider.
             *       This sample version just returns all files in the repository
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            return this.RetrieveFilesByFolder(token, "", sortProperty, sortAscending, filters);
        }

        /// <summary>
        /// Retrieves a list of files for a specific folder
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="folderKey">The folder we want the files for</param>
        /// <param name="filters">Any filters to apply to the results</param>
        /// <param name="sortAscending">Do we want to sort the results ascending or descending</param>
        /// <param name="sortProperty">What fields do we want to sort the results on</param>
        /// <returns>List of files</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlFile> RetrieveFilesByFolder(object token, string folderKey, string sortProperty, bool sortAscending, Hashtable filters)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider. Ideally sorting/filtering should be done natively by the SCM system
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //First create the list
            List<VersionControlFile> versionControlFiles = new List<VersionControlFile>();
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename1.ext", "Document Filename1.doc", 100, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Added));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename2.ext", "Document Filename2.xls", 150, "Fred Bloggs", "0002", "rev0002", DateTime.Now, VersionControlFile.VersionControlActionEnum.Added));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename3.ext", "Document Filename3.docx", 180, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Added));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename4.ext", "Document Filename4.xlsx", 100, "Fred Bloggs", "0004", "rev0004", DateTime.Now, VersionControlFile.VersionControlActionEnum.Deleted));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename5.ext", "Document Filename5.ppt", 125, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Other));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename6.ext", "Document Filename6.txt", 20, "Fred Bloggs", "0002", "rev0002", DateTime.Now, VersionControlFile.VersionControlActionEnum.Modified));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename7.ext", "Document Filename7.ai", 1005, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Deleted));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename8.ext", "Document Filename8.pdf", 87, "Fred Bloggs", "0003", "rev0003", DateTime.Now, VersionControlFile.VersionControlActionEnum.Other));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename9.ext", "Document Filename9.vsd", 100, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Modified));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename10.ext", "Document Filename10.pptx", 105, "Fred Bloggs", "0005", "rev0005", DateTime.Now, VersionControlFile.VersionControlActionEnum.Deleted));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename11.ext", "Document Filename11.htm", 75, "Fred Bloggs", "0006", "rev0006", DateTime.Now, VersionControlFile.VersionControlActionEnum.Replaced));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename21.ext", "Document Filename21.cs", 100, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Undefined));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename22.ext", "Document Filename22.vb", 150, "Fred Bloggs", "0002", "rev0002", DateTime.Now, VersionControlFile.VersionControlActionEnum.Deleted));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename23.ext", "Document Filename23.cpp", 180, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Undefined));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename24.ext", "Document Filename24.java", 100, "Fred Bloggs", "0004", "rev0004", DateTime.Now, VersionControlFile.VersionControlActionEnum.Replaced));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename25.ext", "Document Filename25.pl", 125, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Replaced));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename26.ext", "Document Filename26.php", 20, "Fred Bloggs", "0002", "rev0002", DateTime.Now, VersionControlFile.VersionControlActionEnum.Replaced));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename27.ext", "Document Filename27.exe", 1005, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Modified));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename28.ext", "Document Filename28.rb", 87, "Fred Bloggs", "0003", "rev0003", DateTime.Now, VersionControlFile.VersionControlActionEnum.Added));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename29.ext", "Document Filename29.aspx", 100, "Fred Bloggs", "0001", "rev0001", DateTime.Now, VersionControlFile.VersionControlActionEnum.Modified));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename30.ext", "Document Filename30.asp", 105, "Fred Bloggs", "0005", "rev0005", DateTime.Now, VersionControlFile.VersionControlActionEnum.Modified));
            versionControlFiles.Add(new VersionControlFile("test://Server/Root/Files/Filename31.ext", "Document Filename31.py", 75, "Fred Bloggs", "0006", "rev0006", DateTime.Now, VersionControlFile.VersionControlActionEnum.Added));

            //Now see if we need to filter the results
            if (filters != null)
            {
                List<VersionControlFile> filteredVersionControlFiles = new List<VersionControlFile>();
                foreach (VersionControlFile versionControlFile in versionControlFiles)
                {
                    bool match = true;
                    //Name filtering
                    if (filters["Name"] != null)
                    {
                        string filterText = (string)filters["Name"];
                        if (!versionControlFile.Name.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //Size filtering
                    if (filters["Size"] != null)
                    {
                        int filterValue = (int)filters["Size"];
                        if (versionControlFile.Size != filterValue)
                        {
                            match = false;
                        }
                    }
                    //Author filtering
                    if (filters["Author"] != null)
                    {
                        string filterText = (string)filters["Author"];
                        if (!versionControlFile.Author.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //Revision filtering
                    if (filters["Revision"] != null)
                    {
                        string filterText = (string)filters["Revision"];
                        if (!versionControlFile.Revision.Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //Action filtering
                    if (filters["Action"] != null)
                    {
                        string filterText = (string)filters["Action"];
                        if (!versionControlFile.Action.ToString().Contains(filterText))
                        {
                            match = false;
                        }
                    }
                    //LastUpdated filtering
                    if (filters["LastUpdated"] != null)
                    {
                        DateRange filterRange = (DateRange)filters["LastUpdated"];
                        if (filterRange.StartDate.HasValue && versionControlFile.LastUpdated < filterRange.StartDate.Value.Date)
                        {
                            match = false;
                        }
                        if (filterRange.EndDate.HasValue && versionControlFile.LastUpdated > filterRange.EndDate.Value.Date)
                        {
                            match = false;
                        }
                    }

                    //Add the item if we have a match
                    if (match)
                    {
                        filteredVersionControlFiles.Add(versionControlFile);
                    }
                }
                versionControlFiles = filteredVersionControlFiles;
            }

            //Now see if we need to sort it
            if (String.IsNullOrEmpty(sortProperty))
            {
                //Use the default sort
                versionControlFiles.Sort();
            }
            else
            {
                if (sortProperty == "Name")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.NameAscComparison : VersionControlFile.NameDescComparison);
                }
                if (sortProperty == "Author")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.AuthorAscComparison : VersionControlFile.AuthorDescComparison);
                }
                if (sortProperty == "Revision")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.RevisionAscComparison : VersionControlFile.RevisionDescComparison);
                }
                if (sortProperty == "Size")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.SizeAscComparison : VersionControlFile.SizeDescComparison);
                }
                if (sortProperty == "LastUpdated")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.LastUpdatedAscComparison : VersionControlFile.LastUpdatedDescComparison);
                }
                if (sortProperty == "Action")
                {
                    versionControlFiles.Sort((sortAscending) ? VersionControlFile.ActionAscComparison : VersionControlFile.ActionDescComparison);
                }
            }
           
            return versionControlFiles;
        }

        /// <summary>
        /// Gets the key of the parent folder to the folder passed in
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="folderKey">The key of the folder whos parent we want</param>
        /// <returns>The key of the parent folder (nullstring if no parent)</returns>
        public string RetrieveParentFolderKey(object token, string folderKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //Hard code the data since this is a test provider
            switch (folderKey)
            {
                case "test://Server/Root/Design":
                case "test://Server/Root/Development":
                case "test://Server/Root/Test":
                case "test://Server/Root/Documentation":
                case "test://Server/Root/Training":
                    return "";

                case "test://Server/Root/Design/Business":
                case "test://Server/Root/Design/Technical":
                    return "test://Server/Root/Design";
                
                case "test://Server/Root/Documentation/EndUser":
                case "test://Server/Root/Documentation/Technical":
                    return "test://Server/Root/Documentation";

                case "test://Server/Root/Documentation/EndUser/Presentations":
                case "test://Server/Root/Documentation/EndUser/Manuals":
                    return "test://Server/Root/Documentation/EndUser";
            }
            return "";
        }
    }
}


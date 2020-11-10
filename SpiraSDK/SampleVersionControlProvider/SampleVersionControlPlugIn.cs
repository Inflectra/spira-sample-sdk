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
    public class SampleVersionControlPlugIn : IVersionControlPlugIn2
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
        public object Initialize(string connection, NetworkCredential credentials, Dictionary<string, string> parameters, EventLog eventLog, string cacheFolder, string custom01, string custom02, string custom03, string custom04, string custom05)
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

        /// <summary>Tests the given settings to verify connectivity to the repository.</summary>
        /// <param name="connection">The connection info</param>
        /// <param name="credentials">The login/password/domain for the provider</param>
        /// <param name="parameters">Any custom parameters</param>
        /// <param name="eventLog">A handle to the Windows Event Log used by Spira</param>
        /// <param name="custom01">Provider-specific parameter</param>
        /// <param name="custom02">Provider-specific parameter</param>
        /// <param name="custom03">Provider-specific parameter</param>
        /// <param name="custom04">Provider-specific parameter</param>
        /// <param name="custom05">Provider-specific parameter</param>
        /// <param name="cacheFolder">The location to the folder where any cached data can be stored</param>
        /// <remarks>True if connection was successful and good. False, or throws exception if failure.</remarks>
        public bool TestConnection(string connection, NetworkCredential credentials, Dictionary<string, string> parameters, EventLog eventLog, string cacheFolder, string custom01, string custom02, string custom03, string custom04, string custom05)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Just returns true
            return true;
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
        /// Opens the contents of a single file by its key, if the revision is specified, need to return the
        /// details of the file for that specific revision, otherwise just return the most recent
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="fileKey">The file identifier</param>
        /// <param name="branchKey">The name of the branch</param>
        /// <param name="revisionKey">The revision identifier (optional)</param>
        /// <returns></returns>
        public VersionControlFileStream OpenFile(object token, string fileKey, string revisionKey, string branchKey)
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

        /// <summary>Returns a list of all avaiable branches.</summary>
        /// <param name="token">The source control library's unique token.</param>
        /// <returns>A list of avaiable branches.</returns>
        public List<VersionControlBranch> RetrieveBranches(object token)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            //Sample code to retrieve some random branch names
            List<VersionControlBranch> retList = new List<VersionControlBranch>();

            //Three branches..
            VersionControlBranch branch1 = new VersionControlBranch();
            branch1.BranchKey = "Master";
            branch1.IsDefault = true;
            retList.Add(branch1);

            VersionControlBranch branch2 = new VersionControlBranch();
            branch2.BranchKey = "Branch_12";
            branch2.IsDefault = false;
            retList.Add(branch2);

            VersionControlBranch branch3 = new VersionControlBranch();
            branch3.BranchKey = "Fork" + DateTime.Now.ToString("yyyyMMdd");
            branch3.IsDefault = false;
            retList.Add(branch3);

            return retList;
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
        /// <param name="branchKey">The name of the branch</param>
        /// <returns>Single version control folder</returns>
        public VersionControlFolder RetrieveFolder(object token, string folderKey, string branchKey)
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
        /// <param name="branchKey">The name of the branch</param>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="parentFolderKey">The parent folder (or NullString if root folders requested)</param>
        /// <returns>List of version control folders</returns>
        public List<VersionControlFolder> RetrieveFolders(object token, string parentFolderKey, string branchKey)
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

        /// <summary>Returns all revisions that have occured since the specified date.</summary>
        /// <param name="token">The source control library's unique token.</param>
        /// <param name="branchKey">The key for the branch to pull the revisions/commits from.</param>
        /// <param name="newerThan">The cutoff date for revisions to return.</param>
        /// <returns>A list of revisions that are newer than the specified date.</returns>
        public List<VersionControlRevision> RetrieveRevisionsSince(object token, string branchKey, DateTime newerThan)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
             */

            //We just return all revisions in this test example
            return this.RetrieveRevisions(token, branchKey);
        }

        /// <summary>
        /// Retrieves a list of revisions for the current repository
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="branchKey">The name of the branch</param>
        /// <returns>List of revisions</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlRevision> RetrieveRevisions(object token, string branchKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider
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

            return versionControlRevisions;
        }

        /// <summary>
        /// Retrieves a list of files for a specific revision
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="revisionKey">The revision we want the files for</param>
        /// <param name="branchKey">The name of the branch</param>
        /// <returns>List of files</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlFile> RetrieveFilesForRevision(object token, string revisionKey, string branchKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider.
             *       This sample version just returns all files in the repository
             */

            //Verify the token
            AuthenticationToken authToken = InternalFunctions.VerifyToken(token);

            return this.RetrieveFilesByFolder(token, "", branchKey);
        }

        /// <summary>
        /// Retrieves a list of files for a specific folder
        /// </summary>
        /// <param name="token">The connection token for the provider</param>
        /// <param name="folderKey">The folder we want the files for</param>
        /// <param name="branchKey">The name of the branch</param>
        /// <returns>List of files</returns>
        /// <remarks>For this test provider, it always returns the same list</remarks>
        public List<VersionControlFile> RetrieveFilesByFolder(object token, string folderKey, string branchKey)
        {
            /*
             * TODO: Replace with a real implementation that is appropriate for the provider.
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


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SampleDataSync
{
    /// <summary>
    /// Stores the constants used by the DataSync class
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// The path to the Spira web service relative to the application's base URL
        /// </summary>
        public const string WEB_SERVICE_URL_SUFFIX = "/Services/v3_0/ImportExport.svc";

        //Spira artifact prefixes
        public const string INCIDENT_PREFIX = "IN";

        #region Enumerations

        /// <summary>
        /// The artifact types used in the data-sync
        /// </summary>
        public enum ArtifactType
        {
            Incident = 3,
            Release = 4
        }

        /// <summary>
        /// The artifact field ids used in the data-sync
        /// </summary>
        public enum ArtifactField
        {
            Severity = 1,
            Priority = 2,
            Status = 3,
            Type = 4
        }

        /// <summary>
        /// The different types of custom property
        /// </summary>
        public enum CustomPropertyType
        {
            Text = 1,
            List = 2
        }

        #endregion
    }
}

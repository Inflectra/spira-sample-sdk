using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SampleVersionControlProvider
{
    /// <summary>
    /// Sample authentication token that just stores the connection, name and password.
    /// A real token could work the same way or use an internal token used by the version control provider if it supports persistent sessions
    /// </summary>
    public class AuthenticationToken
    {
        /// <summary>
        /// The connection info for the instance of the SCM system
        /// </summary>
        public string Connection
        {
            get;
            set;
        }

        /// <summary>
        /// The username used to connect to the SCM system
        /// </summary>
        public string UserName
        {
            get;
            set;
        }

        /// <summary>
        /// The password used to connect to the SCM system
        /// </summary>
        public string Password
        {
            get;
            set;
        }
    }
}

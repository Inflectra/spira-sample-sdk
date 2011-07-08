using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Inflectra.SpiraTest.PlugIns;

namespace SampleVersionControlProvider
{
    /// <summary>
    /// Internal helper functions used by the provider plug-in
    /// </summary>
    public static class InternalFunctions
    {
        /// <summary>
        /// Checks that the provided token is of the correct type and returns the correctly-typed instance
        /// </summary>
        /// <param name="token">The untyped generic token object</param>
        /// <returns>The strongly-typed token</returns>
        public static AuthenticationToken VerifyToken (object token)
        {
            //Get the passed in token
            if (token.GetType() != typeof(AuthenticationToken))
            {
                throw new VersionControlGeneralException("The passed in token is not of the correct type");
            }
            AuthenticationToken authToken = (AuthenticationToken)token;

            //Make sure the token is populated
            if (String.IsNullOrEmpty(authToken.Connection))
            {
                throw new VersionControlGeneralException("The passed in token does not contain the Connection");
            }
            if (String.IsNullOrEmpty(authToken.UserName))
            {
                throw new VersionControlGeneralException("The passed in token does not contain the User Name");
            }
            if (String.IsNullOrEmpty(authToken.Password))
            {
                throw new VersionControlGeneralException("The passed in token does not contain the Password");
            }

            return authToken;
        }
    }
}

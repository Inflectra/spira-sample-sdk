using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Inflectra.Rapise.RapiseLauncher.Business.DataObjects
{
    /// <summary>
    /// Represents a parameter value that is being passed to the execution engine
    /// </summary>
    public class TestRunParameter
    {
        /// <summary>
        /// The name of the test case parameter
        /// </summary>
        public string Name;

        /// <summary>
        /// The value of the parameter
        /// </summary>
        public string Value;
    }
}
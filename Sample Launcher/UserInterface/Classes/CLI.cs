using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface
{
	internal class CLI
	{
		public event EventHandler FilenamePassed;
        public event EventHandler TestSetPassed;

		#region Internal Vars
		private string _filename;
		private bool _showStatus;
		private bool _parameterError;
		private bool _poll;
		private bool _paused;
		private bool _trace;
		private bool _unRegistered;
		private bool _logToFile;
        private Nullable<int> _testSetId;
		#endregion

        public CLI(System.Collections.ObjectModel.ReadOnlyCollection<string> args)
        {
            if (args != null)
            {
                //Convert the args
                List<string> args2 = new List<string>();
                foreach (string arg in args)
                {
                    args2.Add(arg);
                }
                Initialize(args2);
            }
        }

        public CLI(List<string> args)
        {
            if (args != null)
            {
                Initialize(args);
            }
        }

        private void Initialize(List<string> args)
		{
			//Parse out the command lines arguments.
			for (int i = 0; i < args.Count; i++)
			{
				string cmdParam = args[i].ToLowerInvariant().Trim().Trim(new char[] { '-', '/' });

                //We want to execute a specific test set
                if (cmdParam.Length > "testset:".Length && cmdParam.Substring(0, "testset:".Length) == "testset:")
                {
                    int testSetId;
                    if (Int32.TryParse(cmdParam.Substring("testset:".Length), out testSetId))
                    {
                        //Tell RemoteLaunch that we need to run a specific test set
                        this._testSetId = testSetId;
                    }
                    else
                    {
                        //We have a parameter error
                        this._parameterError = true;
                    }
                }

				switch (cmdParam)
				{
					case "status":  //Start with status window open.
						this._showStatus = true;
						break;

					case "poll":  //Start with a poll to the server.
						this._poll = true;
						break;

					case "paused":  //Start the timers paused.
						this._paused = true;
						break;

					case "trace":  //Enable tracelogging.
						this._trace = true;
						break;

					case "unreg":  //App is unregistered.
						this._unRegistered = true;
						break;

					case "logfile":  //Log to a file instead of the Eventlog.
						this._logToFile = true;
						break;
					default:
						//Check to see if it's a valid file.
						if (File.Exists(args[i].Trim()))
						{
							this._filename = args[i].Trim();
							//Force other needed options..
							this._paused = true;
							this._poll = false;
							this._showStatus = ((this._unRegistered) ? true : false);
							//Stop handling parameters..
							i = args.Count;
						}
						else
							this._parameterError = true;
						break;
				}
			}
		}

		/// <summary>Called when someone tries to re-run the application.</summary>
		public void SecondInstanceCLI(string[] args)
		{
			//After the initial run, we only care about the filename passed.
            //and the test set id
			//Parse out the command lines arguments.
			for (int i = 0; i < args.Length; i++)
			{
				string cmdParam = args[i].ToLowerInvariant().Trim();

                //Check to see if it's a test set id
                if (cmdParam.Length > "-testset:".Length && cmdParam.Substring(0, "-testset:".Length) == "-testset:")
                {
                    int testSetId;
                    if (Int32.TryParse(cmdParam.Substring("-testset:".Length), out testSetId))
                    {
                        //Tell RemoteLaunch that we need to run a specific test set
                        this._testSetId = testSetId;

                        //Force other needed options..
                        this._paused = true;
                        this._poll = false;
                        this._showStatus = ((this._unRegistered) ? true : false);
                        //Stop handling parameters..
                        i = args.Length;

                        //Raise event. 
                        this.TestSetPassed(this, new EventArgs());
                    }
                    else
                    {
                        //We have a parameter error
                        this._parameterError = true;
                    }
                }

				if (!cmdParam.StartsWith("-") && !cmdParam.StartsWith("/"))
				{
					//Check to see if it's a valid file.
					if (File.Exists(args[i].Trim()))
					{
						this._filename = args[i].Trim();
						//Force other needed options..
						this._paused = true;
						this._poll = false;
						this._showStatus = ((this._unRegistered) ? true : false);
						//Stop handling parameters..
						i = args.Length;

						//Raise event. 
						this.FilenamePassed(this, new EventArgs());
					}
					else
						this._parameterError = true;

				}
			}
		}

		#region Properties
		/// <summary>The filename passed on the command line, if specified.</summary>
		public string Filename
		{
			get
			{
				return this._filename;
			}
		}

		/// <summary>Whether the user specified to show the status window upon execution.</summary>
		public bool ShowStatus
		{
			get
			{
				return this._showStatus;
			}
		}

		/// <summary>Whether or not there was an error in a parameter given on the command-line.</summary>
		public bool ParameterError
		{
			get
			{
				return this._parameterError;
			}
		}

		/// <summary>Whether or not we want to poll upon startup.</summary>
		public bool PollOnStartup
		{
			get
			{
				return this._poll;
			}
		}

		/// <summary>Whether to start the application paused or not.</summary>
		public bool StartupPaused
		{
			get
			{
				return this._paused;
			}
		}

		/// <summary>Whether or not the user wants to enable trace-logging.</summary>
		public bool TraceLogging
		{
			get
			{
				return this._trace;
			}
		}

		/// <summary>Whether or not we're logging to a file instead of the eventlog.</summary>
		public bool LogToFile
		{
			get
			{
				return this._logToFile;
			}
		}

        /// <summary>
        /// The id of a test set we've been asked to execute
        /// </summary>
        public Nullable<int> TestSetId
        {
            get
            {
                return this._testSetId;
            }
        }

		#endregion
	}

	/// Available command-line parameters:
	/// -status		- Show the status window upon startup. (Otherwise, start minimized.)
	/// -paused		- Start the timers paused.
	/// -poll		- Poll the server as soon as program's started.
	/// -trace		- Enables tracelogging.
	/// <file>		- Execute the TST file given.
	/// -logfile	- Log to a file instead of the EventLog.
}

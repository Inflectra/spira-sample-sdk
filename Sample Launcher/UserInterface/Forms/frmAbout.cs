using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Reflection;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface.Forms
{
    public partial class frmAbout : Form
    {
        public frmAbout()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Called when the form is first loaded
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void frmAbout_Load(object sender, EventArgs e)
        {
            //Set the various icons from Branding Resources
            this.Icon = Branding.Resources.Main.App;
            this.imgProduct.Image = Branding.Resources.Main.App_Logo;

            //Display the title bar and group title
            this.Text = Branding.Resources.Main.App_AboutFormName;
            this.lblProductName.Text = Branding.Resources.Main.App_FullName;
            this.lblProductVersion.Text = "v" + Assembly.GetExecutingAssembly().GetName().Version.ToString();
            this.lblDescription.Text = Branding.Resources.Main.About_Description;
            this.lblCompany.Text = Branding.Resources.Main.App_CompanyCopyright;
        }
    }
}

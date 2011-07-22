using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Drawing;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface.Classes
{
    /// <summary>
    /// Displays a transparent image
    /// </summary>
    public class TransparentControl : Control
    {
        private readonly Timer refresher;
        private Image _image;
        public TransparentControl()
        {
            SetStyle(ControlStyles.SupportsTransparentBackColor, true);
            BackColor = Color.Transparent;
            refresher = new Timer();
            refresher.Tick += TimerOnTick;
            refresher.Interval = 50;
            refresher.Enabled = true;
            refresher.Start();
        }

        protected override CreateParams CreateParams
        {
            get
            {
                CreateParams cp = base.CreateParams;
                cp.ExStyle |= 0x20;
                return cp;
            }
        }
        protected override void OnMove(EventArgs e)
        {
            RecreateHandle();
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            if (_image != null)
            {
                e.Graphics.DrawImage(_image, 0, 0, _image.Width, _image.Height);
                //e.Graphics.DrawImage(_image, (Width / 2) - (_image.Width / 2), (Height / 2) - (_image.Height / 2));
            }
        }
        protected override void OnPaintBackground(PaintEventArgs e)
        {
            //Do not paint background  
        }

        public void Redraw()
        {
            RecreateHandle();
        }
        private void TimerOnTick(object source, EventArgs e)
        {
            RecreateHandle();
            refresher.Stop();
        }

        public Image Image
        {
            get
            {
                return _image;
            }
            set
            {
                _image = value; RecreateHandle();
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;
using System.Drawing.Drawing2D;

namespace Inflectra.Rapise.RapiseLauncher.UserInterface.Classes
{
    /// <summary>
    /// Provides a label that supports gradient-shaded backgrounds
    /// </summary>
    public class GradientLabel : System.Windows.Forms.Label
    {
        // declare two color for linear gradient

        private Color cLeft;
        private Color cRight;

        // property of begin color in linear gradient

        public Color BeginColor
        {
            get
            {
                return cLeft;
            }
            set
            {
                cLeft = value;
            }
        }
        // property of end color in linear gradient

        public Color EndColor
        {
            get
            {
                return cRight;
            }
            set
            {
                cRight = value;
            }
        }
        public GradientLabel()
        {
            // Default get system color 

            cLeft = SystemColors.ActiveCaption;
            cRight = SystemColors.Control;
        }
        protected override void OnPaint(System.Windows.Forms.PaintEventArgs e)
        {
            // declare linear gradient brush for fill background of label

            LinearGradientBrush GBrush = new LinearGradientBrush(
                new Point(0, 0),
                new Point(this.Width, 0), cLeft, cRight);
            Rectangle rect = new Rectangle(0, 0, this.Width, this.Height);
            // Fill with gradient 

            e.Graphics.FillRectangle(GBrush, rect);

            // draw text on label

            SolidBrush drawBrush = new SolidBrush(this.ForeColor);
            StringFormat sf = new StringFormat();
            
            // align text based on TextAlign property
            switch (this.TextAlign)
            {
                case ContentAlignment.TopLeft:
                case ContentAlignment.BottomLeft:
                case ContentAlignment.MiddleLeft:
                    sf.Alignment = StringAlignment.Near;
                    break;
                case ContentAlignment.TopCenter:
                case ContentAlignment.BottomCenter:
                case ContentAlignment.MiddleCenter:
                    sf.Alignment = StringAlignment.Center;
                    break;
                case ContentAlignment.TopRight:
                case ContentAlignment.BottomRight:
                case ContentAlignment.MiddleRight:
                    sf.Alignment = StringAlignment.Far;
                    break;
            }

            // set rectangle bound text
            float paddingX = this.Padding.Left;
            RectangleF rectF = new
            RectangleF(paddingX, this.Height / 2 - Font.Height / 2, this.Width, this.Height);
            // output string

            e.Graphics.DrawString(this.Text, this.Font, drawBrush, rectF, sf);
        }
    } 
}

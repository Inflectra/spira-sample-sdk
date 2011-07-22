using System.Xml.Serialization;
using System.Collections.Generic;
using System.Xml.Schema;
using System;

namespace Inflectra.Rapise.RapiseLauncher.Business
{
	[System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "2.0.50727.3038")]
	[System.SerializableAttribute()]
	[System.Diagnostics.DebuggerStepThroughAttribute()]
	[System.ComponentModel.DesignerCategoryAttribute("code")]
	[XmlTypeAttribute(AnonymousType = true)]
	[XmlRootAttribute(Namespace = "", IsNullable = false, ElementName = "TestRun")]
	public partial class TestSetLaunch
	{
		[XmlElementAttribute(Form = XmlSchemaForm.Unqualified, IsNullable = false, DataType = "int", Type = typeof(int))]
		public int TestSetId
		{
			get;
			set;
		}

		[XmlElementAttribute(Form = XmlSchemaForm.Unqualified, IsNullable = false, DataType = "anyURI", Type = typeof(string))]
		public string ServerUrl
		{
			get;
			set;
		}

		[XmlElementAttribute(Form = XmlSchemaForm.Unqualified, IsNullable = true, DataType = "int", Type = typeof(int?))]
		public int? ProjectId
		{
			get;
			set;
		}
	}
}

<%@ Page Title="Home Page" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Default.aspx.cs" Inherits="VesselTracker._Default" %>

<asp:Content ID="BodyContent" ContentPlaceHolderID="MainContent" runat="server">

    <div class="row">
        <div class="col-sm">
            <h2>VesselTracker</h2>
            <br />
            <asp:Button ID="btnProcess" runat="server" OnClick="btnProcess_Click" Text="Process" CssClass="btn btn-primary" />
        </div>
    </div>

</asp:Content>

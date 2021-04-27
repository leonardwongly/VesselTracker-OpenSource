using Newtonsoft.Json;
using RestSharp;
using System;
using System.Web.UI;
using System.Net.Sockets;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System.Threading;
using System.Diagnostics;

namespace VesselTracker
{
    public partial class _Default : Page
    {
        public static Table vfTable;
        public static CancellationTokenSource source = new CancellationTokenSource();
        public static CancellationToken token = source.Token;
        private static AmazonDynamoDBClient awsClient = new AmazonDynamoDBClient();
        private static string tableName = "vt-table";
        private static bool operationSucceeded, operationFailed;
        private const int COUNTER_MAX = 1000000;
        private const int OUTPUT_FREQUENCY = 1000;
        static int counter = 0;
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void btnProcess_Click(object sender, EventArgs e)
        {
            vtLoop();
        }

        private static void vtLoop()
        {
            try
            {
                // Iterate counter.
                counter++;

                // Output counter value every so often.
                if (counter % OUTPUT_FREQUENCY == 0)
                {
                    Debug.WriteLine($"Current counter: {counter}.");
                }

                // Check if counter has reached maximum value; if not, allow recursion.
                if (counter <= COUNTER_MAX)
                {
                    // Recursively call self method.
                    var client = new RestClient("");
                    client.Timeout = -1;
                    var request = new RestRequest(Method.GET);
                    request.AddHeader("Authorization", "");
                    IRestResponse response = client.Execute(request);
                    dynamic jsonObj = JsonConvert.DeserializeObject(response.Content);
                    for (int i = 0; i < jsonObj["vessels"].Count; i++)
                    {
                        Document newItem = new Document();
                        newItem["ID"] = DateTime.Now.ToShortDateString().ToString() + "-" + jsonObj["vessels"][i]["aisStatic"]["imo"].ToString() + "-" + DateTime.Now.ToLongTimeString().ToString() + "-" + jsonObj["vessels"][i]["aisStatic"]["mmsi"].ToString();

                        //aisStatic
                        newItem["name"] = jsonObj["vessels"][i]["aisStatic"]["name"].ToString();
                        newItem["mmsi"] = jsonObj["vessels"][i]["aisStatic"]["mmsi"].ToString();
                        newItem["imo"] = jsonObj["vessels"][i]["aisStatic"]["imo"].ToString();
                        newItem["callsign"] = jsonObj["vessels"][i]["aisStatic"]["callsign"].ToString();
                        newItem["flag"] = jsonObj["vessels"][i]["aisStatic"]["flag"].ToString();
                        newItem["dimA"] = jsonObj["vessels"][i]["aisStatic"]["dimA"].ToString();
                        newItem["dimB"] = jsonObj["vessels"][i]["aisStatic"]["dimB"].ToString();
                        newItem["dimC"] = jsonObj["vessels"][i]["aisStatic"]["dimC"].ToString();
                        newItem["dimD"] = jsonObj["vessels"][i]["aisStatic"]["dimD"].ToString();
                        newItem["length"] = jsonObj["vessels"][i]["aisStatic"]["length"].ToString();
                        newItem["width"] = jsonObj["vessels"][i]["aisStatic"]["width"].ToString();
                        newItem["typeOfShipAndCargo"] = jsonObj["vessels"][i]["aisStatic"]["typeOfShipAndCargo"].ToString();
                        newItem["aisShiptype"] = jsonObj["vessels"][i]["aisStatic"]["aisShiptype"].ToString();
                        newItem["updateTime"] = jsonObj["vessels"][i]["aisStatic"]["updateTime"].ToString();
                        newItem["aisClass"] = jsonObj["vessels"][i]["aisStatic"]["aisClass"].ToString();

                        //aisVoyage
                        newItem["vUpdateTime"] = jsonObj["vessels"][i]["aisVoyage"]["updateTime"].ToString();
                        newItem["eta"] = jsonObj["vessels"][i]["aisVoyage"]["eta"].ToString();
                        newItem["dest"] = jsonObj["vessels"][i]["aisVoyage"]["dest"].ToString();
                        newItem["draught"] = jsonObj["vessels"][i]["aisVoyage"]["draught"].ToString();
                        newItem["source"] = jsonObj["vessels"][i]["aisVoyage"]["source"].ToString();
                        newItem["cargotype"] = jsonObj["vessels"][i]["aisVoyage"]["cargotype"].ToString();

                        //aisPosition
                        newItem["timeReceived"] = jsonObj["vessels"][i]["aisPosition"]["timeReceived"].ToString();
                        newItem["src"] = jsonObj["vessels"][i]["aisPosition"]["src"].ToString();
                        newItem["lon"] = jsonObj["vessels"][i]["aisPosition"]["lon"].ToString();
                        newItem["lat"] = jsonObj["vessels"][i]["aisPosition"]["lat"].ToString();
                        newItem["sog"] = jsonObj["vessels"][i]["aisPosition"]["sog"].ToString();
                        newItem["cog"] = jsonObj["vessels"][i]["aisPosition"]["cog"].ToString();
                        newItem["hdg"] = jsonObj["vessels"][i]["aisPosition"]["hdg"].ToString();
                        newItem["rot"] = jsonObj["vessels"][i]["aisPosition"]["rot"].ToString();
                        newItem["navStatus"] = jsonObj["vessels"][i]["aisPosition"]["navStatus"].ToString();

                        //geoDetails
                        newItem["timeOfATChange"] = jsonObj["vessels"][i]["geoDetails"]["timeOfATChange"].ToString();
                        newItem["status"] = jsonObj["vessels"][i]["geoDetails"]["status"].ToString();
                        newItem["area"] = jsonObj["vessels"][i]["geoDetails"]["area"].ToString();

                        //vesselDetails
                        newItem["shipType"] = jsonObj["vessels"][i]["vesselDetails"]["shipType"].ToString();
                        newItem["sizeClass"] = jsonObj["vessels"][i]["vesselDetails"]["sizeClass"].ToString();
                        newItem["grossTonnage"] = jsonObj["vessels"][i]["vesselDetails"]["grossTonnage"].ToString();
                        newItem["deadWeight"] = jsonObj["vessels"][i]["vesselDetails"]["deadWeight"].ToString();
                        newItem["teu"] = jsonObj["vessels"][i]["vesselDetails"]["teu"].ToString();
                        newItem["shipDBName"] = jsonObj["vessels"][i]["vesselDetails"]["shipDBName"].ToString();

                        Table vfDataAWS = Table.LoadTable(awsClient, tableName);

                        vfDataAWS.PutItem(newItem);
                        Debug.WriteLine("Data Inserted!");
                    }
                    Thread.Sleep(900000); //15 mins
                    vtLoop();
                }
                else
                {
                    Debug.WriteLine("Recursion halted.");
                }
            }
            catch (StackOverflowException exception)
            {
                Debug.WriteLine(exception);
            }
        }

       

        public static bool createClient(bool useDynamoDBLocal)
        {

            if (useDynamoDBLocal)
            {
                operationSucceeded = false;
                operationFailed = false;

                // First, check to see whether anyone is listening on the DynamoDB local port
                // (by default, this is port 8000, so if you are using a different port, modify this accordingly)
                bool localFound = false;
                try
                {
                    using (var tcp_client = new TcpClient())
                    {
                        var result = tcp_client.BeginConnect("localhost", 8000, null, null);
                        localFound = result.AsyncWaitHandle.WaitOne(3000); // Wait 3 seconds
                        tcp_client.EndConnect(result);
                    }
                }
                catch
                {
                    localFound = false;
                }
                if (!localFound)
                {
                    Debug.WriteLine("\n      ERROR: DynamoDB Local does not appear to have been started..." +
                                      "\n        (checked port 8000)");
                    operationFailed = true;
                    return (false);
                }

                // If DynamoDB-Local does seem to be running, so create a client
                Debug.WriteLine("  -- Setting up a DynamoDB-Local client (DynamoDB Local seems to be running)");
                AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
                ddbConfig.ServiceURL = "http://localhost:8000";
                try { awsClient = new AmazonDynamoDBClient(ddbConfig); }
                catch (Exception ex)
                {
                    Debug.WriteLine("     FAILED to create a DynamoDBLocal client; " + ex.Message);
                    operationFailed = true;
                    return false;
                }
            }

            else
            {
                try { awsClient = new AmazonDynamoDBClient(); }
                catch (Exception ex)
                {
                    Debug.WriteLine("     FAILED to create a DynamoDB client; " + ex.Message);
                    operationFailed = true;
                }
            }
            operationSucceeded = true;
            return true;
        }
    }
}
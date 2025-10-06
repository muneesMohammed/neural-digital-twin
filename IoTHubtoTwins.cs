// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventGrid;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Net.Http;
using Azure;
using System.Threading.Tasks;

namespace FunctionApp1
{
    public class IoTHubtoTwins
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IoTHubtoTwins")]
        public async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log) // Changed from async void to async Task
        {
            if (adtInstanceUrl == null)
            {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            try
            {
                // Authenticate with Digital Twins
                var cred = new DefaultAzureCredential();
                var client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred);
                log.LogInformation($"ADT service client connection created.");

                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // <Find_device_ID_and_temperature>
                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                    var MotorId = deviceMessage["body"]["MotorId"];
                    var MotorSpeed = deviceMessage["body"]["MotorSpeed"];
                    var AirCleanerId = deviceMessage["body"]["AirCleanerId"];
                    var TankId = deviceMessage["body"]["TankId"];
                    var Pressure = deviceMessage["body"]["Pressure"];
                    var waterOutletValvePressure = deviceMessage["body"]["waterOutletValvePressure"];
                    var PumpId = deviceMessage["body"]["PumpId"];
                    var Vibration = deviceMessage["body"]["Vibration"];
                    var OilCoolerId = deviceMessage["body"]["OilCoolerId"];
                    var EnergyConsumed = deviceMessage["body"]["EnergyConsumed"];
                    var Temperature = deviceMessage["body"]["Temperature"];
                    var NoiseLevel = deviceMessage["body"]["NoiseLevel"];

                    log.LogInformation($"Device:{deviceId}  MotorId is:{MotorId} MotorSpeed:{MotorSpeed}  AirCleanerId:{AirCleanerId} TankId:{TankId} Pressure:{Pressure} waterOutletValvePressure:{waterOutletValvePressure} PumpId {PumpId} Vibration{Vibration} OilCoolerId{OilCoolerId} EnergyConsumed{EnergyConsumed} Temperature{Temperature} NoiseLevel{NoiseLevel}");

                    // <Update_twin_with_device_temperature>
                    var updateTwinData = new JsonPatchDocument();
                    updateTwinData.AppendReplace("/Motor/MotorId", MotorId.Value<string>());
                    updateTwinData.AppendReplace("/Motor/MotorSpeed", MotorSpeed.Value<double>());
                    updateTwinData.AppendReplace("/AirCleaner/AirCleanerId", AirCleanerId.Value<string>());
                    updateTwinData.AppendReplace("/Tank/TankId", TankId.Value<string>());
                    updateTwinData.AppendReplace("/Tank/Pressure", Pressure.Value<double>());
                    updateTwinData.AppendReplace("/Tank/waterOutletValvePressure", waterOutletValvePressure.Value<double>());
                    updateTwinData.AppendReplace("/Pump/PumpId", PumpId.Value<string>());
                    updateTwinData.AppendReplace("/Pump/Vibration", Vibration.Value<double>());
                    updateTwinData.AppendReplace("/OilCooler/OilCoolerId", OilCoolerId.Value<string>());
                    updateTwinData.AppendReplace("/OilCooler/EnergyConsumed", EnergyConsumed.Value<double>());
                    updateTwinData.AppendReplace("/OilCooler/NoiseLevel", NoiseLevel.Value<double>());
                    updateTwinData.AppendReplace("/OilCooler/Temperature", Temperature.Value<double>());
                    await client.UpdateDigitalTwinAsync(deviceId, updateTwinData);
                    // </Update_twin_with_device_temperature>
                }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}
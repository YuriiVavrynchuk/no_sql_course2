# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
"""
This sample demonstrates a simple recurring telemetry using an IoTHubSession

It's set to be used in the following MS Learn Tutorial:
https://learn.microsoft.com/en-us/azure/iot-develop/quickstart-send-telemetry-iot-hub?pivots=programming-language-python
"""

import csv
import asyncio
import os
from azure.iot.device import IoTHubSession, MQTTConnectionFailedError, Message

CONNECTION_STRING = ""


async def main():
    print("Starting telemetry sample")
    print("Press Ctrl-C to exit")
    try:
        print("Connecting to IoT Hub...")
        async with IoTHubSession.from_connection_string(CONNECTION_STRING) as session:
            print("Connected to IoT Hub")
            while True:
                print("Sending Message #{}...")
                with open("data.csv", 'r') as csv_file:
                    reader = csv.reader(csv_file)
                    header = next(reader, None)
                    result_dict = {}
                    for row in reader:
                        for key, value in zip(header, row):
                            result_dict[key] = value
                        temperature_msg1 = {
                            "datetime": "{}".format(result_dict['datetime']),
                            "volt": "{}".format(result_dict['volt']),
                            "rotate": "{}".format(result_dict['rotate']),
                            "pressure": "{}".format(result_dict['pressure']),
                            "vibration": "{}".format(result_dict['vibration']),
                        }
                        msg = Message(temperature_msg1)
                        await session.send_message(msg)
                        print("Send Complete")
                        await asyncio.sleep(10)

    except MQTTConnectionFailedError:
        # Connection failed to be established.
        print("Could not connect. Exiting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Exit application because user indicated they wish to exit.
        # This will have cancelled `main()` implicitly.
        print("User initiated exit. Exiting")
    finally:
        print("Sent n messages in total")
# Lighting-IOT

This project was done in March 2017.

This contains part of the project that is code snippets written for lighting device and RaspberryPi(gateway).
LightDevice_1.py is a file ececuted by light device for registeration when its newly added in the network.
LightSpecification_1.py is run by RaspberryPi(gateway) to write down the details of the light and its status
in its database.
GWtoCloudJsonTest.py is run by RaspberryPi to send the light status details to the cloud.

The file format used for traserring data is JSON, publishing and subscribing data is done through mosquitto mqtt broker.

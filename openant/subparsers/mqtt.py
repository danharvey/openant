import os
import uuid
from typing import List, Optional
import time

from ..easy.node import Node

from ..devices import device_profiles, ANTPLUS_NETWORK_KEY
from ..devices.utilities import auto_create_device, read_json
from ..devices.common import DeviceType, DeviceData, AntPlusDevice
from ..devices.fitness_equipment import FitnessEquipment, Workout

from ha_mqtt_discoverable import Settings, DeviceInfo
from ha_mqtt_discoverable.sensors import Sensor, SensorInfo

# def write_device_data_influx(
#     data: DeviceData,
#     client: idb.InfluxDBClient,
#     bucket: str,
#     wuuid: str,
#     device_name: str,
#     verbose=True,
# ):
#     host = os.uname().nodename

#     if not data:
#         raise ValueError("Device has no data")

#     try:
#         influx_tags = {
#             "device": device_name,
#             "uuid": wuuid,
#             "host": host,
#         }

#         json = data.to_influx_json(tags=influx_tags)

#         if verbose:
#             print(f"Writing: {json}")

#         point = point = idb.Point.from_dict(json)

#         with client.write_api(write_options=SYNCHRONOUS) as c:
#             c.write(bucket=bucket, record=point)

#     except Exception as e:
#         print(f"Exception during influx write: {e}")


# def write_all_device_data_influx(
#     device: AntPlusDevice,
#     client: idb.InfluxDBClient,
#     bucket: str,
#     wuuid: str,
#     verbose=True,
# ):
#     host = os.uname().nodename

#     try:
#         influx_tags = {
#             "device": str(device),
#             "serial_no": device.data["common"].serial_no,
#             "uuid": wuuid,
#             "host": host,
#         }

#         # drop common since it's just const meta
#         data = device.data.copy()
#         data.pop("common")

#         json = [v.to_influx_json(tags=influx_tags) for v in data.values()]

#         if verbose:
#             print(f"Writing: {json}")

#         with client.write_api(write_options=SYNCHRONOUS) as c:
#             for j in json:
#                 point = point = idb.Point.from_dict(j)
#                 c.write(bucket=bucket, record=point)

#     except Exception as e:
#         print(f"Exception during influx write: {e}")


def device_data_mqtt_importer(
    node: Node,
    client: Settings.MQTT,
    bucket: str,
    devices: List[AntPlusDevice],
    session_uuid: str = str(uuid.uuid4()),
    verbose: bool = False,
    workouts: Optional[List[Workout]] = None,
):
    print(f"Starting device data importer UUID {session_uuid} for {devices}")

    device_to_entity = {}

    # method with args saved to pass to device page update callback - could pass args list with callback instead if this gets messsy...
    def write_device_data(device, page_name, data):
        print(f"Device {device} broadcast {page_name} data: {data}")
        # Different pages have different data set
        # Page names
        # Data Page # (1)
        # if page_name == 'speed_system':
        #     #pass
    
        # Data Page # (2/34)
        if page_name == 'speed_distance':
            device_to_entity[str(device)]["odometer"].set_state(data.odometer)

        # Data Page # (3)
        if page_name == 'system_speed_2':
            device_to_entity[str(device)]["battery_soc"].set_state(data.battery_soc)
        
        # Data Page # (4)
        if page_name == 'battery':
            device_to_entity[str(device)]["battery_cycles"].set_state(data.battery_cycles)
            device_to_entity[str(device)]["battery_voltage"].set_state(data.battery_voltage)
        
        # Data Page # (5)
        # if page_name == 'capabilities':
        #     #pass

    def on_found(device):
        print(f"Device {device} found and receiving")

        device_info = DeviceInfo(name=str(device), identifiers=str(device))
        device_to_entity[str(device)] = {}

        # Battery SOC
        battery_soc_info = SensorInfo(
            name="Battery Capacity",
            device_class="battery",
            unit_of_measurement="%",
            unique_id="{0}_battery_soc".format(device),
            device=device_info
        )
        battery_soc_settings = Settings(mqtt=client, entity=battery_soc_info)
        device_to_entity[str(device)]["battery_soc"] = Sensor(battery_soc_settings)

        # Battery Cycles
        battery_cycles_info = SensorInfo(
            name="Battery Cycles",
            unique_id="{0}_battery_cycles".format(device),
            device=device_info
        )
        battery_cycles_settings = Settings(mqtt=client, entity=battery_cycles_info)
        device_to_entity[str(device)]["battery_cycles"] = Sensor(battery_cycles_settings)

        # Odometer
        odometer_info = SensorInfo(
            name="Odometer",
            unique_id="{0}_odometer".format(device),
            device=device_info,
            device_class="distance"
        )
        odometer_settings = Settings(mqtt=client, entity=odometer_info)
        device_to_entity[str(device)]["odometer"] = Sensor(odometer_settings)

        # Battery Voltage
        battery_voltage_info = SensorInfo(
            name="Battery Voltage",
            unique_id="{0}_battery_voltage".format(device),
            device=device_info,
            device_class="voltage"
        )
        battery_voltage_settings = Settings(mqtt=client, entity=battery_voltage_info)
        device_to_entity[str(device)]["battery_voltage"] = Sensor(battery_voltage_settings)
    
    for dev in devices:
        dev.on_found = lambda dev=dev: on_found(dev)
        dev.on_device_data = lambda page, page_name, data, dev=dev: write_device_data(
            dev, page_name, data
        )

    try:
        print(f"Starting {devices}, press Ctrl-C to finish")
        node.start()
    except KeyboardInterrupt:
        print(f"Closing ANT+ devices...")
    finally:
        for dev in devices:
            dev.close_channel()

        node.stop()
        #client.disconnect()


def _run(args):
    # Connect to MQTT
    mqtt_settings = Settings.MQTT(host="10.1.10.75",)

    if args.verbose:
        print(f"Connect to MQTT {mqtt_settings}")

    node = Node()
    node.set_network_key(0x00, ANTPLUS_NETWORK_KEY)

    devices = []
    workouts = None

    LAI = 37984
    DAN = 21751

    # devices.append(
    #     auto_create_device(
    #         node,
    #         device_id=args.id,
    #         device_type=args.device,
    #         trans_type=args.transtype,
    #     )
    # )     
    devices.append(
        auto_create_device(
            node,
            device_id=LAI,
            device_type=DeviceType.Lev,
            trans_type=args.transtype,
        )
    )
    devices.append(
        auto_create_device(
            node,
            device_id=DAN,
            device_type=DeviceType.Lev,
            trans_type=args.transtype,
        )
    )

    device_data_mqtt_importer(
        node=node,
        client=mqtt_settings,
        bucket=args.bucket,
        devices=devices,
        verbose=args.verbose,
        workouts=workouts,
    )


def add_subparser(subparsers, name="mqtt"):
    antmqtt = subparsers.add_parser(
        name,
        description=("Capture DeviceData from an ANT+ device and send to MQTT"),
    )
    antmqtt.add_argument(
        "device",
        choices=list(device_profiles.keys()).append("config"),
        help=f"Device {list(device_profiles.keys())} to use or 'config' for --config flag file",
    )
    antmqtt.add_argument(
        "--config",
        type=str,
        help=".json config file for use with 'config' type",
    )
    antmqtt.add_argument(
        "--host",
        default="...",
        type=str,
        help="Host for MQTT server",
    )
    antmqtt.add_argument(
        "--token",
        type=str,
        help="port of InfluxDB server",
    )
    antmqtt.add_argument(
        "--org",
        default="my-org",
        type=str,
        help="organisation to use on InfluxDB server",
    )
    antmqtt.add_argument(
        "--bucket",
        type=str,
        default="my-bucket",
        help="influxDB bucket to write to",
    )
    antmqtt.add_argument(
        "-I",
        "--id",
        type=int,
        default=0,
        help="Device ID, default zero will attach to first found",
    )
    antmqtt.add_argument(
        "-T",
        "--transtype",
        type=int,
        default=0,
        help="Transmission type, default zero will attach to first found",
    )
    antmqtt.add_argument(
        "-V", "--verbose", action="store_true", help="verbose output"
    )
    antmqtt.set_defaults(func=_run)

import datetime as dt
import os
import re
import signal
import threading
import uuid
from typing import List, Optional
import time

from ..easy.node import Node

from ..devices import device_profiles, ANTPLUS_NETWORK_KEY
from ..devices.utilities import auto_create_device, read_json
from ..devices.common import DeviceType, DeviceData, AntPlusDevice
from ..devices.fitness_equipment import FitnessEquipment, Workout
from ..devices.lev import TemperatureState

from ha_mqtt_discoverable import Settings, DeviceInfo
from ha_mqtt_discoverable.sensors import (
    BinarySensor,
    BinarySensorInfo,
    Sensor,
    SensorInfo,
)

# The bikes only broadcast over ANT+ while they are switched on or charging, so
# the last reading has to outlive both bridge and Home Assistant restarts.
LAST_SEEN_INTERVAL = 60  # seconds between "last seen" publishes per bike
HEARTBEAT_INTERVAL = 60  # seconds between "bridge online" re-assertions


def _temperature_state(state: TemperatureState) -> str:
    """TemperatureState.ColdWarm -> "cold_warm" (a Home Assistant enum option)."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", state.name).lower()


BATTERY_TEMPERATURE_STATES = [_temperature_state(s) for s in TemperatureState]


class RetainedSensor(Sensor):
    """Sensor whose state is retained on the broker.

    ha-mqtt-discoverable's Sensor.set_state() publishes with retain=False, so
    every Home Assistant restart blanks the sensor until the next broadcast.
    """

    def set_state(self, state, last_reset=None, force_update=True):
        self._update_state(
            str(state),
            last_reset=last_reset,
            retain=True,
            force_update=force_update,
        )


class BridgeStatus(BinarySensor):
    """Retained online/offline marker for the bridge itself.

    Uses manual availability, so the broker publishes "offline" via the
    last-will message if the process dies without disconnecting cleanly.
    """

    def announce(self):
        self._update_state(
            self._entity.payload_on, retain=True, force_update=True
        )
        return self.set_online(True)

    def set_online(self, online: bool):
        return self._update_state(
            "online" if online else "offline",
            topic=self.availability_topic,
            retain=True,
            force_update=True,
        )


def _raise_keyboard_interrupt(signum, frame):
    # Let systemd's SIGTERM take the same clean-shutdown path as Ctrl-C.
    raise KeyboardInterrupt

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
    last_seen_published = {}

    # Bridge-level online/offline entity (last-will -> "offline" if we die)
    status = BridgeStatus(
        Settings(
            mqtt=client,
            entity=BinarySensorInfo(
                name="Status",
                device_class="connectivity",
                unique_id="ant_bridge_status",
                device=DeviceInfo(name="ant_bridge", identifiers="ant_bridge"),
            ),
            manual_availability=True,
        )
    )
    status.announce()
    stop_heartbeat = threading.Event()

    def heartbeat():
        # Re-assert "online" so a stale last-will from a previous run (the
        # broker only fires it after the old keepalive expires) can't leave
        # the retained availability stuck on "offline".
        while not stop_heartbeat.wait(HEARTBEAT_INTERVAL):
            status.set_online(True)

    threading.Thread(target=heartbeat, name="bridge-heartbeat", daemon=True).start()

    # method with args saved to pass to device page update callback - could pass args list with callback instead if this gets messsy...
    def write_device_data(device, page_name, data):
        print(f"Device {device} broadcast {page_name} data: {data}")

        entities = device_to_entity.get(str(device))
        if entities is None:
            # Data arrived before on_found registered the entities
            return

        # Retained "last seen": lets Home Assistant tell a fresh reading from
        # a remembered one. Throttled - pages arrive every few seconds.
        now = time.monotonic()
        if now - last_seen_published.get(str(device), -LAST_SEEN_INTERVAL) >= LAST_SEEN_INTERVAL:
            entities["last_seen"].set_state(
                dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            )
            last_seen_published[str(device)] = now

        # Different pages have different data set
        # Page names
        # Data Page # (1)
        if page_name == 'speed_system':
            entities["battery_temperature"].set_state(
                _temperature_state(data.battery_temperature)
            )

        # Data Page # (2)
        # (page 34 arrives as 'alt_speed_distance' and has no remaining_range)
        if page_name == 'speed_distance':
            entities["odometer"].set_state(data.odometer)
            entities["remaining_range"].set_state(data.remaining_range)

        # Data Page # (3)
        if page_name == 'system_speed_2':
            entities["battery_soc"].set_state(data.battery_soc)

        # Data Page # (4)
        if page_name == 'battery':
            entities["battery_cycles"].set_state(data.battery_cycles)
            entities["battery_voltage"].set_state(data.battery_voltage)

        # Data Page # (5)
        # if page_name == 'capabilities':
        #     #pass

    def on_found(device):
        print(f"Device {device} found and receiving")

        if str(device) in device_to_entity:
            # Re-found after losing the channel: reuse the existing entities
            # (creating them again would open four more MQTT connections).
            return

        device_info = DeviceInfo(name=str(device), identifiers=str(device))
        entities = {}

        # Battery SOC
        # state_class makes Home Assistant keep long-term statistics (and chart
        # them) instead of only ~10 days of raw state history.

        battery_soc_info = SensorInfo(
            name="Battery Capacity",
            device_class="battery",
            unit_of_measurement="%",
            state_class="measurement",
            unique_id="{0}_battery_soc".format(device),
            device=device_info
        )
        battery_soc_settings = Settings(mqtt=client, entity=battery_soc_info)
        entities["battery_soc"] = RetainedSensor(battery_soc_settings)

        # Battery Cycles
        battery_cycles_info = SensorInfo(
            name="Battery Cycles",
            state_class="total_increasing",
            unique_id="{0}_battery_cycles".format(device),
            device=device_info
        )
        battery_cycles_settings = Settings(mqtt=client, entity=battery_cycles_info)
        entities["battery_cycles"] = RetainedSensor(battery_cycles_settings)

        # Odometer
        odometer_info = SensorInfo(
            name="Odometer",
            unique_id="{0}_odometer".format(device),
            device=device_info,
            device_class="distance",
            unit_of_measurement="km",
            state_class="total_increasing"
        )
        odometer_settings = Settings(mqtt=client, entity=odometer_info)
        entities["odometer"] = RetainedSensor(odometer_settings)

        # Battery Voltage
        battery_voltage_info = SensorInfo(
            name="Battery Voltage",
            unique_id="{0}_battery_voltage".format(device),
            device=device_info,
            device_class="voltage",
            unit_of_measurement="V",
            state_class="measurement"
        )
        battery_voltage_settings = Settings(mqtt=client, entity=battery_voltage_info)
        entities["battery_voltage"] = RetainedSensor(battery_voltage_settings)

        # Remaining range (the bike's own estimate)
        remaining_range_info = SensorInfo(
            name="Remaining Range",
            unique_id="{0}_remaining_range".format(device),
            device=device_info,
            device_class="distance",
            unit_of_measurement="km",
            state_class="measurement"
        )
        remaining_range_settings = Settings(mqtt=client, entity=remaining_range_info)
        entities["remaining_range"] = RetainedSensor(remaining_range_settings)

        # Battery temperature state (cold / cold_warm / warm / warm_hot / hot)
        battery_temperature_info = SensorInfo(
            name="Battery Temperature",
            unique_id="{0}_battery_temperature".format(device),
            device=device_info,
            device_class="enum",
            options=BATTERY_TEMPERATURE_STATES
        )
        battery_temperature_settings = Settings(mqtt=client, entity=battery_temperature_info)
        entities["battery_temperature"] = RetainedSensor(battery_temperature_settings)

        # Last seen (timestamp of the most recent broadcast from the bike)
        last_seen_info = SensorInfo(
            name="Last Seen",
            unique_id="{0}_last_seen".format(device),
            device=device_info,
            device_class="timestamp"
        )
        last_seen_settings = Settings(mqtt=client, entity=last_seen_info)
        entities["last_seen"] = RetainedSensor(last_seen_settings)

        # Publish only once complete so write_device_data never sees a half-built set
        device_to_entity[str(device)] = entities

    for dev in devices:
        dev.on_found = lambda dev=dev: on_found(dev)
        dev.on_device_data = lambda page, page_name, data, dev=dev: write_device_data(
            dev, page_name, data
        )

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    try:
        print(f"Starting {devices}, press Ctrl-C to finish")
        node.start()
    except KeyboardInterrupt:
        print(f"Closing ANT+ devices...")
    finally:
        stop_heartbeat.set()
        for dev in devices:
            dev.close_channel()

        node.stop()

        # Mark the bridge offline ourselves, then disconnect cleanly so the
        # broker doesn't also fire the last-will.
        try:
            info = status.set_online(False)
            if info is not None:
                info.wait_for_publish(timeout=3)
            status.mqtt_client.disconnect()
        except Exception as e:
            print(f"Could not mark bridge offline cleanly: {e}")


def _run(args):
    # Connect to MQTT
    mqtt_settings = Settings.MQTT(host=args.host)

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
        default="10.1.10.75",
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

"""MQTT client helper for publishing piano key events."""
from __future__ import annotations

import json
import threading
import time
from typing import Optional

from lib.log_setup import logger

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - handled gracefully if dependency is missing
    mqtt = None  # type: ignore


class MQTTClient:
    """Simple wrapper around paho-mqtt for publishing note events."""

    def __init__(self, usersettings):
        self.usersettings = usersettings
        self.enabled = str(self.usersettings.get_setting_value("mqtt_enabled") or "0") == "1"
        self._topic = self.usersettings.get_setting_value("mqtt_topic") or ""
        self._host = self.usersettings.get_setting_value("mqtt_host") or ""
        self._port = int(self.usersettings.get_setting_value("mqtt_port") or 1883)
        self._username = self.usersettings.get_setting_value("mqtt_username") or None
        self._password = self.usersettings.get_setting_value("mqtt_password") or None
        self._client_id = self.usersettings.get_setting_value("mqtt_client_id") or "piano-led-visualizer"
        self._keepalive = int(self.usersettings.get_setting_value("mqtt_keepalive") or 60)
        self._retain = str(self.usersettings.get_setting_value("mqtt_retain") or "0") == "1"
        self._qos = int(self.usersettings.get_setting_value("mqtt_qos") or 0)

        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._connect_lock = threading.Lock()

        if not self.enabled:
            logger.info("MQTT publishing disabled via settings")
            return

        if mqtt is None:
            logger.warning("paho-mqtt is not installed; disabling MQTT publishing")
            self.enabled = False
            return

        if not self._host or not self._topic:
            logger.warning("MQTT host or topic not configured; disabling MQTT publishing")
            self.enabled = False
            return

        try:
            self._client = mqtt.Client(client_id=self._client_id, clean_session=True)
            if self._username:
                self._client.username_pw_set(self._username, self._password)

            self._client.on_connect = self._handle_connect
            self._client.on_disconnect = self._handle_disconnect

            # Use async connection so we do not block startup.
            self._client.connect_async(self._host, self._port, self._keepalive)
            self._client.loop_start()
        except Exception as exc:  # pragma: no cover - safeguard
            logger.warning(f"Unable to initialize MQTT client: {exc}")
            self.enabled = False

    # region paho callbacks
    def _handle_connect(self, client, userdata, flags, rc):  # pragma: no cover - relies on paho callbacks
        if rc == 0:
            self._connected = True
            logger.info("Connected to MQTT broker")
        else:
            logger.warning(f"MQTT connection failed with result code {rc}")

    def _handle_disconnect(self, client, userdata, rc):  # pragma: no cover - relies on paho callbacks
        self._connected = False
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (rc={rc}). Reconnecting...")
            if self._client is not None:
                try:
                    self._client.reconnect()
                except Exception as exc:
                    logger.warning(f"Failed to reconnect to MQTT broker: {exc}")
        else:
            logger.info("MQTT client disconnected")
    # endregion

    def publish_note_event(self, note: int, velocity: int, event_type: str, channel: Optional[int] = None):
        """Publish a MIDI note event to the configured MQTT topic."""
        if not self.enabled or self._client is None:
            return

        if not self._connected:
            # Attempt a reconnect without blocking the main loop.
            with self._connect_lock:
                if not self._connected:
                    try:
                        self._client.reconnect()
                    except Exception:
                        return

        payload = {
            "event": event_type,
            "note": note,
            "velocity": velocity,
            "channel": channel,
            "timestamp": time.time(),
        }

        try:
            self._client.publish(self._topic, json.dumps(payload), qos=self._qos, retain=self._retain)
        except Exception as exc:
            logger.warning(f"Failed to publish MQTT message: {exc}")

    def shutdown(self):  # pragma: no cover - used during teardown
        if self._client is None:
            return
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as exc:
            logger.warning(f"Error while shutting down MQTT client: {exc}")

"""
Real-time pub/sub module for WebSocket events.

Provides a broker abstraction that uses Redis when available,
falling back to in-memory pub/sub for local development.
"""

from api.realtime.broker import Broker, get_broker
from api.realtime.events import Event, EventType

__all__ = ["get_broker", "Broker", "Event", "EventType"]

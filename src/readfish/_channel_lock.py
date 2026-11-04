"""Lease-based MinKNOW channel state locking for accepted reads.

When readfish accepts a read with ``Action.lock`` the channel is locked to a
custom MinKNOW channel state. Every new chunk from that read renews a lease of
``LEASE_SECONDS``. The channel is unlocked when a different read appears on the
channel, when the lease expires without a new chunk, or when the read has held
the lock for longer than ``MAX_HOLD_SECONDS``.

While a read holds a lock its chunks bypass the strand prefilter
(``client.protected_reads``), so the lease keeps renewing even if MinKNOW
classifies the signal as ``unavailable``. Genuine blocks are therefore only
released by ``MAX_HOLD_SECONDS``, or by MinKNOW starting a new read.

All lock and unlock calls are made from the readfish main loop thread, so
MinKNOW never receives concurrent lock/unlock requests.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import attrs
import grpc

from readfish._loggers import setup_logger
from readfish.plugins.utils import Action

if TYPE_CHECKING:
    from readfish._config import Conf
    from readfish._read_until_client import RUClient

#: Channel state applied while a channel holds an accepted read.
#: Must exist in MinKNOW's channel states configuration.
LOCK_STATE_NAME = "locked_target"
#: Seconds a lock survives without a new chunk from the read holding it (T).
LEASE_SECONDS = 2.0
#: Maximum seconds a single read may hold a lock.
MAX_HOLD_SECONDS = 300.0
#: Timeout, in seconds, for each lock or unlock RPC.
RPC_TIMEOUT = 1.0
#: Minimum max_unblock_read_length_seconds when locking is used
LOCK_STREAM_SECONDS = 600.0
#: Columns of the lock event TSV.
EVENT_HEADER = "timestamp\tevent\tchannel\tread_id\theld_s\tgap_s\tclassifications"


def conf_uses_lock(conf: Conf) -> bool:
    """Return True if any region or barcode maps a decision to ``Action.lock``."""
    conditions = [*conf.regions, *conf.barcodes.values()]
    return any(
        Action.lock in attrs.asdict(c, recurse=False).values() or bool(c.lock_targets.value)
        for c in conditions
    )


@attrs.define
class _Lease:
    read_id: str
    expiry: float
    locked_at: float
    #: When the last chunk from this read was seen, for renewal gap logging
    last_seen: float


class ChannelLocker:
    """Track and apply channel state locks for reads accepted with ``Action.lock``.

    :param client: The readfish Read Until client, used for its MinKNOW
        connection and to send ``stop_receiving`` for released reads.
    :param logger: Logger for lock events and RPC failures.
    """

    def __init__(
        self,
        client: RUClient,
        logger: logging.Logger,
        state_name: str = LOCK_STATE_NAME,
        lease_seconds: float = LEASE_SECONDS,
        max_hold_seconds: float = MAX_HOLD_SECONDS,
        event_log: str | None = None,
    ):
        self.client = client
        self.logger = logger
        self.state_name = state_name
        self.lease_seconds = lease_seconds
        self.max_hold_seconds = max_hold_seconds
        # Channels MinKNOW has locked, and the read holding each lock
        self.held: dict[int, _Lease] = {}
        # channel -> read_id, waiting for a lock call
        self.to_lock: dict[int, str] = {}
        # Channels waiting for an unlock call
        self.to_unlock: set[int] = set()
        # TSV of lock events, one line per lock/renew/unlock. None disables it.
        self.events = (
            setup_logger("readfish_channel_locks", header=EVENT_HEADER, log_file=event_log)
            if event_log
            else None
        )
        # Read classification id -> name, built on first use
        self._class_names: dict[int, str] | None = None

    def validate(self) -> None:
        """Check that the lock state exists in MinKNOW's channel states."""
        desc = self.client.connection.analysis_configuration.get_channel_states_desc()
        names = {state.name for group in desc.groups for state in group.states}
        if self.state_name == "unclassified" or self.state_name not in names:
            raise ValueError(
                f"Channel state {self.state_name!r} is not defined in the MinKNOW "
                "channel states configuration, so channels cannot be locked to it."
            )

    def filter_chunks(self, chunks: list) -> list:
        """Renew leases for lock-holding reads and drop them from the batch.

        Also schedules an unlock for any locked channel on which a different
        read has appeared, as the read holding the lock has ended.

        :param chunks: ``(channel, ReadData)`` tuples from ``get_read_chunks``.
        :return: The chunks that still need basecalling and a decision.
        """
        now = time.monotonic()
        keep = []
        for channel, read in chunks:
            lease = self.held.get(channel)
            if lease is not None and channel not in self.to_unlock:
                if lease.read_id == read.id:
                    self._event(
                        "renew", channel, lease, now,
                        gap=now - lease.last_seen,
                        classes=read.chunk_classifications,
                    )
                    lease.expiry = now + self.lease_seconds
                    lease.last_seen = now
                    continue
                self._event("unlock_new_read", channel, lease, now)
                self._end_lease(channel)
            # Drop a pending lock request whose read has already ended
            if self.to_lock.get(channel, read.id) != read.id:
                del self.to_lock[channel]
            keep.append((channel, read))
        return keep

    def request(self, channel: int, read_id: str) -> None:
        """Ask for ``channel`` to be locked for ``read_id`` on the next flush."""
        self.to_lock[channel] = read_id

    def flush(self) -> None:
        """Expire leases, then send one batched unlock and one batched lock call."""
        now = time.monotonic()
        released = []
        for channel, lease in self.held.items():
            if channel in self.to_unlock:
                continue
            if (
                now > lease.expiry
                or now - lease.locked_at > self.max_hold_seconds
            ):
                capped = now - lease.locked_at > self.max_hold_seconds
                self._event(
                    "cap" if capped else "expire", channel, lease, now,
                    gap=now - lease.last_seen,
                )
                self._end_lease(channel)
                released.append((channel, lease.read_id))
        # Released reads are no longer protected. Stop receiving their data so
        #   readfish never evaluates (and possibly unblocks) them again.
        if released:
            self.client.stop_receiving_batch(released)

        unlock = sorted(self.to_unlock - self.to_lock.keys())
        if unlock and self._call(
            self.client.connection.data.unlock_channel_states, channels=unlock
        ):
            for channel in unlock:
                self.held.pop(channel, None)
            self.to_unlock.difference_update(unlock)

        if self.to_lock:
            channels = sorted(self.to_lock)
            if self._call(
                self.client.connection.data.lock_channel_states,
                channels=channels,
                state_name=self.state_name,
            ):
                now = time.monotonic()
                for channel in channels:
                    read_id = self.to_lock[channel]
                    self.held[channel] = _Lease(
                        read_id, now + self.lease_seconds, now, now
                    )
                    self._event("lock", channel, self.held[channel], now)
                    # A new lock on the channel replaces any pending unlock
                    self.to_unlock.discard(channel)
                    # Let this read's chunks through the strand prefilter
                    self.client.protected_reads[channel] = read_id
                self.to_lock.clear()

    def release_all(self) -> None:
        """Unlock every channel readfish has locked. Called on shutdown."""
        channels = sorted(self.held.keys() | self.to_unlock)
        now = time.monotonic()
        for channel, lease in self.held.items():
            self._event("release", channel, lease, now)
        if channels:
            # Acquisition may already have ended, in which case MinKNOW drops
            #   the locks itself and this call fails harmlessly.
            self._call(
                self.client.connection.data.unlock_channel_states, channels=channels
            )
        self.client.protected_reads.clear()
        self.held.clear()
        self.to_lock.clear()
        self.to_unlock.clear()

    def _end_lease(self, channel: int) -> None:
        """Stop protecting the read on ``channel`` and queue the channel for unlock."""
        self.client.protected_reads.pop(channel, None)
        self.to_unlock.add(channel)

    def _event(
        self,
        event: str,
        channel: int,
        lease: _Lease,
        now: float,
        gap: float | None = None,
        classes=(),
    ) -> None:
        """Write one line to the lock event TSV, if enabled.

        ``held_s`` is time since the lock went on; ``gap_s`` is time since the
        previous chunk from the read holding the lock.
        """
        if self.events is None:
            return
        if self._class_names is None:
            lookup = getattr(self.client, "lookup_read_class", {}) or {}
            self._class_names = {v: k for k, v in lookup.items()}
        names = ",".join(self._class_names.get(c, str(c)) for c in classes)
        self.events.debug(
            f"{time.time():.3f}\t{event}\t{channel}\t{lease.read_id}\t"
            f"{now - lease.locked_at:.3f}\t"
            f"{'' if gap is None else f'{gap:.3f}'}\t{names}"
        )

    def _call(self, rpc, **kwargs) -> bool:
        try:
            rpc(_timeout=RPC_TIMEOUT, **kwargs)
            return True
        except grpc.RpcError as e:
            self.logger.warning(
                f"Channel state RPC failed: {e.code().name} {e.details()}"
            )
            return False
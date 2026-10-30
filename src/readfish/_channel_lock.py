"""Lease-based MinKNOW channel state locking for accepted reads.

When readfish accepts a read with ``Action.lock`` the channel is locked to a
custom MinKNOW channel state. Every new chunk from that read renews a lease of
``LEASE_SECONDS``. The channel is unlocked when a different read appears on the
channel, when the lease expires without a new chunk, or when the read has held
the lock for longer than ``MAX_HOLD_SECONDS``.

All lock and unlock calls are made from the readfish main loop thread, so
MinKNOW never receives concurrent lock/unlock requests.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import attrs
import grpc

from readfish.plugins.utils import Action

if TYPE_CHECKING:
    from readfish._config import Conf
    from readfish._read_until_client import RUClient

#: Channel state applied while a channel holds an accepted read.
#: Must exist in MinKNOW's channel states configuration.
#: Currently implemented in ag_sequencing_MIN004RA_RNA.toml in /data/userscripts
LOCK_STATE_NAME = "locked_target"
#: Seconds a lock survives without a new chunk from the read holding it (T).
LEASE_SECONDS = 2.0
#: For real blocks - maximum seconds a single read may hold a lock.
MAX_HOLD_SECONDS = 300.0
#: Timeout, in seconds, for each lock or unlock RPC (ie., before treating it as failed)
RPC_TIMEOUT = 1.0


def conf_uses_lock(conf: Conf) -> bool:
    """Return True if any region or barcode maps a decision to ``Action.lock``."""
    conditions = [*conf.regions, *conf.barcodes.values()]
    return any(
        Action.lock in attrs.asdict(c, recurse=False).values() for c in conditions
    )


@attrs.define
class _Lease:
    read_id: str
    expiry: float
    locked_at: float


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
                    lease.expiry = now + self.lease_seconds
                    continue
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
                    self.held[channel] = _Lease(read_id, now + self.lease_seconds, now)
                    # A new lock on the channel replaces any pending unlock
                    self.to_unlock.discard(channel)
                    self.client.protected_reads[channel] = read_id
                self.to_lock.clear()



    def release_all(self) -> None:
        """Unlock every channel readfish has locked. Called on shutdown."""
        channels = sorted(self.held.keys() | self.to_unlock)
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

    def _call(self, rpc, **kwargs) -> bool:
        try:
            rpc(_timeout=RPC_TIMEOUT, **kwargs)
            return True
        except grpc.RpcError as e:
            self.logger.warning(
                f"Channel state RPC failed: {e.code().name} {e.details()}"
            )
            return False
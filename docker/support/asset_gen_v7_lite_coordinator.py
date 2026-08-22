#!/usr/bin/env python3
"""Fail-closed, process-local GPU residency coordinator for asset_gen_v7_lite.

The HTTP surface is provided by asset_gen_v7_lite_gateway.py.  This module owns
lease fencing, deterministic workload transitions, llama process residency and
the on-disk llama slot cache.  It intentionally uses only the Python standard
library so it can run in the Vast base image before optional dependencies load.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path


HOLDERS = {"inference", "comfy", "mining"}
FOREGROUND = {"inference", "comfy"}
CACHE_HANDLE_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}\.[a-f0-9]{64}$")


class CoordinatorError(RuntimeError):
    code = "coordinator_error"


class LeaseConflict(CoordinatorError):
    code = "gpu_busy"

    def __init__(self, message, retry_after=5):
        super().__init__(message)
        self.retry_after = max(1, int(retry_after))


class StaleLease(CoordinatorError):
    code = "stale_gpu_lease"


class SnapshotStore:
    """Filesystem backend for atomic, compatibility-scoped llama slot files."""

    def __init__(
        self,
        root,
        fingerprint,
        ttl_seconds=48 * 3600,
        quota_bytes=48 * 1024**3,
        min_free_bytes=20 * 1024**3,
        max_entry_bytes=16 * 1024**3,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self.fingerprint = str(fingerprint)
        self.ttl_seconds = int(ttl_seconds)
        self.quota_bytes = int(quota_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.max_entry_bytes = int(max_entry_bytes)
        self.lock = threading.RLock()
        self._recover_filesystem()

    def _recover_filesystem(self):
        """Discard crash remnants; only manifest-backed snapshots are valid."""
        with self.lock:
            for temporary in self.root.glob(".*.tmp"):
                temporary.unlink(missing_ok=True)
            for slot_path in self.root.glob("*.slot"):
                if not slot_path.with_suffix(".json").is_file():
                    slot_path.unlink(missing_ok=True)
            for manifest_path in self.root.glob("*.json"):
                manifest = self._read_manifest(manifest_path)
                slot_path = self.root / str((manifest or {}).get("filename", ""))
                if not manifest or not slot_path.is_file():
                    manifest_path.unlink(missing_ok=True)

    def _paths(self, handle):
        if not CACHE_HANDLE_RE.fullmatch(handle or ""):
            raise ValueError("invalid opaque prompt cache handle")
        return self.root / f"{handle}.slot", self.root / f"{handle}.json"

    def _read_manifest(self, path):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else None
        except (OSError, ValueError):
            return None

    def entries(self):
        result = []
        with self.lock:
            for manifest_path in self.root.glob("*.json"):
                manifest = self._read_manifest(manifest_path)
                if not manifest:
                    continue
                slot_path = self.root / str(manifest.get("filename", ""))
                if not slot_path.is_file():
                    continue
                manifest = dict(manifest)
                manifest["path"] = str(slot_path)
                result.append(manifest)
        return result

    # Stable backend surface. A future llama implementation can replace the
    # storage mechanics without changing coordinator scheduling or callers.
    def list(self):
        return self.entries()

    def capacity(self, required_bytes=0):
        required_bytes = max(0, int(required_bytes))
        with self.lock:
            self.prune(required_bytes=required_bytes)
            entries = self.entries()
            committed_bytes = sum(int(item.get("bytes", 0)) for item in entries)
            temporary_bytes = 0
            for path in self.root.glob(".*.tmp"):
                try:
                    temporary_bytes += path.stat().st_size
                except OSError:
                    pass
            used_bytes = committed_bytes + temporary_bytes
            free_bytes = shutil.disk_usage(self.root).free
            can_write = (
                0 < required_bytes <= self.max_entry_bytes
                and used_bytes + required_bytes <= self.quota_bytes
                and free_bytes - required_bytes >= self.min_free_bytes
            )
            return {
                "canWrite": can_write,
                "requiredBytes": required_bytes,
                "usedBytes": used_bytes,
                "freeBytes": free_bytes,
                "quotaBytes": self.quota_bytes,
                "minFreeBytes": self.min_free_bytes,
                "maxEntryBytes": self.max_entry_bytes,
            }

    def peek(self, handle, now=None):
        now = time.time() if now is None else float(now)
        slot_path, manifest_path = self._paths(handle)
        with self.lock:
            manifest = self._read_manifest(manifest_path)
            if not manifest or not slot_path.is_file():
                return None
            if manifest.get("fingerprint") != self.fingerprint:
                self.delete(handle)
                return None
            if now - float(manifest.get("updatedAt", 0)) > self.ttl_seconds:
                self.delete(handle)
                return None
            try:
                actual_size = slot_path.stat().st_size
            except OSError:
                return None
            if actual_size != int(manifest.get("bytes", -1)) or actual_size > self.max_entry_bytes:
                self.delete(handle)
                return None
            return dict(manifest, path=str(slot_path))

    def get(self, handle, now=None):
        now = time.time() if now is None else float(now)
        validation_started = time.monotonic()
        entry = self.peek(handle, now=now)
        if not entry:
            return None
        slot_path, manifest_path = self._paths(handle)
        expected_sha256 = str(entry.get("sha256") or "")
        try:
            initial_stat = slot_path.stat()
        except OSError:
            return None
        digest = hashlib.sha256()
        try:
            # Integrity validation is intentionally outside the store lock so
            # a multi-GiB read never blocks capacity/status operations.
            with slot_path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return None
        with self.lock:
            current = self._read_manifest(manifest_path)
            try:
                current_stat = slot_path.stat()
            except OSError:
                return None
            stable_file = (
                current_stat.st_dev == initial_stat.st_dev
                and current_stat.st_ino == initial_stat.st_ino
                and current_stat.st_size == initial_stat.st_size
                and current_stat.st_mtime_ns == initial_stat.st_mtime_ns
            )
            if not current or not stable_file or str(current.get("sha256") or "") != expected_sha256:
                return None
            if not expected_sha256 or digest.hexdigest() != expected_sha256:
                self.delete(handle)
                return None
            manifest = dict(current)
            manifest["lastUsedAt"] = now
            manifest["validationMs"] = round((time.monotonic() - validation_started) * 1000)
            manifest.pop("path", None)
            self._write_manifest_atomic(manifest_path, manifest)
            return dict(manifest, path=str(slot_path))

    def temporary_filename(self, handle):
        self._paths(handle)
        return f".{handle}.{uuid.uuid4().hex}.tmp"

    def commit(self, handle, temporary_filename, metadata):
        slot_path, manifest_path = self._paths(handle)
        temporary_path = self.root / temporary_filename
        if temporary_path.parent != self.root or not temporary_path.is_file():
            raise ValueError("snapshot temporary file is missing or outside cache root")
        initial_stat = temporary_path.stat()
        size = initial_stat.st_size
        if size <= 0 or size > self.max_entry_bytes:
            temporary_path.unlink(missing_ok=True)
            raise ValueError("snapshot exceeds entry bounds")
        # Hashing a multi-GiB slot is deliberately outside the store lock.
        # Revalidate the inode before the atomic rename so a concurrent
        # capacity/status operation is never held behind sequential disk I/O.
        digest = hashlib.sha256()
        with temporary_path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
            os.fsync(stream.fileno())
        with self.lock:
            current_stat = temporary_path.stat()
            stable_file = (
                current_stat.st_dev == initial_stat.st_dev
                and current_stat.st_ino == initial_stat.st_ino
                and current_stat.st_size == initial_stat.st_size
                and current_stat.st_mtime_ns == initial_stat.st_mtime_ns
            )
            if not stable_file:
                temporary_path.unlink(missing_ok=True)
                raise OSError("snapshot temporary file changed during validation")
            self.prune(required_bytes=size)
            free = shutil.disk_usage(self.root).free
            # The same-filesystem llama temporary already consumes `size`;
            # rename is allocation-neutral. Preflight capacity reserved the
            # bytes before slot save began, so only enforce the live floor here.
            if free < self.min_free_bytes:
                temporary_path.unlink(missing_ok=True)
                raise OSError("snapshot would violate free disk floor")
            os.replace(temporary_path, slot_path)
            os.chmod(slot_path, 0o600)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            now = time.time()
            manifest = {
                **metadata,
                "handle": handle,
                "filename": slot_path.name,
                "fingerprint": self.fingerprint,
                "bytes": size,
                "sha256": digest.hexdigest(),
                "updatedAt": now,
                "lastUsedAt": now,
            }
            self._write_manifest_atomic(manifest_path, manifest)
            self.prune()
            return manifest

    def put(self, handle, temporary_filename, metadata):
        return self.commit(handle, temporary_filename, metadata)

    def _write_manifest_atomic(self, path, manifest):
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(manifest, stream, separators=(",", ":"), sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

    def delete(self, handle):
        slot_path, manifest_path = self._paths(handle)
        with self.lock:
            slot_path.unlink(missing_ok=True)
            manifest_path.unlink(missing_ok=True)

    def record_restore_failure(self, handle, now=None):
        now = time.time() if now is None else float(now)
        _slot_path, manifest_path = self._paths(handle)
        with self.lock:
            manifest = self._read_manifest(manifest_path)
            if not manifest:
                return
            failures = int(manifest.get("restoreFailureCount", 0)) + 1
            manifest["restoreFailureCount"] = failures
            manifest["restoreFailureAt"] = now
            manifest["restoreBackoffUntil"] = now + min(15 * 60, 30 * (2 ** min(failures - 1, 5)))
            self._write_manifest_atomic(manifest_path, manifest)

    def prune(self, required_bytes=0, now=None):
        now = time.time() if now is None else float(now)
        with self.lock:
            entries = self.entries()
            for entry in entries:
                if now - float(entry.get("updatedAt", 0)) > self.ttl_seconds:
                    self.delete(str(entry["handle"]))
            entries = self.entries()
            used = sum(int(item.get("bytes", 0)) for item in entries)
            target = max(0, self.quota_bytes - int(required_bytes))
            if used <= target:
                return
            # Lowest latency benefit per byte is evicted first.
            def score(item):
                age = max(0.0, now - float(item.get("lastUsedAt", item.get("updatedAt", now))))
                recency = 0.5 ** (age / max(1, self.ttl_seconds))
                benefit = float(item.get("reuseProbability", 0.3)) * float(item.get("savedPrefillMs", 0))
                return benefit * recency / max(1, int(item.get("bytes", 1)))

            for entry in sorted(entries, key=score):
                self.delete(str(entry["handle"]))
                used -= int(entry.get("bytes", 0))
                if used <= target:
                    break

    def status(self):
        entries = self.entries()
        occupied_tokens = sum(max(0, int(item.get("promptTokens", 0))) for item in entries)
        cache_bytes = sum(int(item.get("bytes", 0)) for item in entries)
        return {
            "entries": len(entries),
            "bytes": cache_bytes,
            "bytesPerOccupiedToken": round(cache_bytes / occupied_tokens, 2) if occupied_tokens else None,
            "quotaBytes": self.quota_bytes,
            "minFreeBytes": self.min_free_bytes,
            "freeBytes": shutil.disk_usage(self.root).free,
            "ttlSeconds": self.ttl_seconds,
        }


class GPUCoordinator:
    """Fenced workload leases and deterministic local GPU transitions."""

    def __init__(
        self,
        llama_base_url,
        comfy_base_url,
        http_request,
        snapshot_store,
        llama_pid_file=None,
        llama_log_file=None,
        llama_command_file=None,
        state_file=None,
        epoch_file=None,
        enabled=True,
        enforce_transitions=True,
        warm_residency=True,
        snapshot_write=True,
        snapshot_restore=True,
        mining_grace_seconds=30,
        comfy_release_vram_max_bytes=3 * 1024**3,
        comfy_idle_baseline_bytes=None,
        comfy_release_vram_headroom_bytes=512 * 1024**2,
    ):
        self.llama_base_url = llama_base_url.rstrip("/")
        self.comfy_base_url = comfy_base_url.rstrip("/")
        self.http_request = http_request
        self.snapshot_store = snapshot_store
        self.llama_pid_file = Path(llama_pid_file) if llama_pid_file else None
        self.llama_log_file = Path(llama_log_file) if llama_log_file else None
        self.llama_command_file = Path(llama_command_file) if llama_command_file else None
        self.state_file = Path(state_file) if state_file else None
        self.epoch_file = Path(epoch_file) if epoch_file else (self.state_file.with_suffix(".epoch") if self.state_file else None)
        self.enabled = bool(enabled)
        self.enforce_transitions = bool(enforce_transitions)
        self.warm_residency = bool(warm_residency)
        self.snapshot_write = bool(snapshot_write)
        self.snapshot_restore = bool(snapshot_restore)
        self.mining_grace_seconds = max(30, int(mining_grace_seconds))
        self.comfy_release_vram_max_bytes = max(0, int(comfy_release_vram_max_bytes))
        self.comfy_idle_baseline_bytes = (
            max(0, int(comfy_idle_baseline_bytes))
            if comfy_idle_baseline_bytes is not None else None
        )
        self.comfy_release_vram_headroom_bytes = max(0, int(comfy_release_vram_headroom_bytes))
        self.last_transition_error = None
        self.last_comfy_baseline_adjustment = None
        self.mining_stop_durations_ms = []
        self.lock = threading.RLock()
        self.lease_condition = threading.Condition(self.lock)
        previous_journal = self._read_journal()
        previous_epoch = max(int((previous_journal or {}).get("epoch", 0)), self._read_persisted_epoch())
        self.epoch = max(int(time.time_ns()), previous_epoch + 1)
        self.state = "RECOVERING"
        self.phase = "RECOVERING"
        self.lease = None
        previous_receipts = (previous_journal or {}).get("completionReceipts")
        self.completion_receipts = dict(previous_receipts) if isinstance(previous_receipts, dict) else {}
        expected_llama_start = ((previous_journal or {}).get("llama") or {}).get("processStartTime")
        self.llama_pid = self._read_pid(expected_start_time=expected_llama_start)
        self.llama_process = None
        self.llama_start_time = self._process_start_time(self.llama_pid)
        self.llama_argv = self._read_argv(self.llama_pid) or self._read_llama_command()
        if self.llama_pid and self.llama_argv:
            self._persist_llama_command(self.llama_argv)
        self.current_cache_handle = None
        self.current_cache_metadata = {}
        self.cache_dirty = False
        self.cache_generation = 0
        self.snapshot_timer = None
        self.recovery_timer = None
        self.draining = False
        self.mining_not_before_ms = int((previous_journal or {}).get("miningNotBeforeMs", 0))
        self.metrics = {
            "leaseConflicts": 0,
            "staleTokenRejections": 0,
            "snapshotSaves": 0,
            "snapshotSaveErrors": 0,
            "snapshotRestores": 0,
            "snapshotRestoreErrors": 0,
            "snapshotRestoreIneffective": 0,
            "snapshotSkippedDirtyEviction": 0,
            "snapshotSkippedKeySwitch": 0,
            "llamaStarts": 0,
            "llamaStops": 0,
            "comfyEvictions": 0,
            "miningRevocations": 0,
            "recoveryCount": 0,
        }
        self.state = "IDLE"
        self.phase = "IDLE"
        if self.llama_running():
            self.state = "WARM"
            self.phase = "WARM"
        self._reconcile_previous_journal(previous_journal)
        self._persist_journal()

    def _read_journal(self):
        if not self.state_file:
            return None
        try:
            value = json.loads(self.state_file.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else None
        except (OSError, ValueError):
            return None

    def _read_persisted_epoch(self):
        if not self.epoch_file:
            return 0
        try:
            return int(self.epoch_file.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return 0

    def _read_llama_command(self):
        if not self.llama_command_file:
            return None
        try:
            argv = json.loads(self.llama_command_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and "\x00" not in item for item in argv):
            return None
        if "llama-server" not in Path(argv[0]).name:
            return None
        return argv

    def _persist_llama_command(self, argv):
        if not self.llama_command_file:
            return
        self.llama_command_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self.llama_command_file.with_name(f".{self.llama_command_file.name}.{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(argv, stream, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.llama_command_file)
        finally:
            temporary.unlink(missing_ok=True)

    def _persist_journal(self):
        if not self.state_file:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.epoch_file:
            epoch_temporary = self.epoch_file.with_name(f".{self.epoch_file.name}.{uuid.uuid4().hex}.tmp")
            epoch_fd = os.open(epoch_temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(epoch_fd, "w", encoding="utf-8") as stream:
                    stream.write(str(self.epoch) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(epoch_temporary, self.epoch_file)
            finally:
                epoch_temporary.unlink(missing_ok=True)
        value = {
            "epoch": self.epoch,
            "state": self.state,
            "phase": self.phase,
            "updatedAt": time.time(),
            "lease": self.lease,
            "llama": {
                "pid": self.llama_pid,
                "processStartTime": self._process_start_time(self.llama_pid),
            },
            "miningNotBeforeMs": self.mining_not_before_ms,
            "completionReceipts": self.completion_receipts,
        }
        temporary = self.state_file.with_name(f".{self.state_file.name}.{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, separators=(",", ":"), sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.state_file)
            directory_fd = os.open(self.state_file.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

    def _reconcile_previous_journal(self, journal):
        previous = (journal or {}).get("lease")
        if not isinstance(previous, dict):
            return
        holder = previous.get("holder")
        if not self.enforce_transitions:
            # Shadow/legacy can fence stale telemetry, but must never signal a
            # workload or alter rollout behavior.
            return
        self.metrics["recoveryCount"] += 1
        if holder == "mining":
            # Mining is revocable and is never adopted across epochs,
            # including a crash during its STARTING registration window.
            self._stop_mining(previous.get("metadata", {}))
            return
        if holder not in FOREGROUND:
            return
        previous_state = str(previous.get("state") or "").upper()
        if previous_state == "WARM":
            # There is no active client to reattach and cache-key residency is
            # deliberately not journaled. Evict the known warm owner so the
            # first post-restart key cannot inherit an unscoped KV slot.
            if holder == "inference":
                self._stop_llama()
            else:
                self._free_comfy(preserve_cache=False)
            self.state = "IDLE"
            self.phase = "IDLE"
            return
        if previous_state not in {"ACTIVE", "RECOVERING"}:
            return
        # Fence the old epoch but provide a short, explicit reattach window.
        # If no client reconciles, terminate/reset the unknown owner before
        # admitting new work.
        self.lease = {
            **previous,
            "epoch": self.epoch,
            "fencingToken": uuid.uuid4().hex,
            "deadlineMs": int(time.time() * 1000) + 10_000,
            "state": "RECOVERING",
            "recoveryPreviousEpoch": journal.get("epoch"),
        }
        self.state = "RECOVERING"
        self.phase = "RECOVERING"
        self._schedule_recovery_timer()

    def _schedule_recovery_timer(self):
        if self.recovery_timer:
            return
        timer = threading.Timer(10.0, self._recover_unattached)
        timer.daemon = True
        self.recovery_timer = timer
        timer.start()

    def reconcile(self, holder, work_id, metadata=None):
        with self.lock:
            if self.draining:
                raise LeaseConflict("GPU coordinator is draining for restart", retry_after=1)
            lease = self.lease
            if not lease or lease.get("state") != "RECOVERING":
                raise StaleLease("no recovering lease is available")
            if lease.get("holder") != holder or lease.get("workId") != str(work_id):
                raise LeaseConflict("recovering foreground work does not match")
            if self.recovery_timer:
                self.recovery_timer.cancel()
                self.recovery_timer = None
            lease["state"] = "ACTIVE"
            lease["deadlineMs"] = int(time.time() * 1000) + 60_000
            lease["metadata"] = dict(metadata or lease.get("metadata") or {})
            self.state = "ACTIVE"
            self.phase = "ACTIVE"
            self._persist_journal()
            return {"epoch": self.epoch, "fencingToken": lease["fencingToken"], "deadlineMs": lease["deadlineMs"]}

    def _recover_unattached(self):
        with self.lock:
            lease = self.lease
            if not lease or lease.get("state") != "RECOVERING":
                return
            holder = lease.get("holder")
            if holder == "inference":
                self._stop_llama()
            elif holder == "comfy":
                self._terminate_comfy_gpu_processes()
            self.lease = None
            self.state = "IDLE"
            self.phase = "IDLE"
            self.recovery_timer = None
            self._persist_journal()

    def _read_pid(self, expected_start_time=None):
        if not self.llama_pid_file:
            return None
        try:
            value = int(self.llama_pid_file.read_text(encoding="utf-8").strip())
            actual_start_time = self._process_start_time(value)
            argv = self._read_argv(value) or []
            try:
                published_start_time = self._llama_start_file.read_text(encoding="utf-8").strip()
            except OSError:
                published_start_time = None
            if actual_start_time is None:
                raise ValueError("llama pid has no live process identity")
            if not any("llama-server" in argument for argument in argv):
                raise ValueError("pidfile process is not llama-server")
            if expected_start_time is not None and str(actual_start_time) != str(expected_start_time):
                if not self.enforce_transitions:
                    # Shadow mode observes but never signals. Adopting this
                    # verified llama PID also prevents an accidental duplicate.
                    self._persist_llama_pid_identity(value, actual_start_time)
                    return value
                if str(published_start_time or "") != str(actual_start_time):
                    raise CoordinatorError(
                        "live llama differs from its journal and lacks a matching persisted start-time sidecar"
                    )
                if not self._terminate_matching_process(value, actual_start_time):
                    raise CoordinatorError("mismatched journal llama could not be terminated safely")
                raise ValueError("terminated llama whose identity was newer than its journal")
            self._persist_llama_pid_identity(value, actual_start_time)
            return value
        except (OSError, ValueError):
            self._remove_llama_pid_identity()
            return None

    @property
    def _llama_start_file(self):
        return Path(str(self.llama_pid_file) + ".start") if self.llama_pid_file else None

    @staticmethod
    def _write_atomic_text(path, value):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

    def _persist_llama_pid_identity(self, pid, start_time):
        if not self.llama_pid_file:
            return
        # Publish the immutable identity before the PID. A concurrent deploy
        # can then either validate the new pair or fail closed during the tiny
        # two-file handoff; it can never signal using a missing identity.
        self._write_atomic_text(self._llama_start_file, str(start_time) + "\n")
        self._write_atomic_text(self.llama_pid_file, str(pid) + "\n")

    def _remove_llama_pid_identity(self):
        if self.llama_pid_file:
            self.llama_pid_file.unlink(missing_ok=True)
        if self._llama_start_file:
            self._llama_start_file.unlink(missing_ok=True)

    @classmethod
    def _terminate_matching_process(cls, pid, start_time):
        for sig, wait_seconds in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            if not cls._process_matches(pid, start_time):
                return True
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                return True
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                if not cls._process_matches(pid, start_time):
                    return True
                time.sleep(0.1)
        return not cls._process_matches(pid, start_time)

    @staticmethod
    def _read_argv(pid):
        if not pid:
            return None
        try:
            raw = Path(f"/proc/{pid}/cmdline").read_bytes()
            argv = [part.decode("utf-8") for part in raw.split(b"\0") if part]
            return argv or None
        except (OSError, UnicodeDecodeError):
            return None

    def llama_running(self):
        pid = self.llama_pid
        if not pid:
            return False
        if not self._process_matches(pid, self.llama_start_time):
            self.llama_pid = None
            self.llama_process = None
            self.llama_start_time = None
            self._remove_llama_pid_identity()
            self._clear_resident_cache_state()
            return False
        if self.llama_process is not None and self.llama_process.pid == pid:
            if self.llama_process.poll() is not None:
                return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _ensure_llama(self):
        if self.llama_running():
            return
        if not self.llama_argv:
            raise CoordinatorError("llama command is unavailable for restart")
        output = open(self.llama_log_file, "ab", buffering=0) if self.llama_log_file else subprocess.DEVNULL
        try:
            process = subprocess.Popen(
                self.llama_argv,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        finally:
            if output is not subprocess.DEVNULL:
                output.close()
        self.llama_pid = process.pid
        self.llama_process = process
        deadline_for_start = time.monotonic() + 2
        while time.monotonic() < deadline_for_start and self._process_start_time(process.pid) is None:
            time.sleep(0.01)
        self.llama_start_time = self._process_start_time(process.pid)
        if self.llama_start_time is None:
            process.terminate()
            raise CoordinatorError("could not validate llama process start time")
        self._persist_llama_pid_identity(process.pid, self.llama_start_time)
        self.metrics["llamaStarts"] += 1
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.llama_pid = None
                self.llama_process = None
                self.llama_start_time = None
                self._remove_llama_pid_identity()
                self._clear_resident_cache_state()
                raise CoordinatorError(f"llama exited during startup with {process.returncode}")
            try:
                status, _, _ = self.http_request(f"{self.llama_base_url}/health", timeout=5, authorize_backend=True)
                if status == 200:
                    return
            except Exception:
                pass
            time.sleep(1)
        self._stop_llama()
        raise CoordinatorError("llama did not become healthy")

    def _stop_llama(self):
        self._cancel_snapshot_timer()
        pid = self.llama_pid
        if not pid or not self.llama_running():
            self.llama_pid = None
            self.llama_start_time = None
            self._remove_llama_pid_identity()
            self._clear_resident_cache_state()
            return
        if not self._process_matches(pid, self.llama_start_time):
            self.llama_pid = None
            self.llama_process = None
            self.llama_start_time = None
            self._remove_llama_pid_identity()
            self._clear_resident_cache_state()
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            self.llama_pid = None
            self.llama_start_time = None
            self._remove_llama_pid_identity()
            self._clear_resident_cache_state()
            return
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if not self.llama_running():
                break
            time.sleep(0.1)
        if self.llama_running():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self.llama_running():
                time.sleep(0.1)
        if self.llama_running():
            raise CoordinatorError("llama process did not release GPU")
        self.llama_pid = None
        self.llama_start_time = None
        self._remove_llama_pid_identity()
        if self.llama_process is not None:
            try:
                self.llama_process.wait(timeout=0)
            except (subprocess.TimeoutExpired, ChildProcessError):
                pass
        self.llama_process = None
        self._clear_resident_cache_state()
        self.metrics["llamaStops"] += 1

    def _clear_resident_cache_state(self):
        # A process stop destroys llama's in-memory KV. Keep disk snapshots in
        # SnapshotStore, but never allow the old handle to be reported as a
        # resident hit after Comfy/mining or recovery evicts llama.
        self.current_cache_handle = None
        self.current_cache_metadata = {}
        self.cache_dirty = False
        self.cache_generation += 1

    def _free_comfy(self, preserve_cache=True):
        status, _, body = self.http_request(
            f"{self.comfy_base_url}/free",
            method="POST",
            payload={"unload_models": True, "free_memory": not preserve_cache},
            timeout=30,
        )
        if not 200 <= status < 300:
            raise CoordinatorError(f"ComfyUI /free returned {status}: {body[:500]!r}")
        self.metrics["comfyEvictions"] += 1
        available = self._memory_status().get("availableBytes")
        memory_pressure = available is not None and available < 24 * 1024**3
        released, probe_ok = self._wait_for_comfy_vram_release(5)
        if released and not (preserve_cache and memory_pressure):
            return
        if not probe_ok:
            raise CoordinatorError("GPU process probe failed while verifying ComfyUI release")
        if preserve_cache:
            status, _, body = self.http_request(
                f"{self.comfy_base_url}/free",
                method="POST",
                payload={"unload_models": True, "free_memory": True},
                timeout=30,
            )
            if not 200 <= status < 300:
                raise CoordinatorError(f"ComfyUI full /free returned {status}: {body[:500]!r}")
            released, probe_ok = self._wait_for_comfy_vram_release(5)
            if not probe_ok:
                raise CoordinatorError("GPU process probe failed after full ComfyUI release")
            if released:
                return
        # Comfy's CUDA allocator can establish a larger persistent process
        # baseline after its first real workflow than it had during boot
        # calibration. At this point either the initial request was already a
        # full free (preserve_cache=False), or the preserve-cache attempt was
        # followed by a full free above. That acknowledgement is the strongest
        # available proof that models and reclaimable allocator memory were
        # released. Learn the post-workload baseline only inside the absolute
        # hard ceiling; the ceiling remains the fail-closed retained-model fence.
        used_bytes = self._comfy_gpu_bytes()
        if used_bytes is not None and used_bytes <= self.comfy_release_vram_max_bytes:
            previous_baseline = self.comfy_idle_baseline_bytes
            adjusted_baseline = max(previous_baseline or 0, used_bytes)
            if previous_baseline is None or adjusted_baseline > previous_baseline:
                self.comfy_idle_baseline_bytes = adjusted_baseline
                self.last_comfy_baseline_adjustment = {
                    "previousBytes": previous_baseline,
                    "adjustedBytes": adjusted_baseline,
                    "hardCeilingBytes": self.comfy_release_vram_max_bytes,
                    "atMs": int(time.time() * 1000),
                }
            if used_bytes <= self._effective_comfy_release_vram_max_bytes():
                self.last_transition_error = None
                return
        used_bytes = self._comfy_gpu_bytes()
        error = CoordinatorError("ComfyUI retained GPU memory after free request")
        self.last_transition_error = {
            "code": "comfy_vram_not_released",
            "message": str(error),
            "observedBytes": used_bytes,
            "allowedBytes": self._effective_comfy_release_vram_max_bytes(),
            "atMs": int(time.time() * 1000),
        }
        raise error

    def _effective_comfy_release_vram_max_bytes(self):
        limit = self.comfy_release_vram_max_bytes
        if self.comfy_idle_baseline_bytes is not None:
            limit = min(
                limit,
                self.comfy_idle_baseline_bytes + self.comfy_release_vram_headroom_bytes,
            )
        return limit

    def _wait_for_comfy_vram_release(self, timeout_seconds):
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            used_bytes = self._comfy_gpu_bytes()
            if used_bytes is None:
                return False, False
            if used_bytes <= self._effective_comfy_release_vram_max_bytes():
                self.last_transition_error = None
                return True, True
            time.sleep(0.25)
        return False, True

    @staticmethod
    def _gpu_processes(timeout_seconds=5):
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits"],
                check=False,
                capture_output=True,
                text=True,
                timeout=max(0.1, float(timeout_seconds)),
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode != 0:
            return None
        processes = []
        for row in result.stdout.splitlines():
            if not row.strip():
                continue
            try:
                raw_pid, raw_mib = row.split(",", 1)
                pid = int(raw_pid.strip())
                used_bytes = int(raw_mib.strip()) * 1024**2
            except ValueError:
                return None
            try:
                cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
                cmdline = re.sub(r"(--api-key(?:=|\s+))\S+", r"\1[redacted]", cmdline)
                try:
                    cwd = os.readlink(f"/proc/{pid}/cwd")
                except OSError:
                    cwd = ""
                processes.append({"pid": pid, "usedBytes": used_bytes, "cmdline": cmdline[:512], "cwd": cwd})
            except OSError:
                continue
        return processes

    def _comfy_gpu_bytes(self):
        processes = self._gpu_processes()
        if processes is None:
            return None
        return sum(
            item["usedBytes"]
            for item in processes
            if self._is_comfy_gpu_process(item)
        )

    def inference_readiness(self):
        """Report whether a stopped llama can safely begin its GPU transition."""
        llama_running = self.llama_running()
        configured = bool(self.llama_argv)
        used_bytes = self._comfy_gpu_bytes()
        allowed_bytes = self._effective_comfy_release_vram_max_bytes()
        lease = self.lease if isinstance(self.lease, dict) else {}
        holder = lease.get("holder")
        lease_state = str(lease.get("state") or "").upper()
        foreground_busy = holder in FOREGROUND and lease_state in {"ACTIVE", "RECOVERING", "STARTING"}
        if llama_running:
            ready = True
            reason = "llama_running"
        elif not configured:
            ready = False
            reason = "llama_command_unavailable"
        elif foreground_busy:
            ready = False
            reason = "gpu_busy"
        elif used_bytes is None:
            ready = False
            reason = "gpu_process_probe_failed"
        elif used_bytes > allowed_bytes:
            ready = False
            reason = "comfy_vram_not_released"
        else:
            ready = True
            reason = "restart_preconditions_satisfied"
        return {
            "ready": ready,
            "reason": reason,
            "llamaConfigured": configured,
            "llamaRunning": llama_running,
            "comfyUsedBytes": used_bytes,
            "comfyIdleBaselineBytes": self.comfy_idle_baseline_bytes,
            "comfyReleaseHeadroomBytes": self.comfy_release_vram_headroom_bytes,
            "comfyReleaseMaxBytes": self.comfy_release_vram_max_bytes,
            "comfyReleaseEffectiveMaxBytes": allowed_bytes,
            "lastComfyBaselineAdjustment": self.last_comfy_baseline_adjustment,
            "lastTransitionError": self.last_transition_error,
        }

    def _is_comfy_gpu_process(self, item):
        cmdline = str(item.get("cmdline", "")).lower()
        cwd = str(item.get("cwd", "")).lower()
        if "comfyui" not in cmdline and "comfyui" not in cwd:
            return False
        pid = int(item.get("pid") or 0)
        # The inference launcher can inherit /workspace/ComfyUI as its cwd.
        # Exclude only the exact fenced llama identity; a reused/stale PID is
        # still treated as Comfy and fails closed through normal verification.
        if pid and pid == self.llama_pid and self.llama_start_time is not None:
            if self._process_matches(pid, self.llama_start_time):
                return False
        return True

    def _terminate_comfy_gpu_processes(self):
        processes = self._gpu_processes()
        if processes is None:
            raise CoordinatorError("GPU process probe failed during ComfyUI recovery")
        target_pids = [
            int(item["pid"])
            for item in processes
            if self._is_comfy_gpu_process(item)
        ]
        targets = []
        for pid in target_pids:
            start_time = self._process_start_time(pid)
            if start_time is None:
                raise CoordinatorError("ComfyUI process identity could not be established during recovery")
            targets.append((pid, start_time))
        for sig, wait_seconds in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            live_targets = []
            for pid, start_time in targets:
                if not self._process_matches(pid, start_time):
                    continue
                try:
                    os.kill(pid, sig)
                    live_targets.append((pid, start_time))
                except ProcessLookupError:
                    pass
            targets = live_targets
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                targets = [
                    (pid, start_time)
                    for pid, start_time in targets
                    if self._process_matches(pid, start_time)
                ]
                if not targets:
                    return
                time.sleep(0.1)
        if targets:
            raise CoordinatorError("ComfyUI process did not release GPU during recovery")

    @staticmethod
    def _memory_status():
        values = {}
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                key, raw = line.split(":", 1)
                values[key] = int(raw.strip().split()[0]) * 1024
        except (OSError, ValueError):
            pass
        return {"totalBytes": values.get("MemTotal"), "availableBytes": values.get("MemAvailable")}

    @staticmethod
    def _process_start_time(pid):
        if not pid:
            return None
        try:
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            fields_after_comm = raw.rsplit(")", 1)[1].split()
            return fields_after_comm[19]
        except (OSError, IndexError):
            return None

    @classmethod
    def _process_matches(cls, pid, expected_start_time=None):
        actual = cls._process_start_time(pid)
        if actual is None:
            return False
        return expected_start_time is None or str(actual) == str(expected_start_time)

    def _stop_mining(self, metadata):
        pid = int((metadata or {}).get("pid") or 0)
        start_time = (metadata or {}).get("processStartTime")
        process_group_id = int((metadata or {}).get("processGroupId") or 0)
        if not pid or not self._process_matches(pid, start_time):
            return
        started = time.monotonic()
        for sig, wait_seconds in ((signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            try:
                if process_group_id > 0 and os.getpgid(pid) == process_group_id:
                    os.killpg(process_group_id, sig)
                else:
                    os.kill(pid, sig)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                if not self._process_matches(pid, start_time):
                    self.metrics["miningRevocations"] += 1
                    self.mining_stop_durations_ms.append(round((time.monotonic() - started) * 1000))
                    self.mining_stop_durations_ms = self.mining_stop_durations_ms[-100:]
                    return
                time.sleep(0.1)
        raise CoordinatorError("mining process did not release GPU")

    def _cancel_snapshot_timer(self):
        timer = self.snapshot_timer
        self.snapshot_timer = None
        if timer:
            timer.cancel()

    def _evict_warm(self, holder):
        self.phase = "EVICTING"
        if holder == "inference":
            self._bounded_dirty_save("eviction")
            self._stop_llama()
        elif holder == "comfy":
            self._free_comfy(preserve_cache=True)
        elif holder == "mining":
            self._stop_mining((self.lease or {}).get("metadata", {}))
        self.phase = "VERIFYING"
        self.state = "IDLE"
        self.phase = "IDLE"

    def _expire_lease(self):
        if self.lease and self.lease.get("state") == "STARTING" and int(self.lease.get("registrationDeadlineMs", 0)) <= int(time.time() * 1000):
            if self.draining and self.lease.get("holder") == "mining":
                # A process may already be between local launch and identity
                # registration. Keep the fence until renew() can validate and
                # synchronously revoke it; clearing here would let the restart
                # observer race ahead of the agent's failure cleanup.
                self.lease["registrationExpiredDuringDrain"] = True
                self._persist_journal()
                return
            self.lease = None
            self.state = "IDLE"
            self.phase = "IDLE"
            self._persist_journal()
            return
        if self.lease and int(self.lease["deadlineMs"]) <= int(time.time() * 1000):
            holder = self.lease["holder"]
            if not self.enforce_transitions:
                self.lease = None
                self.state = "IDLE"
                self.phase = "IDLE"
                self._persist_journal()
                return
            if self.lease.get("state") in {"ACTIVE", "RECOVERING"} and holder in FOREGROUND:
                # Never infer that an active foreground operation stopped just
                # because its heartbeat expired. Fence all new owners until it
                # reattaches or explicit recovery resets the GPU process.
                self.lease["state"] = "RECOVERING"
                self.lease["expired"] = True
                self.state = "RECOVERING"
                self.phase = "RECOVERING"
                self._schedule_recovery_timer()
                self._persist_journal()
                return
            if holder == "mining":
                self._stop_mining(self.lease.get("metadata", {}))
            self.lease = None
            self.state = "IDLE"
            self.phase = "IDLE"
            self._persist_journal()

    def _effective_mining_grace_seconds(self):
        if not self.mining_stop_durations_ms:
            return self.mining_grace_seconds
        ordered = sorted(self.mining_stop_durations_ms)
        p99_ms = ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]
        return max(self.mining_grace_seconds, int((10 * p99_ms + 999) // 1000))

    def acquire(self, holder, work_id, ttl_ms, metadata=None):
        if holder not in HOLDERS:
            raise ValueError("invalid GPU lease holder")
        ttl_ms = min(max(int(ttl_ms or 60_000), 5_000), 30 * 60_000)
        with self.lock:
            self._expire_lease()
            if self.draining:
                self.metrics["leaseConflicts"] += 1
                raise LeaseConflict("GPU coordinator is draining for restart", retry_after=1)
            if not self.enabled:
                token = uuid.uuid4().hex
                return {"epoch": self.epoch, "fencingToken": token, "deadlineMs": int(time.time() * 1000) + ttl_ms}
            if self.enforce_transitions and holder in FOREGROUND and self.phase == "SNAPSHOTTING":
                # Foreground latency wins. Terminating llama aborts its slot
                # write; the persistence thread removes the incomplete file.
                self._stop_llama()
            elif holder == "mining" and self.phase == "SNAPSHOTTING":
                # Persistence is allowed to delay only the lowest-priority
                # workload. Keep retries bounded so a failed slot write cannot
                # strand mining indefinitely.
                self.metrics["leaseConflicts"] += 1
                raise LeaseConflict("eligible inference snapshot is still being persisted", retry_after=5)
            if self.enforce_transitions and holder == "mining" and self.mining_not_before_ms > int(time.time() * 1000):
                retry_seconds = max(1, int((self.mining_not_before_ms - int(time.time() * 1000) + 999) // 1000))
                self.metrics["leaseConflicts"] += 1
                raise LeaseConflict("foreground idle grace has not elapsed", retry_after=retry_seconds)
            existing = self.lease
            comfy_verified_free = False
            if existing and not self.enforce_transitions:
                # Shadow mode records the most recent desired owner without
                # affecting scheduling or processes.
                self.lease = None
                existing = None
            if existing:
                if existing["holder"] == "mining" and existing.get("state") == "STARTING":
                    if holder in FOREGROUND and self.enforce_transitions:
                        # The agent launches behind a non-CUDA gate. Request
                        # cancellation, then briefly wait for its registration
                        # renew to identify and synchronously revoke the gated
                        # wrapper before granting foreground ownership.
                        existing["cancelPending"] = True
                        self._persist_journal()
                        cancellation_deadline = time.monotonic() + 1.0
                        while self.lease is existing and time.monotonic() < cancellation_deadline:
                            self.lease_condition.wait(timeout=0.05)
                        if self.lease is not existing:
                            return self.acquire(holder, work_id, ttl_ms, metadata)
                    retry_seconds = max(1, int((existing["registrationDeadlineMs"] - int(time.time() * 1000) + 999) // 1000))
                    self.metrics["leaseConflicts"] += 1
                    raise LeaseConflict("miner launch cancellation is awaiting acknowledgement", retry_after=min(5, retry_seconds))
                if (
                    holder == "mining"
                    and existing["holder"] in FOREGROUND
                    and existing.get("state") == "WARM"
                    and int(existing.get("warmDeadlineMs", 0)) > int(time.time() * 1000)
                ):
                    retry_seconds = max(1, int((existing["warmDeadlineMs"] - int(time.time() * 1000) + 999) // 1000))
                    self.metrics["leaseConflicts"] += 1
                    raise LeaseConflict("foreground warm grace has not elapsed", retry_after=retry_seconds)
                if existing["holder"] == "mining" and holder in FOREGROUND:
                    self._stop_mining(existing.get("metadata", {}))
                    self.lease = None
                elif existing["state"] == "WARM":
                    if existing["holder"] != holder:
                        evicted_holder = existing["holder"]
                        self._evict_warm(existing["holder"])
                        comfy_verified_free = evicted_holder == "comfy"
                    elif holder == "inference":
                        # The prior inference transition already verified Comfy
                        # release. Preserve that proof for same-owner warm KV
                        # reuse instead of probing/evicting Comfy again.
                        comfy_verified_free = True
                    self.lease = None
                else:
                    self.metrics["leaseConflicts"] += 1
                    raise LeaseConflict(f"GPU is active for {existing['holder']}")
            self.state = "GRANTING"
            self.phase = "GRANTING"
            try:
                if holder == "inference" and self.enforce_transitions:
                    if not comfy_verified_free:
                        self._free_comfy(preserve_cache=True)
                    self._ensure_llama()
                elif holder == "inference" and self.llama_argv and not self.llama_running():
                    # Shadow mode normally observes the legacy sleeping server,
                    # but can restart it after crash recovery reset it.
                    self._ensure_llama()
                elif holder in {"comfy", "mining"} and self.enforce_transitions:
                    if self.llama_running():
                        self._stop_llama()
                    if holder == "mining":
                        if not comfy_verified_free:
                            self._free_comfy(preserve_cache=True)
            except Exception:
                # A failed transition must never strand the coordinator in a
                # synthetic GRANTING state with no fenced owner.
                self.lease = None
                self.state = "IDLE"
                self.phase = "IDLE"
                self._persist_journal()
                self.lease_condition.notify_all()
                raise
            token = uuid.uuid4().hex
            deadline = int(time.time() * 1000) + ttl_ms
            lease_state = "STARTING" if holder == "mining" else "ACTIVE"
            self.lease = {
                "holder": holder,
                "workId": str(work_id),
                "epoch": self.epoch,
                "fencingToken": token,
                "deadlineMs": deadline,
                "state": lease_state,
                "registrationDeadlineMs": int(time.time() * 1000) + 5_000 if lease_state == "STARTING" else None,
                "metadata": dict(metadata or {}),
                "acquiredAt": time.time(),
            }
            self.state = lease_state
            self.phase = lease_state
            self._persist_journal()
            return {"epoch": self.epoch, "fencingToken": token, "deadlineMs": deadline}

    def _validate(self, token, epoch):
        self._expire_lease()
        if not self.lease or token != self.lease["fencingToken"] or int(epoch) != self.epoch:
            self.metrics["staleTokenRejections"] += 1
            raise StaleLease("GPU lease is absent, expired, or fenced")
        return self.lease

    def renew(self, token, epoch, ttl_ms, metadata=None):
        with self.lock:
            lease = self._validate(token, epoch)
            ttl_ms = min(max(int(ttl_ms or 60_000), 5_000), 30 * 60_000)
            lease["deadlineMs"] = int(time.time() * 1000) + ttl_ms
            if metadata:
                lease["metadata"].update(metadata)
            if lease["holder"] == "mining" and (metadata or lease.get("state") == "STARTING"):
                pid = int(lease["metadata"].get("pid") or 0)
                start_time = lease["metadata"].get("processStartTime")
                process_group_id = int(lease["metadata"].get("processGroupId") or 0)
                try:
                    actual_process_group_id = os.getpgid(pid) if pid > 0 else 0
                except (OSError, ProcessLookupError):
                    actual_process_group_id = 0
                if (
                    not pid
                    or start_time is None
                    or process_group_id <= 0
                    or actual_process_group_id != process_group_id
                    or not self._process_matches(pid, start_time)
                ):
                    raise StaleLease("miner PID/start time/process group registration is missing or invalid")
            if lease["holder"] == "mining" and lease.get("state") == "STARTING":
                lease["state"] = "ACTIVE"
                lease["registrationDeadlineMs"] = None
                self.state = "ACTIVE"
                self.phase = "ACTIVE"
                if self.draining and self.enforce_transitions:
                    self._stop_mining(lease.get("metadata", {}))
                    self.lease = None
                    self.state = "IDLE"
                    self.phase = "IDLE"
                    self._persist_journal()
                    self.lease_condition.notify_all()
                    raise StaleLease("miner was revoked during coordinator drain")
                if lease.get("cancelPending") and self.enforce_transitions:
                    self._stop_mining(lease.get("metadata", {}))
                    self.lease = None
                    self.state = "IDLE"
                    self.phase = "IDLE"
                    self._persist_journal()
                    self.lease_condition.notify_all()
                    raise StaleLease("miner launch was revoked for foreground work")
            lease.pop("expired", None)
            if lease.get("state") == "RECOVERING":
                if self.recovery_timer:
                    self.recovery_timer.cancel()
                    self.recovery_timer = None
                lease["state"] = "ACTIVE"
                self.state = "ACTIVE"
                self.phase = "ACTIVE"
            self._persist_journal()
            return {"epoch": self.epoch, "fencingToken": token, "deadlineMs": lease["deadlineMs"]}

    def release(self, token, epoch, keep_warm=True, reason=None):
        with self.lock:
            lease = self._validate(token, epoch)
            holder = lease["holder"]
            if self.recovery_timer:
                self.recovery_timer.cancel()
                self.recovery_timer = None
            if holder in FOREGROUND:
                self.mining_not_before_ms = int(time.time() * 1000) + self._effective_mining_grace_seconds() * 1000
            keep_warm = bool(keep_warm and not self.draining and self.warm_residency and holder in FOREGROUND)
            if keep_warm:
                lease["state"] = "WARM"
                lease["deadlineMs"] = int(time.time() * 1000) + 24 * 3600 * 1000
                lease["warmDeadlineMs"] = int(time.time() * 1000) + self._effective_mining_grace_seconds() * 1000
                lease["releasedReason"] = reason
                self.state = "WARM"
                self.phase = "WARM"
                if holder == "inference" and self.current_cache_handle and self.cache_dirty:
                    self._schedule_snapshot()
            else:
                if holder == "inference" and self.enforce_transitions:
                    self._stop_llama()
                elif holder == "comfy" and self.enforce_transitions:
                    self._free_comfy(preserve_cache=True)
                elif holder == "mining" and self.enforce_transitions:
                    self._stop_mining(lease.get("metadata", {}))
                self.lease = None
                self.state = "IDLE"
                self.phase = "IDLE"
                self.lease_condition.notify_all()
            self._persist_journal()
            return {"safe": not keep_warm, "requestComplete": True, "gpuReleased": not keep_warm, "state": self.state}

    def record_completion(self, work_id, **values):
        with self.lock:
            now = time.time()
            cutoff = now - 900
            self.completion_receipts = {
                key: value for key, value in self.completion_receipts.items()
                if isinstance(value, dict) and float(value.get("updated_at", 0)) >= cutoff
            }
            receipt = {
                **values,
                "request_id": str(work_id),
                "phase": "request_complete",
                "request_complete": True,
                "updated_at": now,
                "epoch": self.epoch,
            }
            self.completion_receipts[str(work_id)] = receipt
            if len(self.completion_receipts) > 256:
                ordered = sorted(
                    self.completion_receipts.items(),
                    key=lambda item: float(item[1].get("updated_at", 0)),
                    reverse=True,
                )[:256]
                self.completion_receipts = dict(ordered)
            self._persist_journal()
            return dict(receipt)

    def get_completion(self, work_id):
        with self.lock:
            receipt = self.completion_receipts.get(str(work_id))
            if not isinstance(receipt, dict):
                return None
            if time.time() - float(receipt.get("updated_at", 0)) > 900:
                self.completion_receipts.pop(str(work_id), None)
                self._persist_journal()
                return None
            return dict(receipt)

    def begin_drain(self):
        with self.lock:
            self.draining = True
            if self.lease and self.lease.get("state") == "WARM":
                if self.enforce_transitions:
                    self._evict_warm(self.lease["holder"])
                self.lease = None
                self.state = "IDLE"
                self.phase = "IDLE"
            elif self.lease and self.lease.get("holder") == "mining" and self.enforce_transitions:
                if self.lease.get("state") == "STARTING":
                    # Keep the fence until the agent registers the process it
                    # may already be launching. renew() then validates and
                    # revokes that exact PID before clearing the drain.
                    self.lease["drainPending"] = True
                else:
                    self._stop_mining(self.lease.get("metadata", {}))
                    self.lease = None
                    self.state = "IDLE"
                    self.phase = "IDLE"
            self._persist_journal()
            return {
                "draining": True,
                "state": self.state,
                "lease": None if not self.lease else {
                    key: value for key, value in self.lease.items() if key != "metadata"
                },
            }

    def _schedule_snapshot(self):
        self._cancel_snapshot_timer()
        timer = threading.Timer(5.0, self._snapshot_if_still_warm)
        timer.daemon = True
        self.snapshot_timer = timer
        timer.start()

    def _snapshot_if_still_warm(self):
        with self.lock:
            self.snapshot_timer = None
            eligible = self.lease and self.lease.get("holder") == "inference" and self.lease.get("state") == "WARM"
        # Never hold the scheduling lock during disk persistence.  A competing
        # foreground acquire may stop llama, which cancels the slot write and
        # turns it into a harmless best-effort cache miss.
        if eligible:
            self.save_current_snapshot(best_effort=True)

    def _erase_llama_slot(self):
        status, _, body = self.http_request(
            f"{self.llama_base_url}/slots/0?action=erase",
            method="POST",
            timeout=30,
            authorize_backend=True,
        )
        if not 200 <= status < 300:
            raise CoordinatorError(f"slot erase returned {status}: {body[:500]!r}")

    def prepare_cache(self, handle):
        with self.lock:
            if handle and handle == self.current_cache_handle:
                observations = int(self.current_cache_metadata.get("reuseObservations", 0)) + 1
                hits = int(self.current_cache_metadata.get("reuseHits", 0)) + 1
                self.current_cache_metadata.update({
                    "reuseObservations": observations,
                    "reuseHits": hits,
                    "reuseProbability": (3 + hits) / (10 + observations),
                })
                return {"classification": "resident", "restored": False}
            if self.current_cache_handle and self.cache_dirty:
                self._bounded_dirty_save("key_switch")
            self.cache_generation += 1
            self.current_cache_handle = handle
            self.current_cache_metadata = {}
            self.cache_dirty = False
            if not handle:
                return {"classification": "unkeyed", "restored": False}
            if not self.snapshot_restore:
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False}
            entry = self.snapshot_store.peek(handle)
            if not entry:
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False}
            if float(entry.get("restoreBackoffUntil", 0)) > time.time():
                # Retain the persisted failure policy while cold-prefilling.
                # A later snapshot write must not erase the backoff and cause
                # alternating keys to pay restore plus cold-prefill repeatedly.
                self.current_cache_metadata = dict(entry)
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False, "skipped": "restore_backoff"}
            estimated_cold = float(entry.get("coldPrefillMs", 0))
            predicted_restore = float(entry.get("restoreMs", 0))
            if predicted_restore <= 0:
                size_mib = int(entry.get("bytes", 0)) / 1024**2
                validation_rate = max(1.0, float(os.environ.get("QWEN_CACHE_VALIDATION_MB_PER_SECOND", "1500")))
                restore_rate = max(1.0, float(os.environ.get("QWEN_CACHE_RESTORE_MB_PER_SECOND", "2000")))
                predicted_restore = (size_mib / validation_rate + size_mib / restore_rate) * 1000
            if predicted_restore and estimated_cold and predicted_restore > estimated_cold * 0.8:
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False, "skipped": "restore_not_beneficial"}
            restore_started = time.monotonic()
            entry = self.snapshot_store.get(handle)
            if not entry:
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False, "restoreFailed": True}
            filename = Path(entry["path"]).name
            invalid_snapshot = False
            try:
                status, _, body = self.http_request(
                    f"{self.llama_base_url}/slots/0?action=restore",
                    method="POST",
                    payload={"filename": filename},
                    timeout=300,
                    authorize_backend=True,
                )
                if not 200 <= status < 300:
                    invalid_snapshot = status in {400, 409, 422}
                    raise CoordinatorError(f"slot restore returned {status}: {body[:500]!r}")
                restore_ms = round((time.monotonic() - restore_started) * 1000)
                observations = int(entry.get("reuseObservations", 0)) + 1
                hits = int(entry.get("reuseHits", 0)) + 1
                self.current_cache_metadata = dict(
                    entry,
                    restoreMs=restore_ms,
                    reuseObservations=observations,
                    reuseHits=hits,
                    reuseProbability=(3 + hits) / (10 + observations),
                )
                for key in ("restoreFailureCount", "restoreFailureAt", "restoreBackoffUntil"):
                    self.current_cache_metadata.pop(key, None)
                self.metrics["snapshotRestores"] += 1
                return {"classification": "restored", "restored": True, "restoreMs": restore_ms}
            except Exception:
                self.metrics["snapshotRestoreErrors"] += 1
                if invalid_snapshot:
                    self.snapshot_store.delete(handle)
                else:
                    self.snapshot_store.record_restore_failure(handle)
                self.current_cache_handle = None
                self.current_cache_metadata = {}
                self.cache_dirty = False
                self.cache_generation += 1
                self._erase_llama_slot()
                return {"classification": "cold", "restored": False, "restoreFailed": True}

    def mark_cache_dirty(self, handle, metadata):
        if not handle:
            return {"restoreIneffective": False}
        metadata = dict(metadata or {})
        with self.lock:
            self.current_cache_handle = handle
            previous_cold_prefill_ms = float(self.current_cache_metadata.get("coldPrefillMs", 0))
            observed_cold_prefill_ms = float(metadata.get("coldPrefillMs", 0))
            restore_ineffective = (
                metadata.get("classification") == "restored"
                and int(metadata.get("promptTokens", 0)) > 0
                and int(metadata.get("cachedTokens", 0)) <= 0
            )
            if restore_ineffective:
                # A successful llama slot-load is not sufficient evidence that
                # the model can reuse it. Hybrid/recurrent layouts may accept
                # the file and then force a complete prompt prefill. Persist a
                # bounded backoff and report a miss instead of repeatedly
                # paying restore plus cold-prefill latency.
                try:
                    self.snapshot_store.record_restore_failure(handle)
                    refreshed = self.snapshot_store.peek(handle)
                    if refreshed:
                        self.current_cache_metadata.update(refreshed)
                except Exception:
                    # Cache policy persistence is best-effort and must never
                    # turn a successfully generated response into a failure.
                    self.metrics["snapshotRestoreErrors"] += 1
                observations = max(1, int(self.current_cache_metadata.get("reuseObservations", 1)))
                hits = max(0, int(self.current_cache_metadata.get("reuseHits", 1)) - 1)
                self.current_cache_metadata.update({
                    "reuseObservations": observations,
                    "reuseHits": hits,
                    "reuseProbability": (3 + hits) / (10 + observations),
                })
                self.metrics["snapshotRestoreIneffective"] += 1
            self.current_cache_metadata.update(metadata)
            self.current_cache_metadata["coldPrefillMs"] = max(previous_cold_prefill_ms, observed_cold_prefill_ms)
            self.current_cache_metadata.setdefault("reuseProbability", 0.3)
            self.cache_dirty = True
            self.cache_generation += 1
            return {"restoreIneffective": restore_ineffective}

    def _snapshot_beneficial(self):
        metadata = self.current_cache_metadata
        reuse = float(metadata.get("reuseProbability", 0.3))
        cold = float(metadata.get("coldPrefillMs", 0))
        restore = float(metadata.get("restoreMs", 0))
        save = float(metadata.get("saveMs", 0))
        if cold <= 0:
            return False
        if save <= 0:
            # First observation uses measured local write throughput estimate.
            tokens = max(1, int(metadata.get("promptTokens", 0)))
            save = max(1.0, tokens * float(os.environ.get("QWEN_CACHE_INITIAL_SAVE_MS_PER_TOKEN", "0.02")))
        return reuse * max(0.0, cold - restore) > 1.2 * save

    def _predicted_snapshot_bytes(self, metadata):
        prompt_tokens = max(1, int((metadata or {}).get("promptTokens", 0)))
        samples = [
            item for item in self.snapshot_store.list()
            if int(item.get("promptTokens", 0)) > 0 and int(item.get("bytes", 0)) > 0
        ]
        sampled_tokens = sum(int(item["promptTokens"]) for item in samples)
        if sampled_tokens:
            bytes_per_token = sum(int(item["bytes"]) for item in samples) / sampled_tokens
        else:
            bytes_per_token = max(1.0, float(os.environ.get("QWEN_CACHE_INITIAL_BYTES_PER_TOKEN", "600000")))
        return min(self.snapshot_store.max_entry_bytes, max(1, int(prompt_tokens * bytes_per_token)))

    def _bounded_dirty_save(self, reason, max_latency_ms=250):
        metric = "snapshotSkippedDirtyEviction" if reason == "eviction" else "snapshotSkippedKeySwitch"
        handle = self.current_cache_handle
        metadata = dict(self.current_cache_metadata)
        generation = self.cache_generation
        if not handle or not self.cache_dirty:
            return False
        if not self.snapshot_write or not self._snapshot_beneficial():
            self.metrics[metric] += 1
            return False
        predicted_ms = float(metadata.get("saveMs") or 0)
        if predicted_ms <= 0:
            predicted_ms = max(1.0, int(metadata.get("promptTokens", 0)) * float(os.environ.get("QWEN_CACHE_INITIAL_SAVE_MS_PER_TOKEN", "0.02")))
        # Leave headroom for request setup; checksum/manifest commit runs after
        # the llama slot endpoint has completed and no longer owns the GPU.
        if predicted_ms > max_latency_ms * 0.8:
            self.metrics[metric] += 1
            return False
        if not self.snapshot_store.capacity(self._predicted_snapshot_bytes(metadata))["canWrite"]:
            self.metrics[metric] += 1
            return False
        temporary = self.snapshot_store.temporary_filename(handle)
        started = time.monotonic()
        try:
            status, _, body = self.http_request(
                f"{self.llama_base_url}/slots/0?action=save",
                method="POST",
                payload={"filename": temporary},
                timeout=max_latency_ms / 1000,
                authorize_backend=True,
            )
            elapsed_ms = round((time.monotonic() - started) * 1000)
            if not 200 <= status < 300 or elapsed_ms > max_latency_ms:
                raise CoordinatorError(f"bounded slot save returned {status}: {body[:200]!r}")
        except Exception:
            (self.snapshot_store.root / temporary).unlink(missing_ok=True)
            self.metrics[metric] += 1
            return False

        def commit_after_gpu_save():
            try:
                committed = self.snapshot_store.commit(
                    handle,
                    temporary,
                    {
                        **metadata,
                        "saveMs": elapsed_ms,
                        "reuseProbability": float(metadata.get("reuseProbability", 0.3)),
                        "savedPrefillMs": max(0, float(metadata.get("coldPrefillMs", 0)) - float(metadata.get("restoreMs", 0))),
                    },
                )
                with self.lock:
                    if self.current_cache_handle == handle and self.cache_generation == generation:
                        self.current_cache_metadata = committed
                        self.cache_dirty = False
                    elif self.current_cache_handle == handle and self.cache_dirty and self.lease and self.lease.get("state") == "WARM":
                        self._schedule_snapshot()
                    self.metrics["snapshotSaves"] += 1
            except Exception:
                (self.snapshot_store.root / temporary).unlink(missing_ok=True)
                with self.lock:
                    self.metrics["snapshotSaveErrors"] += 1

        commit_thread = threading.Thread(target=commit_after_gpu_save, daemon=True)
        commit_thread.start()
        return True

    def save_current_snapshot(self, best_effort=False):
        with self.lock:
            handle = self.current_cache_handle
            metadata_at_start = dict(self.current_cache_metadata)
            generation_at_start = self.cache_generation
            if not self.snapshot_write or not handle or not self.cache_dirty or not self._snapshot_beneficial():
                return None
            if not self.snapshot_store.capacity(self._predicted_snapshot_bytes(metadata_at_start))["canWrite"]:
                self.metrics["snapshotSaveErrors"] += 1
                return None
            self.phase = "SNAPSHOTTING"
        temporary = self.snapshot_store.temporary_filename(handle)
        started = time.monotonic()
        try:
            status, _, body = self.http_request(
                f"{self.llama_base_url}/slots/0?action=save",
                method="POST",
                payload={"filename": temporary},
                timeout=300,
                authorize_backend=True,
            )
            if not 200 <= status < 300:
                raise CoordinatorError(f"slot save returned {status}: {body[:500]!r}")
            save_ms = round((time.monotonic() - started) * 1000)
            # The GPU-dependent portion is complete. Let new foreground work
            # acquire immediately while hashing/fsync/manifest commit finishes
            # in this background thread.
            with self.lock:
                if self.phase == "SNAPSHOTTING":
                    self.phase = self.state
            metadata = dict(metadata_at_start, saveMs=save_ms)
            metadata.setdefault("reuseProbability", 0.3)
            metadata.setdefault("savedPrefillMs", max(0, float(metadata.get("coldPrefillMs", 0)) - float(metadata.get("restoreMs", 0))))
            result = self.snapshot_store.commit(handle, temporary, metadata)
            with self.lock:
                if self.current_cache_handle == handle and self.cache_generation == generation_at_start:
                    self.current_cache_metadata = result
                    self.cache_dirty = False
                elif self.current_cache_handle == handle and self.cache_dirty and self.lease and self.lease.get("state") == "WARM":
                    self._schedule_snapshot()
                self.metrics["snapshotSaves"] += 1
            return result
        except Exception:
            (self.snapshot_store.root / temporary).unlink(missing_ok=True)
            with self.lock:
                self.metrics["snapshotSaveErrors"] += 1
            if not best_effort:
                raise
            return None
        finally:
            with self.lock:
                if self.phase == "SNAPSHOTTING":
                    self.phase = self.state

    def quick_status(self):
        acquired = self.lock.acquire(timeout=0.05)
        try:
            lease = None
            if acquired and self.lease:
                lease = {key: value for key, value in self.lease.items() if key not in {"metadata"}}
            return {
                "enabled": self.enabled,
                "enforcing": self.enforce_transitions,
                "state": self.state,
                "phase": self.phase,
                "epoch": self.epoch,
                "lease": lease,
                "llamaPid": self.llama_pid,
                "llamaStartTime": self.llama_start_time,
                "miningGraceSeconds": self._effective_mining_grace_seconds(),
                "miningNotBeforeMs": self.mining_not_before_ms,
                "metrics": dict(self.metrics),
                "diagnosticsBusy": not acquired,
                "draining": self.draining,
            }
        finally:
            if acquired:
                self.lock.release()

    def status(self):
        result = self.quick_status()
        llama_pid = result.pop("llamaPid", None)
        llama_start_time = result.pop("llamaStartTime", None)
        result.update({
            "llama": {
                "pid": llama_pid,
                "running": bool(llama_pid and self._process_matches(llama_pid, llama_start_time)),
            },
            "gpuProcesses": self._gpu_processes(timeout_seconds=1),
            "memory": self._memory_status(),
            "cache": self.snapshot_store.status(),
            "inferenceReadiness": self.inference_readiness(),
        })
        return result

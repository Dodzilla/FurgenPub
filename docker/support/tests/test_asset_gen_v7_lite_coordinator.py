import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


SUPPORT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SUPPORT_DIR))

from asset_gen_v7_lite_coordinator import (  # noqa: E402
    GPUCoordinator,
    LeaseConflict,
    SnapshotStore,
    StaleLease,
)


class FakeHttp:
    def __init__(self, root):
        self.root = Path(root)
        self.free_payloads = []
        self.slot_actions = []

    def __call__(self, url, method="GET", payload=None, **_kwargs):
        if url.endswith("/free"):
            self.free_payloads.append(payload)
            return 200, "application/json", b"{}"
        if "action=save" in url:
            (self.root / payload["filename"]).write_bytes(b"slot-state")
            return 200, "application/json", b"{}"
        if "action=restore" in url or "action=erase" in url:
            self.slot_actions.append(url)
            return 200, "application/json", b"{}"
        if url.endswith("/health"):
            return 200, "application/json", b"{}"
        return 404, "application/json", b"{}"


class SnapshotStoreTest(unittest.TestCase):
    def test_commit_lookup_and_fingerprint_invalidation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fingerprint-a", min_free_bytes=0)
            handle = "v1." + "a" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"occupied-kv")
            store.commit(handle, temporary, {"coldPrefillMs": 1000})
            entry = store.get(handle)
            self.assertEqual(entry["bytes"], len(b"occupied-kv"))
            self.assertEqual(len(entry["sha256"]), 64)
            incompatible = SnapshotStore(directory, "fingerprint-b", min_free_bytes=0)
            self.assertIsNone(incompatible.get(handle))
            self.assertFalse((Path(directory) / (handle + ".slot")).exists())

    def test_checksum_mismatch_deletes_corrupt_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            handle = "v1." + "c" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"original")
            store.commit(handle, temporary, {"coldPrefillMs": 1000})
            (Path(directory) / f"{handle}.slot").write_bytes(b"tampered")
            self.assertIsNone(store.get(handle))
            self.assertFalse((Path(directory) / f"{handle}.slot").exists())

    def test_ttl_and_quota_prune(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", ttl_seconds=10, quota_bytes=15, min_free_bytes=0)
            for handle in ("v1." + "a" * 64, "v1." + "b" * 64):
                temporary = store.temporary_filename(handle)
                (Path(directory) / temporary).write_bytes(b"1234567890")
                store.commit(handle, temporary, {"coldPrefillMs": 100})
            self.assertLessEqual(store.status()["bytes"], 15)
            manifest = next(Path(directory).glob("*.json"))
            value = json.loads(manifest.read_text())
            value["updatedAt"] = time.time() - 20
            manifest.write_text(json.dumps(value))
            store.prune()
            self.assertEqual(store.status()["entries"], 0)

    def test_modular_backend_capacity_honors_quota_before_write(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", quota_bytes=1024, min_free_bytes=0, max_entry_bytes=1024)
            self.assertTrue(store.capacity(512)["canWrite"])
            temporary = store.temporary_filename("v1." + "a" * 64)
            (Path(directory) / temporary).write_bytes(b"x" * 800)
            self.assertFalse(store.capacity(512)["canWrite"])
            self.assertEqual(store.list(), [])

    def test_commit_does_not_double_count_existing_temporary_bytes_at_disk_floor(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", quota_bytes=1024, min_free_bytes=100, max_entry_bytes=1024)
            handle = "v1." + "b" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"x" * 80)
            with mock.patch("asset_gen_v7_lite_coordinator.shutil.disk_usage", return_value=mock.Mock(free=100)):
                committed = store.commit(handle, temporary, {"promptTokens": 1})
            self.assertEqual(committed["bytes"], 80)


class CoordinatorLeaseTest(unittest.TestCase):
    def make_coordinator(self, directory, enforcing=False):
        http = FakeHttp(directory)
        store = SnapshotStore(directory, "fp", min_free_bytes=0)
        coordinator = GPUCoordinator(
            "http://127.0.0.1:8081",
            "http://127.0.0.1:8188",
            http,
            store,
            enabled=True,
            enforce_transitions=enforcing,
            warm_residency=True,
        )
        coordinator._gpu_processes = lambda *_args, **_kwargs: []
        return coordinator, http

    def test_llama_eviction_invalidates_resident_handle_but_restores_disk_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            http = FakeHttp(directory)
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081",
                "http://127.0.0.1:8188",
                http,
                store,
                enabled=True,
                enforce_transitions=True,
                warm_residency=True,
                snapshot_write=True,
                snapshot_restore=True,
            )
            handle = "v1." + "a" * 64
            coordinator.mark_cache_dirty(handle, {
                "promptTokens": 4000,
                "coldPrefillMs": 2000,
                "reuseProbability": 0.3,
            })
            self.assertIsNotNone(coordinator.save_current_snapshot())
            coordinator.lease = {
                "holder": "inference",
                "workId": "request-1",
                "epoch": coordinator.epoch,
                "fencingToken": "a" * 32,
                "deadlineMs": int(time.time() * 1000) + 60_000,
                "state": "WARM",
                "metadata": {},
                "acquiredAt": time.time(),
            }
            coordinator.state = "WARM"
            coordinator.phase = "WARM"
            coordinator.acquire("comfy", "job-1", 60_000)
            self.assertIsNone(coordinator.current_cache_handle)
            cache_state = coordinator.prepare_cache(handle)
            self.assertNotEqual(cache_state["classification"], "resident")
            self.assertEqual(cache_state["classification"], "restored")

    def test_resident_hit_preserves_full_cold_baseline_for_updated_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.snapshot_write = True
            handle = "v1." + "9" * 64
            coordinator.mark_cache_dirty(handle, {"promptTokens": 1000, "coldPrefillMs": 1000})
            self.assertIsNotNone(coordinator.save_current_snapshot())
            self.assertEqual(coordinator.prepare_cache(handle)["classification"], "resident")
            coordinator.mark_cache_dirty(handle, {"promptTokens": 1100, "cachedTokens": 1000, "coldPrefillMs": 25})
            self.assertEqual(coordinator.current_cache_metadata["coldPrefillMs"], 1000)
            self.assertGreater(coordinator.current_cache_metadata["reuseProbability"], 0.3)
            self.assertTrue(coordinator._snapshot_beneficial())

    def test_fencing_conflict_renew_and_stale_release(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            first = coordinator.acquire("comfy", "job-1", 60_000)
            with self.assertRaises(LeaseConflict):
                coordinator.acquire("inference", "request-1", 60_000)
            renewed = coordinator.renew(first["fencingToken"], first["epoch"], 90_000)
            self.assertGreaterEqual(renewed["deadlineMs"], first["deadlineMs"])
            coordinator.release(first["fencingToken"], first["epoch"], keep_warm=False)
            with self.assertRaises(StaleLease):
                coordinator.release(first["fencingToken"], first["epoch"], keep_warm=False)

    def test_changed_key_without_snapshot_erases_residual_slot(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory, enforcing=True)
            coordinator.snapshot_restore = True
            coordinator.current_cache_handle = "v1." + "1" * 64
            state = coordinator.prepare_cache("v1." + "2" * 64)
            self.assertEqual(state["classification"], "cold")
            self.assertTrue(any("action=erase" in url for url in http.slot_actions))

    def test_unkeyed_requests_are_never_classified_as_keyed_resident_hits(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            self.assertEqual(coordinator.prepare_cache(None)["classification"], "unkeyed")
            self.assertEqual(coordinator.prepare_cache(None)["classification"], "unkeyed")

    def test_shadow_mode_never_blocks_a_new_desired_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator.acquire("comfy", "job-1", 60_000)
            lease = coordinator.acquire("inference", "request-1", 60_000)
            self.assertEqual(coordinator.status()["lease"]["holder"], "inference")
            self.assertTrue(lease["fencingToken"])

    def test_shadow_mode_does_not_enforce_mining_grace(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=False)
            coordinator.mining_not_before_ms = int(time.time() * 1000) + 60_000
            lease = coordinator.acquire("mining", "shadow-miner", 60_000)
            self.assertTrue(lease["fencingToken"])

    def test_inference_arrival_during_snapshot_cancels_write_and_acquires(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.phase = "SNAPSHOTTING"
            coordinator._stop_llama = mock.Mock()
            coordinator._free_comfy = mock.Mock()
            coordinator._ensure_llama = mock.Mock()
            lease = coordinator.acquire("inference", "request-during-save", 60_000)
            coordinator._stop_llama.assert_called_once_with()
            coordinator._ensure_llama.assert_called_once_with()
            self.assertTrue(lease["fencingToken"])

    def test_shadow_expired_leases_never_signal_or_enter_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=False)
            coordinator._stop_mining = mock.Mock()
            coordinator._schedule_recovery_timer = mock.Mock()
            for holder in ("mining", "comfy"):
                coordinator.lease = {
                    "holder": holder, "workId": "expired", "epoch": coordinator.epoch,
                    "fencingToken": "a" * 32, "deadlineMs": int(time.time() * 1000) - 1,
                    "state": "ACTIVE", "metadata": {}, "acquiredAt": time.time(),
                }
                coordinator.state = "ACTIVE"
                coordinator.phase = "ACTIVE"
                coordinator._expire_lease()
                self.assertIsNone(coordinator.lease)
                self.assertEqual(coordinator.state, "IDLE")
            coordinator._stop_mining.assert_not_called()
            coordinator._schedule_recovery_timer.assert_not_called()

    def test_drain_rejects_new_work_and_active_release_cannot_stay_warm(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator._free_comfy = mock.Mock()
            coordinator._ensure_llama = mock.Mock()
            coordinator._stop_llama = mock.Mock()
            lease = coordinator.acquire("inference", "active-request", 60_000)
            drained = coordinator.begin_drain()
            self.assertTrue(drained["draining"])
            with self.assertRaises(LeaseConflict):
                coordinator.acquire("comfy", "late-job", 60_000)
            released = coordinator.release(lease["fencingToken"], lease["epoch"], keep_warm=True)
            self.assertTrue(released["gpuReleased"])
            self.assertEqual(coordinator.state, "IDLE")
            self.assertIsNone(coordinator.lease)
            coordinator._stop_llama.assert_called_once_with()

    def test_drain_holds_starting_mining_fence_until_late_pid_is_revoked(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.mining_not_before_ms = int(time.time() * 1000) - 1
            lease = coordinator.acquire("mining", "starting-miner", 60_000)
            drained = coordinator.begin_drain()
            self.assertEqual(drained["lease"]["state"], "STARTING")
            self.assertTrue(coordinator.lease["drainPending"])
            pid = os.getpid()
            start_time = "known-start"
            process_group_id = 4321
            coordinator._process_matches = lambda candidate, expected: candidate == pid and expected == start_time
            coordinator._stop_mining = mock.Mock()
            coordinator.lease["registrationDeadlineMs"] = int(time.time() * 1000) - 1
            with mock.patch("asset_gen_v7_lite_coordinator.os.getpgid", return_value=process_group_id):
                with self.assertRaises(StaleLease):
                    coordinator.renew(lease["fencingToken"], lease["epoch"], 60_000, {
                        "pid": pid,
                        "processStartTime": start_time,
                        "processGroupId": process_group_id,
                    })
            coordinator._stop_mining.assert_called_once()
            self.assertIsNone(coordinator.lease)
            self.assertEqual(coordinator.state, "IDLE")

    def test_foreground_waits_for_gated_miner_cancellation_ack_before_grant(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.mining_not_before_ms = int(time.time() * 1000) - 1
            mining = coordinator.acquire("mining", "gated-miner", 60_000)
            result = {}

            def acquire_foreground():
                result["lease"] = coordinator.acquire("comfy", "foreground-job", 60_000)

            foreground = threading.Thread(target=acquire_foreground, daemon=True)
            foreground.start()
            deadline = time.time() + 1
            while not coordinator.lease.get("cancelPending") and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue(coordinator.lease["cancelPending"])
            self.assertTrue(foreground.is_alive())

            pid = os.getpid()
            start_time = "known-start"
            process_group_id = 4321
            coordinator._process_matches = lambda candidate, expected: candidate == pid and expected == start_time
            coordinator._stop_mining = mock.Mock()
            with mock.patch("asset_gen_v7_lite_coordinator.os.getpgid", return_value=process_group_id):
                with self.assertRaises(StaleLease):
                    coordinator.renew(mining["fencingToken"], mining["epoch"], 60_000, {
                        "pid": pid, "processStartTime": start_time, "processGroupId": process_group_id,
                    })
            foreground.join(2)
            self.assertFalse(foreground.is_alive())
            self.assertTrue(result["lease"]["fencingToken"])
            self.assertEqual(coordinator.lease["holder"], "comfy")
            coordinator._stop_mining.assert_called_once()

    def test_enforcing_inference_preserves_comfy_cpu_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory, enforcing=True)
            coordinator.llama_pid = os.getpid()
            coordinator.llama_argv = ["unused"]
            coordinator.llama_running = lambda: True
            lease = coordinator.acquire("inference", "request-1", 60_000)
            self.assertEqual(http.free_payloads[-1], {"unload_models": True, "free_memory": False})
            coordinator.release(lease["fencingToken"], lease["epoch"], keep_warm=True)

    def test_foreground_release_gates_mining_for_at_least_thirty_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            lease = coordinator.acquire("comfy", "job-1", 60_000)
            coordinator.release(lease["fencingToken"], lease["epoch"], keep_warm=False)
            with self.assertRaises(LeaseConflict) as caught:
                coordinator.acquire("mining", "miner-1", 60_000)
            self.assertGreaterEqual(caught.exception.retry_after, 29)
            coordinator.mining_not_before_ms = int(time.time() * 1000) - 1
            mining = coordinator.acquire("mining", "miner-1", 60_000)
            self.assertTrue(mining["fencingToken"])

    def test_mining_waits_for_snapshot_after_grace_expires(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.lease = {
                "holder": "inference",
                "workId": "request-snapshot",
                "epoch": coordinator.epoch,
                "fencingToken": "a" * 32,
                "deadlineMs": int(time.time() * 1000) + 60_000,
                "warmDeadlineMs": int(time.time() * 1000) - 1,
                "state": "WARM",
                "metadata": {},
                "acquiredAt": time.time(),
            }
            coordinator.state = "WARM"
            coordinator.phase = "SNAPSHOTTING"
            coordinator.mining_not_before_ms = int(time.time() * 1000) - 1
            with self.assertRaises(LeaseConflict) as caught:
                coordinator.acquire("mining", "miner-snapshot", 60_000)
            self.assertEqual(caught.exception.retry_after, 5)
            self.assertEqual(coordinator.lease["holder"], "inference")
            self.assertEqual(coordinator.phase, "SNAPSHOTTING")

    def test_expired_active_foreground_fails_closed_and_can_renew(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            lease = coordinator.acquire("comfy", "job-1", 60_000)
            coordinator.lease["deadlineMs"] = int(time.time() * 1000) - 1
            with self.assertRaises(LeaseConflict):
                coordinator.acquire("inference", "request-1", 60_000)
            self.assertEqual(coordinator.status()["state"], "RECOVERING")
            self.assertIsNotNone(coordinator.recovery_timer)
            renewed = coordinator.renew(lease["fencingToken"], lease["epoch"], 60_000)
            self.assertTrue(renewed["fencingToken"])
            self.assertEqual(coordinator.status()["state"], "ACTIVE")

    def test_miner_start_registration_window_blocks_foreground(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            mining = coordinator.acquire("mining", "miner-start", 60_000)
            self.assertEqual(coordinator.lease["state"], "STARTING")
            with self.assertRaises(LeaseConflict) as caught:
                coordinator.acquire("comfy", "job-race", 60_000)
            self.assertLessEqual(caught.exception.retry_after, 5)
            coordinator.lease["registrationDeadlineMs"] = int(time.time() * 1000) - 1
            comfy = coordinator.acquire("comfy", "job-after-failed-start", 60_000)
            self.assertNotEqual(comfy["fencingToken"], mining["fencingToken"])

    def test_miner_registration_requires_matching_process_group(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            mining = coordinator.acquire("mining", "miner-register", 60_000)
            pid = os.getpid()
            start_time = "known-start-time"
            coordinator._process_matches = lambda candidate, expected: candidate == pid and expected == start_time
            with mock.patch("os.getpgid", return_value=1234):
                with self.assertRaises(StaleLease):
                    coordinator.renew(
                        mining["fencingToken"], mining["epoch"], 60_000,
                        {"pid": pid, "processStartTime": start_time, "processGroupId": 1235},
                    )
                coordinator.renew(
                    mining["fencingToken"], mining["epoch"], 60_000,
                    {"pid": pid, "processStartTime": start_time, "processGroupId": 1234},
                )
            self.assertEqual(coordinator.lease["state"], "ACTIVE")

    def test_persisted_epoch_fences_previous_active_lease_and_reconciles(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "coordinator.json"
            http = FakeHttp(directory)
            store = SnapshotStore(Path(directory) / "cache", "fp", min_free_bytes=0)
            first = GPUCoordinator(
                "http://127.0.0.1:8081",
                "http://127.0.0.1:8188",
                http,
                store,
                state_file=state_file,
                enabled=True,
                enforce_transitions=True,
            )
            old = first.acquire("comfy", "job-recover", 60_000)
            second = GPUCoordinator(
                "http://127.0.0.1:8081",
                "http://127.0.0.1:8188",
                http,
                store,
                state_file=state_file,
                enabled=True,
                enforce_transitions=True,
            )
            self.assertGreater(second.epoch, old["epoch"])
            self.assertEqual(second.status()["state"], "RECOVERING")
            replacement = second.reconcile("comfy", "job-recover")
            self.assertEqual(replacement["epoch"], second.epoch)
            self.assertNotEqual(replacement["fencingToken"], old["fencingToken"])

    def test_process_start_time_parser_handles_current_process(self):
        if not Path("/proc/self/stat").exists():
            self.skipTest("Linux /proc is required")
        start_time = GPUCoordinator._process_start_time(os.getpid())
        self.assertIsNotNone(start_time)
        self.assertTrue(GPUCoordinator._process_matches(os.getpid(), start_time))

    def test_stale_llama_pid_start_time_is_never_adopted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "llama.pid"
            state_file = root / "coordinator.json"
            pid_file.write_text(str(os.getpid()))
            Path(str(pid_file) + ".start").write_text("current-start")
            state_file.write_text(json.dumps({
                "epoch": 7,
                "llama": {"pid": os.getpid(), "processStartTime": "definitely-not-current"},
                "lease": None,
            }))
            http = FakeHttp(root)
            store = SnapshotStore(root / "cache", "fp", min_free_bytes=0)
            with mock.patch.object(GPUCoordinator, "_process_start_time", return_value="current-start"), \
                    mock.patch.object(GPUCoordinator, "_read_argv", return_value=["/tmp/llama-server"]), \
                    mock.patch.object(GPUCoordinator, "_terminate_matching_process", return_value=True):
                coordinator = GPUCoordinator(
                    "http://127.0.0.1:8081",
                    "http://127.0.0.1:8188",
                    http,
                    store,
                    llama_pid_file=pid_file,
                    state_file=state_file,
                    enabled=True,
                    enforce_transitions=True,
                )
            self.assertIsNone(coordinator.llama_pid)
            self.assertFalse(pid_file.exists())

    def test_new_valid_sidecar_with_stale_journal_is_terminated_not_orphaned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "llama.pid"
            state_file = root / "coordinator.json"
            pid = os.getpid()
            actual_start = "new-start"
            pid_file.write_text(str(pid))
            Path(str(pid_file) + ".start").write_text(actual_start)
            state_file.write_text(json.dumps({
                "epoch": 7,
                "llama": {"pid": pid, "processStartTime": "old-start"},
                "lease": None,
            }))
            store = SnapshotStore(root / "cache", "fp", min_free_bytes=0)
            with mock.patch.object(GPUCoordinator, "_process_start_time", return_value=actual_start), \
                    mock.patch.object(GPUCoordinator, "_read_argv", return_value=["/tmp/llama-server"]), \
                    mock.patch.object(GPUCoordinator, "_terminate_matching_process", return_value=True) as terminate:
                coordinator = GPUCoordinator(
                    "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root), store,
                    llama_pid_file=pid_file,
                    state_file=state_file,
                    enabled=True,
                    enforce_transitions=True,
                )
            terminate.assert_called_once_with(pid, actual_start)
            self.assertIsNone(coordinator.llama_pid)
            self.assertFalse(pid_file.exists())
            self.assertFalse(Path(str(pid_file) + ".start").exists())

    def test_immutable_llama_command_loads_when_process_is_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command_file = root / "llama-command.json"
            argv = ["/workspace/bin/llama-server", "--model", "/workspace/model.gguf", "--api-key", "secret"]
            command_file.write_text(json.dumps(argv))
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root),
                SnapshotStore(root / "cache", "fp", min_free_bytes=0),
                llama_command_file=command_file,
            )
            self.assertEqual(coordinator.llama_argv, argv)

    def test_coordinator_llama_restart_persists_and_removes_pid_identity_sidecars(self):
        class FakeProcess:
            pid = 4321
            returncode = None

            def poll(self):
                return None

            def wait(self, timeout=0):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "llama.pid"
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root),
                SnapshotStore(root / "cache", "fp", min_free_bytes=0),
                llama_pid_file=pid_file,
            )
            coordinator.llama_argv = ["/workspace/bin/llama-server", "--model", "/workspace/model.gguf"]
            running = {"value": True}
            with mock.patch("asset_gen_v7_lite_coordinator.subprocess.Popen", return_value=FakeProcess()), \
                    mock.patch.object(coordinator, "_process_start_time", return_value="777"), \
                    mock.patch.object(coordinator, "llama_running", return_value=False):
                coordinator._ensure_llama()
            self.assertEqual(pid_file.read_text().strip(), "4321")
            self.assertEqual(Path(str(pid_file) + ".start").read_text().strip(), "777")

            coordinator.llama_running = lambda: running["value"]
            coordinator._process_matches = lambda *_args: True

            def stop_process(_pid, _signal):
                running["value"] = False

            with mock.patch("asset_gen_v7_lite_coordinator.os.kill", side_effect=stop_process):
                coordinator._stop_llama()
            self.assertFalse(pid_file.exists())
            self.assertFalse(Path(str(pid_file) + ".start").exists())

    def test_comfy_release_waits_for_delayed_vram_drop(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory)
            samples = [[{"pid": 1, "usedBytes": 1024**3, "cmdline": "python ComfyUI/main.py", "cwd": "/workspace/ComfyUI"}], []]
            coordinator._gpu_processes = lambda *_args, **_kwargs: samples.pop(0) if samples else []
            coordinator._free_comfy(preserve_cache=True)
            self.assertEqual(len(http.free_payloads), 1)

    def test_comfy_release_accepts_verified_process_baseline_below_configured_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory)
            coordinator.comfy_release_vram_max_bytes = 2 * 1024**3
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 1, "usedBytes": 1024**3, "cmdline": "python ComfyUI/main.py", "cwd": "/workspace/ComfyUI"},
            ]
            coordinator._free_comfy(preserve_cache=True)
            self.assertEqual(len(http.free_payloads), 1)

    def test_live_comfy_baseline_is_accepted_by_three_gib_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory)
            live_baseline = 2_724_200_448
            coordinator.comfy_release_vram_max_bytes = 3 * 1024**3
            coordinator.comfy_idle_baseline_bytes = live_baseline
            coordinator.comfy_release_vram_headroom_bytes = 512 * 1024**2
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 1, "usedBytes": live_baseline, "cmdline": "python ComfyUI/main.py", "cwd": "/workspace/ComfyUI"},
            ]
            coordinator._free_comfy(preserve_cache=True)
            self.assertEqual(len(http.free_payloads), 1)
            self.assertEqual(coordinator._effective_comfy_release_vram_max_bytes(), 3 * 1024**3)

    def test_calibrated_baseline_headroom_rejects_retained_model_below_hard_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator.comfy_release_vram_max_bytes = 4 * 1024**3
            coordinator.comfy_idle_baseline_bytes = 2 * 1024**3
            coordinator.comfy_release_vram_headroom_bytes = 256 * 1024**2
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 1, "usedBytes": 3 * 1024**3, "cmdline": "python ComfyUI/main.py", "cwd": "/workspace/ComfyUI"},
            ]
            released, probe_ok = coordinator._wait_for_comfy_vram_release(0.01)
            self.assertFalse(released)
            self.assertTrue(probe_ok)
            self.assertEqual(coordinator._effective_comfy_release_vram_max_bytes(), 2304 * 1024**2)

    def test_full_free_rebaselines_post_workload_comfy_allocator_below_hard_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory)
            boot_baseline = 522_190_848
            post_workload_baseline = 2_722_103_296
            coordinator.comfy_release_vram_max_bytes = 3 * 1024**3
            coordinator.comfy_idle_baseline_bytes = boot_baseline
            coordinator.comfy_release_vram_headroom_bytes = 512 * 1024**2
            coordinator._wait_for_comfy_vram_release = mock.Mock(side_effect=[(False, True), (False, True)])
            coordinator._comfy_gpu_bytes = mock.Mock(return_value=post_workload_baseline)

            coordinator._free_comfy(preserve_cache=True)

            self.assertEqual(len(http.free_payloads), 2)
            self.assertEqual(http.free_payloads[-1], {"unload_models": True, "free_memory": True})
            self.assertEqual(coordinator.comfy_idle_baseline_bytes, post_workload_baseline)
            self.assertEqual(
                coordinator.last_comfy_baseline_adjustment,
                {
                    "previousBytes": boot_baseline,
                    "adjustedBytes": post_workload_baseline,
                    "hardCeilingBytes": 3 * 1024**3,
                    "atMs": mock.ANY,
                },
            )
            self.assertIsNone(coordinator.last_transition_error)

    def test_full_free_does_not_rebaseline_comfy_above_hard_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, http = self.make_coordinator(directory)
            boot_baseline = 512 * 1024**2
            coordinator.comfy_release_vram_max_bytes = 3 * 1024**3
            coordinator.comfy_idle_baseline_bytes = boot_baseline
            coordinator.comfy_release_vram_headroom_bytes = 512 * 1024**2
            coordinator._wait_for_comfy_vram_release = mock.Mock(side_effect=[(False, True), (False, True)])
            coordinator._comfy_gpu_bytes = mock.Mock(return_value=4 * 1024**3)

            with self.assertRaisesRegex(Exception, "ComfyUI retained GPU memory"):
                coordinator._free_comfy(preserve_cache=True)

            self.assertEqual(len(http.free_payloads), 2)
            self.assertEqual(coordinator.comfy_idle_baseline_bytes, boot_baseline)
            self.assertIsNone(coordinator.last_comfy_baseline_adjustment)
            self.assertEqual(coordinator.last_transition_error["allowedBytes"], 1024**3)

    def test_stopped_llama_readiness_requires_vram_handoff_precondition(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.llama_argv = ["/workspace/bin/llama-server", "--model", "/workspace/model.gguf"]
            coordinator.llama_running = lambda: False
            coordinator.comfy_release_vram_max_bytes = 3 * 1024**3
            used = {"bytes": 3500 * 1024**2}
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 1, "usedBytes": used["bytes"], "cmdline": "python ComfyUI/main.py", "cwd": "/workspace/ComfyUI"},
            ]
            self.assertEqual(coordinator.inference_readiness()["reason"], "comfy_vram_not_released")
            used["bytes"] = 2598 * 1024**2
            self.assertTrue(coordinator.inference_readiness()["ready"])

    def test_comfy_vram_probe_excludes_only_verified_llama_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator.llama_pid = 22
            coordinator.llama_start_time = "llama-start"
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 11, "usedBytes": 512 * 1024**2, "cmdline": "python main.py", "cwd": "/workspace/ComfyUI"},
                {"pid": 22, "usedBytes": 25 * 1024**3, "cmdline": "llama-server", "cwd": "/workspace/ComfyUI"},
            ]
            coordinator._process_matches = lambda pid, start: pid == 22 and start == "llama-start"
            self.assertEqual(coordinator._comfy_gpu_bytes(), 512 * 1024**2)

    def test_comfy_recovery_never_targets_verified_llama_with_comfy_cwd(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator.llama_pid = 22
            coordinator.llama_start_time = "llama-start"
            coordinator._gpu_processes = lambda *_args, **_kwargs: [
                {"pid": 22, "usedBytes": 25 * 1024**3, "cmdline": "llama-server", "cwd": "/workspace/ComfyUI"},
            ]
            coordinator._process_matches = lambda pid, start: pid == 22 and start == "llama-start"
            with mock.patch("os.kill") as kill:
                coordinator._terminate_comfy_gpu_processes()
            kill.assert_not_called()

    def test_same_inference_warm_reuse_skips_redundant_comfy_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.lease = {
                "holder": "inference", "workId": "first", "state": "WARM",
                "deadlineMs": int(time.time() * 1000) + 60_000,
                "warmDeadlineMs": int(time.time() * 1000) + 30_000,
                "metadata": {},
            }
            coordinator.llama_running = lambda: True
            coordinator._free_comfy = mock.Mock(side_effect=AssertionError("must not evict Comfy on warm KV reuse"))
            lease = coordinator.acquire("inference", "second", 60_000)
            self.assertEqual(lease["epoch"], coordinator.epoch)
            coordinator._free_comfy.assert_not_called()

    def test_failed_transition_resets_granting_state(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator._free_comfy = mock.Mock(side_effect=RuntimeError("baseline not released"))
            with self.assertRaisesRegex(RuntimeError, "baseline not released"):
                coordinator.acquire("inference", "request", 60_000)
            self.assertEqual(coordinator.state, "IDLE")
            self.assertEqual(coordinator.phase, "IDLE")
            self.assertIsNone(coordinator.lease)

    def test_comfy_release_fails_closed_when_gpu_probe_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator._gpu_processes = lambda *_args, **_kwargs: None
            with self.assertRaises(Exception):
                coordinator._free_comfy(preserve_cache=True)

    def test_shadow_restart_discards_active_comfy_without_signaling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            state.write_text(json.dumps({
                "epoch": 4,
                "lease": {"holder": "comfy", "workId": "job", "state": "ACTIVE", "metadata": {}, "deadlineMs": int(time.time() * 1000) + 60_000},
                "llama": {},
            }))
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root),
                SnapshotStore(root / "cache", "fp", min_free_bytes=0),
                state_file=state, enabled=True, enforce_transitions=False,
            )
            self.assertIsNone(coordinator.lease)
            self.assertEqual(coordinator.state, "IDLE")
            self.assertIsNone(coordinator.recovery_timer)

    def test_enforcing_restart_always_terminates_starting_mining(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            state.write_text(json.dumps({
                "epoch": 4,
                "lease": {
                    "holder": "mining", "workId": "miner-starting", "state": "STARTING",
                    "metadata": {"pid": 9876, "processStartTime": "123", "processGroupId": 9876},
                    "deadlineMs": int(time.time() * 1000) + 60_000,
                },
                "llama": {},
            }))
            with mock.patch.object(GPUCoordinator, "_stop_mining") as stop_mining:
                coordinator = GPUCoordinator(
                    "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root),
                    SnapshotStore(root / "cache", "fp", min_free_bytes=0),
                    state_file=state, enabled=True, enforce_transitions=True,
                )
            stop_mining.assert_called_once()
            self.assertIsNone(coordinator.lease)
            self.assertEqual(coordinator.metrics["recoveryCount"], 1)

    def test_enforcing_restart_evicts_unscoped_warm_inference(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            state.write_text(json.dumps({
                "epoch": 4,
                "lease": {
                    "holder": "inference", "workId": "finished", "state": "WARM",
                    "metadata": {}, "deadlineMs": int(time.time() * 1000) + 60_000,
                },
                "llama": {},
            }))
            with mock.patch.object(GPUCoordinator, "_stop_llama") as stop_llama:
                coordinator = GPUCoordinator(
                    "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(root),
                    SnapshotStore(root / "cache", "fp", min_free_bytes=0),
                    state_file=state, enabled=True, enforce_transitions=True,
                )
            stop_llama.assert_called_once_with()
            self.assertIsNone(coordinator.lease)
            self.assertEqual(coordinator.state, "IDLE")

    def test_newer_dirty_generation_survives_older_bounded_save_commit(self):
        class BlockingStore(SnapshotStore):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.entered = threading.Event()
                self.proceed = threading.Event()

            def commit(self, *args, **kwargs):
                self.entered.set()
                self.proceed.wait(2)
                return super().commit(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            store = BlockingStore(directory, "fp", min_free_bytes=0)
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(directory), store,
                snapshot_write=True,
            )
            handle = "v2." + "d" * 64
            coordinator.mark_cache_dirty(handle, {"promptTokens": 100, "coldPrefillMs": 1000, "reuseProbability": 0.3})
            self.assertTrue(coordinator._bounded_dirty_save("key_switch"))
            self.assertTrue(store.entered.wait(1))
            coordinator.mark_cache_dirty(handle, {"coldPrefillMs": 2000})
            store.proceed.set()
            deadline = time.time() + 2
            while coordinator.metrics["snapshotSaves"] == 0 and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue(coordinator.cache_dirty)
            self.assertEqual(coordinator.current_cache_metadata["coldPrefillMs"], 2000)

    def test_snapshot_commit_finishes_after_gpu_phase_is_released(self):
        class BlockingStore(SnapshotStore):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.entered = threading.Event()
                self.proceed = threading.Event()

            def commit(self, *args, **kwargs):
                self.entered.set()
                self.proceed.wait(2)
                return super().commit(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            store = BlockingStore(directory, "fp", min_free_bytes=0)
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(directory), store,
                snapshot_write=True,
            )
            handle = "v2." + "e" * 64
            coordinator.mark_cache_dirty(handle, {
                "promptTokens": 100, "coldPrefillMs": 1000, "reuseProbability": 0.3,
            })
            worker = threading.Thread(target=coordinator.save_current_snapshot, daemon=True)
            worker.start()
            self.assertTrue(store.entered.wait(1))
            self.assertNotEqual(coordinator.phase, "SNAPSHOTTING")
            store.proceed.set()
            worker.join(2)
            self.assertFalse(worker.is_alive())

    def test_warm_comfy_handoff_runs_one_verified_free_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.lease = {
                "holder": "comfy", "workId": "job-warm", "epoch": coordinator.epoch,
                "fencingToken": "a" * 32, "deadlineMs": int(time.time() * 1000) + 60_000,
                "state": "WARM", "metadata": {}, "acquiredAt": time.time(),
            }
            coordinator.state = "WARM"
            coordinator.phase = "WARM"
            coordinator._free_comfy = mock.Mock()
            coordinator._ensure_llama = mock.Mock()
            coordinator.acquire("inference", "request-after-comfy", 60_000)
            coordinator._free_comfy.assert_called_once_with(preserve_cache=True)

    def test_restore_benefit_gate_peeks_before_checksum_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            handle = "v1." + "e" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"x" * 1024 * 1024)
            store.commit(handle, temporary, {"coldPrefillMs": 10, "reuseProbability": 0.3})
            original_get = store.get
            store.get = mock.Mock(wraps=original_get)
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(directory), store,
                snapshot_restore=True,
            )
            old_validation = os.environ.get("QWEN_CACHE_VALIDATION_MB_PER_SECOND")
            os.environ["QWEN_CACHE_VALIDATION_MB_PER_SECOND"] = "1"
            try:
                state = coordinator.prepare_cache(handle)
            finally:
                if old_validation is None:
                    os.environ.pop("QWEN_CACHE_VALIDATION_MB_PER_SECOND", None)
                else:
                    os.environ["QWEN_CACHE_VALIDATION_MB_PER_SECOND"] = old_validation
            self.assertEqual(state.get("skipped"), "restore_not_beneficial")
            store.get.assert_not_called()

    def test_transient_restore_failure_retains_snapshot_with_backoff(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            handle = "v1." + "8" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"valid-slot")
            store.commit(handle, temporary, {"promptTokens": 1000, "coldPrefillMs": 1000, "restoreMs": 10})

            def transient_http(url, **_kwargs):
                if "action=restore" in url:
                    return 503, "application/json", b"temporarily unavailable"
                return 200, "application/json", b"{}"

            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", transient_http, store,
                snapshot_restore=True,
            )
            failed = coordinator.prepare_cache(handle)
            self.assertTrue(failed["restoreFailed"])
            self.assertIsNone(coordinator.current_cache_handle)
            self.assertIsNotNone(store.peek(handle))
            self.assertEqual(coordinator.prepare_cache(handle).get("skipped"), "restore_backoff")

    def test_successful_slot_load_without_cached_tokens_is_backed_off(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            handle = "v1." + "7" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"valid-slot")
            store.commit(handle, temporary, {
                "promptTokens": 1000,
                "coldPrefillMs": 1000,
                "restoreMs": 10,
                "reuseProbability": 0.3,
            })
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(directory), store,
                snapshot_restore=True,
            )
            self.assertEqual(coordinator.prepare_cache(handle)["classification"], "restored")
            result = coordinator.mark_cache_dirty(handle, {
                "classification": "restored",
                "promptTokens": 1000,
                "cachedTokens": 0,
                "coldPrefillMs": 1000,
            })
            self.assertTrue(result["restoreIneffective"])
            self.assertEqual(coordinator.metrics["snapshotRestoreIneffective"], 1)
            self.assertGreater(coordinator.current_cache_metadata["restoreBackoffUntil"], time.time())
            self.assertEqual(coordinator.current_cache_metadata["reuseHits"], 0)

    def test_ineffective_restore_backoff_disk_failure_never_fails_response(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory, enforcing=True)
            coordinator.current_cache_handle = "v1." + "6" * 64
            coordinator.current_cache_metadata = {"reuseObservations": 1, "reuseHits": 1}
            coordinator.snapshot_store.record_restore_failure = mock.Mock(side_effect=OSError("disk failure"))
            result = coordinator.mark_cache_dirty(coordinator.current_cache_handle, {
                "classification": "restored",
                "promptTokens": 1000,
                "cachedTokens": 0,
                "coldPrefillMs": 1000,
            })
            self.assertTrue(result["restoreIneffective"])
            self.assertEqual(coordinator.metrics["snapshotRestoreErrors"], 1)

    def test_backoff_survives_cold_refill_and_snapshot_rewrite(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(directory, "fp", min_free_bytes=0)
            handle = "v1." + "5" * 64
            temporary = store.temporary_filename(handle)
            (Path(directory) / temporary).write_bytes(b"valid-slot")
            store.commit(handle, temporary, {
                "promptTokens": 1000,
                "coldPrefillMs": 1000,
                "restoreMs": 10,
                "reuseProbability": 0.3,
            })
            store.record_restore_failure(handle, now=time.time())
            coordinator = GPUCoordinator(
                "http://127.0.0.1:8081", "http://127.0.0.1:8188", FakeHttp(directory), store,
                snapshot_write=True, snapshot_restore=True,
            )
            state = coordinator.prepare_cache(handle)
            self.assertEqual(state.get("skipped"), "restore_backoff")
            original_backoff = coordinator.current_cache_metadata["restoreBackoffUntil"]
            coordinator.mark_cache_dirty(handle, {
                "classification": "cold",
                "promptTokens": 1000,
                "cachedTokens": 0,
                "coldPrefillMs": 1000,
            })
            self.assertIsNotNone(coordinator.save_current_snapshot())
            rewritten = store.peek(handle)
            self.assertEqual(rewritten["restoreBackoffUntil"], original_backoff)
            coordinator.current_cache_handle = None
            coordinator.current_cache_metadata = {}
            self.assertEqual(coordinator.prepare_cache(handle).get("skipped"), "restore_backoff")

    def test_stop_never_signals_pid_after_start_time_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator, _ = self.make_coordinator(directory)
            coordinator.llama_pid = os.getpid()
            coordinator.llama_start_time = "stale-start-time"
            coordinator.current_cache_handle = "v1." + "f" * 64
            with mock.patch("os.kill") as kill:
                coordinator._stop_llama()
            kill.assert_not_called()
            self.assertIsNone(coordinator.llama_pid)
            self.assertIsNone(coordinator.current_cache_handle)


if __name__ == "__main__":
    unittest.main()

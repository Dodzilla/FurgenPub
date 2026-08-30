import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from asset_gen_v7_lite_tts import TTSResidency, TTSError, classify_workflow, GIB


class Owner:
    def __init__(self):
        self.enabled = self.enforce_transitions = True
        self.lock = threading.RLock()
        self.epoch = 11
        self.draining = False
        self.state = self.phase = "IDLE"
        self.lease = None
        self.mining_not_before_ms = 0
        self._persist_journal = mock.Mock()
        self._process_matches = mock.Mock(return_value=False)
        self._process_start_time = mock.Mock(return_value=None)
        self._gpu_processes = mock.Mock(return_value=[])
        self._comfy_gpu_bytes = mock.Mock(return_value=GIB)
        self._stop_mining = mock.Mock()
        self._free_comfy = mock.Mock()
        self._evict_warm = mock.Mock()
        self.llama_running = mock.Mock(return_value=False)


class ResidencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.owner = Owner()
        self.config = {"enabled": True, "canaryInstanceIds": ["canary"], "version": "test", "stateDir": self.tmp.name,
                       "coexistenceApproved": True, "routingApproved": True,
                       "measuredPeaks": {"warmupBytes": 20 * GIB, "idleBytes": 15 * GIB,
                                         "executionBytes": 19 * GIB, "miningBytes": 6 * GIB}}
        path = Path(self.tmp.name) / "config.json"
        path.write_text(json.dumps(self.config))
        env = mock.patch.dict(os.environ, {"SERVER_TYPE": "asset_gen_v7_lite", "DM_INSTANCE_ID": "canary"})
        env.start()
        self.addCleanup(env.stop)
        with mock.patch.object(TTSResidency, "_serve"), mock.patch.object(threading.Thread, "start"):
            self.tts = TTSResidency(self.owner, path)
        self.tts._gpu = mock.Mock(return_value=(32 * GIB, 25 * GIB))
        self.tts.not_before = 0

    def ready(self):
        self.tts.state = "ready"
        self.tts.identity = {"pid": 100, "processStartTime": 200, "processGroupId": 100}
        self.owner._process_matches.return_value = True
        self.owner._process_start_time.return_value = 200

    def test_memory_probe_failure_stops_miner_then_evicts(self):
        self.ready()
        self.owner.lease = {"holder": "mining", "metadata": {"pid": 200}}
        self.tts._gpu.side_effect = OSError("unavailable")
        calls = []
        self.owner._stop_mining.side_effect = lambda _: calls.append("mining")
        self.tts.evict = mock.Mock(side_effect=lambda _: calls.append("tts"))
        self.tts._monitor_memory()
        self.assertEqual(calls, ["mining", "tts"])
        self.assertEqual(self.tts.last_error, "gpu_probe_failed")

    def test_memory_envelope_overrun_stops_mining_even_above_safety_margin(self):
        self.ready()
        self.owner.lease = {"holder": "mining", "metadata": {"pid": 200}}
        self.owner._gpu_processes.return_value = [{"pid": 100, "usedBytes": 15 * GIB}, {"pid": 200, "usedBytes": 8 * GIB}]
        self.tts.evict = mock.Mock()
        self.tts._monitor_memory()
        self.owner._stop_mining.assert_called_once()
        self.tts.evict.assert_called_once_with("memory_budget_exceeded")

    def test_cooperative_cancel_retains_healthy_graphs(self):
        self.ready()
        self.tts.binding = {"requestId": "r", "fencingToken": "token"}
        self.tts.generating = True
        self.tts.evict = mock.Mock()
        with mock.patch("asset_gen_v7_lite_tts.rpc", side_effect=[{}, {"inFlight": None, "ready": True}]):
            with self.owner.lock:
                self.tts.before_release({"fencingToken": "token"})
        self.tts.evict.assert_not_called()
        self.assertFalse(self.tts.generating)
        self.assertIsNone(self.tts.binding)

    def test_unconfirmed_cancel_evicts_before_release(self):
        self.ready()
        self.tts.binding = {"requestId": "r", "fencingToken": "token"}
        self.tts.generating = True
        self.tts.evict = mock.Mock()
        with mock.patch("asset_gen_v7_lite_tts.rpc", side_effect=OSError("gone")):
            with self.owner.lock:
                self.tts.before_release({"fencingToken": "token"})
        self.tts.evict.assert_called_once_with("execution_release_before_stop")

    def test_cancel_health_wait_does_not_hold_coordinator_lock(self):
        self.ready()
        self.tts.binding = {"requestId": "r", "fencingToken": "token"}
        self.tts.generating = True
        acquired = threading.Event()
        def rpc(payload_path, payload, **kwargs):
            if payload["method"] == "health":
                def take_lock():
                    with self.owner.lock:
                        acquired.set()
                thread = threading.Thread(target=take_lock)
                thread.start()
                self.assertTrue(acquired.wait(1))
                thread.join()
                return {"inFlight": None, "ready": True}
            return {}
        with mock.patch("asset_gen_v7_lite_tts.rpc", side_effect=rpc):
            with self.owner.lock:
                self.tts.before_release({"fencingToken": "token"})

    def test_heartbeat_renews_only_existing_idle_permit(self):
        self.tts.idle_heartbeat(False)
        self.assertEqual(self.tts.state, "absent")
        self.assertIsNone(self.tts.permit)
        with mock.patch.object(threading.Thread, "start"):
            self.tts.idle_tick(False)
        self.tts.permit["deadline"] = time.monotonic() + 1
        self.tts.idle_heartbeat(False)
        self.assertTrue(self.tts.validate(self.tts.permit))
        self.tts.idle_heartbeat(True)
        self.assertEqual(self.tts.state, "absent")
        self.assertEqual(self.tts.failures, 0)

    def test_expired_permit_cannot_be_renewed(self):
        with mock.patch.object(threading.Thread, "start"):
            self.tts.idle_tick(False)
        permit = dict(self.tts.permit)
        self.tts.permit["deadline"] = time.monotonic() - 1
        self.tts.idle_heartbeat(False)
        self.assertFalse(self.tts.validate(permit))
        self.assertEqual(self.tts.state, "absent")
        self.assertEqual(self.tts.failures, 1)

    def test_canary_allowlist_is_required(self):
        with mock.patch.dict(os.environ, {"DM_INSTANCE_ID": "other"}):
            self.assertFalse(self.tts.enabled)

    def test_mining_keeps_ready_runtime_without_computing(self):
        self.ready()
        self.tts.evict = mock.Mock()
        self.tts.before_acquire("mining", {})
        self.tts.evict.assert_not_called()
        self.assertTrue(self.tts.idle_tick(False)["canMine"])
        self.assertIsNone(self.tts.permit)

    def test_unknown_workload_evicts_and_tts_keeps(self):
        self.ready()
        self.tts.evict = mock.Mock()
        self.tts.before_acquire("comfy", {"tts": {"runtimePolicy": "auto_fast_all"}})
        self.tts.evict.assert_not_called()
        self.tts.before_acquire("comfy", {})
        self.tts.evict.assert_called_once_with("memory_handoff")

    def test_mining_requires_measured_peak_and_approval(self):
        self.ready()
        self.tts.config["measuredPeaks"].pop("miningBytes")
        self.tts.evict = mock.Mock()
        self.tts.before_acquire("mining", {})
        self.tts.evict.assert_called_once_with("memory_handoff")

    def test_qwen_never_shares_residency(self):
        self.ready()
        self.tts.evict = mock.Mock()
        self.tts.before_acquire("inference", {"tts": {"runtimePolicy": "auto_fast_all"}})
        self.tts.evict.assert_called_once_with("memory_handoff")

    def test_idle_warmup_uses_fenced_permit_outside_lock(self):
        with mock.patch.object(threading.Thread, "start") as start:
            result = self.tts.idle_tick(False)
        self.assertFalse(result["canMine"])
        self.assertEqual(self.tts.state, "warming")
        self.assertTrue(self.tts.validate(self.tts.permit))
        start.assert_called_once()
        self.assertFalse(self.tts.validate({**self.tts.permit, "epoch": 10}))
        self.tts.last_demand_check -= 11
        self.assertFalse(self.tts.validate(self.tts.permit))

    def test_mining_does_not_preempt_warmup(self):
        self.tts.state = "warming"
        self.tts.evict = mock.Mock()
        with self.assertRaises(TTSError):
            self.tts.before_acquire("mining", {})
        self.tts.evict.assert_not_called()

    def test_foreground_demand_preempts_and_does_not_count_failure(self):
        self.tts.state = "warming"
        self.tts.permit = {"anything": "present"}
        self.assertFalse(self.tts.idle_tick(True)["canMine"])
        self.assertEqual(self.tts.state, "absent")
        self.assertEqual(self.tts.failures, 0)
        self.assertEqual(self.tts.backoff, 60)
        self.assertTrue(self.tts.idle_tick(False)["canMine"])

    def test_preemption_backoff_caps_without_disabling(self):
        for expected in (60, 120, 240, 300, 300):
            self.tts.evict("preempt", preempt=True)
            self.assertEqual(self.tts.backoff, expected)
        self.assertFalse(self.tts.disabled)

    def test_three_real_failures_disable_but_allow_mining(self):
        for _ in range(3):
            self.tts._failure("warmup_timeout")
        self.assertTrue(self.tts.disabled)
        self.assertTrue(self.tts.idle_tick(False)["canMine"])

    def test_active_foreground_and_snapshot_block_setup(self):
        self.owner.lease = {"holder": "comfy", "state": "ACTIVE"}
        self.assertFalse(self.tts.idle_tick(False)["canMine"])
        self.owner.lease = None
        self.owner.phase = "SNAPSHOTTING"
        self.assertFalse(self.tts.idle_tick(False)["canMine"])
        self.owner._free_comfy.assert_not_called()

    def test_draining_does_not_restart_warmup_or_stop_foreground(self):
        self.owner.draining = True
        self.owner.lease = {"holder": "mining", "state": "ACTIVE"}
        self.assertFalse(self.tts.idle_tick(False)["canMine"])
        self.owner._stop_mining.assert_not_called()

    def test_cooldown_keeps_existing_miner_running(self):
        self.owner.lease = {"holder": "mining", "state": "ACTIVE"}
        self.tts.retry_at = time.monotonic() + 100
        self.assertTrue(self.tts.idle_tick(False)["canMine"])
        self.owner._stop_mining.assert_not_called()

    def test_generation_requires_matching_current_fence(self):
        lease = {"holder": "comfy", "state": "ACTIVE", "epoch": 11, "fencingToken": "token", "workId": "job",
                 "deadlineMs": time.time() * 1000 + 10000, "metadata": {"tts": {"requestId": "request"}}}
        self.owner.lease = lease
        self.tts.bind(lease)
        self.assertEqual(self.tts.bridge({"method": "route", "requestId": "request"})["backend"], "comfy")
        for change in ({"epoch": 10}, {"fencingToken": "bad"}, {"workId": "other"}):
            self.assertFalse(self.tts.validate({"kind": "generate", **lease, **change}))
        self.owner.lease["state"] = "RECOVERING"
        with self.assertRaises(TTSError):
            self.tts.bridge({"method": "route", "requestId": "request"})

    def test_pid_reuse_is_never_signalled(self):
        self.ready()
        self.owner._process_start_time.return_value = 999
        with mock.patch("os.killpg") as kill:
            with self.assertRaises(TTSError):
                self.tts.evict("test")
            kill.assert_not_called()
        self.assertEqual(self.tts.state, "evicting")


class ClassificationTest(unittest.TestCase):
    def workflow(self):
        return {
            "1": {"class_type": "BreezeTTS2LoadModel", "inputs": {
                "model": "official bf16 (BreezeBlue/Breeze-TTS-2)", "dtype": "bf16", "device": "auto",
                "attention": "sdpa", "decode_mode": "cuda_graphs", "download_if_missing": False,
                "runtime_policy": "auto_fast_all"}},
            "2": {"class_type": "BreezeTTS2VoiceDesign", "inputs": {"breeze_model": ["1", 0]}},
            "7": {"class_type": "SaveAudioOpus", "inputs": {"audio": ["2", 0], "quality": "128k"}},
        }

    def test_full_graph_classification_rejects_hidden_gpu_nodes(self):
        workflow = self.workflow()
        self.assertIsNotNone(classify_workflow(workflow))
        workflow["hidden"] = {"class_type": "UNETLoader", "inputs": {}}
        self.assertIsNone(classify_workflow(workflow))

    def test_legacy_and_int8_are_not_automatically_opted_in(self):
        for change in ({"runtime_policy": "comfy"}, {"model": "int8 hybrid (recommended)"}, {"decode_mode": "eager"}):
            workflow = self.workflow()
            workflow["1"]["inputs"].update(change)
            self.assertIsNone(classify_workflow(workflow))

    def test_wrong_output_connection_is_not_safe(self):
        workflow = self.workflow()
        workflow["7"]["inputs"]["audio"] = ["1", 0]
        self.assertIsNone(classify_workflow(workflow))

    def test_parameter_changes_change_profile_identity(self):
        first = self.workflow()
        second = copy.deepcopy(first)
        second["2"]["inputs"]["text"] = "different"
        self.assertNotEqual(classify_workflow(first)["graphSha256"], classify_workflow(second)["graphSha256"])


if __name__ == "__main__":
    unittest.main()

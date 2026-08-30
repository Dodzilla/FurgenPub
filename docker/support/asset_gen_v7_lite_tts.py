"""Optional, fenced TTS residency; never an additional concurrent GPU owner.

The coordinator owns this object. Slow compilation runs on a child process,
outside its lock. Only the private bridge proxies generation to that child.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import signal
import socket
import socketserver
import subprocess
import threading
import time
import uuid
from pathlib import Path

LOG = logging.getLogger("tts-residency")
LOG.setLevel(logging.INFO)
if not LOG.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
    LOG.addHandler(handler)
    LOG.propagate = False
GIB = 1024**3
MAX_RPC_BYTES = 32 * 1024**2
MODES = {"BreezeTTS2VoiceClone", "BreezeTTS2VoiceDesign", "BreezeTTS2VoiceDirection"}
SAMPLING_KEYS = {"temperature", "top_k", "top_p", "repetition_penalty",
                 "depth_temperature", "depth_top_k", "depth_top_p"}


class TTSError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def rpc(path, payload, timeout=5):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(path))
        client.sendall(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        with client.makefile("rb") as stream:
            raw = stream.readline(MAX_RPC_BYTES + 1)
        if len(raw) > MAX_RPC_BYTES or not raw.endswith(b"\n"):
            raise TTSError("invalid_response", "Invalid TTS runtime response")
        response = json.loads(raw)
        if not response.get("ok"):
            error = response.get("error") or {}
            raise TTSError(error.get("code", "runtime_failure"), error.get("message", "TTS runtime failed"))
        return response.get("result", {})


def classify_workflow(workflow):
    """Strict full-graph classification, not a user-supplied memory hint."""
    if not isinstance(workflow, dict) or not workflow:
        return None
    nodes = list(workflow.values())
    if any(not isinstance(n, dict) or not isinstance(n.get("inputs"), dict) for n in nodes):
        return None
    loaders = [(key, n) for key, n in workflow.items() if n.get("class_type") == "BreezeTTS2LoadModel"]
    generators = [(key, n) for key, n in workflow.items() if n.get("class_type") in MODES]
    if len(loaders) != 1 or len(generators) != 1:
        return None
    lid, load = loaders[0]
    gid, gen = generators[0]
    inputs = load["inputs"]
    expected = {"model": "official bf16 (BreezeBlue/Breeze-TTS-2)", "dtype": "bf16", "device": "auto",
                "attention": "sdpa", "decode_mode": "cuda_graphs", "download_if_missing": False,
                "runtime_policy": "auto_fast_all"}
    if any(inputs.get(k) != v for k, v in expected.items()):
        return None
    if gen["inputs"].get("breeze_model") != [lid, 0]:
        return None
    allowed = {"LoadAudio", "SaveAudioOpus", "BreezeTTS2LoadModel", gen["class_type"]}
    if any(n.get("class_type") not in allowed for n in nodes):
        return None
    outputs = [n for n in nodes if n.get("class_type") == "SaveAudioOpus"]
    refs = [(key, n) for key, n in workflow.items() if n.get("class_type") == "LoadAudio"]
    if len(outputs) != 1 or outputs[0]["inputs"].get("audio") != [gid, 0] or outputs[0]["inputs"].get("quality") != "128k":
        return None
    if gen["class_type"] == "BreezeTTS2VoiceDesign":
        if refs or len(nodes) != 3:
            return None
    elif len(refs) != 1 or len(nodes) != 4 or gen["inputs"].get("reference_audio") != [refs[0][0], 0]:
        return None
    if gen["class_type"] == "BreezeTTS2VoiceDirection" and gen["inputs"].get("stitch_reference") != "none":
        return None
    return {"graphSha256": hashlib.sha256(json.dumps(workflow, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "mode": gen["class_type"], "runtimePolicy": "auto_fast_all"}


class _Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


class TTSResidency:
    def __init__(self, coordinator, config_path, previous=None):
        self.owner = coordinator
        self.config_path = Path(config_path)
        self.config = self._read_config()
        self.root = Path(self.config.get("stateDir", "/workspace/.fcs/tts"))
        self.socket = self.root / "bridge.sock"
        self.child_socket = self.root / "runtime.sock"
        self.state = "absent"
        self.process = None
        self.identity = dict((previous or {}).get("identity") or {})
        self.permit = None
        self.binding = None
        self.generating = False
        self.generation_request_id = None
        same_failure_scope = ((previous or {}).get("version") == self.config.get("version")
                              and (previous or {}).get("failureResetToken") == self.config.get("failureResetToken"))
        self.failures = int((previous or {}).get("failures", 0)) if same_failure_scope else 0
        self.policy_fingerprint = hashlib.sha256(json.dumps({k: self.config.get(k) for k in
            ("version", "profile", "failureResetToken", "measuredRuntimeVersion", "measuredProfile", "measuredRuntimeFingerprint", "miningFingerprint", "measuredPeaks")}, sort_keys=True).encode()).hexdigest()
        self.policy_failure = (previous or {}).get("policyFailure") if (previous or {}).get("policyFingerprint") == self.policy_fingerprint else None
        self.disabled = self.failures >= 3 or bool(self.policy_failure)
        self.backoff = 0
        self.retry_at = 0.0
        self.last_error = None
        self.health = {}
        self.transitions = []
        self.last_demand_check = 0.0
        self.not_before = time.monotonic() + 30
        self.server = None
        self.stopped = threading.Event()
        coordinator.tts = self
        # A process from another epoch is never silently adopted.
        if self.identity:
            self.evict("coordinator_restart")
        if self.enabled:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.root, 0o700)
            self._serve()
            threading.Thread(target=self._watchdog, daemon=True, name="tts-residency-watchdog").start()

    def _read_config(self):
        try:
            config = json.loads(self.config_path.read_text())
            return config if isinstance(config, dict) else {}
        except FileNotFoundError:
            return {}

    @property
    def enabled(self):
        instance = os.environ.get("DM_INSTANCE_ID") or os.environ.get("VAST_CONTAINERLABEL", "").removeprefix("C.")
        return bool(self.config.get("enabled") and self.owner.enabled and self.owner.enforce_transitions
                    and os.environ.get("SERVER_TYPE") == "asset_gen_v7_lite"
                    and str(instance) in self.config.get("canaryInstanceIds", [])
                    and time.time() * 1000 < self.config.get("validUntilMs", float("inf")))

    def journal(self):
        return {"identity": self.identity, "failures": self.failures, "version": self.config.get("version"),
                "failureResetToken": self.config.get("failureResetToken"),
                "policyFailure": self.policy_failure, "policyFingerprint": self.policy_fingerprint}

    def status(self):
        return {"enabled": self.enabled, "state": "disabled" if self.disabled else self.state,
                "identity": self.identity, "version": self.config.get("version"),
                "profile": self.config.get("profile"), "failures": self.failures,
                "retryAfterSeconds": max(0, round(self.retry_at - time.monotonic())),
                "lastError": self.last_error, "health": self.health,
                "generating": self.generating, "transitions": self.transitions[-20:]}

    def _save(self):
        self.owner._persist_journal()

    def _event(self, kind, **values):
        self.transitions.append({"kind": kind, "at": time.time(), **values})
        self.transitions = self.transitions[-50:]
        LOG.info("TTS residency %s", json.dumps(self.transitions[-1], sort_keys=True))

    def _gpu(self):
        proc = subprocess.run(["nvidia-smi", "--query-gpu=memory.total,memory.free", "--format=csv,noheader,nounits"],
                              capture_output=True, text=True, timeout=2, check=True)
        rows = proc.stdout.strip().splitlines()
        if len(rows) != 1:
            raise TTSError("gpu_probe_failed", "TTS canary requires one GPU")
        total, free = (int(v.strip()) * 1024**2 for v in rows[0].split(","))
        return total, free

    def _fits(self, budget):
        total, free = self._gpu()
        baseline = self.owner._comfy_gpu_bytes()
        if baseline is None or not budget or budget < 0:
            return False
        return budget + baseline + 2 * GIB <= total and free >= 2 * GIB

    def _budget(self, key):
        peak = self.config.get("measuredPeaks", {}).get(key)
        return math.ceil(float(peak) * 1.15) if isinstance(peak, (int, float)) and peak > 0 else 0

    def _policy_mismatch(self, reason):
        self.evict(reason)
        self.policy_failure = self.last_error = reason
        self.disabled = True
        self._event("policy_disabled", reason=reason)
        self._save()

    def _runtime_measurement_matches(self):
        expected = self.config.get("measuredRuntimeFingerprint")
        return bool(expected and expected == self.health.get("fingerprint"))

    def _mining_measurement_matches(self, metadata):
        """Verify the gated launch before its coordinator registration permits CUDA."""
        expected = self.config.get("miningFingerprint") or {}
        try:
            pid = int(metadata["pid"])
            if not self.owner._process_matches(pid, metadata.get("processStartTime")):
                return False
            parts = (Path("/proc") / str(pid) / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
            if len(parts) > 4 and parts[1] == b"-c":
                gate = Path(os.fsdecode(parts[3]))
                if (hashlib.sha256(parts[2]).hexdigest() != expected.get("gateWrapperSha256")
                        or gate.parent != Path("/workspace/.fcs/prl/launch_gates") or gate.suffix != ".gate"):
                    return False
                parts = parts[4:]
            binary = Path("/workspace/.fcs/prl/prl_gpu_miner")
            return bool(parts and os.fsdecode(parts[0]) == str(binary)
                        and hashlib.sha256(b"\0".join(parts) + b"\0").hexdigest() == expected.get("commandSha256")
                        and hashlib.sha256(binary.read_bytes()).hexdigest() == expected.get("binarySha256"))
        except (OSError, KeyError, ValueError):
            return False

    def before_mining_registration(self, metadata):
        if self.state != "ready" or not self.config.get("coexistenceApproved"):
            return
        if not self._runtime_measurement_matches() or not self._mining_measurement_matches(metadata):
            self.owner._stop_mining(metadata)
            self._policy_mismatch("unmeasured_mining_configuration")
            raise TTSError("unmeasured_mining_configuration", "Miner/runtime configuration no longer matches its memory measurement")

    def _failure(self, code):
        self.failures += 1
        self.last_error = code
        self.disabled = self.failures >= 3
        self.retry_at = time.monotonic() + 300
        self._event("failure", code=code, failures=self.failures)
        self._save()

    def _identity_alive(self):
        return bool(self.identity and self.owner._process_matches(self.identity.get("pid"), self.identity.get("processStartTime")))

    @staticmethod
    def _group_alive(pgid):
        # Include CPU compilation children: no runnable member may survive an
        # eviction merely because it has not allocated CUDA memory yet.
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if int(fields[2]) == pgid and fields[0] != "Z":
                    return True
            except (FileNotFoundError, ProcessLookupError):
                continue
        return False

    def evict(self, reason, preempt=False):
        started = time.monotonic()
        self.permit = None
        identity = dict(self.identity)
        self.state = "evicting"
        self._save()
        if identity:
            pid, pgid = identity.get("pid"), identity.get("processGroupId")
            actual_start = self.owner._process_start_time(pid)
            if actual_start is not None and actual_start != identity.get("processStartTime"):
                raise TTSError("process_identity_mismatch", "TTS PID was reused; refusing to signal an unrelated process")
            if self.owner._process_matches(pid, identity.get("processStartTime")):
                if not pgid or os.getpgid(pid) != pgid or pgid == os.getpgrp():
                    raise TTSError("process_identity_mismatch", "Cannot safely evict TTS process")
                os.killpg(pgid, signal.SIGTERM)
                deadline = time.monotonic() + 1
                while self.owner._process_matches(pid, identity.get("processStartTime")) and time.monotonic() < deadline:
                    if self.process:
                        self.process.poll()
                    time.sleep(0.025)
            # A dead leader can leave compilation children in its process group.
            if pgid and pgid == pid and pgid != os.getpgrp():
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 10
            while True:
                if self.process:
                    self.process.poll()
                processes = self.owner._gpu_processes()
                def owned(p):
                    if p["pid"] == pid:
                        return True
                    try:
                        return os.getpgid(p["pid"]) == pgid
                    except ProcessLookupError:
                        return False
                remaining = None if processes is None else [p for p in processes if owned(p)]
                if remaining == [] and not self.owner._process_matches(pid, identity.get("processStartTime")) and not self._group_alive(pgid):
                    break
                if time.monotonic() >= deadline:
                    self.last_error = "tts_vram_not_released"
                    self._save()
                    raise TTSError("tts_vram_not_released", "TTS process release was not confirmed")
                time.sleep(0.05)
        self.identity = {}
        self.process = None
        self.health = {}
        self.state = "absent"
        if preempt:
            self.backoff = min(300, max(60, self.backoff * 2))
            self.retry_at = time.monotonic() + self.backoff
        self._event("evicted", reason=reason, durationMs=round((time.monotonic() - started) * 1000))
        self._save()

    def before_acquire(self, holder, metadata):
        if not self.enabled and not self.identity:
            return
        if self.generating:
            raise TTSError("tts_busy", "TTS execution has not stopped")
        eligible = bool((metadata or {}).get("tts", {}).get("runtimePolicy") == "auto_fast_all")
        eligible = eligible and bool(self.config.get("routingApproved") or self.config.get("diagnosticsEnabled"))
        if self.state == "warming":
            if holder == "mining":
                raise TTSError("tts_warming", "Idle warmup has not completed")
            self.evict("foreground_preemption", preempt=True)
        if self.state == "evicting":
            self.evict("pending_eviction")
        if self.state == "ready":
            if not self._identity_alive():
                self.evict("runtime_not_alive")
                self._failure("runtime_crash")
                return
            if holder == "mining":
                if self.config.get("diagnosticsEnabled") and not self.config.get("coexistenceApproved"):
                    raise TTSError("tts_diagnostics_hold", "Mining waits for the diagnostic memory gate")
                budget = self._budget("idleBytes") + self._budget("miningBytes")
                keep = self.config.get("coexistenceApproved") and self._runtime_measurement_matches() and self._budget("idleBytes") and self._budget("miningBytes") and self._fits(budget)
            else:
                execution_budget = self._budget("executionBytes")
                if not execution_budget and self.config.get("diagnosticsEnabled") and not self.config.get("coexistenceApproved"):
                    execution_budget = int(self.config.get("diagnosticExecutionBudgetBytes", 0))
                keep = holder == "comfy" and eligible and self._fits(execution_budget)
            if not keep:
                self.evict("memory_handoff")

    def bind(self, lease):
        self.binding = None
        tts = lease.get("metadata", {}).get("tts")
        if lease["holder"] != "comfy" or not isinstance(tts, dict):
            return
        self.binding = {"requestId": tts["requestId"], "workId": lease["workId"], "epoch": lease["epoch"],
                        "fencingToken": lease["fencingToken"], "backend": "sidecar" if self.state == "ready" else "comfy"}

    def validate(self, permit):
        with self.owner.lock:
            if not self.enabled or self.owner.draining or int(permit.get("epoch", -1)) != self.owner.epoch:
                return False
            if permit.get("kind") == "warmup":
                expected = self.permit or {}
                return bool(expected and self.state == "warming" and time.monotonic() < expected["deadline"]
                            and time.monotonic() - self.last_demand_check < 10
                            and all(permit.get(k) == expected.get(k) for k in ("epoch", "fencingToken", "workId")))
            lease = self.owner.lease or {}
            return bool(permit.get("kind") == "generate" and lease.get("holder") == "comfy"
                        and lease.get("state") == "ACTIVE" and lease.get("deadlineMs", 0) > time.time() * 1000
                        and all(permit.get(k) == lease.get(k) for k in ("epoch", "fencingToken", "workId")))

    def idle_tick(self, demand):
        with self.owner.lock:
            self.last_demand_check = time.monotonic()
            if demand:
                self.not_before = time.monotonic() + 30
                if self.state == "warming":
                    self.evict("queued_foreground", preempt=True)
                return {"canMine": False, **self.status()}
            if not self.enabled or self.disabled:
                return {"canMine": True, **self.status()}
            if self.state == "warming":
                self.idle_heartbeat(False)
                return {"canMine": False, **self.status()}
            if self.state == "ready":
                return {"canMine": bool(self.config.get("coexistenceApproved")), **self.status()}
            if self.state == "evicting":
                raise TTSError("tts_vram_not_released", "TTS eviction remains unconfirmed")
            lease = self.owner.lease or {}
            if self.owner.draining or self.owner.phase == "SNAPSHOTTING":
                return {"canMine": False, **self.status()}
            if time.monotonic() < max(self.retry_at, self.not_before) or time.time() * 1000 < self.owner.mining_not_before_ms:
                return {"canMine": True, **self.status()}
            if not self.config.get("coexistenceApproved") and not self.config.get("diagnosticsEnabled"):
                return {"canMine": True, **self.status()}
            if (self.config.get("coexistenceApproved") or self.config.get("routingApproved")) and (
                self.config.get("measuredRuntimeVersion") != self.config.get("version")
                or self.config.get("measuredProfile") != self.config.get("profile")
            ):
                self._policy_mismatch("warmup_measurement_mismatch")
                return {"canMine": True, **self.status()}
            if lease.get("state") in {"ACTIVE", "STARTING", "RECOVERING"}:
                if lease.get("holder") != "mining" or lease.get("state") != "ACTIVE":
                    return {"canMine": False, **self.status()}
                self.owner._stop_mining(lease.get("metadata", {}))
                self.owner.lease = None
                lease = {}
            # After the existing foreground grace, release other runtimes first.
            if lease:
                self.owner._evict_warm(lease["holder"])
                self.owner.lease = None
            if self.owner.llama_running():
                self.owner._stop_llama()
            self.owner._free_comfy(preserve_cache=True)
            budget = self._budget("warmupBytes")
            if not budget and self.config.get("diagnosticsEnabled"):
                budget = int(self.config.get("diagnosticWarmupBudgetBytes", 0))
            if not self._fits(budget):
                self.retry_at = time.monotonic() + 300
                return {"canMine": True, **self.status()}
            self.permit = {"kind": "warmup", "epoch": self.owner.epoch, "fencingToken": uuid.uuid4().hex,
                           "workId": "tts-warmup-" + uuid.uuid4().hex, "deadline": time.monotonic() + 10,
                           "started": time.monotonic()}
            self.state = "warming"
            self.owner.state = self.owner.phase = "IDLE"
            permit = dict(self.permit)
            self._save()
            threading.Thread(target=self._warm, args=(permit,), daemon=True, name="tts-warmup").start()
            return {"canMine": False, **self.status()}

    def _warm(self, permit):
        try:
            with self.owner.lock:
                if not self.permit or self.permit["fencingToken"] != permit["fencingToken"]:
                    return
                self.child_socket.unlink(missing_ok=True)
                argv = [self.config["python"], self.config["runtimeScript"], "--socket", str(self.child_socket),
                        "--config", str(self.config_path), "--coordinator", self.config.get("coordinatorUrl", "http://127.0.0.1:8189")]
                with (self.root / "runtime.log").open("ab") as log:
                    self.process = subprocess.Popen(argv, stdout=log, stderr=log, start_new_session=True, cwd=self.root)
                self.identity = {"pid": self.process.pid, "processStartTime": self.owner._process_start_time(self.process.pid),
                                 "processGroupId": self.process.pid}
                if self.identity["processStartTime"] is None:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=5)
                    self.identity = {}
                    raise TTSError("process_registration_failed", "Cannot register TTS process identity")
                self._save()
            for _ in range(100):
                if self.child_socket.exists():
                    break
                if not self.validate(permit):
                    raise TTSError("stale_lease", "Warmup permit revoked")
                time.sleep(0.05)
            result = rpc(self.child_socket, {"method": "warmup", "permit": permit}, timeout=120)
            with self.owner.lock:
                if not self.validate(permit):
                    if self.permit and self.permit.get("fencingToken") == permit["fencingToken"]:
                        self.evict("warmup_permit_lost", preempt=True)
                    return
                self.health = result
                if (self.config.get("coexistenceApproved") or self.config.get("routingApproved")) and not self._runtime_measurement_matches():
                    self._policy_mismatch("runtime_measurement_mismatch")
                    return
                self.state = "ready"
                self.permit = None
                self.backoff = 0
                self._event("ready", profile=self.config.get("profile"))
                self._save()
        except Exception as error:
            with self.owner.lock:
                if self.permit and self.permit["fencingToken"] == permit["fencingToken"]:
                    self._event("warmup_error", code=getattr(error, "code", "warmup_failed"), message=str(error)[:240])
                    self.evict("warmup_failed")
                    self._failure(getattr(error, "code", "warmup_failed"))

    def _stop_request(self, reason):
        # Caller holds one coordinator lock level. The foreground lease remains
        # ACTIVE while we wait, so competing acquire calls still cannot run.
        identity = dict(self.identity)
        request_id = (self.binding or {}).get("requestId")
        confirmed = False
        self.owner.lock.release()
        try:
            try:
                rpc(self.child_socket, {"method": "cancel", "requestId": request_id}, timeout=0.3)
            except Exception:
                pass
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                try:
                    health = rpc(self.child_socket, {"method": "health"}, timeout=0.2)
                    if health.get("inFlight") is None and health.get("ready") is True:
                        confirmed = True
                        break
                except Exception:
                    break
                time.sleep(0.025)
        finally:
            self.owner.lock.acquire()
        if self.identity == identity and not confirmed:
            self.evict(reason)
        if self.generation_request_id in {None, request_id}:
            self.generating = False
            self.generation_request_id = None

    def idle_heartbeat(self, demand):
        with self.owner.lock:
            self.last_demand_check = time.monotonic()
            if demand:
                self.not_before = time.monotonic() + 30
                if self.state == "warming":
                    self.evict("queued_foreground", preempt=True)
            elif self.state == "warming" and self.permit and self.enabled and not self.owner.draining:
                if time.monotonic() >= self.permit["deadline"]:
                    self.evict("idle_permit_expired")
                    self._failure("idle_permit_expired")
                else:
                    self.permit["deadline"] = time.monotonic() + 10
            return {"state": self.state, "enabled": self.enabled}

    def _monitor_memory(self):
        try:
            _, free = self._gpu()
            processes = self.owner._gpu_processes()
            if processes is None:
                raise TTSError("gpu_probe_failed", "Cannot verify resident GPU memory")
            resident = self._group_gpu_bytes(processes, self.identity)
            mining = (self.owner.lease or {}).get("holder") == "mining"
            key = "warmupBytes" if self.state == "warming" else "executionBytes" if self.generating else "idleBytes"
            budget = self._budget(key)
            if not budget and self.config.get("diagnosticsEnabled"):
                budget = int(self.config.get("diagnosticWarmupBudgetBytes" if self.state == "warming" else "diagnosticExecutionBudgetBytes", 0))
            # Diagnostics measure envelopes before coexistence can be approved.
            exceeded = free < 2 * GIB or (budget and resident > budget)
            if mining:
                miner_budget = self._budget("miningBytes")
                miner = self._group_gpu_bytes(processes, self.owner.lease.get("metadata") or {})
                exceeded = exceeded or not miner_budget or miner > miner_budget
            if not exceeded:
                return
            reason = "memory_budget_exceeded"
        except Exception:
            reason = "gpu_probe_failed"
        # Unknown memory ownership is a safety failure, never just a log message.
        if (self.owner.lease or {}).get("holder") == "mining":
            self.owner._stop_mining(self.owner.lease.get("metadata", {}))
            self.owner.lease = None
            self.owner.state = self.owner.phase = "IDLE"
        self.evict(reason)
        self._failure(reason)

    @staticmethod
    def _group_gpu_bytes(processes, identity):
        total = 0
        for process in processes:
            if process["pid"] == identity.get("pid"):
                total += process["usedBytes"]
            elif identity.get("processGroupId"):
                try:
                    if os.getpgid(process["pid"]) == identity["processGroupId"]:
                        total += process["usedBytes"]
                except ProcessLookupError:
                    # It exited between NVML and procfs; the next probe reconciles it.
                    continue
        return total

    def _watchdog(self):
        while not self.stopped.wait(1):
            try:
                with self.owner.lock:
                    if not self.enabled and self.identity:
                        self.evict("feature_disabled")
                    if self.state == "warming" and (not self.permit or time.monotonic() > self.permit["deadline"]
                                                     or time.monotonic() - self.permit["started"] > 120):
                        self.evict("warmup_timeout")
                        self._failure("warmup_timeout")
                    if self.identity and self.state in {"ready", "warming"} and not self._identity_alive():
                        self.evict("runtime_crash")
                        self._failure("runtime_crash")
                    if self.identity:
                        self._monitor_memory()
            except Exception:
                LOG.exception("TTS residency watchdog failed closed")

    def _binding(self, request_id):
        binding = self.binding or {}
        if binding.get("requestId") != request_id or not self.validate({"kind": "generate", **binding}):
            raise TTSError("stale_lease", "TTS request does not hold the current Comfy lease")
        return dict(binding)

    def bridge(self, request):
        method = request.get("method")
        with self.owner.lock:
            binding = self._binding(request.get("requestId"))
            if method == "route":
                return {"backend": binding["backend"], "epoch": binding["epoch"], "requestId": binding["requestId"]}
            if method == "fallback":
                if self.generating:
                    raise TTSError("tts_busy", "Cannot fall back while official generation is active")
                self.evict("comfy_fallback")
                self.binding["backend"] = "comfy"
                return {"backend": "comfy"}
            if method == "cancel":
                return rpc(self.child_socket, {"method": "cancel", "requestId": binding["requestId"]}, timeout=1)
            if method != "generate" or binding["backend"] != "sidecar" or self.state != "ready":
                raise TTSError("unsupported_profile", "Official runtime is not ready for this request")
            if self.generating:
                raise TTSError("busy", "Official generation already active")
            controls = self.config.get("validatedSamplingControls")
            if controls is not None or self.config.get("routingApproved"):
                params = request.get("params") or {}
                if (not isinstance(controls, dict) or set(controls) != SAMPLING_KEYS
                        or any(type(params.get(k)) not in (int, float) or params[k] != controls[k] for k in SAMPLING_KEYS)):
                    raise TTSError("unsupported_profile", "Sampling controls are outside the validated serving profile; use Comfy unchanged")
            self.generating = True
            self.generation_request_id = binding["requestId"]
        try:
            result = rpc(self.child_socket, {**request, "permit": {"kind": "generate", **binding}}, timeout=180)
            with self.owner.lock:
                self._binding(request.get("requestId"))
                timing = {k: v for k, v in result.get("timing", {}).items() if isinstance(v, (int, float))}
                self.health["lastGeneration"] = timing
                self._event("generation", requestId=binding["requestId"], timing=timing)
            return result
        except BaseException as error:
            with self.owner.lock:
                self._event("generation_error", requestId=binding["requestId"], code=getattr(error, "code", type(error).__name__), message=str(error)[:240])
                # A socket timeout is not a CUDA completion acknowledgement.
                if self.identity and self.generation_request_id == binding["requestId"]:
                    self._stop_request("unconfirmed_generation_stop")
            raise
        finally:
            with self.owner.lock:
                if self.generation_request_id == binding["requestId"]:
                    self.generating = False
                    self.generation_request_id = None

    def before_release(self, lease):
        if not self.binding or self.binding.get("fencingToken") != lease.get("fencingToken"):
            return
        if self.generating:
            self._stop_request("execution_release_before_stop")
        if self.binding and self.binding.get("fencingToken") != lease.get("fencingToken"):
            raise TTSError("stale_lease", "Execution lease changed during cancellation")
        self.binding = None
        self.not_before = time.monotonic() + 30

    def _serve(self):
        self.socket.unlink(missing_ok=True)
        manager = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                self.request.settimeout(190)
                try:
                    raw = self.rfile.readline(MAX_RPC_BYTES + 1)
                    if len(raw) > MAX_RPC_BYTES or not raw.endswith(b"\n"):
                        raise TTSError("invalid_request", "TTS request is too large")
                    result = manager.bridge(json.loads(raw))
                    response = {"ok": True, "result": result}
                except Exception as error:
                    response = {"ok": False, "error": {"code": getattr(error, "code", "runtime_failure"),
                                                          "message": "TTS runtime request failed"}}
                try:
                    self.wfile.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = _Server(str(self.socket), Handler)
        os.chmod(self.socket, 0o600)
        threading.Thread(target=self.server.serve_forever, daemon=True, name="tts-bridge").start()

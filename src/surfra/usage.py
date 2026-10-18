"""CPU, RAM, GPU, and VRAM utilization, sent to Surfra once a minute.

A daemon thread samples every SAMPLE seconds and posts the average and peak
every REPORT seconds. Inside a container the numbers are the container's own:
CPU against its quota or allowed cores, RAM against its memory limit without
reclaimable page cache. NVIDIA is read through NVML, or nvidia-smi without it.
AMD is read from amdgpu sysfs, then amd-smi, then rocm-smi. Anything that
cannot be read is left out, never guessed. SURFRA_USAGE=0 turns it off.
"""

from __future__ import annotations

import ctypes
import glob
import json
import os
import shutil
import subprocess
import threading
import time
from typing import Any

import httpx

SAMPLE = 10.0
REPORT = 60.0
AMD = "0x1002"
CGROUP = "/sys/fs/cgroup"


class Meter:
    def __init__(self, root: str = CGROUP) -> None:
        self.root = root
        self._cpu: tuple[float, float] | None = None
        self._nvml = _Nvml.open()
        self._tools = {name: shutil.which(name) for name in ("nvidia-smi", "amd-smi", "rocm-smi")}

    def sample(self) -> dict[str, Any]:
        used, total = _safe(self._ram, (None, None))
        gpus = _safe(self._gpus, [])
        return {"cpu_pct": _safe(self._read_cpu, None), "ram_pct": _pct(used, total), "ram_used": used, "ram_total": total, "gpus": gpus}

    def _gpus(self) -> list[dict[str, Any]]:
        gpus = self._nvml.gpus() if self._nvml else _nvidia_smi(self._tools["nvidia-smi"])
        gpus.extend(_amd_sysfs("/sys/class/drm"))
        if not any(gpu["vendor"] == "amd" for gpu in gpus):
            gpus.extend(_amd_tools(self._tools))
        return gpus

    def _read_cpu(self) -> float | None:
        """Busy CPU-seconds over the cores this process may use since the last call. None on the first call."""

        now = time.monotonic()
        busy = self._cpu_seconds()
        if busy is None:
            return None
        previous = self._cpu
        self._cpu = (now, busy)
        if previous is None or now <= previous[0]:
            return None
        return _clamp((busy - previous[1]) / ((now - previous[0]) * self._cores()) * 100)

    def _cpu_seconds(self) -> float | None:
        folder = _cgroup(self.root)
        if folder:
            stat = _read(os.path.join(folder, "cpu.stat"))
            for line in (stat or "").splitlines():
                key, _, value = line.partition(" ")
                if key == "usage_usec":
                    return int(value) / 1e6
        v1 = _read(os.path.join(self.root, "cpuacct", "cpuacct.usage")) or _read(os.path.join(self.root, "cpu,cpuacct", "cpuacct.usage"))
        if v1:
            return int(v1) / 1e9
        stat = _read("/proc/stat")
        if not stat:
            return None
        parts = [int(part) for part in stat.splitlines()[0].split()[1:]]
        idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
        return (sum(parts) - idle) / os.sysconf("SC_CLK_TCK")

    def _cores(self) -> float:
        try:
            allowed = float(len(os.sched_getaffinity(0)))
        except (AttributeError, OSError):
            allowed = float(os.cpu_count() or 1)
        folder = _cgroup(self.root)
        quota = None
        if folder:
            parts = (_read(os.path.join(folder, "cpu.max")) or "").split()
            if len(parts) == 2 and parts[0] != "max" and int(parts[1]) > 0:
                quota = int(parts[0]) / int(parts[1])
        else:
            limit = _read(os.path.join(self.root, "cpu", "cpu.cfs_quota_us")) or _read(os.path.join(self.root, "cpu,cpuacct", "cpu.cfs_quota_us"))
            period = _read(os.path.join(self.root, "cpu", "cpu.cfs_period_us")) or _read(os.path.join(self.root, "cpu,cpuacct", "cpu.cfs_period_us"))
            if limit and period and int(limit) > 0 and int(period) > 0:
                quota = int(limit) / int(period)
        return max(0.01, min(allowed, quota) if quota else allowed)

    def _ram(self) -> tuple[int | None, int | None]:
        host = _meminfo()
        folder = _cgroup(self.root)
        if folder:
            limit = _read(os.path.join(folder, "memory.max"))
            current = _read(os.path.join(folder, "memory.current"))
            if current:
                used = max(0, int(current) - _stat_value(os.path.join(folder, "memory.stat"), "inactive_file"))
                if limit and limit != "max":
                    return used, int(limit)
                if host:
                    return used, host[1]
        limit = _read(os.path.join(self.root, "memory", "memory.limit_in_bytes"))
        current = _read(os.path.join(self.root, "memory", "memory.usage_in_bytes"))
        if limit and current and host and int(limit) < host[1]:
            used = max(0, int(current) - _stat_value(os.path.join(self.root, "memory", "memory.stat"), "total_inactive_file"))
            return used, int(limit)
        return host or (None, None)


def _cgroup(root: str) -> str | None:
    """The cgroup v2 folder for this process. In a cgroup namespace that is the mount root itself."""

    if not os.path.exists(os.path.join(root, "cgroup.controllers")):
        return None
    for line in (_read("/proc/self/cgroup") or "").splitlines():
        if not line.startswith("0::"):
            continue
        path = line[3:].strip()
        folder = os.path.join(root, path.lstrip("/"))
        if path not in ("", "/") and os.path.exists(os.path.join(folder, "cpu.stat")):
            return folder
    return root if os.path.exists(os.path.join(root, "cpu.stat")) else None


def _read(path: str) -> str | None:
    try:
        with open(path) as handle:
            return handle.read().strip()
    except (OSError, ValueError):
        return None


def _stat_value(path: str, key: str) -> int:
    for line in (_read(path) or "").splitlines():
        name, _, value = line.partition(" ")
        if name == key and value.strip().isdigit():
            return int(value)
    return 0


def _meminfo() -> tuple[int, int] | None:
    info = {}
    for line in (_read("/proc/meminfo") or "").splitlines():
        key, _, value = line.partition(":")
        parts = value.split()
        if parts and parts[0].isdigit():
            info[key] = int(parts[0]) * 1024
    if "MemTotal" not in info or "MemAvailable" not in info:
        return None
    return info["MemTotal"] - info["MemAvailable"], info["MemTotal"]


class _Util(ctypes.Structure):
    _fields_ = [("gpu", ctypes.c_uint), ("memory", ctypes.c_uint)]


class _Mem(ctypes.Structure):
    _fields_ = [("total", ctypes.c_ulonglong), ("free", ctypes.c_ulonglong), ("used", ctypes.c_ulonglong)]


class _Nvml:
    def __init__(self, lib: Any) -> None:
        self.lib = lib

    @classmethod
    def open(cls) -> "_Nvml | None":
        for name in ("libnvidia-ml.so.1", "nvml.dll"):
            try:
                lib = ctypes.CDLL(name)
            except OSError:
                continue
            try:
                if lib.nvmlInit_v2() != 0:
                    return None
                lib.nvmlDeviceGetCount_v2.argtypes = [ctypes.POINTER(ctypes.c_uint)]
                lib.nvmlDeviceGetHandleByIndex_v2.argtypes = [ctypes.c_uint, ctypes.POINTER(ctypes.c_void_p)]
                lib.nvmlDeviceGetUtilizationRates.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Util)]
                lib.nvmlDeviceGetMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Mem)]
            except AttributeError:
                return None
            return cls(lib)
        return None

    def gpus(self) -> list[dict[str, Any]]:
        count = ctypes.c_uint()
        if self.lib.nvmlDeviceGetCount_v2(ctypes.byref(count)) != 0:
            return []
        found = []
        for index in range(count.value):
            handle = ctypes.c_void_p()
            if self.lib.nvmlDeviceGetHandleByIndex_v2(index, ctypes.byref(handle)) != 0:
                continue
            memory = _Mem()
            if self.lib.nvmlDeviceGetMemoryInfo(handle, ctypes.byref(memory)) != 0:
                continue
            util = _Util()
            gpu_pct = float(util.gpu) if self.lib.nvmlDeviceGetUtilizationRates(handle, ctypes.byref(util)) == 0 else None
            found.append(_gpu("nvidia", index, gpu_pct, memory.used, memory.total))
        return found


def _run(argv: list[str]) -> str | None:
    try:
        done = subprocess.run(argv, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 and done.stdout else None


def _nvidia_smi(binary: str | None) -> list[dict[str, Any]]:
    if not binary:
        return []
    out = _run([binary, "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"])
    return parse_nvidia_smi(out or "")


def parse_nvidia_smi(text: str) -> list[dict[str, Any]]:
    found = []
    for index, line in enumerate(text.splitlines()):
        cells = [cell.strip() for cell in line.split(",")]
        if len(cells) != 3:
            continue
        gpu_pct = _number(cells[0])
        used = _number(cells[1])
        total = _number(cells[2])
        found.append(_gpu("nvidia", index, gpu_pct, None if used is None else int(used * 1024 * 1024), None if total is None else int(total * 1024 * 1024)))
    return found


def _amd_sysfs(root: str) -> list[dict[str, Any]]:
    found = []
    for card in sorted(glob.glob(os.path.join(root, "card[0-9]*"))):
        name = os.path.basename(card)
        if not name[4:].isdigit():
            continue
        device = os.path.join(card, "device")
        if (_read(os.path.join(device, "vendor")) or "").lower() != AMD:
            continue
        used = _read(os.path.join(device, "mem_info_vram_used"))
        total = _read(os.path.join(device, "mem_info_vram_total"))
        if not used or not total or not used.isdigit() or not total.isdigit():
            continue
        found.append(_gpu("amd", int(name[4:]), _number(_read(os.path.join(device, "gpu_busy_percent")) or ""), int(used), int(total)))
    return found


def _amd_tools(tools: dict[str, str | None]) -> list[dict[str, Any]]:
    if tools.get("amd-smi"):
        out = _run([tools["amd-smi"], "metric", "--json"])
        if out:
            try:
                return parse_amd_smi(json.loads(out))
            except (ValueError, TypeError):
                pass
    if tools.get("rocm-smi"):
        out = _run([tools["rocm-smi"], "--showuse", "--showmeminfo", "vram", "--json"])
        if out:
            try:
                return parse_rocm_smi(json.loads(out))
            except (ValueError, TypeError):
                pass
    return []


def parse_rocm_smi(payload: Any) -> list[dict[str, Any]]:
    found = []
    if not isinstance(payload, dict):
        return found
    for name, info in payload.items():
        if not isinstance(info, dict) or not name.startswith("card") or not name[4:].isdigit():
            continue
        used = _field(info, "VRAM Total Used Memory (B)", "VRAM Used Memory (B)")
        total = _field(info, "VRAM Total Memory (B)", "VRAM Total Memory")
        found.append(_gpu("amd", int(name[4:]), _field(info, "GPU use (%)", "GPU Utilization", "GPU use"), None if used is None else int(used), None if total is None else int(total)))
    return found


def parse_amd_smi(payload: Any) -> list[dict[str, Any]]:
    rows = payload if isinstance(payload, list) else payload.get("gpu_data") or payload.get("gpu") if isinstance(payload, dict) else None
    found = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        usage = row.get("usage") or {}
        activity = usage.get("gfx_activity") if isinstance(usage, dict) else None
        memory = row.get("mem_usage") or row.get("vram_usage") or {}
        used = memory.get("used_vram") or memory.get("vram_used") or memory.get("used") or {}
        total = memory.get("total_vram") or memory.get("vram_total") or memory.get("total") or {}
        index = row.get("gpu", row.get("id", len(found)))
        found.append(_gpu(
            "amd",
            int(index) if isinstance(index, (int, str)) and str(index).isdigit() else len(found),
            _value(activity),
            _as_bytes(_value(used), used.get("unit") if isinstance(used, dict) else None),
            _as_bytes(_value(total), total.get("unit") if isinstance(total, dict) else None),
        ))
    return found


def _gpu(vendor: str, index: int, gpu_pct: float | None, used: int | None, total: int | None) -> dict[str, Any]:
    return {
        "vendor": vendor,
        "index": index,
        "gpu_pct": None if gpu_pct is None else _clamp(gpu_pct),
        "vram_pct": _pct(used, total),
        "vram_used": used,
        "vram_total": total,
    }


def _field(info: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in info:
            value = _number(str(info[key]))
            if value is not None:
                return value
    return None


def _value(node: Any) -> float | None:
    if isinstance(node, dict):
        node = node.get("value")
    return _number(str(node)) if node is not None else None


def _number(text: str) -> float | None:
    try:
        value = float(text.strip().rstrip("%"))
    except (ValueError, AttributeError):
        return None
    return value if value == value and value not in (float("inf"), float("-inf")) else None


def _as_bytes(value: float | None, unit: str | None) -> int | None:
    if value is None:
        return None
    scale = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "kib": 1024, "mib": 1024**2, "gib": 1024**3}
    return int(value * scale.get((unit or "b").lower(), 1))


def _pct(used: int | float | None, total: int | float | None) -> float | None:
    return _clamp(used / total * 100) if used is not None and total else None


def _clamp(value: float) -> float:
    return round(min(100.0, max(0.0, float(value))), 1)


def _safe(read: Any, fallback: Any) -> Any:
    try:
        return read()
    except Exception:
        return fallback


def aggregate(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """One report from a minute of samples: averages, CPU and GPU peaks, the latest byte counts."""

    def mean(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 1) if values else None

    cpu = [s["cpu_pct"] for s in samples if s.get("cpu_pct") is not None]
    ram = [s["ram_pct"] for s in samples if s.get("ram_pct") is not None]
    gpu = [mean([g["gpu_pct"] for g in s["gpus"] if g.get("gpu_pct") is not None]) for s in samples]
    gpu = [value for value in gpu if value is not None]
    vram = []
    for s in samples:
        used = sum(g["vram_used"] for g in s["gpus"] if g.get("vram_used") is not None and g.get("vram_total"))
        total = sum(g["vram_total"] for g in s["gpus"] if g.get("vram_used") is not None and g.get("vram_total"))
        if total:
            vram.append(used / total * 100)
    peaks = [g["gpu_pct"] for s in samples for g in s["gpus"] if g.get("gpu_pct") is not None]
    last = samples[-1] if samples else {}
    last_gpus = [g for g in last.get("gpus") or [] if g.get("vram_total")]
    return {
        "samples": len(samples),
        "cpu_pct": mean(cpu),
        "cpu_peak": max(cpu) if cpu else None,
        "ram_pct": mean(ram),
        "ram_used": last.get("ram_used"),
        "ram_total": last.get("ram_total"),
        "gpu_pct": mean(gpu),
        "gpu_peak": max(peaks) if peaks else None,
        "vram_pct": mean(vram),
        "vram_used": sum(g["vram_used"] or 0 for g in last_gpus) if last_gpus else None,
        "vram_total": sum(g["vram_total"] for g in last_gpus) if last_gpus else None,
        "gpus": len(last.get("gpus") or []),
    }


class Reporter:
    def __init__(self, master: str, token: str, meter: Meter | None = None) -> None:
        self.master = master.rstrip("/")
        self.token = token
        self.meter = meter or Meter()
        self.stopped = threading.Event()

    def run(self) -> None:
        self.meter.sample()
        samples: list[dict[str, Any]] = []
        sent = time.monotonic()
        while not self.stopped.wait(SAMPLE):
            samples.append(_safe(self.meter.sample, {"cpu_pct": None, "ram_pct": None, "gpus": []}))
            if time.monotonic() - sent < REPORT:
                continue
            report, samples, sent = aggregate(samples), [], time.monotonic()
            try:
                httpx.post(f"{self.master}/v1/workers/usage", json={"replica_token": self.token, "sample": report}, timeout=10)
            except httpx.HTTPError:
                pass


def start(master: str, token: str) -> Reporter | None:
    if os.environ.get("SURFRA_USAGE", "").strip().lower() in {"0", "false", "no", "off"}:
        return None
    reporter = Reporter(master, token)
    threading.Thread(target=reporter.run, name="surfra-usage", daemon=True).start()
    return reporter

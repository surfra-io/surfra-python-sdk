import os

from surfra import usage


def write(root, name, text):
    path = os.path.join(root, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write(text)


def v2(tmp_path, *, usec=0, quota="max 100000", current=0, limit="max", inactive=0):
    root = str(tmp_path / "cg")
    write(root, "cgroup.controllers", "cpu memory")
    write(root, "cpu.stat", f"usage_usec {usec}\nuser_usec 0\n")
    write(root, "cpu.max", quota)
    write(root, "memory.current", str(current))
    write(root, "memory.max", limit)
    write(root, "memory.stat", f"anon 1\ninactive_file {inactive}\nactive_file 5\n")
    return root


def test_a_namespaced_container_reads_its_own_cgroup_not_the_host(tmp_path):
    root = v2(tmp_path, current=3 << 30, limit=str(4 << 30), inactive=1 << 30)
    meter = usage.Meter(root)
    used, total = meter._ram()
    # Page cache the kernel can drop is not counted as used.
    assert (used, total) == (2 << 30, 4 << 30)


def test_cpu_is_measured_against_the_quota(tmp_path, monkeypatch):
    root = v2(tmp_path, usec=0, quota="200000 100000")
    meter = usage.Meter(root)
    clock = iter([100.0, 110.0])
    monkeypatch.setattr(usage.time, "monotonic", lambda: next(clock))
    assert meter._read_cpu() is None
    write(root, "cpu.stat", f"usage_usec {10_000_000}\n")
    # 10 CPU-seconds over 10 seconds on a 2 core quota is half.
    assert meter._read_cpu() == 50.0


def test_cpu_without_a_quota_uses_the_allowed_cores(tmp_path, monkeypatch):
    root = v2(tmp_path)
    meter = usage.Meter(root)
    monkeypatch.setattr(usage.os, "sched_getaffinity", lambda pid: {0, 1, 2, 3}, raising=False)
    assert meter._cores() == 4.0
    write(root, "cpu.max", "100000 100000")
    assert meter._cores() == 1.0


def test_cgroup_v1_limits_are_read(tmp_path, monkeypatch):
    root = str(tmp_path / "v1")
    write(root, "memory/memory.limit_in_bytes", str(2 << 30))
    write(root, "memory/memory.usage_in_bytes", str(1 << 30))
    write(root, "memory/memory.stat", f"total_inactive_file {256 << 20}\n")
    write(root, "cpu/cpu.cfs_quota_us", "150000")
    write(root, "cpu/cpu.cfs_period_us", "100000")
    meter = usage.Meter(root)
    monkeypatch.setattr(usage, "_meminfo", lambda: (1, 64 << 30))
    monkeypatch.setattr(usage.os, "sched_getaffinity", lambda pid: set(range(8)), raising=False)
    assert meter._ram() == ((1 << 30) - (256 << 20), 2 << 30)
    assert meter._cores() == 1.5


def test_amd_sysfs_skips_other_vendors(tmp_path):
    drm = str(tmp_path / "drm")
    write(drm, "card3/device/vendor", "0x1002\n")
    write(drm, "card3/device/gpu_busy_percent", "42\n")
    write(drm, "card3/device/mem_info_vram_used", "1024\n")
    write(drm, "card3/device/mem_info_vram_total", "4096\n")
    write(drm, "card1/device/vendor", "0x10de\n")
    write(drm, "card3-DP-1/device/vendor", "0x1002\n")
    assert usage._amd_sysfs(drm) == [{"vendor": "amd", "index": 3, "gpu_pct": 42.0, "vram_pct": 25.0, "vram_used": 1024, "vram_total": 4096}]


def test_tool_outputs_parse():
    nv = usage.parse_nvidia_smi("37, 1024, 49140\n[N/A], 10, 20\n")
    assert nv[0]["gpu_pct"] == 37.0 and nv[0]["vram_used"] == 1024 << 20
    assert nv[1]["gpu_pct"] is None
    rocm = usage.parse_rocm_smi({"card0": {"GPU use (%)": "7", "VRAM Total Memory (B)": "17179869184", "VRAM Total Used Memory (B)": "8589934592"}, "system": {}})
    assert rocm[0]["gpu_pct"] == 7 and rocm[0]["vram_pct"] == 50
    smi = usage.parse_amd_smi([{"gpu": 1, "usage": {"gfx_activity": {"value": 15, "unit": "%"}}, "mem_usage": {"used_vram": {"value": 1024, "unit": "MB"}, "total_vram": {"value": 8192, "unit": "MB"}}}])
    assert smi[0]["index"] == 1 and smi[0]["gpu_pct"] == 15 and smi[0]["vram_used"] == 1 << 30
    assert usage.parse_amd_smi({"nothing": 1}) == []


def test_a_minute_of_samples_becomes_one_report():
    gpu = lambda pct, used: {"vendor": "nvidia", "index": 0, "gpu_pct": pct, "vram_pct": None, "vram_used": used, "vram_total": 100}
    samples = [
        {"cpu_pct": None, "ram_pct": 10.0, "ram_used": 1, "ram_total": 10, "gpus": [gpu(20.0, 10)]},
        {"cpu_pct": 30.0, "ram_pct": 20.0, "ram_used": 2, "ram_total": 10, "gpus": [gpu(80.0, 30)]},
        {"cpu_pct": 90.0, "ram_pct": 30.0, "ram_used": 3, "ram_total": 10, "gpus": [gpu(None, 50)]},
    ]
    report = usage.aggregate(samples)
    assert report["samples"] == 3
    assert report["cpu_pct"] == 60.0 and report["cpu_peak"] == 90.0
    assert report["ram_pct"] == 20.0 and report["ram_used"] == 3
    assert report["gpu_pct"] == 50.0 and report["gpu_peak"] == 80.0
    assert report["vram_pct"] == 30.0 and report["vram_used"] == 50 and report["vram_total"] == 100
    assert report["gpus"] == 1
    assert usage.aggregate([])["cpu_pct"] is None


def test_it_can_be_turned_off(monkeypatch):
    monkeypatch.setenv("SURFRA_USAGE", "0")
    assert usage.start("http://x", "t") is None


def test_this_machine_samples_without_raising():
    meter = usage.Meter()
    meter.sample()
    report = meter.sample()
    assert report["ram_total"] is None or report["ram_total"] > 0
    assert report["cpu_pct"] is None or 0 <= report["cpu_pct"] <= 100

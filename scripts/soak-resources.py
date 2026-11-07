#!/usr/bin/env python3
"""Checks the soak's workloads against the namespace igs-soak's quota
(REQ-008 fix 12, docs/runbooks/igs-soak.md): every container has CPU and
memory requests and limits (the ResourceQuota refuses pods without them),
and the running pods' limits fit the quota agreed with the platform. It
renders igs's chart with deploy/soak/values-soak.yaml and reads the
kustomization's resources.

    python3 scripts/soak-resources.py --helm helm
"""
import argparse
import os
import subprocess
import sys

import yaml

# The quota the platform sets on igs-soak (limits), with room for one hook
# job at a time.
QUOTA_CPU, QUOTA_MEM_GI = 6.0, 12.0

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cpu(v):
    v = str(v)
    return float(v[:-1]) / 1000 if v.endswith("m") else float(v)


def mem_gi(v):
    v = str(v)
    for suffix, scale in (("Gi", 1.0), ("Mi", 1 / 1024), ("Ki", 1 / 1024 / 1024)):
        if v.endswith(suffix):
            return float(v[: -len(suffix)]) * scale
    return float(v) / 2**30


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--helm", default="helm")
    a = ap.parse_args()
    chart = subprocess.run([a.helm, "template", "igs", os.path.join(ROOT, "deploy/helm/igs"), "-n", "igs-soak",
                            "-f", os.path.join(ROOT, "deploy/soak/values-soak.yaml")],
                           check=True, capture_output=True, text=True).stdout
    docs = [d for d in yaml.safe_load_all(chart) if d]
    kust = yaml.safe_load(open(os.path.join(ROOT, "deploy/soak/kustomization.yaml")))
    for f in kust["resources"]:
        docs += [d for d in yaml.safe_load_all(open(os.path.join(ROOT, "deploy/soak", f))) if d]
    bad, lim_cpu, lim_mem, hook_cpu, hook_mem = [], 0.0, 0.0, 0.0, 0.0
    for d in docs:
        if d.get("kind") not in ("Deployment", "StatefulSet", "Job", "DaemonSet"):
            continue
        hook = "helm.sh/hook" in (d["metadata"].get("annotations") or {})
        replicas = d["spec"].get("replicas", 1)
        spec = d["spec"]["template"]["spec"]
        for c in spec.get("initContainers", []) + spec.get("containers", []):
            r = c.get("resources") or {}
            req, lim = r.get("requests") or {}, r.get("limits") or {}
            missing = [k for k, v in (("requests.cpu", req.get("cpu")), ("requests.memory", req.get("memory")),
                                      ("limits.cpu", lim.get("cpu")), ("limits.memory", lim.get("memory"))) if v is None]
            if missing:
                bad.append(f"{d['kind']} {d['metadata']['name']}, container {c['name']}: no {', '.join(missing)}")
                continue
            if hook:
                hook_cpu, hook_mem = max(hook_cpu, cpu(lim["cpu"])), max(hook_mem, mem_gi(lim["memory"]))
            else:
                lim_cpu += replicas * cpu(lim["cpu"])
                lim_mem += replicas * mem_gi(lim["memory"])
    print(f"soak: running pods limited to {lim_cpu:.2f} CPU and {lim_mem:.1f} Gi; a hook job to {hook_cpu:.2f} CPU "
          f"and {hook_mem:.2f} Gi; quota {QUOTA_CPU} CPU and {QUOTA_MEM_GI} Gi")
    if lim_cpu + hook_cpu > QUOTA_CPU or lim_mem + hook_mem > QUOTA_MEM_GI:
        bad.append("the limits exceed the namespace's quota")
    for b in bad:
        print("soak:", b, file=sys.stderr)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

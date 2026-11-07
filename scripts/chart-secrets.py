#!/usr/bin/env python3
"""Checks which Secrets igs's chart makes its pods wait for.

- Production (deploy/helm/igs/values-production.yaml): only the
  components that reach Instagram through the proxy tiers (crawler-profile,
  crawler-post, registry-sync) mount the proxies Secret; the scheduler
  mounts it as optional; the hook Jobs and every other component never
  name it, so igs migrates and starts before the platform adds the proxies.
- The soak (deploy/soak/values-soak.yaml): every Secret a rendered pod or
  hook names is one deploy/soak creates, so nothing waits on a Secret that
  is not in igs-soak.

    python3 scripts/chart-secrets.py --helm helm
"""
import argparse
import os
import subprocess
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART = os.path.join(ROOT, "deploy/helm/igs")
PROXIES_REQUIRED = {"crawler-profile", "crawler-post", "registry-sync"}
PROXIES_OPTIONAL = {"scheduler"}


def render(helm, ns, values):
    out = subprocess.run([helm, "template", "igs", CHART, "-n", ns, "-f", values],
                         check=True, capture_output=True, text=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def pods(docs):
    """(kind, component or job name, pod spec) of each Deployment and Job."""
    for d in docs:
        if d["kind"] in ("Deployment", "Job"):
            name = d["metadata"]["name"].removeprefix("igs-")
            yield d["kind"], name, d["spec"]["template"]["spec"]


def secrets_named(spec):
    """{secret name: optional} for every Secret the pod spec names."""
    names = {}
    for v in spec.get("volumes", []):
        if "secret" in v:
            names[v["secret"]["secretName"]] = bool(v["secret"].get("optional"))
    for c in spec.get("containers", []) + spec.get("initContainers", []):
        for e in c.get("envFrom", []):
            if "secretRef" in e:
                names[e["secretRef"]["name"]] = bool(e["secretRef"].get("optional"))
        for e in c.get("env", []):
            ref = (e.get("valueFrom") or {}).get("secretKeyRef")
            if ref:
                names[ref["name"]] = bool(ref.get("optional"))
    return names


def config_files(spec):
    for e in spec["containers"][0].get("env", []):
        if e["name"] == "IGS_CONFIG":
            return e["value"]
    return ""


def check_production(helm):
    errs = []
    values = os.path.join(CHART, "values-production.yaml")
    with open(values) as f:
        proxies = yaml.safe_load(f)["secretFiles"]["existingSecret"]
    for kind, name, spec in pods(render(helm, "igs", values)):
        named = secrets_named(spec)
        cfg = config_files(spec)
        if kind == "Deployment" and name in PROXIES_REQUIRED:
            if named.get(proxies) is not False or "/etc/igs-secret/" not in cfg or ",?" in cfg:
                errs.append(f"production {name}: must mount {proxies} (required) and read it: {named} {cfg}")
        elif kind == "Deployment" and name in PROXIES_OPTIONAL:
            if named.get(proxies) is not True or ",?/etc/igs-secret/" not in cfg:
                errs.append(f"production {name}: must mount {proxies} as optional and read it as optional: {named} {cfg}")
        elif proxies in named or "igs-secret" in cfg:
            errs.append(f"production {kind} {name}: names {proxies}, so it waits for the proxies: {named} {cfg}")
    return errs


def check_soak(helm):
    errs = []
    created = set()
    for f in os.listdir(os.path.join(ROOT, "deploy/soak")):
        if f.endswith(".yaml") and f != "values-soak.yaml":
            with open(os.path.join(ROOT, "deploy/soak", f)) as fh:
                for d in yaml.safe_load_all(fh):
                    if d and d.get("kind") == "Secret":
                        created.add(d["metadata"]["name"])
    for kind, name, spec in pods(render(helm, "igs-soak", os.path.join(ROOT, "deploy/soak/values-soak.yaml"))):
        for secret, optional in secrets_named(spec).items():
            if secret not in created and not optional:
                errs.append(f"soak {kind} {name}: names Secret {secret}, which deploy/soak does not create")
    return errs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--helm", default="helm")
    a = ap.parse_args()
    errs = check_production(a.helm) + check_soak(a.helm)
    for e in errs:
        print(e, file=sys.stderr)
    if errs:
        sys.exit(1)
    print("chart secrets: production mounts the proxies in the crawlers and registry-sync only "
          "(the scheduler optional, hooks none); the soak names only its own Secrets")


if __name__ == "__main__":
    main()

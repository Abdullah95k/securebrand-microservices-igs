#!/usr/bin/env python3
"""Checks the chart's PrometheusRule: every alert of files/alerts.yaml, with
Prometheus's own annotation templates ({{ $value }}) kept as written, except
the alerts left out on purpose:
- IgsIgNormalizerLag while components.normalizer is off or at 0 replicas
  (igs publishes no content.* until it adopts the contract, REQ-019);
- any alert named in alerts.disabled.

    python3 scripts/chart-alerts.py --helm helm
"""
import argparse
import os
import subprocess
import sys

import yaml

CHART = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "deploy", "helm", "igs")


def alerts(helm, *sets):
    args = [helm, "template", "igs", CHART, "-n", "igs", "-f", os.path.join(CHART, "values-production.yaml"),
            "-s", "templates/prometheusrule.yaml"]
    for s in sets:
        args += ["--set", s]
    rule = yaml.safe_load(subprocess.run(args, check=True, capture_output=True, text=True).stdout)
    return [r for g in rule["spec"]["groups"] for r in g["rules"] if "alert" in r]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--helm", default="helm")
    a = ap.parse_args()
    with open(os.path.join(CHART, "files", "alerts.yaml")) as f:
        want = [r["alert"] for g in yaml.safe_load(f)["groups"] for r in g["rules"] if "alert" in r]
    errs = []
    full = alerts(a.helm)
    if [r["alert"] for r in full] != want:
        errs.append(f"production renders {len(full)} alerts, files/alerts.yaml has {len(want)}")
    if not any("{{ $value" in str(r.get("annotations", {})) for r in full):
        errs.append("the annotations lost Prometheus's {{ $value }} templates")
    for sets in (["components.normalizer.replicas=0"], ["components.normalizer.enabled=false"]):
        names = [r["alert"] for r in alerts(a.helm, *sets)]
        if "IgsIgNormalizerLag" in names or len(names) != len(want) - 1:
            errs.append(f"{sets[0]}: IgsIgNormalizerLag should be the only alert left out")
    names = [r["alert"] for r in alerts(a.helm, "alerts.disabled={IgsIgBlockRateHigh}")]
    if "IgsIgBlockRateHigh" in names or len(names) != len(want) - 1:
        errs.append("alerts.disabled: IgsIgBlockRateHigh should be the only alert left out")
    for e in errs:
        print(e, file=sys.stderr)
    if errs:
        sys.exit(1)
    print(f"chart alerts: production renders all {len(want)} alerts with their templates; IgsIgNormalizerLag is left out "
          "while the normalizer is off or at 0 replicas, and alerts.disabled leaves out what it names")


if __name__ == "__main__":
    main()

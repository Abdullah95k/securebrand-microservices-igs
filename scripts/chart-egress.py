#!/usr/bin/env python3
"""Checks what igs's chart lets its pods reach (REQ-008 fix 21).

- Production (deploy/helm/igs/values-production.yaml): every igs Deployment
  may reach DNS in kube-system, the VPC (10.30.0.0/16, every port), the
  managed services' public ports (25060-25080) and HTTPS; exactly the
  Deployments that mount the proxies Secret as required may reach the proxy
  gateway; no hook Job is restricted.
- The chart's defaults and the soak (deploy/soak/values-soak.yaml) render no
  egress policy: their pods reach everything, as before.

    python3 scripts/chart-egress.py --helm helm
"""
import argparse
import os
import subprocess
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART = os.path.join(ROOT, "deploy/helm/igs")


def render(helm, ns, *args):
    out = subprocess.run([helm, "template", "igs", CHART, "-n", ns, *args],
                         check=True, capture_output=True, text=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def selects(selector, labels):
    """Whether a label selector (matchLabels, matchExpressions In) selects labels."""
    for k, v in (selector.get("matchLabels") or {}).items():
        if labels.get(k) != v:
            return False
    for e in selector.get("matchExpressions") or []:
        if e["operator"] != "In":
            raise ValueError(f"operator {e['operator']} is not checked here")
        if labels.get(e["key"]) not in e["values"]:
            return False
    return True


def egress_policies(docs):
    return {d["metadata"]["name"]: d for d in docs
            if d["kind"] == "NetworkPolicy" and "Egress" in d["spec"].get("policyTypes", [])}


def ports(rule):
    return sorted((p.get("protocol", "TCP"), p["port"], p.get("endPort")) for p in rule.get("ports", []))


def reaches(policy):
    """{(peer, ports)} of an egress policy, peers as 'ns:<name>' or a CIDR."""
    out = set()
    for rule in policy["spec"]["egress"]:
        for peer in rule["to"]:
            if "ipBlock" in peer:
                key = peer["ipBlock"]["cidr"]
            else:
                key = "ns:" + peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
            out.add((key, tuple(ports(rule))))
    return out


def mounts_proxies(spec, secret):
    return any(v.get("secret", {}).get("secretName") == secret and not v["secret"].get("optional")
               for v in spec.get("volumes", []))


def check_production(helm):
    errs = []
    values = os.path.join(CHART, "values-production.yaml")
    with open(values) as f:
        secret = yaml.safe_load(f)["secretFiles"]["existingSecret"]
    docs = render(helm, "igs", "-f", values)
    pols = egress_policies(docs)
    every, proxies = pols.get("igs-egress"), pols.get("igs-egress-proxies")
    if not every or not proxies:
        return [f"production: want the policies igs-egress and igs-egress-proxies, have {sorted(pols)}"]
    want = {
        ("ns:kube-system", (("TCP", 53, None), ("UDP", 53, None))),
        ("10.30.0.0/16", ()),
        ("0.0.0.0/0", (("TCP", 25060, 25080),)),
        ("0.0.0.0/0", (("TCP", 443, None),)),
    }
    if reaches(every) != want:
        errs.append(f"production igs-egress reaches {sorted(reaches(every))}, want {sorted(want)}")
    if reaches(proxies) != {("0.0.0.0/0", (("TCP", 10000, 10999),))}:
        errs.append(f"production igs-egress-proxies reaches {sorted(reaches(proxies))}")
    crawl = set()
    for d in docs:
        if d["kind"] not in ("Deployment", "Job"):
            continue
        name = d["metadata"]["name"].removeprefix("igs-")
        tpl = d["spec"]["template"]
        labels = (tpl.get("metadata") or {}).get("labels") or {}
        sel_every = selects(every["spec"]["podSelector"], labels)
        sel_proxies = selects(proxies["spec"]["podSelector"], labels)
        if d["kind"] == "Job":
            if sel_every or sel_proxies:
                errs.append(f"production hook {name}: restricted by an egress policy")
            continue
        if not sel_every:
            errs.append(f"production {name}: not selected by igs-egress")
        needs = mounts_proxies(tpl["spec"], secret)
        if needs:
            crawl.add(name)
        if needs != sel_proxies:
            errs.append(f"production {name}: mounts the proxies {needs}, may reach them {sel_proxies}")
    if not crawl:
        errs.append("production: no Deployment mounts the proxies")
    return errs


def check_none(helm):
    errs = []
    for ns, args in (("igs", ()), ("igs-soak", ("-f", os.path.join(ROOT, "deploy/soak/values-soak.yaml")))):
        if pols := egress_policies(render(helm, ns, *args)):
            errs.append(f"{ns}: renders egress policies {sorted(pols)}, want none")
    return errs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--helm", default="helm")
    a = ap.parse_args()
    errs = check_production(a.helm) + check_none(a.helm)
    for e in errs:
        print(e, file=sys.stderr)
    if errs:
        sys.exit(1)
    print("chart egress: production's pods reach DNS, the VPC, the managed ports and HTTPS, and only the "
          "components that mount the proxies reach the gateway; hooks, the defaults and the soak are unrestricted")


if __name__ == "__main__":
    main()

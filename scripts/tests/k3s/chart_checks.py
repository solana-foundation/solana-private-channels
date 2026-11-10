"""Cross-cutting chart contract checks over rendered manifests (no cluster).

usage: chart_checks.py <rendered.yaml> <values.yaml> <compose-file> <sentinels.txt>
"""
import sys

import yaml

rendered, values_path, compose_path, sentinels_path = sys.argv[1:5]
docs = [d for d in yaml.safe_load_all(open(rendered)) if d]
values = yaml.safe_load(open(values_path))
compose = yaml.safe_load(open(compose_path))["services"]
sentinels = [s.strip() for s in open(sentinels_path) if s.strip()]
failures = []


def fail(msg):
    failures.append(msg)


def pod_spec(d):
    if d["kind"] in ("Deployment", "StatefulSet", "Job"):
        return d["spec"]["template"]["spec"]
    return None


APP = {"write-node", "read-node", "gateway", "auth", "auth-migrate", "streamer", "indexer-solana",
       "indexer-private-channel", "operator-solana", "operator-private-channel",
       "postgres-exporter-primary", "postgres-exporter-indexer", "blackbox-exporter"}
secret_keys = values.get("secretKeys", {})
workloads = {}
for d in docs:
    spec = pod_spec(d)
    if spec is None:
        continue
    name = d["metadata"]["name"]
    workloads[name] = (d, spec)
    c = spec["containers"][0]
    env = c.get("env", [])
    names = {e["name"] for e in env}
    want = set(secret_keys.get(name, []))
    # 1. KEY_FILE for exactly the service's own keys.
    files = {e["name"][:-5] for e in env if e["name"].endswith("_FILE") and e.get("value", "").startswith("/run/secrets/")}
    if name in APP and name not in ("postgres-exporter-primary", "postgres-exporter-indexer", "blackbox-exporter"):
        if files != want:
            fail(f"{name}: KEY_FILE set {sorted(files)} != secret_files keys {sorted(want)}")
    # 2. A secret key is never also plain env, and no sentinel value reaches any env value.
    for k in want & names:
        fail(f"{name}: secret key {k} is also plain env")
    for e in env:
        v = str(e.get("value", ""))
        for s in sentinels:
            if s in v:
                fail(f"{name}: env {e['name']} carries a secret value")
    # The app image entrypoint (with-secrets) turns KEY_FILE into KEY; command would bypass it.
    if name in APP - {"postgres-exporter-primary", "postgres-exporter-indexer", "blackbox-exporter"} and "command" in c:
        fail(f"{name}: container sets command, which replaces the with-secrets entrypoint")
    # 3. Secret volume contract.
    for v in spec.get("volumes", []):
        if "secret" in v and v["name"] == "secrets":
            if v["secret"].get("defaultMode") != 0o440:
                fail(f"{name}: secret volume mode {v['secret'].get('defaultMode')}")
    for m in c.get("volumeMounts", []):
        if "subPath" in m:
            fail(f"{name}: subPath mount {m}")
    if name in APP:
        sc = spec.get("securityContext", {})
        if not sc.get("runAsNonRoot") or spec.get("automountServiceAccountToken") is not False:
            fail(f"{name}: runAsNonRoot/automountServiceAccountToken not set")
    for k in ("hostNetwork", "hostPID", "hostIPC", "nodeName", "nodeSelector"):
        if k in spec:
            fail(f"{name}: uses {k}")
    for v in spec.get("volumes", []):
        if "hostPath" in v:
            fail(f"{name}: hostPath volume")
    for ctr in spec.get("containers", []) + spec.get("initContainers", []):
        prof = (ctr.get("securityContext") or {}).get("seccompProfile", {})
        if prof.get("type") == "Unconfined" or (spec.get("securityContext") or {}).get("seccompProfile", {}).get("type") == "Unconfined":
            fail(f"{name}: Unconfined seccomp")
        for p in ctr.get("ports", []):
            if "hostPort" in p:
                fail(f"{name}: hostPort")
    # 4. Probes: liveness never /health, readiness on /health.
    if name in APP - {"auth-migrate", "postgres-exporter-primary", "postgres-exporter-indexer", "blackbox-exporter"}:
        if "httpGet" in c.get("livenessProbe", {}):
            fail(f"{name}: liveness uses httpGet")
        if c.get("readinessProbe", {}).get("httpGet", {}).get("path") != "/health":
            fail(f"{name}: readiness is not /health")

# 5. Waits equal compose depends_on (postgres-* collapse to postgres, auth-migrate is the hook).
for name, (d, spec) in workloads.items():
    if name not in compose:
        continue
    dep = compose[name].get("depends_on", {})
    if isinstance(dep, list):
        dep = {k: {"condition": "service_started"} for k in dep}
    want = set()
    for k, v in dep.items():
        if k == "auth-migrate":
            continue
        cond = v.get("condition", "service_started")
        if k.startswith("postgres-"):
            want.add(("postgres", k))
        else:
            want.add(("healthy" if cond == "service_healthy" else "started", k))
    got = set()
    for ic in spec.get("initContainers", []):
        for e in ic.get("env", []):
            if e["name"] == "WAIT_POSTGRES":
                got |= {("postgres", h) for h in e["value"].split()}
            if e["name"] == "WAIT_HTTP":
                got |= {(x.split(":")[0], x.split(":")[1]) for x in e["value"].split()}
    # The operators' gateway-internal wait is a deliberate addition beyond compose.
    got.discard(("started", "gateway-internal"))
    if got != want:
        fail(f"{name}: waits {sorted(got)} != compose depends_on {sorted(want)}")

# 6. Public Services: only gateway 8899 and auth 8903.
for d in docs:
    if d["kind"] == "Service" and d["spec"].get("type") in ("LoadBalancer", "NodePort"):
        ports = [p["port"] for p in d["spec"]["ports"]]
        if (d["metadata"]["name"], ports) not in (("gateway", [8899]), ("auth", [8903])):
            fail(f"public Service {d['metadata']['name']} {ports}")

# 7. NetworkPolicy: default deny plus exactly the allow table.
def sel(ms):
    l = ms.get("matchLabels", {})
    if "private-channel.io/db" in l:
        return "db:" + l["private-channel.io/db"] + (":" + l["cnpg.io/instanceRole"] if "cnpg.io/instanceRole" in l else "")
    return l.get("app.kubernetes.io/name", "?")


def src(f):
    if "namespaceSelector" in f:
        ns = f["namespaceSelector"]["matchLabels"].get("kubernetes.io/metadata.name")
        pod = f.get("podSelector", {}).get("matchLabels")
        if not pod:
            fail(f"source by namespace alone: {ns}")
        return {"prometheus": "prometheus", "grafana": "grafana", "cloudnative-pg": "cnpg-operator"}[pod["app.kubernetes.io/name"]]
    return sel(f["podSelector"])


got = {}
deny = False
for d in docs:
    if d["kind"] != "NetworkPolicy":
        continue
    if d["spec"]["podSelector"] == {} and not d["spec"].get("ingress"):
        deny = True
        continue
    to = sel(d["spec"]["podSelector"])
    for rule in d["spec"]["ingress"]:
        srcs = {"*"} if "from" not in rule else {src(f) for f in rule["from"]}
        for p in rule["ports"]:
            got.setdefault((to, p["port"]), set()).update(srcs)
if not deny:
    fail("no default-deny NetworkPolicy")
OPS = {"operator-solana", "operator-private-channel"}
PROM = {"prometheus"}
EXPECTED = {
    ("gateway", 8899): {"*"}, ("auth", 8903): {"*"},
    ("gateway", 8904): OPS, ("gateway", 9101): PROM,
    ("write-node", 8900): {"gateway", "read-node", "streamer", "blackbox-exporter"} | OPS,
    ("read-node", 8901): {"gateway", "indexer-solana", "indexer-private-channel", "blackbox-exporter"},
    ("write-node", 9090): PROM, ("read-node", 9090): PROM,
    ("indexer-solana", 9100): PROM | {"blackbox-exporter"},
    ("indexer-private-channel", 9100): PROM | {"blackbox-exporter"},
    ("operator-solana", 9102): PROM | {"blackbox-exporter"},
    ("operator-private-channel", 9103): PROM | {"blackbox-exporter"},
    ("db:ledger:primary", 5432): {"write-node", "gateway", "auth", "auth-migrate", "pc-db-grants", "postgres-exporter-primary", "pc-truncate"},
    ("db:ledger:replica", 5432): {"read-node", "streamer"},
    ("db:indexer", 5432): {"indexer-solana", "indexer-private-channel", "streamer", "pc-db-grants",
                           "postgres-exporter-indexer", "grafana", "cnpg-operator", "db:indexer", "pc-truncate"} | OPS,
    ("db:ledger", 5432): {"cnpg-operator", "db:ledger"}, ("db:ledger", 8000): {"cnpg-operator", "db:ledger"},
    ("db:indexer", 8000): {"cnpg-operator", "db:indexer"},
    ("validator", 8899): {"indexer-solana", "streamer", "gateway"} | OPS,
    ("validator", 8900): {"indexer-solana", "streamer", "gateway"} | OPS,
    ("validator", 10000): {"indexer-solana", "streamer", "gateway"} | OPS,
    ("validator", 8999): {"indexer-solana", "streamer", "gateway"} | OPS,
    ("postgres-exporter-primary", 9187): PROM, ("postgres-exporter-indexer", 9187): PROM,
    ("blackbox-exporter", 9115): PROM,
}
if got != EXPECTED:
    for k in sorted(set(got) | set(EXPECTED), key=str):
        if got.get(k) != EXPECTED.get(k):
            fail(f"NetworkPolicy {k}: rendered {sorted(got.get(k, []))} != table {sorted(EXPECTED.get(k, []))}")

for f in failures:
    print("FAIL:", f)
sys.exit(1 if failures else 0)

#!/usr/bin/env bash
# install-prereqs.sh — install everything private-channel-deploy preflight checks for.
# Idempotent: re-runs are no-ops on already-satisfied prereqs.
# Targets Ubuntu/Debian. For other OSes, install manually per README.
#   --k3s  also install the k3s control-node tools (helm, kubectl, crane at the versions.env
#          pins, the kubernetes.core collection and the Python kubernetes client).
set -euo pipefail

K3S=false
for a in "$@"; do
  case "$a" in
    --k3s) K3S=true ;;
    *) echo "unknown flag $a (usage: install-prereqs.sh [--k3s])" >&2; exit 2 ;;
  esac
done

ANSIBLE_MIN=2.16
DOCKER_MIN=26

log()   { echo "[install-prereqs] $*"; }
ok()    { echo "  PASS  $*"; }
inst()  { echo "  INSTALL  $*"; }

# Compare two semver-ish versions: returns 0 if $1 >= $2.
ver_ge() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
}

# 1) Ansible >= ANSIBLE_MIN  ----------------------------------------------------
ansible_v="$(ansible --version 2>/dev/null | head -n1 | sed -nE 's/.*core ([0-9.]+).*/\1/p' || true)"
if [ -n "$ansible_v" ] && ver_ge "$ansible_v" "$ANSIBLE_MIN"; then
  ok "ansible-core $ansible_v"
else
  inst "ansible-core >= $ANSIBLE_MIN via pipx"
  if ! command -v pipx >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y pipx
    pipx ensurepath
    export PATH="$HOME/.local/bin:$PATH"
  fi
  pipx install --force "ansible-core>=$ANSIBLE_MIN"
fi

# 2) community.general collection  ---------------------------------------------
if ansible-galaxy collection list community.general 2>/dev/null | grep -q community.general; then
  ok "community.general collection"
else
  inst "community.general collection"
  ansible-galaxy collection install community.general
fi

# 3) Docker Engine + Compose plugin >= DOCKER_MIN  ----------------------------
# Install via Docker's official APT repo, with the GPG key fingerprint
# verified by apt on every subsequent update. This replaces the older
# `curl https://get.docker.com | sh` flow, which executed unverified remote
# code as root and ran the same risk every time the script was re-run.
# Both engine and compose plugin come from the same repo, so they're
# installed together in one apt invocation.
need_docker_install=false
docker_v="$(docker version --format '{{.Server.Version}}' 2>/dev/null || true)"
if [ -n "$docker_v" ] && ver_ge "$docker_v" "$DOCKER_MIN"; then
  ok "docker $docker_v"
else
  need_docker_install=true
fi
if docker compose version >/dev/null 2>&1; then
  ok "docker compose plugin ($(docker compose version --short))"
else
  need_docker_install=true
fi

if [ "$need_docker_install" = true ]; then
  inst "docker-ce + docker-compose-plugin >= $DOCKER_MIN via download.docker.com APT repo"

  # Distro detection: only Ubuntu/Debian are supported by this script.
  if ! [ -r /etc/os-release ]; then
    log "ERROR: cannot read /etc/os-release; install Docker manually per README."
    exit 1
  fi
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID:-}" in
    ubuntu|debian) ;;
    *) log "ERROR: unsupported distro '${ID:-?}'. Install Docker manually per README."; exit 1 ;;
  esac

  sudo apt-get update -qq
  sudo apt-get install -y ca-certificates curl gnupg

  # Pin Docker's GPG key under /etc/apt/keyrings/. Subsequent `apt-get update`
  # invocations verify every package signature against this key, so the
  # integrity of every install is enforced by the package manager rather
  # than by trusting whatever bytes a `curl | sh` happens to receive.
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL "https://download.docker.com/linux/${ID}/gpg" \
    | sudo gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg

  arch="$(dpkg --print-architecture)"
  codename="${VERSION_CODENAME:-$(lsb_release -cs 2>/dev/null || true)}"
  if [ -z "$codename" ]; then
    log "ERROR: cannot determine distro codename for the APT source line."
    exit 1
  fi

  echo "deb [arch=${arch} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${ID} ${codename} stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null

  sudo apt-get update -qq
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  sudo usermod -aG docker "$USER" || true
  log "If 'docker' commands fail with 'permission denied', log out and back in (or run 'newgrp docker')."
fi

# 4) k3s control-node tools (--k3s)  ---------------------------------------------
# Same pins as the k3s preflight. Each download is checked against the checksum file the
# project publishes next to it, as bootstrap does for the k3s binary.
if [ "$K3S" = true ]; then
  # shellcheck disable=SC1091
  . "$(cd "$(dirname "$0")/../.." && pwd)/versions.env"
  case "$(uname -m)" in
    x86_64) arch=amd64 carch=x86_64 ;;
    aarch64|arm64) arch=arm64 carch=arm64 ;;
    *) log "ERROR: unsupported arch $(uname -m); install helm, kubectl and crane by hand."; exit 1 ;;
  esac
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  # check <file> <sha256>: refuse a download that does not match its published checksum.
  check() { echo "$2  $1" | sha256sum -c --quiet - || { log "ERROR: checksum mismatch for $1"; exit 1; }; }

  if [ "$(helm version --template '{{.Version}}' 2>/dev/null || true)" = "$HELM_VERSION" ]; then
    ok "helm $HELM_VERSION"
  else
    inst "helm $HELM_VERSION"
    f="helm-$HELM_VERSION-linux-$arch.tar.gz"
    curl -fsSL -o "$tmp/$f" "https://get.helm.sh/$f"
    check "$tmp/$f" "$(curl -fsSL "https://get.helm.sh/$f.sha256sum" | awk '{print $1}')"
    tar -xzf "$tmp/$f" -C "$tmp" "linux-$arch/helm"
    sudo install -m 0755 "$tmp/linux-$arch/helm" /usr/local/bin/helm
  fi

  kv="$(kubectl version --client -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["clientVersion"]["gitVersion"])' 2>/dev/null || true)"
  if [ "$kv" = "$KUBECTL_VERSION" ]; then
    ok "kubectl $KUBECTL_VERSION"
  else
    inst "kubectl $KUBECTL_VERSION"
    u="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$arch/kubectl"
    curl -fsSL -o "$tmp/kubectl" "$u"
    check "$tmp/kubectl" "$(curl -fsSL "$u.sha256")"
    sudo install -m 0755 "$tmp/kubectl" /usr/local/bin/kubectl
  fi

  if [ "$(crane version 2>/dev/null || true)" = "${CRANE_VERSION#v}" ]; then
    ok "crane $CRANE_VERSION"
  else
    inst "crane $CRANE_VERSION"
    f="go-containerregistry_Linux_$carch.tar.gz"
    u="https://github.com/google/go-containerregistry/releases/download/$CRANE_VERSION"
    curl -fsSL -o "$tmp/$f" "$u/$f"
    check "$tmp/$f" "$(curl -fsSL "$u/checksums.txt" | awk -v f="$f" '$2 == f {print $1}')"
    tar -xzf "$tmp/$f" -C "$tmp" crane
    sudo install -m 0755 "$tmp/crane" /usr/local/bin/crane
  fi

  if ansible-galaxy collection list kubernetes.core 2>/dev/null | grep -q "kubernetes.core *$KUBERNETES_CORE_VERSION"; then
    ok "kubernetes.core $KUBERNETES_CORE_VERSION"
  else
    inst "kubernetes.core $KUBERNETES_CORE_VERSION"
    ansible-galaxy collection install --force "kubernetes.core:==$KUBERNETES_CORE_VERSION"
  fi

  # The client must live in the Python that runs ansible-playbook.
  py="$(head -n1 "$(command -v ansible-playbook)" | sed -E 's/^#! *//; s/ .*//')"
  if "$py" -c 'import kubernetes' 2>/dev/null; then
    ok "python kubernetes client ($py)"
  else
    inst "python kubernetes client into $py"
    case "$py" in
      # pipx: inject into the venv that runs ansible-playbook (ansible or ansible-core).
      */pipx/venvs/*) v="${py#*/pipx/venvs/}"; pipx inject "${v%%/*}" kubernetes ;;
      # OS Python (apt ansible): pip is blocked there (PEP 668), so use the OS package.
      /usr/bin/*) sudo apt-get install -y python3-kubernetes ;;
      *) "$py" -m pip install kubernetes \
           || { log "ERROR: could not install it; run: $py -m pip install kubernetes"; exit 1; } ;;
    esac
  fi
fi

log "All prerequisites satisfied."
if [ "$K3S" = true ]; then
  log "Next: edit inventory.ini + vars/<env>.yml + secrets.yml, then:"
  log "      ansible-playbook k3s.yml -l <env>"
else
  log "Next: edit inventory.ini + vars/dev.yml + secrets.yml, then:"
  log "      ansible-playbook deploy.yml -l dev"
fi

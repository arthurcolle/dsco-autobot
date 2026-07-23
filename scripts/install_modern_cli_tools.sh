#!/usr/bin/env bash
set -euo pipefail

INSTALL_CASKS=true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-casks)
      INSTALL_CASKS=false
      shift
      ;;
    *)
      echo "[install_modern_cli_tools] Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

BREW_TAPS=()

BREW_FORMULAE=(
  atuin
  bandwhich
  bat
  bottom
  btop
  dust
  dua-cli
  diskonaut
  direnv
  duf
  eza
  fd
  fzf
  git-delta
  gitui
  glow
  gping
  gdu
  gh
  git
  httpie
  k9s
  lazygit
  lnav
  mdp
  mise
  mprocs
  mtr
  newsboat
  nnn
  pipx
  procs
  ripgrep
  ripgrep-all
  skim
  stern
  task  # Taskwarrior CLI was renamed in Homebrew
  termscp
  uv
  wtutil_placeholder
  xplr
  yazi
  yq
  zellij
  zoxide
)

# Replace placeholder with actual formula name once verified
BREW_FORMULAE=("${BREW_FORMULAE[@]/wtutil_placeholder/wtfutil}")

BREW_CASKS=(
  font-jetbrains-mono-nerd-font
  wezterm
)

PIPX_APPS=(
  rainbowstream
  tuir
)

GH_EXTENSIONS=(
  dlvhdr/gh-dash
)

function ensure_homebrew() {
  if command -v brew >/dev/null 2>&1; then
    return
  fi

  local os
  os=$(uname -s)
  echo "[install_modern_cli_tools] Installing Homebrew for ${os}..."
  if [[ "${os}" == "Darwin" ]]; then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  elif [[ "${os}" == "Linux" ]]; then
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  else
    echo "[install_modern_cli_tools] Unsupported OS: ${os}" >&2
    exit 1
  fi
}

function source_brew_env() {
  if command -v brew >/dev/null 2>&1; then
    return
  fi
  local candidates=(
    /opt/homebrew/bin/brew
    /usr/local/bin/brew
    /home/linuxbrew/.linuxbrew/bin/brew
  )
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]]; then
      eval "$(${candidate} shellenv)"
      return
    fi
  done
  echo "[install_modern_cli_tools] Unable to locate brew binary after installation" >&2
  exit 1
}

function ensure_taps() {
  if ((${#BREW_TAPS[@]} == 0)); then
    return
  fi
  for tap in "${BREW_TAPS[@]}"; do
    if ! brew tap | grep -q "^${tap}$"; then
      if ! brew tap "${tap}"; then
        echo "[install_modern_cli_tools] Warning: failed to tap ${tap}; continuing" >&2
      fi
    fi
  done
}

function install_formulae() {
  for formula in "${BREW_FORMULAE[@]}"; do
    if brew list --formula --versions "${formula}" >/dev/null 2>&1; then
      brew upgrade "${formula}" || true
    else
      brew install "${formula}"
    fi
  done
}

function install_casks() {
  if [[ "${INSTALL_CASKS}" != true ]]; then
    echo "[install_modern_cli_tools] Skipping casks per flag"
    return
  fi

  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "[install_modern_cli_tools] Skipping casks (not macOS)"
    return
  fi

  for cask in "${BREW_CASKS[@]}"; do
    if brew list --cask --versions "${cask}" >/dev/null 2>&1; then
      brew upgrade --cask "${cask}" || true
    else
      brew install --cask "${cask}"
    fi
  done
}

function install_pipx_apps() {
  if ! command -v pipx >/dev/null 2>&1; then
    echo "[install_modern_cli_tools] pipx not found; was Homebrew allowed to install it?" >&2
    return
  fi
  pipx ensurepath >/dev/null 2>&1 || true
  for app in "${PIPX_APPS[@]}"; do
    if ! pipx install --force "${app}"; then
      echo "[install_modern_cli_tools] Warning: failed to install ${app} via pipx; continuing" >&2
    fi
  done
}

function install_gh_extensions() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "[install_modern_cli_tools] GitHub CLI unavailable; skipping extension install"
    return
  fi
  local existing
  if ! existing=$(gh extension list 2>/dev/null | awk '{print $1}'); then
    echo "[install_modern_cli_tools] Warning: unable to list gh extensions (auth configured?); skipping" >&2
    return
  fi
  for ext in "${GH_EXTENSIONS[@]}"; do
    if grep -q "${ext}" <<<"${existing}"; then
      gh extension upgrade "${ext}" || true
    else
      gh extension install "${ext}" || true
    fi
  done
}

ensure_homebrew
source_brew_env
brew update
ensure_taps
install_formulae
install_casks
install_pipx_apps
install_gh_extensions
brew cleanup

echo "[install_modern_cli_tools] Tool installation complete."

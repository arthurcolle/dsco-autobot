#!/usr/bin/env bash
set -uo pipefail
IFS=$'\n\t'

# Require bash 4+ for associative arrays
if ((BASH_VERSINFO[0] < 4)); then
    echo "Error: This script requires bash 4.0 or later"
    echo "Your version: $BASH_VERSION"
    echo ""
    echo "Install newer bash: brew install bash"
    echo "Then run: /opt/homebrew/bin/bash $0 $*"
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════════════════
#  MODERN CLI ARSENAL INSTALLER
#  Transform your terminal into a powerhouse
# ═══════════════════════════════════════════════════════════════════════════════

VERSION="2.0.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPING_FILE="${SCRIPT_DIR}/mapping_entries.txt"
STATE_DIR="${HOME}/.local/share/modern-cli-installer"
STATE_FILE="${STATE_DIR}/installed.json"
LOG_FILE="${STATE_DIR}/install.log"
CONFIG_FILE="${STATE_DIR}/config.json"

# ─────────────────────────────────────────────────────────────────────────────
# ANSI Escape Codes (using $'...' for proper escape interpretation)
# ─────────────────────────────────────────────────────────────────────────────
readonly RESET=$'\033[0m'
readonly BOLD=$'\033[1m'
readonly DIM=$'\033[2m'
readonly ITALIC=$'\033[3m'
readonly UNDERLINE=$'\033[4m'

# Colors
readonly BLACK=$'\033[0;30m'
readonly RED=$'\033[0;31m'
readonly GREEN=$'\033[0;32m'
readonly YELLOW=$'\033[0;33m'
readonly BLUE=$'\033[0;34m'
readonly MAGENTA=$'\033[0;35m'
readonly CYAN=$'\033[0;36m'
readonly WHITE=$'\033[0;37m'

# Bright colors
readonly BRIGHT_RED=$'\033[1;31m'
readonly BRIGHT_GREEN=$'\033[1;32m'
readonly BRIGHT_YELLOW=$'\033[1;33m'
readonly BRIGHT_BLUE=$'\033[1;34m'
readonly BRIGHT_MAGENTA=$'\033[1;35m'
readonly BRIGHT_CYAN=$'\033[1;36m'

# Background colors
readonly BG_RED=$'\033[41m'
readonly BG_GREEN=$'\033[42m'
readonly BG_YELLOW=$'\033[43m'
readonly BG_BLUE=$'\033[44m'

# Icons (requires Nerd Font for best results, fallback to Unicode)
readonly ICON_CHECK="✓"
readonly ICON_CROSS="✗"
readonly ICON_ARROW="→"
readonly ICON_BULLET="•"
readonly ICON_STAR="★"
readonly ICON_GEAR="⚙"
readonly ICON_PACKAGE="📦"
readonly ICON_ROCKET="🚀"
readonly ICON_WARNING="⚠"
readonly ICON_INFO="ℹ"
readonly ICON_LOCK="🔒"
readonly ICON_CLOUD="☁"
readonly ICON_TERMINAL="❯"

# ─────────────────────────────────────────────────────────────────────────────
# Globals
# ─────────────────────────────────────────────────────────────────────────────
PARALLEL_JOBS=4
DRY_RUN=false
VERBOSE=false
FORCE=false
QUIET=false
PARALLEL_AVAILABLE=false
declare -a FAILED_INSTALLS=()
declare -a SUCCESSFUL_INSTALLS=()
declare -A TOOL_CATEGORIES=()
declare -A CATEGORY_TOOLS=()
declare -A ALL_TOOLS=()
declare -a ORDERED_TOOLS=()

# ─────────────────────────────────────────────────────────────────────────────
# Logging Functions
# ─────────────────────────────────────────────────────────────────────────────
log() {
    local level="$1"
    shift
    local msg="$*"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    mkdir -p "$STATE_DIR"
    echo "[$timestamp] [$level] $msg" >> "$LOG_FILE"

    if [[ "$QUIET" == "true" && "$level" != "ERROR" ]]; then
        return
    fi

    case "$level" in
        INFO)    echo -e "${CYAN}${ICON_INFO}${RESET}  $msg" ;;
        OK)      echo -e "${GREEN}${ICON_CHECK}${RESET}  $msg" ;;
        WARN)    echo -e "${YELLOW}${ICON_WARNING}${RESET}  $msg" ;;
        ERROR)   echo -e "${RED}${ICON_CROSS}${RESET}  $msg" ;;
        DEBUG)   [[ "$VERBOSE" == "true" ]] && echo -e "${DIM}   $msg${RESET}" ;;
        *)       echo -e "$msg" ;;
    esac
}

info()  { log INFO "$@"; }
ok()    { log OK "$@"; }
warn()  { log WARN "$@"; }
error() { log ERROR "$@"; }
debug() { log DEBUG "$@"; }

# ─────────────────────────────────────────────────────────────────────────────
# UI Components
# ─────────────────────────────────────────────────────────────────────────────
print_header() {
    echo
    echo "${BRIGHT_CYAN}╔══════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                      ║"
    echo "║   ${BOLD}${WHITE}███╗   ███╗ ██████╗ ██████╗ ███████╗██████╗ ███╗   ██╗${RESET}${BRIGHT_CYAN}             ║"
    echo "║   ${BOLD}${WHITE}████╗ ████║██╔═══██╗██╔══██╗██╔════╝██╔══██╗████╗  ██║${RESET}${BRIGHT_CYAN}             ║"
    echo "║   ${BOLD}${WHITE}██╔████╔██║██║   ██║██║  ██║█████╗  ██████╔╝██╔██╗ ██║${RESET}${BRIGHT_CYAN}             ║"
    echo "║   ${BOLD}${WHITE}██║╚██╔╝██║██║   ██║██║  ██║██╔══╝  ██╔══██╗██║╚██╗██║${RESET}${BRIGHT_CYAN}             ║"
    echo "║   ${BOLD}${WHITE}██║ ╚═╝ ██║╚██████╔╝██████╔╝███████╗██║  ██║██║ ╚████║${RESET}${BRIGHT_CYAN}             ║"
    echo "║   ${BOLD}${WHITE}╚═╝     ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝${RESET}${BRIGHT_CYAN}             ║"
    echo "║                                                                      ║"
    echo "║         ${WHITE}CLI Arsenal Installer ${DIM}v${VERSION}${RESET}${BRIGHT_CYAN}                              ║"
    echo "║         ${DIM}Transform your terminal into a powerhouse${RESET}${BRIGHT_CYAN}                 ║"
    echo "║                                                                      ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝${RESET}"
    echo
}

print_section() {
    local title="$1"
    echo
    echo -e "${BRIGHT_BLUE}┌─${BOLD} $title ${RESET}${BRIGHT_BLUE}$( printf '─%.0s' $(seq 1 $((65 - ${#title}))) )┐${RESET}"
}

print_section_end() {
    echo -e "${BRIGHT_BLUE}└$( printf '─%.0s' $(seq 1 70) )┘${RESET}"
}

print_category() {
    local cat="$1"
    local count="$2"
    echo -e "  ${MAGENTA}${ICON_BULLET}${RESET} ${BOLD}$cat${RESET} ${DIM}($count tools)${RESET}"
}

progress_bar() {
    local current=$1
    local total=$2
    local width=40
    local percent=$((current * 100 / total))
    local filled=$((current * width / total))
    local empty=$((width - filled))

    printf "\r  %s[%s" "$DIM" "$RESET"
    printf "%s%${filled}s%s" "$GREEN" "" "$RESET" | tr ' ' '█'
    printf "%s%${empty}s%s" "$DIM" "" "$RESET" | tr ' ' '░'
    printf "%s]%s %s%3d%%%s (%d/%d)" "$DIM" "$RESET" "$BOLD" "$percent" "$RESET" "$current" "$total"
}

spinner() {
    local pid=$1
    local msg="${2:-Processing}"
    local chars="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local i=0

    while kill -0 "$pid" 2>/dev/null; do
        printf "\r  ${CYAN}${chars:$i:1}${RESET} %s..." "$msg"
        i=$(( (i + 1) % ${#chars} ))
        sleep 0.1
    done
    printf "\r"
}

# ─────────────────────────────────────────────────────────────────────────────
# Brew Package Name Mappings (when command != package name)
# ─────────────────────────────────────────────────────────────────────────────
declare -A BREW_NAMES=(
    # Core tools
    ["ripgrep"]="ripgrep"
    ["rga"]="ripgrep-all"
    ["btm"]="bottom"
    ["delta"]="git-delta"
    ["tldr"]="tealdeer"
    ["nvim"]="neovim"

    # Data processing
    ["mlr"]="miller"
    ["choose"]="choose-rust"
    ["nu"]="nushell"

    # Container/K8s
    ["kubectl"]="kubernetes-cli"
    ["docker-compose"]="docker-compose"
    ["mongodb"]="mongodb-community"
    ["buildah"]="buildah"
    ["kdash"]="kdash-rs/kdash/kdash"
    ["kail"]="boz/repo/kail"
    ["kubefwd"]="txn2/tap/kubefwd"
    ["ketall"]="corneliusweig/ketall/ketall"
    ["kubectl-neat"]="itaysk/kube-capacity/kubectl-neat"

    # Cloud CLIs
    ["aws"]="awscli"
    ["gcloud"]="google-cloud-sdk"
    ["az"]="azure-cli"
    ["flyctl"]="flyctl"
    ["awslocal"]="localstack/tap/localstack-cli"

    # Dev tools
    ["cargo"]="rust"
    ["node"]="node"
    ["termtosvg"]="term-to-svg"
    ["pet"]="knqyf263/pet/pet"
    ["taskwarrior"]="task"
    ["turbo"]="turborepo"
    ["devbox"]="jetify-com/devbox/devbox"

    # Networking
    ["dog"]="doggo"
    ["ss"]="iproute2mac"
    ["ip"]="iproute2mac"
    ["borg"]="borgbackup"

    # Database tools
    ["usql"]="xo/xo/usql"
    ["pg_activity"]="pg_activity"

    # Data tools
    ["frawk"]="frawk"
    ["vector"]="vectordotdev/brew/vector"
    ["inferno-flamegraph"]="inferno"
    ["mamba"]="micromamba"

    # Terminal tools
    ["rip"]="rm-improved"
    ["qmv"]="renameutils"
    ["dstat"]="dstat"
    ["atop"]="atop"

    # Casks (GUI apps that need --cask)
    ["postman"]="postman"
    ["insomnia"]="insomnia"
    ["browsh"]="browsh-org/homebrew-browsh/browsh"

    # Taps required
    ["flux"]="fluxcd/tap/flux"
    ["turso"]="tursodatabase/tap/turso"
    ["doppler"]="dopplerhq/cli/doppler"
    ["wrangler"]="cloudflare-wrangler2"
    ["cilium"]="cilium-cli"
    ["1password"]="1password-cli"
    ["bitwarden"]="bitwarden-cli"
    ["vercel"]="vercel-cli"
    ["netlify"]="netlify-cli"
    ["rich-cli"]="rich"
    ["ab"]="httpd"
    ["sha256sum"]="coreutils"
    ["flock"]="util-linux"
    ["timeout"]="coreutils"
    ["xargs"]="findutils"
    ["dig"]="bind"
    ["tshark"]="wireshark"
    ["dot"]="graphviz"
    ["speedtest"]="speedtest-cli"
    ["rpk"]="redpanda-data/tap/redpanda"
    ["toxiproxy"]="shopify/shopify/toxiproxy"
    ["qalc"]="qalculate-gtk"
    ["vector"]="vectordotdev/brew/vector"
    ["temporal"]="temporalio/brew/temporal"
    ["browsh"]="browsh-org/homebrew-browsh/browsh"
    ["aria2c"]="aria2"
    ["sqlite3"]="sqlite"
    ["sgpt"]="shell-gpt"
    ["redis-cli"]="redis"
    ["litestream"]="benbjohnson/litestream/litestream"
    ["mutagen"]="mutagen-io/mutagen/mutagen"
    ["dotenvx"]="dotenvx/brew/dotenvx"
    ["gomuks"]="gomuks"
    ["ticker"]="achannarasappa/tap/ticker"
    ["kmon"]="orhun/tap/kmon"
)

# ─────────────────────────────────────────────────────────────────────────────
# Tools requiring special handling
# ─────────────────────────────────────────────────────────────────────────────
declare -A SKIP_TOOLS=(
    # Linux-only tools
    ["rizin"]="complex install - use: brew install rizin/tap/rizin"
    ["r2strings"]="part of rizin"
    ["bcc"]="requires kernel headers (Linux only)"
    ["bpftrace"]="requires kernel headers (Linux only)"
    ["ebpf"]="kernel module (Linux only)"
    ["iotop-bpf"]="Linux only - requires BPF"
    ["bpftrace top"]="Linux only"
    ["lsfd"]="Linux util-linux only"
    ["zenith"]="requires sensors (Linux)"
    ["nvtop"]="GPU monitoring - install separately"
    ["plocate"]="Linux only (mlocate replacement)"
    ["distrobox"]="Linux only (container distro manager)"
    ["wavemon"]="Linux only (WiFi monitor)"
    ["pulsemixer"]="Linux only (PulseAudio)"

    # pip installs
    ["pwndbg"]="pip install pwndbg"
    ["frida-trace"]="pip install frida-tools"
    ["s-tui"]="pip install s-tui"
    ["bropages"]="gem install bropages"
    ["cht.sh"]="curl https://cht.sh/:cht.sh > /usr/local/bin/cht.sh"
    ["lookatme"]="pip install lookatme"
    ["shot-scraper"]="pip install shot-scraper"
    ["data-diff"]="pip install data-diff"
    ["mlflow"]="pip install mlflow"
    ["aim"]="pip install aim"
    ["grab-site"]="pip install grab-site"
    ["termgraph"]="pip install termgraph"
    ["tiptop"]="pip install tiptop"
    ["pyclean"]="pip install pyclean"

    # npm installs
    ["carbon-now-cli"]="npm install -g carbon-now-cli"
    ["markmap-cli"]="npm install -g markmap-cli"
    ["hygen"]="npm install -g hygen"
    ["plop"]="npm install -g plop"
    ["changeset"]="npm install -g @changesets/cli"
    ["cz"]="npm install -g commitizen"
    ["release-please"]="npm install -g release-please"
    ["mockoon-cli"]="npm install -g @mockoon/cli"
    ["spectral"]="npm install -g @stoplight/spectral-cli"
    ["miniflare"]="npm install -g miniflare"
    ["azurite"]="npm install -g azurite"
    ["taskbook"]="npm install -g taskbook"

    # Rust/cargo installs
    ["termusic"]="cargo install termusic"
    ["pipes-rs"]="cargo install pipes-rs"
    ["battop"]="cargo install battop"
    ["dijo"]="cargo install dijo"
    ["hackernews-tui"]="cargo install hackernews_tui"
    ["calcure"]="pip install calcure"

    # Other special installs
    ["inspec"]="gem install inspec"
    ["wrp"]="go install github.com/nickstenning/wrp@latest"
    ["netdata-cli"]="part of netdata package"
    ["tuptime"]="brew install tuptime (may need tap)"
    ["mtail"]="go install github.com/google/mtail@latest"
    ["promql-cli"]="go install github.com/nalbury/promql-cli@latest"
    ["pgcenter"]="go install github.com/lesovsky/pgcenter@latest"
    ["kaf"]="brew tap birdayz/kaf && brew install kaf"
    ["rpk"]="use redpanda-cli instead"
    ["toxiproxy-cli"]="brew install shopify/shopify/toxiproxy"
    ["ptw"]="pip install pytest-watch"
    ["diec"]="cargo install detect-it-easy"
    ["restler"]="pip install ms-restler"
    ["wiremock"]="docker run -it wiremock/wiremock"
    ["lakectl"]="from lakeFS releases"
    ["guild"]="pip install guildai"
    ["temporalite"]="from temporal releases"
    ["devdash"]="go install github.com/Phantas0s/devdash@latest"
    ["peaclock"]="cargo install peaclock"
    ["termdbms"]="go install github.com/mathaou/termdbms@latest"
    ["chadburn"]="docker image"

    # macOS unavailable
    ["dstat"]="Linux only (use vm_stat on macOS)"
    ["atop"]="Linux only (use htop/btop on macOS)"
    ["inferno-flamegraph"]="cargo install inferno"
    ["dog"]="use doggo instead: brew install doggo"
    ["frawk"]="cargo install frawk"
    ["pg_activity"]="pip install pg_activity"
    ["buildah"]="Linux only (use docker on macOS)"
    ["kubectl-neat"]="krew install neat"
    ["ketall"]="krew install get-all"
    ["devbox"]="curl -fsSL https://get.jetpack.io/devbox | bash"
    ["awslocal"]="pip install awscli-local"
    ["kmon"]="cargo install kmon"
    ["gomuks"]="go install maunium.net/go/gomuks@latest"
    ["tabview"]="pip install tabview"
)

# ─────────────────────────────────────────────────────────────────────────────
# Linux-only tools (skip on macOS)
# ─────────────────────────────────────────────────────────────────────────────
declare -A LINUX_ONLY=(
    ["dstat"]=1 ["atop"]=1 ["ss"]=1 ["plocate"]=1 ["kmon"]=1
    ["wavemon"]=1 ["battop"]=1 ["pulsemixer"]=1 ["buildah"]=1
    ["distrobox"]=1
)

# ─────────────────────────────────────────────────────────────────────────────
# Fallback installers (pip, npm, cargo, go)
# ─────────────────────────────────────────────────────────────────────────────
declare -A PIP_INSTALL=(
    ["tabview"]="tabview" ["pudb"]="pudb" ["pg_activity"]="pg_activity"
    ["lookatme"]="lookatme" ["termgraph"]="termgraph" ["shot-scraper"]="shot-scraper"
    ["mlflow"]="mlflow" ["aim"]="aim" ["guildai"]="guildai"
    ["awslocal"]="awscli-local" ["localstack"]="localstack"
    ["httpie"]="httpie" ["glances"]="glances" ["pre-commit"]="pre-commit"
    ["bropages"]="bropages" ["ptw"]="pytest-watch" ["data-diff"]="data-diff"
    ["frictionless"]="frictionless" ["great-expectations"]="great-expectations"
    ["pandera"]="pandera" ["python-lsp-server"]="python-lsp-server"
)

declare -A NPM_INSTALL=(
    ["turbo"]="turbo" ["changeset"]="@changesets/cli" ["cz"]="commitizen"
    ["release-please"]="release-it" ["hygen"]="hygen" ["plop"]="plop"
    ["spectral"]="@stoplight/spectral-cli" ["markmap-cli"]="markmap-cli"
    ["carbon-now-cli"]="carbon-now-cli" ["mockoon-cli"]="@mockoon/cli"
    ["azurite"]="azurite" ["miniflare"]="wrangler"
)

declare -A CARGO_INSTALL=(
    ["inferno-flamegraph"]="inferno" ["frawk"]="frawk" ["kmon"]="kmon"
    ["battop"]="battop" ["pipes-rs"]="pipes-rs" ["termusic"]="termusic"
    ["rip"]="rm-improved" ["kdash"]="kdash" ["dijo"]="dijo"
    ["hackernews-tui"]="hackernews_tui" ["peaclock"]="peaclock"
)

declare -A GO_INSTALL=(
    ["gomuks"]="maunium.net/go/gomuks@latest"
    ["devdash"]="github.com/Phantas0s/devdash@latest"
    ["mtail"]="github.com/google/mtail/cmd/mtail@latest"
    ["termdbms"]="github.com/mathaou/termdbms@latest"
)

# ─────────────────────────────────────────────────────────────────────────────
# Curated Tool Profiles
# ─────────────────────────────────────────────────────────────────────────────
declare -A PROFILES

PROFILES[essential]="bat eza fd ripgrep fzf zoxide starship delta lazygit btop dust duf jq yq httpie tealdeer glow atuin"

PROFILES[developer]="${PROFILES[essential]} helix micro neovim just watchexec gh glab direnv mise tokei hyperfine shellcheck shfmt"

PROFILES[devops]="${PROFILES[developer]} k9s kubectl kubectx stern helm kustomize dive lazydocker ctop terraform trivy grype syft act"

PROFILES[security]="${PROFILES[essential]} age sops gitleaks semgrep nmap rustscan nuclei httpx subfinder amass b3sum"

PROFILES[data]="${PROFILES[essential]} duckdb miller xsv qsv visidata jless fx dasel csvkit litecli pgcli mycli usql"

PROFILES[sysadmin]="${PROFILES[essential]} htop glances procs bandwhich mtr gping dog ncdu restic rclone mosh croc"

PROFILES[full]="bat eza lsd fd ripgrep ripgrep-all fzf broot zoxide starship delta difftastic lazygit gitui tig
    btop bottom procs dust duf dua-cli gdu ncdu jq jaq yq dasel gron fx jless httpie xh curlie
    helix micro neovim just task watchexec entr gh glab direnv mise asdf pipx uv
    tealdeer glow mdcat rich hexyl tokei scc hyperfine
    k9s kubectl kubectx kubens stern helm kustomize dive lazydocker ctop podman
    terraform opentofu terragrunt trivy grype syft semgrep gitleaks age sops
    shellcheck shfmt ruff biome dprint
    mtr gping dog bandwhich nmap termshark mosh croc magic-wormhole
    restic rclone aria2 atuin mcfly navi pet
    duckdb miller xsv qsv visidata litecli pgcli mycli iredis usql
    asciinema vhs silicon chafa viu
    ollama pass gopass chezmoi stow tmux zellij
    act earthly dagger miniserve
    tree htop glances watch viddy parallel
    onefetch git-sizer git-filter-repo
    gum charm newsboat khal khard calcurse"

# ─────────────────────────────────────────────────────────────────────────────
# Category Definitions (for display and filtering)
# ─────────────────────────────────────────────────────────────────────────────
declare -A CATEGORIES=(
    ["viewing"]="bat moar most hexyl glow mdcat jless rich-cli"
    ["search"]="ripgrep ripgrep-all fd fzf broot skim plocate atuin"
    ["files"]="eza lsd dust duf dua-cli gdu ncdu zoxide broot yazi nnn xcp ouch trash-cli"
    ["process"]="btop bottom procs htop glances viddy zenith"
    ["network"]="httpie xh curlie gping mtr dog bandwhich termshark mosh nmap rustscan"
    ["git"]="lazygit gitui tig delta difftastic gh glab git-absorb"
    ["data"]="jq jaq yq sd miller xsv qsv dasel gron fx jless visidata duckdb"
    ["container"]="lazydocker ctop dive podman buildah skopeo k9s kubectl kubectx stern helm"
    ["security"]="age sops gitleaks trivy grype syft semgrep nmap nuclei b3sum"
    ["productivity"]="direnv mise starship zellij tmux atuin mcfly navi pet taskwarrior chezmoi"
    ["cloud"]="awscli azure-cli google-cloud-sdk terraform opentofu terragrunt pulumi"
    ["database"]="pgcli mycli litecli iredis usql duckdb"
)

# ─────────────────────────────────────────────────────────────────────────────
# Prerequisite Checks
# ─────────────────────────────────────────────────────────────────────────────
check_prerequisites() {
    local missing=()

    # Check for Homebrew
    if ! command -v brew &>/dev/null; then
        error "Homebrew not found"
        echo
        echo -e "  Install Homebrew first:"
        echo -e "  ${DIM}/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\"${RESET}"
        echo
        exit 1
    fi
    ok "Homebrew $(brew --version | head -1 | awk '{print $2}')"

    # Check for mapping file
    if [[ ! -f "$MAPPING_FILE" ]]; then
        error "Mapping file not found: $MAPPING_FILE"
        exit 1
    fi

    # Create state directory
    mkdir -p "$STATE_DIR"

    # Check for optional tools
    if command -v parallel &>/dev/null; then
        ok "GNU Parallel available (faster installs)"
        PARALLEL_AVAILABLE=true
    else
        info "GNU Parallel not found (install for faster parallel installs)"
        PARALLEL_AVAILABLE=false
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Tool Name Resolution
# ─────────────────────────────────────────────────────────────────────────────
extract_tool_name() {
    local line="$1"
    local tool
    tool=$(echo "$line" | sed -n 's/.*-> `\([^`]*\)`.*/\1/p' | head -1)
    tool=$(echo "$tool" | awk '{print $1}')
    tool=$(basename "$tool" 2>/dev/null || echo "$tool")
    echo "$tool"
}

get_brew_name() {
    local tool="$1"
    if [[ -n "${BREW_NAMES[$tool]:-}" ]]; then
        echo "${BREW_NAMES[$tool]}"
    else
        echo "$tool"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Installation Status Checking
# ─────────────────────────────────────────────────────────────────────────────
declare -A BREW_INSTALLED_CACHE=()
BREW_CACHE_LOADED=false

load_brew_cache() {
    if [[ "$BREW_CACHE_LOADED" == "true" ]]; then
        return 0
    fi

    # Load all installed formulae and casks into cache
    local pkg
    while IFS= read -r pkg; do
        [[ -n "$pkg" ]] && BREW_INSTALLED_CACHE["$pkg"]=1
    done < <(brew list --formula -1 2>/dev/null)

    while IFS= read -r pkg; do
        [[ -n "$pkg" ]] && BREW_INSTALLED_CACHE["$pkg"]=1
    done < <(brew list --cask -1 2>/dev/null)

    BREW_CACHE_LOADED=true
}

is_installed() {
    local pkg="$1"

    # First check if command exists (fast)
    if command -v "$pkg" &>/dev/null; then
        return 0
    fi

    # Load brew cache if needed
    load_brew_cache

    # Check cache
    if [[ -n "${BREW_INSTALLED_CACHE[$pkg]:-}" ]]; then
        return 0
    fi

    # Check by brew name mapping
    local brew_name
    brew_name=$(get_brew_name "$pkg")
    if [[ "$brew_name" != "$pkg" ]]; then
        if [[ -n "${BREW_INSTALLED_CACHE[$brew_name]:-}" ]]; then
            return 0
        fi
    fi

    return 1
}

get_installed_version() {
    local pkg="$1"
    local version=""

    # Try to get version from brew
    if brew list "$pkg" &>/dev/null 2>&1; then
        version=$(brew info "$pkg" 2>/dev/null | head -1 | awk '{print $3}')
    fi

    echo "${version:-unknown}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Parse Mapping File
# ─────────────────────────────────────────────────────────────────────────────
parse_mapping_file() {
    local current_category=""
    ALL_TOOLS=()
    ORDERED_TOOLS=()

    while IFS= read -r line || [[ -n "$line" ]]; do
        # Category header
        if [[ "$line" =~ ^#[[:space:]]*(.*) ]]; then
            current_category="${BASH_REMATCH[1]}"
            continue
        fi

        # Skip empty lines
        [[ -z "$line" ]] && continue

        local tool
        tool=$(extract_tool_name "$line")
        [[ -z "$tool" ]] && continue

        # Skip if already seen
        if [[ -n "${ALL_TOOLS[$tool]:-}" ]]; then
            continue
        fi

        # Skip problematic tools
        if [[ -n "${SKIP_TOOLS[$tool]:-}" ]]; then
            debug "Skipping $tool: ${SKIP_TOOLS[$tool]}"
            continue
        fi

        local brew_name
        brew_name=$(get_brew_name "$tool")

        ALL_TOOLS[$tool]="$brew_name"
        ORDERED_TOOLS+=("$tool")
        TOOL_CATEGORIES[$tool]="$current_category"

        # Add to category list
        if [[ -n "$current_category" ]]; then
            CATEGORY_TOOLS[$current_category]+="$tool "
        fi

    done < "$MAPPING_FILE"

    debug "Parsed ${#ALL_TOOLS[@]} unique tools from mapping file"
}

# ─────────────────────────────────────────────────────────────────────────────
# Installation Functions
# ─────────────────────────────────────────────────────────────────────────────
install_tool() {
    local tool="$1"
    local brew_name
    brew_name=$(get_brew_name "$tool")

    # Skip Linux-only tools on macOS
    if [[ "$(uname)" == "Darwin" && -n "${LINUX_ONLY[$tool]:-}" ]]; then
        debug "Skipping $tool (Linux only)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${DIM}Would install:${RESET} $brew_name"
        return 0
    fi

    debug "Installing $brew_name..."

    # Try formula first, then cask
    if brew install "$brew_name" &>/dev/null 2>&1; then
        SUCCESSFUL_INSTALLS+=("$tool")
        return 0
    elif brew install --cask "$brew_name" &>/dev/null 2>&1; then
        SUCCESSFUL_INSTALLS+=("$tool")
        return 0
    fi

    # Fallback: try pip
    if [[ -n "${PIP_INSTALL[$tool]:-}" ]]; then
        debug "Trying pip for $tool..."
        if pip3 install --user "${PIP_INSTALL[$tool]}" &>/dev/null 2>&1; then
            SUCCESSFUL_INSTALLS+=("$tool")
            return 0
        fi
    fi

    # Fallback: try npm
    if [[ -n "${NPM_INSTALL[$tool]:-}" ]]; then
        debug "Trying npm for $tool..."
        if npm install -g "${NPM_INSTALL[$tool]}" &>/dev/null 2>&1; then
            SUCCESSFUL_INSTALLS+=("$tool")
            return 0
        fi
    fi

    # Fallback: try cargo
    if [[ -n "${CARGO_INSTALL[$tool]:-}" ]]; then
        debug "Trying cargo for $tool..."
        if cargo install "${CARGO_INSTALL[$tool]}" &>/dev/null 2>&1; then
            SUCCESSFUL_INSTALLS+=("$tool")
            return 0
        fi
    fi

    # Fallback: try go
    if [[ -n "${GO_INSTALL[$tool]:-}" ]]; then
        debug "Trying go for $tool..."
        if go install "${GO_INSTALL[$tool]}" &>/dev/null 2>&1; then
            SUCCESSFUL_INSTALLS+=("$tool")
            return 0
        fi
    fi

    # All methods failed
    if true; then
        FAILED_INSTALLS+=("$tool")
        return 1
    fi
}

install_tools_parallel() {
    local -a tools=("$@")
    local total=${#tools[@]}
    local completed=0
    local failed=0

    if [[ "$PARALLEL_AVAILABLE" == "true" && ${#tools[@]} -gt 3 ]]; then
        # Use GNU Parallel
        export -f install_tool get_brew_name is_installed
        export BREW_NAMES DRY_RUN

        printf '%s\n' "${tools[@]}" | \
            parallel --bar --jobs "$PARALLEL_JOBS" install_tool {}
    else
        # Sequential fallback with progress
        for tool in "${tools[@]}"; do
            ((completed++))

            local brew_name
            brew_name=$(get_brew_name "$tool")

            if is_installed "$tool"; then
                printf "\r  ${GREEN}${ICON_CHECK}${RESET} %-30s ${DIM}(already installed)${RESET}\n" "$tool"
                continue
            fi

            if [[ "$DRY_RUN" == "true" ]]; then
                printf "\r  ${CYAN}○${RESET} %-30s ${DIM}(would install: %s)${RESET}\n" "$tool" "$brew_name"
            elif install_tool "$tool"; then
                printf "\r  ${GREEN}${ICON_CHECK}${RESET} %-30s ${GREEN}installed${RESET}\n" "$tool"
            else
                printf "\r  ${RED}${ICON_CROSS}${RESET} %-30s ${RED}failed${RESET}\n" "$tool"
                ((failed++))
            fi
        done
        echo
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────
cmd_list() {
    local filter="${1:-all}"
    local show_installed="${2:-true}"

    parse_mapping_file

    local installed_count=0
    local missing_count=0
    local current_cat=""

    print_section "Tool Inventory"

    for tool in "${ORDERED_TOOLS[@]}"; do
        local cat="${TOOL_CATEGORIES[$tool]:-Uncategorized}"

        if [[ "$cat" != "$current_cat" ]]; then
            [[ -n "$current_cat" ]] && echo
            current_cat="$cat"
            echo -e "  ${MAGENTA}${BOLD}$cat${RESET}"
        fi

        local brew_name
        brew_name=$(get_brew_name "$tool")

        if is_installed "$tool"; then
            ((installed_count++))
            if [[ "$show_installed" == "true" ]]; then
                local version
                version=$(get_installed_version "$brew_name")
                echo -e "    ${GREEN}${ICON_CHECK}${RESET} $tool ${DIM}($version)${RESET}"
            fi
        else
            ((missing_count++))
            echo -e "    ${DIM}○${RESET} $tool"
        fi
    done

    echo
    print_section_end
    echo
    echo -e "  ${GREEN}${installed_count}${RESET} installed  ${DIM}|${RESET}  ${YELLOW}${missing_count}${RESET} available"
    echo
}

cmd_install() {
    local profile="${1:-}"
    local tools_to_install=()

    parse_mapping_file

    if [[ -z "$profile" ]]; then
        # Interactive mode - show menu
        echo
        echo -e "  ${BOLD}Select installation profile:${RESET}"
        echo
        echo -e "    ${CYAN}1)${RESET} essential   ${DIM}- Core productivity tools (${#PROFILES[essential]})${RESET}"
        echo -e "    ${CYAN}2)${RESET} developer   ${DIM}- Full dev environment${RESET}"
        echo -e "    ${CYAN}3)${RESET} devops      ${DIM}- Container & cloud tools${RESET}"
        echo -e "    ${CYAN}4)${RESET} security    ${DIM}- Security & forensics${RESET}"
        echo -e "    ${CYAN}5)${RESET} data        ${DIM}- Data processing & DB tools${RESET}"
        echo -e "    ${CYAN}6)${RESET} sysadmin    ${DIM}- System administration${RESET}"
        echo -e "    ${CYAN}7)${RESET} full        ${DIM}- Everything (${#ALL_TOOLS[@]} tools)${RESET}"
        echo -e "    ${CYAN}8)${RESET} missing     ${DIM}- Only uninstalled tools${RESET}"
        echo
        read -rp "  Choice [1-8]: " choice

        case "$choice" in
            1) profile="essential" ;;
            2) profile="developer" ;;
            3) profile="devops" ;;
            4) profile="security" ;;
            5) profile="data" ;;
            6) profile="sysadmin" ;;
            7) profile="full" ;;
            8) profile="missing" ;;
            *) error "Invalid choice"; exit 1 ;;
        esac
    fi

    # Build tool list based on profile
    if [[ "$profile" == "missing" ]]; then
        for tool in "${ORDERED_TOOLS[@]}"; do
            if ! is_installed "$tool"; then
                tools_to_install+=("$tool")
            fi
        done
    elif [[ "$profile" == "all" || "$profile" == "full" ]]; then
        tools_to_install=("${ORDERED_TOOLS[@]}")
    elif [[ -n "${PROFILES[$profile]:-}" ]]; then
        read -ra tools_to_install <<< "${PROFILES[$profile]}"
    else
        error "Unknown profile: $profile"
        exit 1
    fi

    local total=${#tools_to_install[@]}
    if [[ $total -eq 0 ]]; then
        ok "Nothing to install - all tools already present!"
        return 0
    fi

    print_section "Installing ${profile} profile (${total} tools)"
    echo

    if [[ "$DRY_RUN" == "true" ]]; then
        info "Dry run mode - no changes will be made"
        echo
    fi

    install_tools_parallel "${tools_to_install[@]}"

    print_section_end
    echo

    # Summary
    echo -e "  ${BOLD}Summary:${RESET}"
    echo -e "    ${GREEN}${#SUCCESSFUL_INSTALLS[@]}${RESET} installed"
    echo -e "    ${RED}${#FAILED_INSTALLS[@]}${RESET} failed"

    if [[ ${#FAILED_INSTALLS[@]} -gt 0 ]]; then
        echo
        warn "Failed installations:"
        for tool in "${FAILED_INSTALLS[@]}"; do
            echo -e "    ${RED}${ICON_CROSS}${RESET} $tool"
        done
    fi
    echo
}

cmd_status() {
    parse_mapping_file

    local installed=0
    local missing=0

    for tool in "${ORDERED_TOOLS[@]}"; do
        if is_installed "$tool"; then
            ((installed++))
        else
            ((missing++))
        fi
    done

    local total=$((installed + missing))
    local percent=$((installed * 100 / total))

    print_section "Arsenal Status"
    echo
    echo -e "  ${BOLD}Installation Progress${RESET}"
    echo

    # Visual progress bar
    local bar_width=50
    local filled=$((installed * bar_width / total))
    local empty=$((bar_width - filled))

    printf "  "
    printf "%s%${filled}s%s" "$GREEN" "" "$RESET" | tr ' ' '█'
    printf "%s%${empty}s%s" "$DIM" "" "$RESET" | tr ' ' '░'
    printf " %s%d%%%s\n" "$BOLD" "$percent" "$RESET"
    echo
    echo -e "  ${GREEN}${installed}${RESET}/${total} tools installed"
    echo

    # Category breakdown
    echo -e "  ${BOLD}By Category:${RESET}"
    for cat in "${!CATEGORY_TOOLS[@]}"; do
        local cat_tools=(${CATEGORY_TOOLS[$cat]})
        local cat_installed=0
        for t in "${cat_tools[@]}"; do
            is_installed "$t" && ((cat_installed++))
        done
        local cat_total=${#cat_tools[@]}
        printf "    %-25s %2d/%2d\n" "$cat" "$cat_installed" "$cat_total"
    done

    print_section_end
    echo
}

cmd_search() {
    local query="$1"

    parse_mapping_file

    print_section "Search: $query"
    echo

    local found=0
    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ $query ]]; then
            local tool
            tool=$(extract_tool_name "$line")
            if [[ -n "$tool" ]]; then
                local status="${DIM}○${RESET}"
                is_installed "$tool" && status="${GREEN}${ICON_CHECK}${RESET}"

                # Extract description
                local desc
                desc=$(echo "$line" | sed 's/.*— //')

                echo -e "  $status ${BOLD}$tool${RESET}"
                echo -e "    ${DIM}$desc${RESET}"
                ((found++))
            fi
        fi
    done < "$MAPPING_FILE"

    if [[ $found -eq 0 ]]; then
        warn "No tools matching '$query'"
    else
        echo
        info "Found $found matching tools"
    fi

    print_section_end
    echo
}

cmd_configure() {
    local tool="${1:-}"

    if [[ -z "$tool" ]]; then
        print_section "Shell Configuration Helpers"
        echo
        echo -e "  Add these to your ${BOLD}~/.zshrc${RESET} or ${BOLD}~/.bashrc${RESET}:"
        echo

        # Starship
        if is_installed "starship"; then
            echo -e "  ${CYAN}# Starship prompt${RESET}"
            echo -e '  eval "$(starship init zsh)"  # or bash'
            echo
        fi

        # Zoxide
        if is_installed "zoxide"; then
            echo -e "  ${CYAN}# Zoxide (smart cd)${RESET}"
            echo -e '  eval "$(zoxide init zsh)"  # or bash'
            echo
        fi

        # Atuin
        if is_installed "atuin"; then
            echo -e "  ${CYAN}# Atuin (shell history)${RESET}"
            echo -e '  eval "$(atuin init zsh)"  # or bash'
            echo
        fi

        # Direnv
        if is_installed "direnv"; then
            echo -e "  ${CYAN}# Direnv (per-directory env)${RESET}"
            echo -e '  eval "$(direnv hook zsh)"  # or bash'
            echo
        fi

        # FZF
        if is_installed "fzf"; then
            echo -e "  ${CYAN}# FZF keybindings${RESET}"
            echo -e '  source <(fzf --zsh)  # or --bash'
            echo
        fi

        # Mise (rtx)
        if is_installed "mise"; then
            echo -e "  ${CYAN}# Mise (version manager)${RESET}"
            echo -e '  eval "$(mise activate zsh)"  # or bash'
            echo
        fi

        # Common aliases
        echo -e "  ${CYAN}# Recommended aliases${RESET}"
        is_installed "bat" && echo -e '  alias cat="bat"'
        is_installed "eza" && echo -e '  alias ls="eza --icons"'
        is_installed "eza" && echo -e '  alias ll="eza -la --icons --git"'
        is_installed "eza" && echo -e '  alias tree="eza --tree --icons"'
        is_installed "dust" && echo -e '  alias du="dust"'
        is_installed "duf" && echo -e '  alias df="duf"'
        is_installed "btop" && echo -e '  alias top="btop"'
        is_installed "fd" && echo -e '  alias find="fd"'
        is_installed "ripgrep" && echo -e '  alias grep="rg"'
        is_installed "procs" && echo -e '  alias ps="procs"'
        is_installed "httpie" && echo -e '  alias curl="http"'

        print_section_end
    fi
    echo
}

cmd_export() {
    local output="${1:-installed_tools.txt}"

    parse_mapping_file

    {
        echo "# Modern CLI Tools - Installed $(date '+%Y-%m-%d')"
        echo "# Generated by install_modern_tools.sh v${VERSION}"
        echo
        for tool in "${ORDERED_TOOLS[@]}"; do
            if is_installed "$tool"; then
                local brew_name
                brew_name=$(get_brew_name "$tool")
                echo "$brew_name"
            fi
        done
    } > "$output"

    ok "Exported installed tools to $output"
    info "Re-import on another machine: brew install \$(cat $output)"
}

cmd_doctor() {
    print_section "System Health Check"
    echo

    # Homebrew health
    echo -e "  ${BOLD}Homebrew:${RESET}"
    brew --version | head -1 | sed 's/^/    /'

    echo
    echo -e "  ${BOLD}Checking for issues...${RESET}"

    # Check for outdated tools
    local outdated
    outdated=$(brew outdated --quiet 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$outdated" -gt 0 ]]; then
        warn "$outdated packages can be updated (brew upgrade)"
    else
        ok "All packages up to date"
    fi

    # Check for broken symlinks
    local broken
    broken=$(brew doctor 2>&1 | grep -c "Warning" || true)
    if [[ "$broken" -gt 0 ]]; then
        warn "$broken warnings from brew doctor"
        info "Run 'brew doctor' for details"
    else
        ok "No brew issues detected"
    fi

    # Disk usage
    local brew_size
    brew_size=$(du -sh "$(brew --prefix)" 2>/dev/null | awk '{print $1}')
    info "Homebrew size: $brew_size"

    print_section_end
    echo
}

cmd_uninstall() {
    local tool="$1"

    if [[ -z "$tool" ]]; then
        error "Usage: $0 uninstall <tool>"
        exit 1
    fi

    local brew_name
    brew_name=$(get_brew_name "$tool")

    if ! is_installed "$tool"; then
        warn "$tool is not installed"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        info "Would uninstall: $brew_name"
        return 0
    fi

    if brew uninstall "$brew_name" 2>/dev/null; then
        ok "Uninstalled $tool"
    else
        error "Failed to uninstall $tool"
    fi
}

cmd_upgrade() {
    print_section "Upgrading All Installed Tools"
    echo

    info "Fetching latest package info..."
    brew update

    echo
    info "Upgrading packages..."
    brew upgrade

    echo
    ok "Upgrade complete"
    print_section_end
    echo
}

# ─────────────────────────────────────────────────────────────────────────────
# Help & Usage
# ─────────────────────────────────────────────────────────────────────────────
print_help() {
    echo
    echo -e "${BOLD}USAGE${RESET}"
    echo -e "    $0 [command] [options]"
    echo
    echo -e "${BOLD}COMMANDS${RESET}"
    echo -e "    ${CYAN}install${RESET} [profile]  Install tools (interactive if no profile given)"
    echo -e "                       Profiles: essential, developer, devops, security,"
    echo -e "                                data, sysadmin, full, missing"
    echo
    echo -e "    ${CYAN}list${RESET}               Show all tools with install status"
    echo -e "    ${CYAN}status${RESET}             Show installation progress dashboard"
    echo -e "    ${CYAN}search${RESET} <query>     Search for tools by name or description"
    echo -e "    ${CYAN}configure${RESET}          Show shell configuration snippets"
    echo
    echo -e "    ${CYAN}upgrade${RESET}            Upgrade all installed packages"
    echo -e "    ${CYAN}uninstall${RESET} <tool>   Remove a specific tool"
    echo -e "    ${CYAN}export${RESET} [file]      Export installed tools to file"
    echo -e "    ${CYAN}doctor${RESET}             Check system health"
    echo
    echo -e "${BOLD}OPTIONS${RESET}"
    echo -e "    ${CYAN}--dry-run${RESET}          Show what would be done without making changes"
    echo -e "    ${CYAN}--verbose${RESET}          Show detailed output"
    echo -e "    ${CYAN}--quiet${RESET}            Minimal output"
    echo -e "    ${CYAN}--parallel${RESET} N       Number of parallel installs (default: 4)"
    echo -e "    ${CYAN}--force${RESET}            Reinstall even if already installed"
    echo
    echo -e "${BOLD}EXAMPLES${RESET}"
    echo -e "    ${DIM}# Install essential tools${RESET}"
    echo -e "    $0 install essential"
    echo
    echo -e "    ${DIM}# Install everything that's missing${RESET}"
    echo -e "    $0 install missing"
    echo
    echo -e "    ${DIM}# Search for JSON tools${RESET}"
    echo -e "    $0 search json"
    echo
    echo -e "    ${DIM}# Preview what would be installed${RESET}"
    echo -e "    $0 install full --dry-run"
    echo
}

# ─────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────────────────────────────────────
main() {
    local cmd=""
    local args=()

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run)   DRY_RUN=true ;;
            --verbose)   VERBOSE=true ;;
            --quiet)     QUIET=true ;;
            --force)     FORCE=true ;;
            --parallel)  shift; PARALLEL_JOBS="${1:-4}" ;;
            --help|-h)   print_help; exit 0 ;;
            --version)   echo "v$VERSION"; exit 0 ;;
            -*)          error "Unknown option: $1"; print_help; exit 1 ;;
            *)
                if [[ -z "$cmd" ]]; then
                    cmd="$1"
                else
                    args+=("$1")
                fi
                ;;
        esac
        shift
    done

    # Default command
    [[ -z "$cmd" ]] && cmd="help"

    # Show header for most commands
    if [[ "$cmd" != "help" && "$cmd" != "--help" && "$QUIET" != "true" ]]; then
        print_header
        check_prerequisites
        echo
    fi

    # Execute command
    case "$cmd" in
        install)    cmd_install "${args[0]:-}" ;;
        list)       cmd_list "${args[@]:-}" ;;
        status)     cmd_status ;;
        search)     cmd_search "${args[0]:-}" ;;
        configure)  cmd_configure "${args[0]:-}" ;;
        export)     cmd_export "${args[0]:-}" ;;
        doctor)     cmd_doctor ;;
        uninstall)  cmd_uninstall "${args[0]:-}" ;;
        upgrade)    cmd_upgrade ;;
        help|--help|-h) print_help ;;
        *)          error "Unknown command: $cmd"; print_help; exit 1 ;;
    esac
}

main "$@"

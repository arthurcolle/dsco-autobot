FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG USERNAME=agent
ARG USER_UID=1000
ARG USER_GID=1000

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       curl \
       file \
       git \
       locales \
       nano \
       python3 \
       python3-pip \
       python3-venv \
       sudo \
       unzip \
       zip \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash --uid "$USER_UID" --gid "$USER_GID" "$USERNAME" \
    && usermod -aG sudo "$USERNAME" \
    && echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER "$USERNAME"
WORKDIR "/home/$USERNAME"
SHELL ["/bin/bash", "-lc"]

RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" \
    && eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)" \
    && echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> ~/.bashrc \
    && echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> ~/.profile

ENV HOMEBREW_PREFIX="/home/linuxbrew/.linuxbrew"
ENV PATH="$HOMEBREW_PREFIX/bin:$HOMEBREW_PREFIX/sbin:$PATH"

COPY scripts/install_modern_cli_tools.sh /tmp/install_modern_cli_tools.sh
RUN chmod +x /tmp/install_modern_cli_tools.sh \
    && /tmp/install_modern_cli_tools.sh --skip-casks

CMD ["/bin/bash"]

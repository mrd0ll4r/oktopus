# Docker environment to build the various IPFS indexer binaries in a stable
# environment.
# Compiled binaries can be found in /ipfs-indexer/target/release/.
# Sources are copied into /ipfs-indexer/.

FROM ubuntu:jammy AS chef

# Get build dependencies for Rust itself.
RUN apt-get update && apt-get install -y \
  curl \
  build-essential \
  git

# Get su-exec, a very minimal tool for dropping privileges.
ENV SUEXEC_VERSION v0.2
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        "amd64" | "armhf" | "arm64") tiniArch="tini-static-$dpkgArch" ;;\
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
  cd /tmp \
  && git clone https://github.com/ncopa/su-exec.git \
  && cd su-exec \
  && git checkout -q $SUEXEC_VERSION \
  && make su-exec-static

# Install Rust.
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install-rust.sh
RUN chmod +x install-rust.sh
RUN ./install-rust.sh -y

# Cargo-chef is used to build dependencies and cache them, for faster
# incremental builds.
RUN cargo install cargo-chef
WORKDIR ipfs-indexer

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

# Install OS-level dependencies.
RUN apt-get update && apt-get install -y \
  libssl-dev \
  protobuf-compiler \
  pkg-config \
  libpq-dev \
  libmagic-dev

# Get a list of Rust dependencies to build.
COPY --from=planner /ipfs-indexer/recipe.json recipe.json

# Build dependencies - this should be cached by docker.
RUN cargo chef cook --release --recipe-path recipe.json

# Build our project.
COPY . .
RUN cargo build --release --locked

# Carry along su-exec
COPY --from=0 /tmp/su-exec/su-exec-static /sbin/su-exec

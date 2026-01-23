#!/usr/bin/env bash
set -euo pipefail

# tidx Docker installer
# Usage: curl -L https://tidx.tempo.xyz/docker | bash

REPO="tempoxyz/tidx"
TIDX_HOME="${TIDX_HOME:-$HOME/.tidx}"
BIN_DIR="${TIDX_BIN:-$HOME/.local/bin}"

main() {
    echo "Installing tidx (Docker)..."

    # Create directories
    mkdir -p "$TIDX_HOME" "$BIN_DIR"

    # Download docker-compose and config
    echo "Downloading docker-compose.yml..."
    curl -sL "https://raw.githubusercontent.com/$REPO/main/docker/prod/docker-compose.yml" -o "$TIDX_HOME/docker-compose.yml"

    if [ ! -f "$TIDX_HOME/config.toml" ]; then
        echo "Downloading config.toml..."
        curl -sL "https://raw.githubusercontent.com/$REPO/main/docker/prod/config.toml" -o "$TIDX_HOME/config.toml"
    fi

    # Create tidx wrapper script
    cat > "$BIN_DIR/tidx" << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

TIDX_HOME="${TIDX_HOME:-$HOME/.tidx}"
cd "$TIDX_HOME"

case "${1:-}" in
    up)
        docker compose up -d
        ;;
    down)
        docker compose down
        ;;
    logs)
        docker compose logs -f tidx
        ;;
    *)
        docker compose exec tidx tidx "$@"
        ;;
esac
EOF
    chmod +x "$BIN_DIR/tidx"

    echo ""
    echo "tidx installed to $BIN_DIR/tidx"
    echo "Config: $TIDX_HOME/config.toml"
    echo ""

    # Check PATH
    if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
        echo "Add to PATH:"
        echo "  export PATH=\"$BIN_DIR:\$PATH\""
        echo ""
    fi

    echo "Run 'tidx up' to start indexing"
}

main

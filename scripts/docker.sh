#!/usr/bin/env bash
set -euo pipefail

# tidx Docker installer
# Usage: curl -L https://tidx.tempo.xyz/docker | bash

BASE_URL="https://tidx.vercel.app"
TIDX_HOME="${TIDX_HOME:-$HOME/.tidx}"
BIN_DIR="${TIDX_BIN:-$HOME/.local/bin}"

main() {
    echo "Installing tidx (Docker)..."

    # Create directories
    mkdir -p "$TIDX_HOME" "$BIN_DIR"

    # Download docker-compose and the files it bind-mounts
    echo "Downloading docker-compose.yml..."
    curl -fsSL "$BASE_URL/docker/prod/docker-compose.yml" -o "$TIDX_HOME/docker-compose.yml"

    for file in config.toml clickhouse-config.xml prometheus.yml alerts.yml; do
        # Docker creates a missing bind-mount source as an empty directory
        if [ -d "$TIDX_HOME/$file" ]; then
            rmdir "$TIDX_HOME/$file"
        fi

        if [ ! -f "$TIDX_HOME/$file" ]; then
            echo "Downloading $file..."
            curl -fsSL "$BASE_URL/docker/prod/$file" -o "$TIDX_HOME/$file"
        fi
    done

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

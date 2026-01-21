#!/usr/bin/env bash
set -euo pipefail

# ak47 installer
# Usage: curl -L https://ak47.wevm.dev/install | bash

REPO="wevm/ak47"
INSTALL_DIR="${AK47_INSTALL_DIR:-$HOME/.ak47/bin}"

main() {
    echo "Installing ak47..."

    # Detect OS and architecture
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$OS" in
        linux) OS="linux" ;;
        darwin) OS="macos" ;;
        *) echo "Unsupported OS: $OS"; exit 1 ;;
    esac

    case "$ARCH" in
        x86_64|amd64) ARCH="x86_64" ;;
        arm64|aarch64) ARCH="aarch64" ;;
        *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac

    # Get latest release
    LATEST=$(curl -s "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    
    if [ -z "$LATEST" ]; then
        echo "Failed to get latest release"
        exit 1
    fi

    echo "Latest version: $LATEST"

    # Build download URL
    FILENAME="ak47-${OS}-${ARCH}.tar.gz"
    URL="https://github.com/$REPO/releases/download/$LATEST/$FILENAME"

    # Create install directory
    mkdir -p "$INSTALL_DIR"

    # Download and extract
    echo "Downloading $URL..."
    curl -L "$URL" | tar xz -C "$INSTALL_DIR"

    # Make executable
    chmod +x "$INSTALL_DIR/ak47"

    echo ""
    echo "ak47 installed to $INSTALL_DIR/ak47"
    echo ""

    # Add to PATH instructions
    SHELL_NAME=$(basename "$SHELL")
    case "$SHELL_NAME" in
        zsh)
            PROFILE="$HOME/.zshrc"
            ;;
        bash)
            if [ -f "$HOME/.bash_profile" ]; then
                PROFILE="$HOME/.bash_profile"
            else
                PROFILE="$HOME/.bashrc"
            fi
            ;;
        fish)
            PROFILE="$HOME/.config/fish/config.fish"
            ;;
        *)
            PROFILE="$HOME/.profile"
            ;;
    esac

    if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
        echo "Add ak47 to your PATH by running:"
        echo ""
        if [ "$SHELL_NAME" = "fish" ]; then
            echo "  echo 'set -gx PATH $INSTALL_DIR \$PATH' >> $PROFILE"
        else
            echo "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> $PROFILE"
        fi
        echo ""
        echo "Then restart your shell or run:"
        echo "  source $PROFILE"
    fi

    echo ""
    echo "Run 'ak47 --help' to get started"
}

main


#!/usr/bin/env bash
# ABOUTME: Full bootstrap script for living-graph on a fresh clone.
# ABOUTME: Creates venv, installs deps, configures .env, verifies Ollama, installs launchd.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
PLIST_NAME="com.weytani.living-graph"
PLIST_TEMPLATE="$PROJECT_DIR/scripts/${PLIST_NAME}.plist.template"
PLIST_TARGET="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"
LOG_DIR="$HOME/Library/Logs/living-graph"

echo "=== Living Graph Setup ==="
echo "Project: $PROJECT_DIR"
echo ""

# --- Step 1: Python ---
echo "[1/6] Checking Python..."
if command -v python3.14 &>/dev/null; then
    PYTHON=python3.14
elif command -v python3.13 &>/dev/null; then
    PYTHON=python3.13
elif command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
    if [ "$PY_MAJOR" -ge 3 ] && [ "$PY_MINOR" -ge 13 ]; then
        PYTHON=python3
    else
        echo "ERROR: Python 3.13+ required, found $PY_VERSION"
        exit 1
    fi
else
    echo "ERROR: Python 3.13+ not found"
    exit 1
fi
echo "  Found: $($PYTHON --version)"

# --- Step 2: Virtual environment ---
echo "[2/6] Setting up virtual environment..."
if [ ! -d "$VENV_DIR" ]; then
    $PYTHON -m venv "$VENV_DIR"
    echo "  Created: $VENV_DIR"
else
    echo "  Exists: $VENV_DIR"
fi
"$VENV_DIR/bin/pip" install --quiet -e ".[dev,surveyor]"
echo "  Dependencies installed."

# --- Step 3: Environment variables ---
echo "[3/6] Configuring environment..."
if [ -f "$PROJECT_DIR/.env" ]; then
    echo "  .env exists — skipping."
else
    echo "  No .env found. Please provide your credentials:"
    echo ""
    read -rp "  ROAM_GRAPH: " ROAM_GRAPH
    read -rp "  ROAM_API_TOKEN: " ROAM_API_TOKEN
    read -rp "  ANTHROPIC_API_KEY: " ANTHROPIC_API_KEY
    cat > "$PROJECT_DIR/.env" <<EOF
ROAM_GRAPH=$ROAM_GRAPH
ROAM_API_TOKEN=$ROAM_API_TOKEN
ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY
EOF
    echo "  .env written."
fi

# --- Step 4: Ollama ---
echo "[4/6] Checking Ollama..."
if command -v ollama &>/dev/null; then
    echo "  Ollama found: $(ollama --version 2>&1 | head -1)"
    if ollama list 2>/dev/null | grep -q "nomic-embed-text"; then
        echo "  nomic-embed-text model: installed"
    else
        echo "  Pulling nomic-embed-text model..."
        ollama pull nomic-embed-text
    fi
else
    echo "  WARNING: Ollama not found. Surveyor requires Ollama + nomic-embed-text."
    echo "  Install from: https://ollama.com"
fi

# --- Step 5: Log directory ---
echo "[5/6] Setting up logs..."
mkdir -p "$LOG_DIR"
echo "  Log dir: $LOG_DIR"

# --- Step 6: launchd ---
echo "[6/6] Installing launchd plist..."
VENV_PYTHON="$VENV_DIR/bin/python"

# Unload existing plist if loaded
if launchctl list 2>/dev/null | grep -q "$PLIST_NAME"; then
    launchctl bootout "gui/$(id -u)/$PLIST_NAME" 2>/dev/null || true
fi

sed \
    -e "s|__VENV_PYTHON__|$VENV_PYTHON|g" \
    -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
    -e "s|__LOG_DIR__|$LOG_DIR|g" \
    "$PLIST_TEMPLATE" > "$PLIST_TARGET"

launchctl bootstrap "gui/$(id -u)" "$PLIST_TARGET"
echo "  Installed: $PLIST_TARGET"
echo "  Schedule: nightly at 2:00 AM"

echo ""
echo "=== Setup Complete ==="
echo "  Manual run:  $VENV_PYTHON -m living_graph run"
echo "  View logs:   tail -f $LOG_DIR/living-graph.log"
echo "  Uninstall:   launchctl bootout gui/$(id -u)/$PLIST_NAME"

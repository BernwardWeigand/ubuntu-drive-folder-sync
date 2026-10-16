#!/usr/bin/env bash

PROJECT_DIR="project"
CONTROL_FILE="$PROJECT_DIR/DEBIAN/control"
PACKAGE_KEY="Package: "
PACKAGE_ENTRY=$(grep "$PACKAGE_KEY" "$CONTROL_FILE")
PACKAGE=${PACKAGE_ENTRY//$PACKAGE_KEY}
VERSION_KEY="Version: "
VERSION_ENTRY=$(grep "$VERSION_KEY" "$CONTROL_FILE")
VERSION=${VERSION_ENTRY//$VERSION_KEY}

# Python bytecode from local test runs must not end up in the package
find "$PROJECT_DIR" -name "__pycache__" -type d -prune -exec rm -rf {} +

dpkg-deb --root-owner-group --build "$PROJECT_DIR" "$PACKAGE-$VERSION.deb"

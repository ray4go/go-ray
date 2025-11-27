# GoRay Documentation

This directory contains the source files for GoRay's API documentation.

## Quick Start

### Preview Documentation Locally

```bash
# Install documentation dependencies
pip install -e ".[docs]"

# Serve documentation locally (auto-reload on changes)
./build_docs.sh serve
# Or directly:
mkdocs serve
```

Visit http://127.0.0.1:8000 to view the documentation.

### Build Static Documentation

```bash
# Build the documentation site
./build_docs.sh build
# Or directly:
mkdocs build
```

The generated site will be in the `site/` directory.

## Documentation Structure

- **`index.md`**: Single-page API reference
  - Automatically extracts all public APIs from `goray/__init__.py`
  - Uses mkdocstrings to generate documentation from docstrings
  - Includes quick start examples

## Technology Stack

- **MkDocs**: Static site generator
- **Material for MkDocs**: Modern, responsive theme
- **mkdocstrings**: Automatic API documentation from Python source code

## Configuration

- **`mkdocs.yml`** (project root): Main configuration file
  - Theme settings (Material theme with dark/light mode)
  - Plugin configuration (search, mkdocstrings)
  - Markdown extensions (syntax highlighting, admonitions, etc.)

## How It Works

The documentation is automatically generated from your source code:

1. `mkdocstrings` reads the `goray/__init__.py` file
2. It extracts all public functions and their docstrings
3. Docstrings are parsed (Sphinx style: `:param`, `:return`, etc.)
4. Beautiful, formatted documentation is generated with syntax highlighting

No manual maintenance needed - documentation updates when you update docstrings!

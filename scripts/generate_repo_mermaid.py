#!/usr/bin/env python3
"""Generate Mermaid diagrams from Python code structure using Tree-sitter.

This script parses Python source files to extract:
1. Internal import dependencies (module → module)
2. Script entrypoints (files with `if __name__ == "__main__"`)

Output files are written to docs/Repository/generated/:
- CODE_IMPORT_GRAPH.md - Internal import dependency graph
- SCRIPTS_FLOW.md - Script entrypoints and their dependencies
"""
from __future__ import annotations

import sys
from pathlib import Path

import tree_sitter_python as tspython
from tree_sitter import Language, Parser

EXCLUDE_DIRS = {
    ".venv", "venv", "__pycache__", ".git", "node_modules", ".cache", "logs",
    "debug_html", "site-packages", "dist-packages", ".eggs", "build", "dist",
}
INTERNAL_PACKAGES = {"trr_backend", "scripts", "api"}


def get_parser() -> Parser:
    """Initialize Tree-sitter parser with Python language."""
    py_language = Language(tspython.language())
    parser = Parser(py_language)
    return parser


def extract_imports_from_node(node, source: bytes, imports: list[str]) -> None:
    """Recursively extract import module names from AST node."""
    if node.type == "import_statement":
        # import foo, import foo.bar
        for child in node.children:
            if child.type == "dotted_name":
                module = source[child.start_byte : child.end_byte].decode()
                imports.append(module)
            elif child.type == "aliased_import":
                # import foo as bar
                for subchild in child.children:
                    if subchild.type == "dotted_name":
                        module = source[subchild.start_byte : subchild.end_byte].decode()
                        imports.append(module)
                        break
    elif node.type == "import_from_statement":
        # from foo import bar
        for child in node.children:
            if child.type == "dotted_name":
                module = source[child.start_byte : child.end_byte].decode()
                imports.append(module)
                break  # Only get the module name, not what's imported

    for child in node.children:
        extract_imports_from_node(child, source, imports)


def extract_imports(parser: Parser, source: bytes) -> list[str]:
    """Extract import module names from Python source using Tree-sitter."""
    tree = parser.parse(source)
    imports: list[str] = []
    extract_imports_from_node(tree.root_node, source, imports)
    return imports


def is_internal(module: str) -> bool:
    """Check if module is internal to this repo."""
    root = module.split(".")[0]
    return root in INTERNAL_PACKAGES


def should_exclude(path: Path) -> bool:
    """Check if path should be excluded from processing."""
    return any(part in EXCLUDE_DIRS for part in path.parts)


def build_import_graph(root: Path, parser: Parser) -> dict[str, set[str]]:
    """Build module dependency graph from all Python files."""
    graph: dict[str, set[str]] = {}

    for py_file in root.rglob("*.py"):
        if should_exclude(py_file):
            continue

        # Compute module name from path
        try:
            rel = py_file.relative_to(root.parent)
        except ValueError:
            continue

        if py_file.name == "__init__.py":
            module_name = ".".join(rel.parent.parts)
        else:
            module_name = ".".join(rel.with_suffix("").parts)

        if not module_name:
            continue

        try:
            source = py_file.read_bytes()
        except OSError:
            continue

        imports = extract_imports(parser, source)
        internal_imports = {m for m in imports if is_internal(m)}

        if internal_imports:
            graph[module_name] = internal_imports

    return graph


def build_package_graph(module_graph: dict[str, set[str]], depth: int = 2) -> dict[str, set[str]]:
    """Collapse module graph to package level for readability."""
    pkg_graph: dict[str, set[str]] = {}

    def to_package(module: str) -> str:
        parts = module.split(".")
        return ".".join(parts[:depth]) if len(parts) > depth else module

    for src, deps in module_graph.items():
        src_pkg = to_package(src)
        dep_pkgs = {to_package(d) for d in deps}
        # Remove self-references
        dep_pkgs.discard(src_pkg)
        if dep_pkgs:
            if src_pkg not in pkg_graph:
                pkg_graph[src_pkg] = set()
            pkg_graph[src_pkg].update(dep_pkgs)

    return pkg_graph


def generate_mermaid_flowchart(graph: dict[str, set[str]], title: str) -> str:
    """Generate Mermaid flowchart from dependency graph."""
    lines = [f"# {title}", "", "```mermaid", "flowchart TB"]

    # Create stable node IDs from sorted module list
    all_modules = set(graph.keys())
    for deps in graph.values():
        all_modules.update(deps)

    sorted_modules = sorted(all_modules)
    node_ids = {m: f"n{i}" for i, m in enumerate(sorted_modules)}

    # Define nodes with full module name for clarity
    for module in sorted_modules:
        # Use last 2 parts of module name for label
        parts = module.split(".")
        label = ".".join(parts[-2:]) if len(parts) > 1 else module
        lines.append(f'    {node_ids[module]}["{label}"]')

    lines.append("")

    # Define edges (sorted for determinism)
    for src in sorted(graph.keys()):
        for dst in sorted(graph[src]):
            if dst in node_ids:  # Only add edge if destination exists
                lines.append(f"    {node_ids[src]} --> {node_ids[dst]}")

    lines.extend(["```", ""])
    return "\n".join(lines)


def has_main_block(source: bytes) -> bool:
    """Check if source contains `if __name__ == "__main__"` pattern."""
    # Simple string check is more reliable than Tree-sitter query for this pattern
    return b'if __name__ == "__main__"' in source or b"if __name__ == '__main__'" in source


def find_entrypoints(scripts_dir: Path) -> list[Path]:
    """Find scripts with if __name__ == '__main__' blocks."""
    entrypoints = []
    for py_file in scripts_dir.rglob("*.py"):
        if should_exclude(py_file):
            continue
        try:
            source = py_file.read_bytes()
            if has_main_block(source):
                entrypoints.append(py_file)
        except OSError:
            continue

    # Sort by full path string for deterministic ordering
    return sorted(entrypoints, key=lambda p: str(p))


def generate_scripts_flow(scripts_dir: Path, parser: Parser) -> str:
    """Generate Mermaid diagram showing script entrypoints and their trr_backend dependencies."""
    entrypoints = find_entrypoints(scripts_dir)

    lines = [
        "# Scripts Flow",
        "",
        "Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.",
        "",
        "```mermaid",
        "flowchart LR",
    ]

    # Group scripts by directory
    script_groups: dict[str, list[Path]] = {}
    for script in entrypoints:
        try:
            rel = script.relative_to(scripts_dir)
            group = rel.parts[0] if len(rel.parts) > 1 else "(root)"
        except ValueError:
            group = "(root)"
        if group not in script_groups:
            script_groups[group] = []
        script_groups[group].append(script)

    # Generate subgraphs for each directory (sorted by group name)
    node_id = 0
    script_ids: dict[Path, str] = {}
    subgraph_id = 0

    for group in sorted(script_groups.keys()):
        if group == "(root)":
            subgraph_label = "scripts (root)"
        else:
            subgraph_label = f"scripts/{group}"

        lines.append(f'    subgraph sg{subgraph_id}["{subgraph_label}"]')
        # Sort scripts by full path string for determinism
        for script in sorted(script_groups[group], key=lambda p: str(p)):
            sid = f"s{node_id}"
            script_ids[script] = sid
            lines.append(f'        {sid}["{script.stem}"]')
            node_id += 1
        lines.append("    end")
        subgraph_id += 1

    # Add trr_backend subgraph with consistent ordering
    lines.append('    subgraph trr["trr_backend/"]')
    lines.append('        ingestion["ingestion"]')
    lines.append('        integrations["integrations"]')
    lines.append('        media["media"]')
    lines.append('        repos["repositories"]')
    lines.append("    end")

    # Collect all edges as tuples for deterministic sorting
    edges: list[tuple[str, str]] = []
    target_submodules = {"ingestion", "integrations", "media", "repositories"}

    for script in entrypoints:
        try:
            source = script.read_bytes()
        except OSError:
            continue

        imports = extract_imports(parser, source)
        trr_imports = {m for m in imports if m.startswith("trr_backend.")}

        sid = script_ids.get(script)
        if not sid:
            continue

        # Map imports to submodules and collect edges
        script_targets: set[str] = set()
        for imp in trr_imports:
            parts = imp.split(".")
            if len(parts) >= 2:
                submod = parts[1]
                if submod in target_submodules:
                    # Map "repositories" to "repos" for the node ID
                    target = "repos" if submod == "repositories" else submod
                    script_targets.add(target)

        # Add edges for this script (sorted targets)
        for target in sorted(script_targets):
            edges.append((sid, target))

    # Emit edges in deterministic order (sorted numerically by source node ID, then target)
    def edge_sort_key(edge: tuple[str, str]) -> tuple[int, str]:
        src, dst = edge
        # Extract numeric part from node ID (e.g., "s123" -> 123)
        src_num = int(src[1:]) if src[1:].isdigit() else 0
        return (src_num, dst)

    for src, dst in sorted(edges, key=edge_sort_key):
        lines.append(f"    {src} --> {dst}")

    lines.extend(["```", ""])
    return "\n".join(lines)


def ensure_git_workflow_template(diagrams_dir: Path) -> None:
    """Ensure git_workflow.md exists with stable template content."""
    git_workflow_path = diagrams_dir / "git_workflow.md"

    template = """# Git Workflow

This diagram shows our standard branching strategy.

```mermaid
gitgraph
    commit id: "Initial commit"
    branch feature/new-feature
    commit id: "Add feature"
    commit id: "Add tests"
    checkout main
    merge feature/new-feature id: "Merge PR"
    commit id: "Deploy"
```

## Branching Strategy

- **main**: Primary branch, always deployable
- **feature/\***: New features and enhancements
- **fix/\***: Bug fixes
- **docs/\***: Documentation updates
- **chore/\***: Maintenance and tooling

## Workflow

1. Create branch from `main`
2. Make changes and commit
3. Open Pull Request
4. CI checks pass
5. Code review
6. Merge to `main`
"""

    git_workflow_path.write_text(template)


def ensure_system_maps_template(diagrams_dir: Path) -> None:
    """Ensure system_maps.md exists with stable template content."""
    system_maps_path = diagrams_dir / "system_maps.md"

    template = """# System Architecture Maps

## Module Boundaries

```mermaid
flowchart TB
    subgraph scripts["scripts/"]
        s1["ShowInfo"]
        s2["CastInfo"]
        s3["RealiteaseInfo"]
        s4["WWHLInfo"]
        s5["FinalList"]
    end

    subgraph api["api/"]
        routers["routers/"]
        realtime["realtime/"]
        auth["auth.py"]
    end

    subgraph trr["trr_backend/"]
        repos["repositories/"]
        integrations["integrations/"]
        ingestion["ingestion/"]
        media["media/"]
    end

    subgraph external["External APIs"]
        tmdb["TMDb"]
        imdb["IMDb"]
        fandom["Fandom"]
    end

    scripts --> repos
    scripts --> ingestion
    api --> repos
    ingestion --> integrations
    integrations --> external
    repos --> db[(Supabase)]
    media --> s3[(S3)]
```

## Data Flow

```mermaid
flowchart LR
    lists["IMDb/TMDb Lists"] --> resolve["resolve_tmdb_ids"]
    resolve --> backfill["backfill_tmdb_details"]
    backfill --> sync["sync_entities"]
    sync --> providers["sync_watch_providers"]
    providers --> api["API serves ShowDetail"]
```

## Key Components

- **scripts/**: Data ingestion and enrichment pipelines
- **api/**: FastAPI REST endpoints and WebSocket realtime
- **trr_backend/**: Core business logic and data access
- **integrations/**: External API clients (TMDb, IMDb, etc.)
"""

    system_maps_path.write_text(template)


def main() -> int:
    """Generate Mermaid diagrams and write to docs/Repository/generated/."""
    root = Path(__file__).parent.parent

    # Create directories
    generated_dir = root / "docs" / "Repository" / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)

    diagrams_dir = root / "docs" / "Repository" / "diagrams"
    diagrams_dir.mkdir(parents=True, exist_ok=True)

    parser = get_parser()

    # Generate Tree-sitter diagrams (existing)
    module_graph = build_import_graph(root / "trr_backend", parser)
    pkg_graph = build_package_graph(module_graph, depth=2)
    import_md = generate_mermaid_flowchart(pkg_graph, "trr_backend Internal Import Graph")
    (generated_dir / "CODE_IMPORT_GRAPH.md").write_text(import_md)
    print(f"Wrote {generated_dir / 'CODE_IMPORT_GRAPH.md'} ({len(pkg_graph)} packages)")

    scripts_flow = generate_scripts_flow(root / "scripts", parser)
    (generated_dir / "SCRIPTS_FLOW.md").write_text(scripts_flow)
    entrypoint_count = len(find_entrypoints(root / "scripts"))
    print(f"Wrote {generated_dir / 'SCRIPTS_FLOW.md'} ({entrypoint_count} entrypoints)")

    # Ensure curated diagram templates exist (NEW)
    ensure_git_workflow_template(diagrams_dir)
    print(f"Wrote {diagrams_dir / 'git_workflow.md'} (template)")

    ensure_system_maps_template(diagrams_dir)
    print(f"Wrote {diagrams_dir / 'system_maps.md'} (template)")

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Fix Mermaid subgraph/node ID conflicts in REPO_STRUCTURE.mermaid.md.

The PGSch/graph-git-repo action generates Mermaid diagrams where subgraph
identifiers are reused as node IDs in edges, causing cycle errors.

This script:
1. Identifies all subgraph declarations
2. Finds edges using those IDs as nodes
3. Renames node usage to {id}_node to avoid conflicts

Example fix:
    Before: trr_backend_src --> init[__init__.py]
    After:  trr_backend_src_node --> init[__init__.py]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def find_subgraph_ids(content: str) -> set[str]:
    """Extract all subgraph identifiers from Mermaid content.

    Args:
        content: Mermaid diagram content

    Returns:
        Set of subgraph identifiers found in the content
    """
    pattern = r"^\s*subgraph\s+(\w+)"
    return {match.group(1) for match in re.finditer(pattern, content, re.MULTILINE)}


def fix_node_conflicts(content: str, subgraph_ids: set[str]) -> str:
    """Replace subgraph IDs used as nodes with {id}_node.

    This prevents Mermaid from detecting cycles when a subgraph identifier
    is reused as a node in an edge declaration.

    Args:
        content: Mermaid diagram content
        subgraph_ids: Set of subgraph identifiers to check for conflicts

    Returns:
        Fixed content with node conflicts resolved
    """
    fixed_content = content

    for sg_id in subgraph_ids:
        # Match edges where subgraph_id is used as source:
        # "    trr_backend_src --> target" or "    trr_backend_src --- target"
        pattern = rf"(\s+)({sg_id})\s+(-->|---)\s+"
        replacement = r"\1\2_node \3 "
        fixed_content = re.sub(pattern, replacement, fixed_content)

        # Match edges where subgraph_id is used as target:
        # "    source --> trr_backend_src"
        pattern = rf"\s+(-->|---)\s+({sg_id})(\s|$)"
        replacement = r" \1 \2_node\3"
        fixed_content = re.sub(pattern, replacement, fixed_content)

    return fixed_content


def main() -> int:
    """Main entry point for the script.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    repo_root = Path(__file__).parent.parent
    target_file = repo_root / "docs/Repository/generated/REPO_STRUCTURE.mermaid.md"

    if not target_file.exists():
        print(f"File not found: {target_file}")
        return 1

    content = target_file.read_text()
    subgraph_ids = find_subgraph_ids(content)

    if not subgraph_ids:
        print("No subgraphs found, nothing to fix")
        return 0

    fixed_content = fix_node_conflicts(content, subgraph_ids)

    if fixed_content == content:
        print("No conflicts detected, file unchanged")
        return 0

    target_file.write_text(fixed_content)
    print(f"Fixed {len(subgraph_ids)} subgraph conflicts in {target_file.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

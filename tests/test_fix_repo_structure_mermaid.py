"""Unit tests for fix_repo_structure_mermaid.py script."""

from __future__ import annotations

import sys
from pathlib import Path

# Add scripts directory to Python path for imports
scripts_dir = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from fix_repo_structure_mermaid import find_subgraph_ids, fix_node_conflicts  # noqa: E402


def test_find_subgraph_ids():
    """Test extraction of subgraph identifiers."""
    content = """
    subgraph trr_backend_src["trr_backend"]
        direction TB
        subgraph db_trr_backend["db"]
            direction TB
        end
    end
    """
    result = find_subgraph_ids(content)
    assert result == {"trr_backend_src", "db_trr_backend"}


def test_find_subgraph_ids_empty():
    """Test extraction when no subgraphs exist."""
    content = """
    flowchart TD
        A --> B
        B --> C
    """
    result = find_subgraph_ids(content)
    assert result == set()


def test_fix_forward_edge_conflict():
    """Test fixing subgraph ID used as edge source."""
    content = "    trr_backend_src --> init[__init__.py]"
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == "    trr_backend_src_node --> init[__init__.py]"


def test_fix_forward_edge_conflict_with_dashes():
    """Test fixing subgraph ID with dashed edge syntax."""
    content = "    trr_backend_src --- init[__init__.py]"
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == "    trr_backend_src_node --- init[__init__.py]"


def test_fix_reverse_edge_conflict():
    """Test fixing subgraph ID used as edge target."""
    content = "    parent --> trr_backend_src"
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == "    parent --> trr_backend_src_node"


def test_fix_reverse_edge_conflict_end_of_line():
    """Test fixing subgraph ID at end of line."""
    content = "    parent --> trr_backend_src\n"
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == "    parent --> trr_backend_src_node\n"


def test_preserve_valid_syntax():
    """Test that valid Mermaid syntax is unchanged."""
    content = """
    subgraph trr_backend_src["trr_backend"]
        direction TB
        some_node --> init[__init__.py]
    end
    """
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == content


def test_preserve_subgraph_declaration():
    """Test that subgraph declarations are not modified."""
    content = '    subgraph trr_backend_src["trr_backend"]'
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == content


def test_idempotent():
    """Test that running fix twice produces same result."""
    content = "    trr_backend_src --> init[__init__.py]"
    subgraph_ids = {"trr_backend_src"}
    fixed_once = fix_node_conflicts(content, subgraph_ids)
    fixed_twice = fix_node_conflicts(fixed_once, subgraph_ids)
    assert fixed_once == fixed_twice


def test_multiple_conflicts():
    """Test fixing multiple conflicts in one content."""
    content = """
    subgraph trr_backend_src["trr_backend"]
        trr_backend_src --> init[__init__.py]
        trr_backend_src --> db_trr_backend
        parent --> trr_backend_src
    end
    """
    fixed = fix_node_conflicts(content, {"trr_backend_src"})

    # All three usages should be fixed
    assert "trr_backend_src_node --> init[__init__.py]" in fixed
    assert "trr_backend_src_node --> db_trr_backend" in fixed
    assert "parent --> trr_backend_src_node" in fixed

    # Subgraph declaration should remain unchanged
    assert 'subgraph trr_backend_src["trr_backend"]' in fixed


def test_multiple_subgraphs():
    """Test fixing conflicts across multiple subgraphs."""
    content = """
    subgraph trr_backend_src["trr_backend"]
        trr_backend_src --> init[__init__.py]
    end
    subgraph db_trr_backend["db"]
        db_trr_backend --> connection[connection.py]
    end
    """
    subgraph_ids = {"trr_backend_src", "db_trr_backend"}
    fixed = fix_node_conflicts(content, subgraph_ids)

    assert "trr_backend_src_node --> init[__init__.py]" in fixed
    assert "db_trr_backend_node --> connection[connection.py]" in fixed


def test_preserves_indentation():
    """Test that indentation is preserved."""
    content = "        trr_backend_src --> init[__init__.py]"
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    assert fixed == "        trr_backend_src_node --> init[__init__.py]"
    # Verify leading whitespace count is preserved
    assert len(fixed) - len(fixed.lstrip()) == 8


def test_no_false_positives():
    """Test that similar but different identifiers are not affected."""
    content = """
    subgraph trr_backend_src["trr_backend"]
        trr_backend_src_other --> init[__init__.py]
        other_trr_backend_src --> db[db.py]
    end
    """
    fixed = fix_node_conflicts(content, {"trr_backend_src"})
    # These should not be modified (they're different identifiers)
    assert "trr_backend_src_other --> init[__init__.py]" in fixed
    assert "other_trr_backend_src --> db[db.py]" in fixed

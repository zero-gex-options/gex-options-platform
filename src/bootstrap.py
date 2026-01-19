"""
Bootstrap module to setup Python path for project imports.

Import this at the very top of any script to enable imports from anywhere:
    from bootstrap import setup_paths
    setup_paths()

Or simply:
    import bootstrap  # Auto-runs setup
"""

import sys
from pathlib import Path

def setup_paths():
    """
    Add project root and src directory to sys.path.
    Can be called from any script regardless of location.
    """
    # Find the src directory by locating this bootstrap.py file
    bootstrap_file = Path(__file__).resolve()
    src_dir = bootstrap_file.parent
    project_root = src_dir.parent
    
    # Add paths if not already there
    for path in [str(project_root), str(src_dir)]:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    return project_root, src_dir

# Auto-run when imported
setup_paths()

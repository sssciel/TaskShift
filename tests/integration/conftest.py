"""Integration test configuration"""

import sys
from pathlib import Path

# Add src and tests to Python path
src_path = Path(__file__).parent.parent.parent / "src"
tests_path = Path(__file__).parent.parent
for p in [src_path, tests_path]:
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

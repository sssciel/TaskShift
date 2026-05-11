"""
TaskShift Test Suite Configuration
"""

import sys
from pathlib import Path

# Add src to Python path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Add tests to Python path for fixtures
tests_path = Path(__file__).parent
if str(tests_path) not in sys.path:
    sys.path.insert(0, str(tests_path))

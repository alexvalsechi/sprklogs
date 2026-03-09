"""Shared pytest configuration."""
import sys
from pathlib import Path

# Add backend to path so imports work without install
sys.path.insert(0, str(Path(__file__).parent.parent))

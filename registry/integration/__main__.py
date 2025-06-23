import sys
import unittest
from pathlib import Path

if __name__ == "__main__":
    # Update the path
    install_project_dir = str(Path(__file__).parent.parent.parent)
    sys.path.append(install_project_dir)

    # Discover all the integration tests
    test_dir = str(Path(__file__).parent)
    test_suites = unittest.defaultTestLoader.discover(start_dir=test_dir)

    # Run them
    for test_suite in test_suites:
        test_runner = unittest.TextTestRunner(verbosity=2)
        test_runner.run(test_suite)

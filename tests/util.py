import os

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_full_path(test_file):
    return os.path.join(current_dir, f"{test_file}")

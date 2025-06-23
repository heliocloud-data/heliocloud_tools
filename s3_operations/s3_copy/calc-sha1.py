import hashlib
import base64
from concurrent.futures import ProcessPoolExecutor


def sha1_checksum_base64(file_path, buffer_size=65536):
    """Compute the SHA-1 checksum of a file and return Base64 encoding."""
    sha1 = hashlib.sha1()
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(buffer_size)
            while chunk:
                sha1.update(chunk)
                chunk = f.read(buffer_size)
        # Convert SHA-1 digest to Base64 encoding
        return file_path, base64.b64encode(sha1.digest()).decode("utf-8")
    except Exception as e:
        return file_path, f"Error: {e}"


def process_files_parallel(file_list, num_workers=None):
    """Compute SHA-1 checksums of multiple files in parallel."""
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = executor.map(sha1_checksum_base64, file_list)
    return list(results)


def read_file_paths(file_path):
    """Read file paths from a text file, stripping extra spaces and empty lines."""
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python make-sha1.py file_list.txt")
        sys.exit(1)

    file_list_path = sys.argv[1]
    file_paths = read_file_paths(file_list_path)

    if not file_paths:
        print("Error: No valid file paths found in the input file.")
        sys.exit(1)

    results = process_files_parallel(file_paths)

    for file, sha1_base64 in results:
        print(f"{file},{sha1_base64}")

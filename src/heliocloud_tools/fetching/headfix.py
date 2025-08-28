file_list_path = 'badfiles'  # contains one filename per line
new_first_line = "#start, stop, key, filesize\n"

with open(file_list_path, 'r') as f:
    file_list = [line.strip() for line in f if line.strip()]  # remove empty lines

for file_path in file_list:
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()

        if lines:  # only replace if file is not empty
            lines[0] = new_first_line
            with open(file_path, 'w') as f:
                f.writelines(lines)
        else:
            print(f"Skipped empty file: {file_path}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

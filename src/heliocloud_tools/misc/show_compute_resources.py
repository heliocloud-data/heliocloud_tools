def get_cgroup_cpu_count():
    try:
        with open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'r') as quota_file:
            quota = int(quota_file.read().strip())
        with open('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'r') as period_file:
            period = int(period_file.read().strip())
        if quota > 0:
            return quota // period
    except Exception as e:
        print(f"Error reading CPU limits: {e}")
    return None

cpu_count = get_cgroup_cpu_count()
print(f"Allocated CPUs: {cpu_count}")

def get_cgroup_memory_limit():
    try:
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as mem_file:
            mem_limit = int(mem_file.read().strip())
        return mem_limit // (1024 ** 3)  # Convert to GB
    except Exception as e:
        print(f"Error reading memory limits: {e}")
    return None

memory_limit = get_cgroup_memory_limit()
print(f"Allocated Memory: {memory_limit} GB")

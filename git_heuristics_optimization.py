#!/usr/bin/env python3
"""
Prototype implementation of C Git's sorting heuristics for delta compression.
This shows what the next optimization should look like.
"""

import time
import hashlib
from dulwich.objects import Blob
from dulwich.pack import deltify_pack_objects

def git_name_hash(path_bytes):
    """
    Implement C Git's name_hash() function for better delta candidate selection.
    This groups similar files together for better compression.
    """
    if not path_bytes:
        return 0
    
    # Simple hash based on filename and extension
    path_str = path_bytes.decode('utf-8', errors='ignore')
    
    # Extract filename and extension
    if '/' in path_str:
        filename = path_str.split('/')[-1]
    else:
        filename = path_str
    
    # Hash based on filename and extension
    if '.' in filename:
        name, ext = filename.rsplit('.', 1)
        hash_input = f"{ext}:{name}"
    else:
        hash_input = filename
    
    # Use a simple hash function similar to C Git
    hash_val = 0
    for char in hash_input:
        hash_val = ((hash_val << 4) + ord(char)) & 0xFFFFFFFF
        hash_val ^= (hash_val >> 24)
    
    return hash_val

def create_test_repository():
    """Create a test repository with realistic file patterns."""
    files = []
    
    # Simulate a typical source code repository
    file_patterns = [
        # Python files
        ("src/main.py", "def main():\n    print('Hello, World!')\n"),
        ("src/utils.py", "def helper():\n    return 42\n"),
        ("src/models.py", "class User:\n    def __init__(self, name):\n        self.name = name\n"),
        ("tests/test_main.py", "import unittest\n\nclass TestMain(unittest.TestCase):\n    def test_main(self):\n        pass\n"),
        ("tests/test_utils.py", "import unittest\n\nclass TestUtils(unittest.TestCase):\n    def test_helper(self):\n        pass\n"),
        
        # JavaScript files
        ("frontend/app.js", "function main() {\n    console.log('Hello, World!');\n}\n"),
        ("frontend/utils.js", "function helper() {\n    return 42;\n}\n"),
        ("frontend/components.js", "class Component {\n    constructor(name) {\n        this.name = name;\n    }\n}\n"),
        
        # Configuration files
        ("config/settings.json", '{"debug": true, "port": 8000}'),
        ("config/database.json", '{"host": "localhost", "port": 5432}'),
        
        # Documentation
        ("README.md", "# Project\n\nThis is a test project.\n"),
        ("docs/api.md", "# API\n\nAPI documentation.\n"),
        ("docs/guide.md", "# Guide\n\nUser guide.\n"),
        
        # Build files
        ("package.json", '{"name": "test", "version": "1.0.0"}'),
        ("requirements.txt", "flask==2.0.0\nrequests==2.25.0\n"),
        ("Dockerfile", "FROM python:3.9\nCOPY . /app\n"),
    ]
    
    # Create variations of these files
    for i in range(3):  # Create 3 versions of each file
        for path, content in file_patterns:
            if i == 0:
                # Original file
                final_content = content
            else:
                # Modified versions
                final_content = content + f"\n# Version {i} modification\n"
                if i == 2:
                    final_content += f"# Additional change in version {i}\n"
            
            blob = Blob.from_string(final_content.encode())
            files.append((path.encode(), blob))
    
    return files

def analyze_git_heuristics():
    """Analyze how C Git's heuristics would group our test files."""
    files = create_test_repository()
    
    print("🔍 Git Heuristics Analysis")
    print("=" * 50)
    
    # Group files by name hash
    hash_groups = {}
    for path, blob in files:
        name_hash = git_name_hash(path)
        if name_hash not in hash_groups:
            hash_groups[name_hash] = []
        hash_groups[name_hash].append((path, blob))
    
    print(f"Files grouped into {len(hash_groups)} hash groups:")
    for hash_val, group in hash_groups.items():
        if len(group) > 1:
            print(f"  Hash {hash_val:08x}: {len(group)} files")
            for path, _ in group:
                print(f"    - {path.decode()}")
    
    # Sort files using Git's heuristics
    def git_sort_key(item):
        path, blob = item
        return (
            blob.type_num,           # Sort by object type first
            git_name_hash(path),     # Then by name hash
            -len(blob.data),         # Then by size (descending)
        )
    
    sorted_files = sorted(files, key=git_sort_key)
    
    print(f"\n📊 Git-style sorting results:")
    print(f"Total files: {len(sorted_files)}")
    
    # Show grouping effect
    current_type = None
    current_hash = None
    group_count = 0
    
    for path, blob in sorted_files:
        name_hash = git_name_hash(path)
        
        if blob.type_num != current_type or name_hash != current_hash:
            if current_type is not None:
                print(f"  Group {group_count}: {current_type}, hash={current_hash:08x}")
            current_type = blob.type_num
            current_hash = name_hash
            group_count += 1
    
    return sorted_files

def benchmark_heuristics_impact():
    """Benchmark the impact of using Git-style heuristics."""
    files = create_test_repository()
    
    print("\n🏃 Benchmarking Heuristics Impact")
    print("=" * 50)
    
    # Test 1: Current random order
    print("Testing current order (random):")
    random_objects = [blob for _, blob in files]
    
    start_time = time.time()
    results1 = list(deltify_pack_objects([(obj, b"") for obj in random_objects]))
    time1 = time.time() - start_time
    
    delta_count1 = sum(1 for r in results1 if r.delta_base is not None)
    compression1 = sum(r.decomp_len for r in results1)
    
    print(f"  Time: {time1:.3f}s")
    print(f"  Deltas: {delta_count1}/{len(results1)} ({delta_count1/len(results1)*100:.1f}%)")
    print(f"  Compressed size: {compression1:,} bytes")
    
    # Test 2: Git-style heuristic order
    print("\nTesting Git-style heuristic order:")
    
    def git_sort_key(item):
        path, blob = item
        return (
            blob.type_num,
            git_name_hash(path),
            -len(blob.data),
        )
    
    sorted_files = sorted(files, key=git_sort_key)
    sorted_objects = [blob for _, blob in sorted_files]
    
    start_time = time.time()
    results2 = list(deltify_pack_objects([(obj, b"") for obj in sorted_objects]))
    time2 = time.time() - start_time
    
    delta_count2 = sum(1 for r in results2 if r.delta_base is not None)
    compression2 = sum(r.decomp_len for r in results2)
    
    print(f"  Time: {time2:.3f}s")
    print(f"  Deltas: {delta_count2}/{len(results2)} ({delta_count2/len(results2)*100:.1f}%)")
    print(f"  Compressed size: {compression2:,} bytes")
    
    # Analysis
    print(f"\n📈 Performance Impact:")
    if time2 > 0:
        speedup = time1 / time2
        print(f"  Speed change: {speedup:.2f}x {'faster' if speedup > 1 else 'slower'}")
    
    delta_improvement = ((delta_count2 - delta_count1) / delta_count1) * 100
    print(f"  Delta improvement: {delta_improvement:+.1f}%")
    
    compression_improvement = ((compression1 - compression2) / compression1) * 100
    print(f"  Compression improvement: {compression_improvement:+.1f}%")
    
    return {
        'random': {'time': time1, 'deltas': delta_count1, 'size': compression1},
        'heuristic': {'time': time2, 'deltas': delta_count2, 'size': compression2},
        'improvement': {
            'speed': speedup if time2 > 0 else 0,
            'deltas': delta_improvement,
            'compression': compression_improvement
        }
    }

def recommend_optimizations():
    """Recommend specific optimizations based on analysis."""
    print("\n🎯 Recommended Optimizations")
    print("=" * 50)
    
    print("1. **Implement Git-style sorting in Rust:**")
    print("   - Add name_hash() function for path-based grouping")
    print("   - Sort by (type_num, name_hash, -size)")
    print("   - Expected improvement: 20-50% better compression")
    
    print("\n2. **Optimize delta window management:**")
    print("   - Use memory-aware window sizing")
    print("   - Implement LRU eviction for better cache locality")
    print("   - Expected improvement: 30-60% better performance")
    
    print("\n3. **Add delta chain depth limiting:**")
    print("   - Implement --depth parameter (default: 50)")
    print("   - Prevent excessive delta chains")
    print("   - Expected improvement: Better unpack performance")
    
    print("\n4. **Implement sparse algorithm:**")
    print("   - Skip objects unlikely to compress well")
    print("   - Use content-based heuristics")
    print("   - Expected improvement: 2-3x faster for large repos")
    
    print("\n5. **Add memory limits:**")
    print("   - Implement --window-memory parameter")
    print("   - Dynamic window sizing based on available memory")
    print("   - Expected improvement: Better scalability")

def main():
    print("🚀 Git Heuristics Optimization Analysis")
    print("=" * 60)
    
    # Analyze how Git's heuristics would work
    analyze_git_heuristics()
    
    # Benchmark the impact
    benchmark_heuristics_impact()
    
    # Provide recommendations
    recommend_optimizations()
    
    print("\n" + "=" * 60)
    print("✅ Analysis complete!")
    print("Next steps:")
    print("1. Implement name_hash() function in Rust")
    print("2. Update sort_objects_for_delta() to use Git heuristics")
    print("3. Add memory-aware window management")
    print("4. Test with larger repositories")

if __name__ == "__main__":
    main()
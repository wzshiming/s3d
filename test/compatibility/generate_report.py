#!/usr/bin/env python3
"""
S3 Compatibility Report Generator

This script analyzes the output from Ceph s3-tests and generates
a comprehensive compatibility report for the s3d server.
"""

import sys
import re
import json
from collections import defaultdict
from datetime import datetime

def parse_pytest_output(output_file):
    """Parse pytest output and extract test results."""
    with open(output_file, 'r') as f:
        content = f.read()
    
    results = {
        'passed': [],
        'failed': [],
        'skipped': [],
        'errors': []
    }
    
    # Parse test results
    # Format: test_file.py::test_name PASSED/FAILED/SKIPPED
    test_pattern = r'(s3tests/functional/[^\s]+)::(test_[^\s]+)\s+(PASSED|FAILED|SKIPPED|ERROR)'
    
    for match in re.finditer(test_pattern, content):
        test_file = match.group(1)
        test_name = match.group(2)
        status = match.group(3)
        
        test_info = {
            'file': test_file,
            'name': test_name,
            'full_name': f"{test_file}::{test_name}"
        }
        
        if status == 'PASSED':
            results['passed'].append(test_info)
        elif status == 'FAILED':
            results['failed'].append(test_info)
        elif status == 'SKIPPED':
            results['skipped'].append(test_info)
        elif status == 'ERROR':
            results['errors'].append(test_info)
    
    # Also try to extract summary line
    summary_pattern = r'=+ (\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?(?:, (\d+) error)?.*in ([\d.]+)s'
    summary_match = re.search(summary_pattern, content)
    
    if summary_match:
        summary = {
            'passed': int(summary_match.group(1) or 0),
            'failed': int(summary_match.group(2) or 0),
            'skipped': int(summary_match.group(3) or 0),
            'errors': int(summary_match.group(4) or 0),
            'duration': float(summary_match.group(5))
        }
    else:
        summary = {
            'passed': len(results['passed']),
            'failed': len(results['failed']),
            'skipped': len(results['skipped']),
            'errors': len(results['errors']),
            'duration': 0
        }
    
    return results, summary

def categorize_tests(test_name):
    """Categorize test by S3 feature."""
    name_lower = test_name.lower()
    
    categories = {
        'Bucket Operations': ['bucket_create', 'bucket_list', 'bucket_delete', 'bucket_head', 
                             'bucket_notexist', 'bucket_recreate', 'list_buckets'],
        'Object Operations': ['object_write', 'object_read', 'object_head', 'object_copy',
                            'get_object', 'put_object', 'delete_object', 'head_object'],
        'Multipart Upload': ['multipart', 'upload_part', 'complete_multipart', 'abort_multipart'],
        'List Objects': ['list_objects', 'list_v2', 'list_prefix', 'list_delimiter', 'list_marker'],
        'Authentication': ['auth', 'signature', 'access_key', 'secret_key', 'anon'],
        'Headers & Metadata': ['header', 'metadata', 'content_type', 'cache_control', 'etag'],
        'Versioning': ['version', 'versioning'],
        'ACL': ['acl', 'grant', 'permission'],
        'CORS': ['cors'],
        'Encryption': ['encrypt', 'sse'],
        'Lifecycle': ['lifecycle', 'expiration'],
        'Tagging': ['tag', 'tagging'],
        'Website': ['website'],
        'Logging': ['logging'],
        'Notifications': ['notification'],
        'Replication': ['replication'],
        'Object Lock': ['lock', 'retention', 'legal_hold'],
        'Range Requests': ['range', 'byte_range'],
        'Conditional Requests': ['if_match', 'if_none_match', 'if_modified', 'if_unmodified'],
    }
    
    for category, keywords in categories.items():
        if any(keyword in name_lower for keyword in keywords):
            return category
    
    return 'Other'

def generate_markdown_report(results, summary, output_file):
    """Generate a detailed markdown compatibility report."""
    
    total = summary['passed'] + summary['failed'] + summary['skipped'] + summary['errors']
    compatibility = (summary['passed'] * 100 / total) if total > 0 else 0
    
    with open(output_file, 'w') as f:
        # Header
        f.write("# S3 Compatibility Test Results\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Test Suite:** Ceph s3-tests\n\n")
        f.write(f"**Duration:** {summary['duration']:.2f}s\n\n")
        
        # Summary Table
        f.write("## Executive Summary\n\n")
        f.write("| Metric | Count | Percentage |\n")
        f.write("|--------|-------|------------|\n")
        f.write(f"| **Total Tests** | {total} | 100% |\n")
        f.write(f"| ✅ Passed | {summary['passed']} | {summary['passed']*100/total if total > 0 else 0:.1f}% |\n")
        f.write(f"| ❌ Failed | {summary['failed']} | {summary['failed']*100/total if total > 0 else 0:.1f}% |\n")
        f.write(f"| ⊘ Skipped | {summary['skipped']} | {summary['skipped']*100/total if total > 0 else 0:.1f}% |\n")
        f.write(f"| ⚠️  Errors | {summary['errors']} | {summary['errors']*100/total if total > 0 else 0:.1f}% |\n\n")
        
        f.write(f"### 🎯 Overall Compatibility Score: **{compatibility:.1f}%**\n\n")
        
        # Feature Breakdown
        f.write("## Feature Compatibility Breakdown\n\n")
        
        # Categorize all tests
        category_stats = defaultdict(lambda: {'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0})
        
        for test in results['passed']:
            category = categorize_tests(test['name'])
            category_stats[category]['passed'] += 1
        
        for test in results['failed']:
            category = categorize_tests(test['name'])
            category_stats[category]['failed'] += 1
        
        for test in results['skipped']:
            category = categorize_tests(test['name'])
            category_stats[category]['skipped'] += 1
        
        for test in results['errors']:
            category = categorize_tests(test['name'])
            category_stats[category]['errors'] += 1
        
        # Write category table
        f.write("| Feature Category | Passed | Failed | Skipped | Errors | Compatibility |\n")
        f.write("|-----------------|--------|--------|---------|--------|---------------|\n")
        
        for category in sorted(category_stats.keys()):
            stats = category_stats[category]
            cat_total = stats['passed'] + stats['failed'] + stats['skipped'] + stats['errors']
            cat_compat = (stats['passed'] * 100 / cat_total) if cat_total > 0 else 0
            
            icon = '✅' if cat_compat >= 90 else '⚠️' if cat_compat >= 50 else '❌'
            
            f.write(f"| {icon} {category} | {stats['passed']} | {stats['failed']} | ")
            f.write(f"{stats['skipped']} | {stats['errors']} | {cat_compat:.1f}% |\n")
        
        f.write("\n")
        
        # Supported Features
        if results['passed']:
            f.write("## ✅ Supported Features (Passing Tests)\n\n")
            f.write(f"The following {len(results['passed'])} tests passed successfully:\n\n")
            
            # Group by category
            passed_by_category = defaultdict(list)
            for test in results['passed']:
                category = categorize_tests(test['name'])
                passed_by_category[category].append(test['name'])
            
            for category in sorted(passed_by_category.keys()):
                f.write(f"### {category}\n\n")
                for test_name in sorted(passed_by_category[category]):
                    f.write(f"- ✓ `{test_name}`\n")
                f.write("\n")
        
        # Failed Features
        if results['failed']:
            f.write(f"## ❌ Unsupported/Failing Features\n\n")
            f.write(f"The following {len(results['failed'])} tests failed:\n\n")
            
            # Group by category
            failed_by_category = defaultdict(list)
            for test in results['failed']:
                category = categorize_tests(test['name'])
                failed_by_category[category].append(test['name'])
            
            for category in sorted(failed_by_category.keys()):
                f.write(f"### {category}\n\n")
                for test_name in sorted(failed_by_category[category]):
                    f.write(f"- ✗ `{test_name}`\n")
                f.write("\n")
        
        # Skipped Tests
        if results['skipped']:
            f.write(f"## ⊘ Skipped Tests\n\n")
            f.write(f"The following {len(results['skipped'])} tests were skipped:\n\n")
            
            for test in sorted(results['skipped'], key=lambda x: x['name']):
                f.write(f"- `{test['name']}`\n")
            f.write("\n")
        
        # Known Limitations
        f.write("## Known Limitations\n\n")
        f.write("Based on the s3d documentation, the following S3 features are **not yet implemented**:\n\n")
        f.write("- ❌ Bucket versioning\n")
        f.write("- ❌ Object locking\n")
        f.write("- ❌ Bucket policies\n")
        f.write("- ❌ ACLs (Access Control Lists)\n")
        f.write("- ❌ Server-side encryption\n")
        f.write("- ❌ Object tagging\n")
        f.write("- ❌ Lifecycle policies\n")
        f.write("- ❌ CORS (Cross-Origin Resource Sharing)\n")
        f.write("- ❌ Website hosting\n")
        f.write("- ❌ Logging\n")
        f.write("- ❌ Replication\n\n")
        
        # Currently Supported
        f.write("## Currently Supported Features\n\n")
        f.write("The s3d server currently supports the following S3 features:\n\n")
        f.write("- ✅ Bucket operations (create, list, delete, head)\n")
        f.write("- ✅ Object operations (put, get, delete, head, copy)\n")
        f.write("- ✅ ListObjects v1 and v2 with prefix/delimiter\n")
        f.write("- ✅ Multipart uploads\n")
        f.write("- ✅ AWS Signature V4 authentication\n\n")
        
        # References
        f.write("## References\n\n")
        f.write("- [AWS S3 API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html)\n")
        f.write("- [Ceph s3-tests Repository](https://github.com/ceph/s3-tests)\n")
        f.write("- [s3d Repository](https://github.com/wzshiming/s3d)\n\n")
        
        # Footer
        f.write("---\n\n")
        f.write("*This report was automatically generated by the s3d compatibility test suite.*\n")

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help']:
        print("S3 Compatibility Report Generator")
        print()
        print("Usage: python3 generate_report.py <test_output_file> [output_report.md]")
        print()
        print("Arguments:")
        print("  test_output_file    Path to the pytest output file")
        print("  output_report.md    Path for the generated markdown report (default: compatibility_report.md)")
        print()
        print("Example:")
        print("  python3 generate_report.py compatibility_results.txt compatibility_report.md")
        sys.exit(0 if len(sys.argv) == 2 and sys.argv[1] in ['-h', '--help'] else 1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'compatibility_report.md'
    
    print(f"Parsing test results from: {input_file}")
    results, summary = parse_pytest_output(input_file)
    
    print(f"Generating report to: {output_file}")
    generate_markdown_report(results, summary, output_file)
    
    print("\nReport Summary:")
    print(f"  Total Tests: {summary['passed'] + summary['failed'] + summary['skipped'] + summary['errors']}")
    print(f"  Passed: {summary['passed']}")
    print(f"  Failed: {summary['failed']}")
    print(f"  Skipped: {summary['skipped']}")
    print(f"  Errors: {summary['errors']}")
    print(f"\nReport generated successfully!")

if __name__ == '__main__':
    main()

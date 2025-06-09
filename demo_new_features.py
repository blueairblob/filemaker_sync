#!/usr/bin/env python3
"""
Demo script showing the new counting and JSON features
"""

import subprocess
import json
import sys
from pathlib import Path


def run_command(cmd_args, description):
    """Run a command and capture output"""
    print(f"\n{'='*60}")
    print(f"🔍 {description}")
    print(f"Command: python filemaker_extract_refactored.py {' '.join(cmd_args)}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, 'filemaker_extract_refactored.py'] + cmd_args,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print("✓ Success!")
            if result.stdout:
                print("Output:")
                print(result.stdout)
        else:
            print("✗ Failed!")
            if result.stderr:
                print("Error:")
                print(result.stderr)
        
        return result.returncode == 0, result.stdout, result.stderr
        
    except subprocess.TimeoutExpired:
        print("⏰ Command timed out")
        return False, "", "Timeout"
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False, "", str(e)


def parse_json_output(output):
    """Try to parse JSON from command output"""
    try:
        # Find JSON in output (might have other text before/after)
        lines = output.strip().split('\n')
        json_lines = []
        in_json = False
        
        for line in lines:
            if line.strip().startswith('{'):
                in_json = True
            if in_json:
                json_lines.append(line)
            if line.strip().endswith('}') and in_json:
                break
        
        if json_lines:
            json_text = '\n'.join(json_lines)
            return json.loads(json_text)
        return None
        
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        return None


def demo_info_commands():
    """Demo the information and counting commands"""
    
    print("🚀 FileMaker Sync Tool - New Features Demo")
    print("This demo shows the new counting and JSON output features")
    
    # 1. Basic info
    success, output, error = run_command(
        ['--info-only'], 
        "Basic Table Information"
    )
    
    # 2. Info as JSON
    success, output, error = run_command(
        ['--info-only', '--json'], 
        "Table Information (JSON Format)"
    )
    
    if success and output:
        parsed = parse_json_output(output)
        if parsed:
            print(f"\n📊 Parsed JSON Data:")
            print(f"   Source DB: {parsed.get('source_database', 'Unknown')}")
            print(f"   Target DB: {parsed.get('target_database', 'Unknown')}")
            print(f"   Table Count: {parsed.get('table_count', 0)}")
    
    # 3. Source table counts
    success, output, error = run_command(
        ['--src-cnt'], 
        "FileMaker Pro Source Table Counts"
    )
    
    # 4. Source counts as JSON
    success, output, error = run_command(
        ['--src-cnt', '--json'], 
        "Source Table Counts (JSON Format)"
    )
    
    if success and output:
        parsed = parse_json_output(output)
        if parsed:
            print(f"\n📊 Source Database Summary:")
            print(f"   Database: {parsed.get('database', 'Unknown')}")
            print(f"   Total Tables: {parsed['summary']['total_tables']}")
            print(f"   Total Rows: {parsed['summary']['total_rows']:,}")
            print(f"   Largest Tables:")
            
            # Show top 3 tables by row count
            tables = parsed.get('tables', {})
            sorted_tables = sorted(tables.items(), key=lambda x: x[1], reverse=True)
            for table, count in sorted_tables[:3]:
                if count > 0:
                    print(f"     • {table}: {count:,} rows")
    
    # 5. Target table counts
    success, output, error = run_command(
        ['--tgt-cnt'], 
        "Supabase Target Table Counts"
    )
    
    # 6. Target counts as JSON
    success, output, error = run_command(
        ['--tgt-cnt', '--json'], 
        "Target Table Counts (JSON Format)"
    )
    
    if success and output:
        parsed = parse_json_output(output)
        if parsed:
            print(f"\n📊 Target Database Summary:")
            print(f"   Database: {parsed.get('database', 'Unknown')}")
            print(f"   Schema: {parsed.get('schema', 'Unknown')}")
            print(f"   Total Rows: {parsed['summary']['total_rows']:,}")
            print(f"   Migrated Tables: {parsed['summary']['tables_migrated']}")
            print(f"   Empty Tables: {parsed['summary']['tables_empty']}")
    
    # 7. Full migration status comparison
    success, output, error = run_command(
        ['--migration-status'], 
        "Migration Status Comparison"
    )
    
    # 8. Migration status as JSON
    success, output, error = run_command(
        ['--migration-status', '--json'], 
        "Migration Status (JSON Format)"
    )
    
    if success and output:
        parsed = parse_json_output(output)
        if parsed:
            print(f"\n📊 Migration Status Summary:")
            print(f"   Source → Target: {parsed.get('source_database')} → {parsed.get('target_database')}")
            print(f"   Tables Migrated: {parsed['summary']['tables_migrated']}/{parsed['summary']['total_tables']}")
            print(f"   Row Migration: {parsed['summary']['target_total_rows']:,} / {parsed['summary']['source_total_rows']:,}")
            
            # Show migration status breakdown
            status_counts = {}
            for table, info in parsed.get('tables', {}).items():
                status = info['status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"   Status Breakdown:")
            status_labels = {
                'fully_migrated': '✓ Fully Migrated',
                'partially_migrated': '⚠ Partially Migrated', 
                'not_migrated': '✗ Not Migrated',
                'source_error': '❌ Source Error',
                'target_error': '❌ Target Error'
            }
            
            for status, count in status_counts.items():
                label = status_labels.get(status, status)
                print(f"     • {label}: {count} tables")


def demo_gui_integration():
    """Show how the JSON output can be used for GUI integration"""
    
    print(f"\n{'='*60}")
    print("🖥️  GUI Integration Example")
    print('='*60)
    
    # Get migration status as JSON
    try:
        result = subprocess.run(
            [sys.executable, 'filemaker_extract_refactored.py', '--migration-status', '--json'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            parsed = parse_json_output(result.stdout)
            if parsed:
                print("✓ JSON data successfully retrieved for GUI")
                print("📋 Sample GUI data structure:")
                
                # Create a simplified structure for GUI display
                gui_data = {
                    'connection_status': {
                        'source': parsed.get('source_database'),
                        'target': parsed.get('target_database'),
                        'timestamp': parsed.get('timestamp')
                    },
                    'migration_summary': parsed.get('summary', {}),
                    'table_details': []
                }
                
                # Add table details for GUI table view
                for table, info in parsed.get('tables', {}).items():
                    gui_data['table_details'].append({
                        'name': table,
                        'source_rows': info['source_rows'],
                        'target_rows': info['target_rows'], 
                        'status': info['status'],
                        'percentage': f"{info['migration_percentage']:.1f}%"
                    })
                
                print(json.dumps(gui_data, indent=2))
                
                print(f"\n💡 This JSON can be easily consumed by:")
                print(f"   • tkinter GUI (your current app)")
                print(f"   • Web dashboard")
                print(f"   • Monitoring systems")
                print(f"   • Automated reports")
                
            else:
                print("✗ Failed to parse JSON output")
        else:
            print("✗ Command failed")
            
    except Exception as e:
        print(f"❌ Error: {e}")


def demo_specific_tables():
    """Demo counting specific tables"""
    
    print(f"\n{'='*60}")
    print("🎯 Specific Table Demo")
    print('='*60)
    
    # Target specific tables
    tables = "ratcatalogue,ratbuilders"
    
    success, output, error = run_command(
        ['--src-cnt', '--json', '-t', tables], 
        f"Source Counts for Specific Tables ({tables})"
    )
    
    if success and output:
        parsed = parse_json_output(output)
        if parsed:
            print(f"\n📊 Specific Table Analysis:")
            for table, count in parsed.get('tables', {}).items():
                print(f"   • {table}: {count:,} rows")


if __name__ == "__main__":
    print("🔧 Checking for required files...")
    
    # Check if refactored script exists
    if not Path('filemaker_extract_refactored.py').exists():
        print("❌ filemaker_extract_refactored.py not found!")
        print("   Please ensure all refactored files are in the current directory")
        sys.exit(1)
    
    if not Path('config.toml').exists():
        print("❌ config.toml not found!")
        print("   Please ensure your configuration file exists")
        sys.exit(1)
    
    print("✓ Required files found")
    
    try:
        demo_info_commands()
        demo_gui_integration()
        demo_specific_tables()
        
        print(f"\n{'='*60}")
        print("🎉 Demo Complete!")
        print('='*60)
        
        print("📝 Summary of new features:")
        print("   ✓ --src-cnt: Get FileMaker table row counts")
        print("   ✓ --tgt-cnt: Get Supabase table row counts") 
        print("   ✓ --migration-status: Compare source vs target")
        print("   ✓ --json: Output all results in JSON format")
        print("   ✓ Better Supabase identification")
        print("   ✓ GUI-ready structured data")
        
        print("\n🔧 Usage for your GUI:")
        print("   import subprocess, json")
        print("   result = subprocess.run(['python', 'filemaker_extract_refactored.py', '--migration-status', '--json'], capture_output=True, text=True)")
        print("   data = json.loads(result.stdout)")
        print("   # Use data to populate GUI tables and status displays")
        
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
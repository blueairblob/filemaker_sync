#!/usr/bin/env python3
"""
Usage Examples for the Refactored FileMaker Sync Tool
Shows how to use the modular components
"""

from config_manager import ConfigManager
from database_connections import DatabaseManager
from data_exporter import DataExporter, ExportOptions


def example_1_test_connections():
    """Example 1: Test all database connections"""
    print("Example 1: Testing Database Connections")
    print("-" * 40)
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Test all connections
        results = db_manager.test_all_connections()
        
        for db_name, (success, message) in results.items():
            status = "✓" if success else "✗"
            print(f"{status} {db_name.title()}: {message}")
        
        # Get FileMaker tables if connection successful
        if results['filemaker'][0]:
            tables = db_manager.get_filemaker_tables()
            print(f"\nFound {len(tables)} tables: {', '.join(tables[:3])}...")
        
        db_manager.close_all_connections()
        
    except Exception as e:
        print(f"Error: {e}")


def example_2_configuration_management():
    """Example 2: Configuration management"""
    print("\nExample 2: Configuration Management")
    print("-" * 40)
    
    try:
        # Load configuration
        config_manager = ConfigManager()
        config = config_manager.load_config()
        
        print(f"Source Database: {config.source_db.name[1]} (DSN: {config.source_db.dsn})")
        print(f"Target Database: {config.target_db.name[1]} ({config.db_type})")
        print(f"Migration Schema: {config.mig_schema}")
        print(f"Export Path: {config.export.path}")
        
        # Validate configuration
        if config_manager.validate_config():
            print("✓ Configuration is valid")
        else:
            print("✗ Configuration has issues")
        
        # Get connection strings
        source_conn_str = config_manager.get_source_connection_string()
        target_conn_url = config_manager.get_target_connection_url()
        
        print(f"\nSource Connection: DSN={config.source_db.dsn};UID=***;PWD=***")
        print(f"Target Connection: {target_conn_url.split('@')[0]}@***")
        
    except Exception as e:
        print(f"Error: {e}")


def example_3_export_setup():
    """Example 3: Setting up data export"""
    print("\nExample 3: Export Setup")
    print("-" * 40)
    
    try:
        # Create export options
        export_options = ExportOptions(
            export_to_files=True,
            export_to_database=False,
            include_ddl=True,
            include_dml=True,
            file_format='multi',
            max_rows='100',
            debug=True
        )
        
        # Load configuration
        config_manager = ConfigManager()
        config = config_manager.load_config()
        
        # Create exporter
        exporter = DataExporter(config, export_options)
        
        print(f"Export Options:")
        print(f"  To Files: {export_options.export_to_files}")
        print(f"  To Database: {export_options.export_to_database}")
        print(f"  Include DDL: {export_options.include_ddl}")
        print(f"  Include DML: {export_options.include_dml}")
        print(f"  Max Rows: {export_options.max_rows}")
        
        print(f"\nExport Paths:")
        for path_type, path in exporter.export_paths.items():
            print(f"  {path_type.title()}: {path}")
        
    except Exception as e:
        print(f"Error: {e}")


def example_4_library_usage():
    """Example 4: Using as a library"""
    print("\nExample 4: Library Usage")
    print("-" * 40)
    
    try:
        # Simple library-style usage
        from filemaker_extract_refactored import FileMakerMigrationManager
        
        # Create a mock args object for library usage
        class MockArgs:
            def __init__(self):
                self.db_exp = True
                self.fn_exp = False
                self.ddl = True
                self.dml = True
                self.debug = False
                self.tables_to_export = 'ratcatalogue'
                self.max_rows = '10'
                self.del_data = False
                self.del_db = False
                self.start_from = None
                self.fn_fmt = 'multi'
                self.get_images = False
                self.info_only = False
                self.get_schema = False
        
        args = MockArgs()
        
        # Create migration manager
        manager = FileMakerMigrationManager(args)
        
        print("Library usage setup complete")
        print(f"Config loaded: {manager.config.source_db.name[1]} → {manager.config.target_db.name[1]}")
        print(f"Export options: DDL={args.ddl}, DML={args.dml}")
        print(f"Target tables: {args.tables_to_export}")
        
        # Note: Would call manager.run_migration() to execute
        print("Ready to run migration with: manager.run_migration()")
        
    except Exception as e:
        print(f"Error: {e}")


def example_5_individual_components():
    """Example 5: Using individual components"""
    print("\nExample 5: Individual Component Usage")
    print("-" * 40)
    
    try:
        # 1. Configuration only
        config_manager = ConfigManager()
        config = config_manager.load_config()
        print(f"✓ Configuration loaded for {config.db_type} target")
        
        # 2. Database connections only
        db_manager = DatabaseManager()
        filemaker_success, fm_msg = db_manager.filemaker.test_connection()
        target_success, tgt_msg = db_manager.target_db.test_connection()
        
        print(f"✓ FileMaker: {'Connected' if filemaker_success else 'Failed'}")
        print(f"✓ Target DB: {'Connected' if target_success else 'Failed'}")
        
        # 3. Export utilities only
        export_options = ExportOptions(export_to_files=True, include_ddl=True)
        exporter = DataExporter(config, export_options)
        
        print(f"✓ Exporter ready - will export to: {exporter.export_paths['export']}")
        
        # 4. Get table list
        if filemaker_success:
            tables = db_manager.get_filemaker_tables()
            print(f"✓ Found {len(tables)} tables available for export")
        
        db_manager.close_all_connections()
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("FileMaker Sync Tool - Refactored Usage Examples")
    print("=" * 50)
    
    try:
        example_1_test_connections()
        example_2_configuration_management()
        example_3_export_setup()
        example_4_library_usage()
        example_5_individual_components()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print("\nNew counting and verification commands:")
        print("  python filemaker_extract_refactored.py --src-cnt          # FileMaker table counts")
        print("  python filemaker_extract_refactored.py --tgt-cnt          # Supabase table counts") 
        print("  python filemaker_extract_refactored.py --migration-status # Full comparison")
        print("  python filemaker_extract_refactored.py --info-only --json # Table info as JSON")
        print("  python filemaker_extract_refactored.py --src-cnt --json   # Counts as JSON")
        print("\nTo run the actual migration:")
        print("  python filemaker_extract_refactored.py --db-exp --ddl --dml")
        print("\nTo export to files:")
        print("  python filemaker_extract_refactored.py --fn-exp --ddl --dml")
        print("\nTo test connections only:")
        print("  python filemaker_extract_refactored.py --info-only")
        
    except Exception as e:
        print(f"\nExample failed: {e}")
        print("\nMake sure you have:")
        print("1. config.toml file configured")
        print("2. FileMaker Pro running with ODBC enabled")
        print("3. Target database accessible")
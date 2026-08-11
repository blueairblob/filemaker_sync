#!/usr/bin/env python3
"""
Configuration Management Module
Handles loading, validation, and access to TOML configuration files
"""

import tomli
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    dsn: str
    user: str
    pwd: str
    port: str
    type: str
    name: List[str]
    schema: Optional[List[str]] = None


@dataclass
class ExportConfig:
    """Export configuration"""
    path: str
    prefix: str
    image_formats_supported: List[str]
    image_path: str


@dataclass
class AppConfig:
    """Main application configuration"""
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    export: ExportConfig
    db_type: str
    mig_schema: str
    tgt_schema: str
    
    # Primary key configurations
    pk_config: Dict[str, List[str]]


class ConfigManager:
    """Manages application configuration from TOML files"""
    
    def __init__(self, config_file: str = 'config.toml'):
        self.config_file = Path(config_file)
        self.logger = logging.getLogger(__name__)
        self._config_data: Optional[Dict[str, Any]] = None
        self._app_config: Optional[AppConfig] = None
    
    def load_config(self) -> AppConfig:
        """Load and parse configuration from TOML file"""
        if self._app_config is not None:
            return self._app_config
            
        try:
            if not self.config_file.exists():
                raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
            
            self.logger.debug(f"Loading configuration from {self.config_file}")
            config_text = self.config_file.read_text(encoding='utf-8')
            self._config_data = tomli.loads(config_text)
            
            self._app_config = self._parse_config()
            self.logger.info("Configuration loaded successfully")
            return self._app_config
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _parse_config(self) -> AppConfig:
        """Parse raw config data into structured configuration"""
        if not self._config_data:
            raise ValueError("No configuration data loaded")
        
        try:
            # Parse source database config
            source_config = self._config_data['database']['source']
            source_db = DatabaseConfig(
                host=source_config.get('host', '127.0.0.1'),
                dsn=source_config['dsn'],
                user=source_config['user'],
                pwd=source_config['pwd'],
                port=source_config.get('port', ''),
                type=source_config['type'],
                name=source_config['name'],
                schema=source_config.get('schema', [])
            )
            
            # Parse target database config
            target_config = self._config_data['database']['target']
            db_type = target_config['db']
            
            target_db = DatabaseConfig(
                host=target_config['host'],
                dsn=target_config['dsn'],
                user=target_config[db_type]['user'],
                pwd=target_config[db_type]['pwd'],
                port=target_config[db_type]['port'],
                type=target_config['type'],
                name=target_config[db_type]['name'],
                schema=target_config['schema']
            )
            
            # Parse export config
            export_config = self._config_data['export']
            export = ExportConfig(
                path=export_config['path'],
                prefix=export_config['prefix'],
                image_formats_supported=export_config['image_formats_supported'],
                image_path=export_config['image_path']
            )
            
            # Get schema indices
            mig_schema_idx = target_config['mig_schema']
            tgt_schema_idx = target_config['tgt_schema']
            mig_schema = target_config['schema'][mig_schema_idx]
            tgt_schema = target_config['schema'][tgt_schema_idx]
            
            # Parse primary key configurations
            pk_config = target_config.get(mig_schema, {}).get('pk', {})
            
            return AppConfig(
                source_db=source_db,
                target_db=target_db,
                export=export,
                db_type=db_type,
                mig_schema=mig_schema,
                tgt_schema=tgt_schema,
                pk_config=pk_config
            )
            
        except KeyError as e:
            self.logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Invalid configuration: missing key {e}")
        except Exception as e:
            self.logger.error(f"Configuration parsing error: {e}")
            raise
    
    def get_source_connection_string(self) -> str:
        """Get formatted connection string for source database"""
        config = self.load_config()
        return f"DSN={config.source_db.dsn};UID={config.source_db.user};PWD={config.source_db.pwd};CHARSET='UTF-8';ansi=True"
    
    def get_target_connection_url(self, use_dsn: bool = True) -> str:
        """Get formatted connection URL for target database"""
        config = self.load_config()
        
        dsn_str = config.target_db.dsn if use_dsn else ''
        
        if config.db_type == 'mysql':
            return f"mysql+pymysql://{config.target_db.user}:{config.target_db.pwd}@{config.target_db.host}:{config.target_db.port}/{dsn_str}"
        elif config.db_type == 'supabase':
            return f"postgresql://{config.target_db.user}:{config.target_db.pwd}@{config.target_db.host}:{config.target_db.port}/{dsn_str}"
        else:
            raise ValueError(f"Unsupported database type: {config.db_type}")
    
    def get_export_paths(self, base_export_dir: Optional[str] = None) -> Dict[str, Path]:
        """Get all export-related paths"""
        config = self.load_config()
        
        if base_export_dir:
            exp_path = Path(base_export_dir).resolve()
        else:
            exp_path = Path(config.export.path)
        
        return {
            'export': exp_path,
            'jpg': exp_path / 'images' / 'jpg',
            'webp': exp_path / 'images' / 'webp',
        }
    
    def validate_config(self) -> bool:
        """Validate configuration completeness and correctness"""
        try:
            config = self.load_config()
            
            # Check required fields
            required_checks = [
                (config.source_db.dsn, "Source database DSN"),
                (config.target_db.host, "Target database host"),
                (config.target_db.user, "Target database user"),
                (config.export.path, "Export path"),
            ]
            
            for value, description in required_checks:
                if not value:
                    self.logger.error(f"Missing required configuration: {description}")
                    return False
            
            # Validate paths
            export_path = Path(config.export.path)
            if not export_path.exists():
                self.logger.warning(f"Export path does not exist: {export_path}")
            
            # Validate database type
            if config.db_type not in ['mysql', 'supabase']:
                self.logger.error(f"Unsupported database type: {config.db_type}")
                return False
            
            self.logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def get_table_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a specific table"""
        config = self.load_config()
        return config.pk_config.get(table_name, [])
    
    def update_dsn(self, new_dsn: str) -> None:
        """Update the source DSN (useful for GUI applications)"""
        if self._app_config:
            self._app_config.source_db.dsn = new_dsn
            self.logger.info(f"Updated source DSN to: {new_dsn}")
    
    def get_raw_config(self) -> Dict[str, Any]:
        """Get raw configuration data for advanced usage"""
        if not self._config_data:
            self.load_config()
        return self._config_data
    
    @classmethod
    def create_sample_config(cls, output_path: str = 'config_sample.toml') -> None:
        """Create a sample configuration file"""
        sample_config = """# FileMaker Sync Tool Configuration

[gui]
api_host = "http://localhost"
api_port = 5000
api_timeout = 5000
api_health_check_interval = 30000

[export]
path = '/path/to/exports'
prefix = 'your_prefix'
image_formats_supported = ['jpg', 'webp']
image_path = 'images'

[database]

  [database.source] 
    host = '127.0.0.1'
    dsn  = 'your_filemaker_dsn'
    user = 'your_user'
    pwd  = 'your_password' 
    port = ''
    type = 'odbc'
    name = ['fmp', 'FileMaker Pro']
    schema = ['FileMaker_Tables', 'FileMaker_Fields', 'FileMaker_BaseTableFields']

  [database.target]
    dsn = 'postgres'
    db = 'supabase'
    dt = '%Y%m%d %H:%M:%S'
    type = 'url'
    host = 'your-supabase-host.supabase.co'
    schema = ['your_migration_schema', 'your_target_schema']
    mig_schema = 0
    tgt_schema = 1
    user = 'your_target_user'
    
  [database.target.your_migration_schema.pk]
    your_table1 = ['column1']
    your_table2 = ['column1', 'column2']

  [database.target.supabase]
    name = ['supabase', 'Supabase']
    user = 'postgres.your_project_id'
    pwd = 'your_password'
    port = '5432'
"""
        
        with open(output_path, 'w') as f:
            f.write(sample_config)
        
        print(f"Sample configuration created at: {output_path}")


# Convenience function for backward compatibility
def load_config(config_file: str = 'config.toml') -> AppConfig:
    """Load configuration - convenience function"""
    manager = ConfigManager(config_file)
    return manager.load_config()


if __name__ == "__main__":
    # Demo usage
    try:
        manager = ConfigManager()
        config = manager.load_config()
        
        print("Configuration loaded successfully!")
        print(f"Source DB: {config.source_db.name[1]} (DSN: {config.source_db.dsn})")
        print(f"Target DB: {config.target_db.name[1]} ({config.db_type})")
        print(f"Export path: {config.export.path}")
        
        if manager.validate_config():
            print("✓ Configuration is valid")
        else:
            print("✗ Configuration has issues")
            
    except Exception as e:
        print(f"Configuration error: {e}")
        ConfigManager.create_sample_config()
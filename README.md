# FileMaker to Supabase Sync Tool

A comprehensive desktop application for migrating and synchronizing data from FileMaker Pro databases to Supabase (PostgreSQL). Features both a command-line interface and user-friendly GUI for seamless database operations.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)

## Features

### Data Migration & Sync
- **Schema Migration (DDL)**: Export and recreate table structures
- **Data Migration (DML)**: Transfer all table data with integrity checking
- **Incremental Sync**: Resume from specific records using image IDs
- **Duplicate Handling**: Intelligent conflict resolution with detailed logging
- **Image Processing**: Extract and convert container images (JPEG → JPG/WebP)

### Dual Interface
- **Desktop GUI**: User-friendly interface with real-time progress monitoring
- **Command Line**: Advanced scripting and automation capabilities
- **Real-time Logging**: Live output streaming with debug modes
- **Connection Testing**: Validate both FileMaker and Supabase connections

### Enterprise Ready
- **Error Recovery**: Comprehensive error handling and rollback capabilities
- **Memory Management**: Chunked processing for large datasets
- **Progress Tracking**: Visual progress bars and detailed status reporting
- **Audit Trail**: Complete logging of all operations and errors

## Quick Start

### Prerequisites
- Windows 10/11 (64-bit)
- Python 3.9+ (for development)
- FileMaker Pro with ODBC driver
- Active Supabase account

### Option 1: Use Pre-built Executable (Recommended)
1. Download `FileMaker_Sync_Setup.exe` from releases
2. Run installer and follow setup wizard
3. Launch from Start Menu or desktop shortcut

### Option 2: Development Setup
```bash
# Clone repository
git clone https://github.com/yourusername/filemaker-sync.git
cd filemaker-sync

# Create virtual environment
python -m venv py3
py3\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run GUI application
python filemaker_gui.py

# Or use command line
python filemaker_extract.py --help
```

## Configuration

### 1. FileMaker ODBC Setup
1. **Install ODBC Driver** (included with FileMaker Pro)
2. **Create System DSN**:
   - Open ODBC Data Source Administrator as Administrator
   - System DSN → Add → FileMaker ODBC
   - Configure connection to your database
3. **Enable ODBC Sharing** in FileMaker Pro:
   - File → Sharing → ODBC/JDBC → Enable

### 2. Supabase Configuration
Edit `config.toml`:
```toml
[database.target.supabase]
user = 'your_postgres_user'
pwd = 'your_password'
host = 'your-project.supabase.co'  # or localhost for local dev
port = '5432'  # or 54322 for local dev
```

### 3. Application Settings
```toml
[export]
path = '/path/to/export/directory'
prefix = 'your_prefix'

[database.source]
dsn = 'your_filemaker_dsn'
user = 'filemaker_user'
pwd = 'password'
```

## Usage

### GUI Application
1. **Launch** the FileMaker Sync application
2. **Configure** your connections in the Configuration tab
3. **Test connections** to verify setup
4. **Choose sync options** (DDL, DML, tables, etc.)
5. **Start sync** and monitor progress in real-time

### Command Line Interface
```bash
# Full sync to database
python filemaker_extract.py --db-exp --ddl --dml

# Export to files only
python filemaker_extract.py --fn-exp --ddl --dml

# Export specific tables
python filemaker_extract.py --db-exp --ddl --dml -t "ratcatalogue,ratbuilders"

# Export images only
python filemaker_extract.py --get-images

# Debug mode with detailed logging
python filemaker_extract.py --db-exp --ddl --dml --debug

# Resume from specific record
python filemaker_extract.py --db-exp --dml --start-from "IMG001"
```

### Key Command Line Options
| Option | Description |
|--------|-------------|
| `--db-exp` | Export directly to database |
| `--fn-exp` | Export to SQL files |
| `--ddl` | Export table structure |
| `--dml` | Export table data |
| `--get-images` | Extract container images |
| `--debug` | Enable verbose logging |
| `--max-rows N` | Limit rows processed |
| `--start-from ID` | Resume from specific record |

## Project Structure

```
filemaker-sync/
├── filemaker_extract.py      # Core migration engine
├── filemaker_gui.py          # Desktop GUI application
├── config.toml               # Configuration file
├── requirements.txt          # Python dependencies
├── deploy.py                 # Build and deployment script
├── build_exe.py              # PyInstaller configuration
├── logs/                     # Application logs
├── exports/                  # Export output directory
│   ├── sql/                  # Generated SQL files
│   └── images/               # Extracted images
│       ├── jpg/              # JPEG format
│       └── webp/             # WebP format
└── docs/                     # Documentation
```

## Building Standalone Executable

### Development Build
```bash
# Build executable only
python build_exe.py

# Complete deployment (executable + installer)
python deploy.py
```

### Distribution Files
- `dist/FileMaker_Sync.exe` - Standalone executable
- `installer/FileMaker_Sync_Setup.exe` - Windows installer
- `installer/FileMaker_Sync_Portable.zip` - Portable package

### Build Requirements
- Python 3.9+
- PyInstaller 6.3.0+
- Inno Setup Compiler (for installer)

## Database Schema

### Source (FileMaker)
The tool automatically discovers tables through FileMaker's metadata:
- `FileMaker_BaseTableFields` - Table structure information
- Dynamic table discovery and field mapping

### Target (Supabase/PostgreSQL)
- Creates schema: `rat_migration` (staging) and `rat` (production)
- Handles data type conversion from FileMaker to PostgreSQL
- Implements primary key constraints and indexes

### Supported Data Types
| FileMaker | PostgreSQL | Notes |
|-----------|------------|-------|
| Text | VARCHAR/TEXT | Automatic sizing |
| Number | INTEGER/DECIMAL | Preserves precision |
| Date | DATE/TIMESTAMP | Timezone handling |
| Container | BYTEA | Image extraction available |

## Troubleshooting

### Common Issues

**"FileMaker DSN not found"**
- Verify ODBC DSN is created as System DSN (not User DSN)
- Run application as Administrator
- Use ODBC Administrator to test DSN

**"Connection timeout"**
- Ensure FileMaker Pro is running
- Verify database is accessible (not password protected)
- Check ODBC/JDBC sharing is enabled in FileMaker

**"Supabase connection failed"**
- Verify connection string in `config.toml`
- Check network connectivity
- Validate Supabase credentials and permissions

**"Permission denied"**
- Run as Administrator
- Check file system permissions for export directory
- Verify antivirus isn't blocking the application

### Debug Mode
Enable debug logging for detailed troubleshooting:
```bash
python filemaker_extract.py --debug --info-only
```

### Log Files
- Location: `logs/filemaker_extract_YYYYMMDD.log`
- Contains: Connection details, SQL operations, error stack traces
- Rotation: Daily log files with timestamp

## Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and test thoroughly
4. Commit with clear messages: `git commit -m "Add feature description"`
5. Push and create Pull Request

### Code Style
- Follow PEP 8 for Python code
- Use type hints where appropriate
- Include docstrings for functions and classes
- Add unit tests for new features

### Testing
```bash
# Test FileMaker connection
python filemaker_extract.py --info-only --max-rows 1

# Test full pipeline with small dataset
python filemaker_extract.py --db-exp --ddl --dml --max-rows 10

# Test GUI application
python filemaker_gui.py
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

### Documentation
- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)
- [API Documentation](docs/api.md)

### Getting Help
- **Issues**: Use GitHub Issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and community support
- **Email**: support@yourcompany.com

### Enterprise Support
For enterprise deployments, custom integrations, or priority support:
- Contact: enterprise@yourcompany.com
- Documentation: [Enterprise Guide](docs/enterprise.md)

## Architecture

### Core Components
- **Migration Engine** (`filemaker_extract.py`): Core data processing logic
- **GUI Application** (`filemaker_gui.py`): User interface wrapper
- **Configuration System**: TOML-based configuration management
- **Logging Framework**: Comprehensive audit trail and debugging

### Data Flow
```
FileMaker Pro → ODBC → Python → SQLAlchemy → Supabase/PostgreSQL
                ↓
            Log Files & Images
```

### Technology Stack
- **Backend**: Python 3.9+, SQLAlchemy, pandas
- **Database**: PostgreSQL (Supabase), FileMaker Pro ODBC
- **GUI**: tkinter (native Python)
- **Build**: PyInstaller, Inno Setup
- **Config**: TOML format

## Performance

### Benchmarks
- **Small Database** (< 1K records): 2-5 minutes
- **Medium Database** (10K records): 10-30 minutes  
- **Large Database** (100K+ records): 1-3 hours
- **Memory Usage**: 50-200MB (with chunked processing)

### Optimization
- Chunked processing prevents memory overflow
- Batch operations for improved performance
- Connection pooling for database efficiency
- Progress tracking for user experience

## Roadmap

### Version 2.0 (Planned)
- [ ] Two-way synchronization
- [ ] Real-time change detection
- [ ] Multiple database target support
- [ ] REST API for integration
- [ ] Docker containerization

### Version 1.1 (In Progress)
- [ ] Automated scheduling
- [ ] Email notifications
- [ ] Advanced filtering options
- [ ] Performance optimizations

## Acknowledgments

- FileMaker Inc. for ODBC driver support
- Supabase team for excellent PostgreSQL hosting
- Python community for amazing libraries
- Contributors and beta testers

---

**Built for seamless database migration**
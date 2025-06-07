# deploy.py
"""
Complete deployment script - builds executable and creates installer
"""

import subprocess
import sys
import os
from pathlib import Path
import shutil

def check_dependencies():
    """Check if all required tools are available"""
    print("Checking deployment dependencies...")
    
    # Check Python packages
    required_packages = ['pyinstaller', 'pandas', 'pyodbc', 'sqlalchemy', 'pillow', 'tqdm', 'tomli']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"✗ {package} - MISSING")
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install with: pip install -r requirements.txt")
        return False
    
    # Check for optional tools
    try:
        subprocess.run(['iscc', '/?'], capture_output=True, check=True)
        print("✓ Inno Setup Compiler found")
        inno_available = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠ Inno Setup Compiler not found - installer creation will be skipped")
        inno_available = False
    
    return True, inno_available

def prepare_build_environment():
    """Prepare the build environment"""
    print("\nPreparing build environment...")
    
    # Create necessary directories
    directories = ['dist', 'build', 'installer', 'logs']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Created/verified directory: {directory}")
    
    # Create version info file
    create_version_file()
    
    # Create installer script
    create_installer_script()
    
    # Create README for users
    readme_content = """
FileMaker Sync Tool
==================

This application synchronizes data from FileMaker Pro databases to Supabase.

Setup Instructions:
1. Ensure FileMaker Pro is installed and your database is accessible
2. Configure ODBC data source for your FileMaker database
3. Run FileMaker_Sync.exe
4. Configure your database connection in the Configuration tab
5. Test the connection before running a full sync

Features:
- Export database structure (DDL) and data (DML)
- Direct sync to Supabase database
- Export to SQL files
- Image extraction and conversion
- Progress monitoring and logging

Support:
For support, please contact your system administrator or refer to the documentation.

Version: 1.0.0
"""
    
    with open("README.txt", "w") as f:
        f.write(readme_content)
    print("✓ Created README.txt")

def build_executable():
    """Build the standalone executable"""
    print("\nBuilding executable...")
    
    # Import and run build script
    from build_exe import build_executable as build_exe
    build_exe()
    
    # Verify build
    exe_path = Path("dist/FileMaker_Sync.exe")
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"✓ Executable built successfully: {exe_path} ({size_mb:.1f} MB)")
        return True
    else:
        print("✗ Executable build failed")
        return False

def create_installer(inno_available):
    """Create Windows installer if Inno Setup is available"""
    if not inno_available:
        print("\nSkipping installer creation (Inno Setup not available)")
        return False
    
    print("\nCreating Windows installer...")
    
    try:
        result = subprocess.run([
            'iscc', 
            'installer_script.iss'
        ], capture_output=True, text=True, check=True)
        
        print("✓ Installer created successfully")
        installer_path = Path("installer/FileMaker_Sync_Setup.exe")
        if installer_path.exists():
            size_mb = installer_path.stat().st_size / (1024 * 1024)
            print(f"✓ Installer location: {installer_path} ({size_mb:.1f} MB)")
            return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Installer creation failed: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
    
    return False

def create_portable_package():
    """Create a portable ZIP package"""
    print("\nCreating portable package...")
    
    try:
        import zipfile
        
        zip_path = Path("installer/FileMaker_Sync_Portable.zip")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add executable
            zipf.write("dist/FileMaker_Sync.exe", "FileMaker_Sync.exe")
            # Add config file
            if Path("config.toml").exists():
                zipf.write("config.toml", "config.toml")
            # Add README
            zipf.write("README.txt", "README.txt")
            # Create logs directory structure
            zipf.writestr("logs/", "")
        
        size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"✓ Portable package created: {zip_path} ({size_mb:.1f} MB)")
        return True
        
    except Exception as e:
        print(f"✗ Portable package creation failed: {e}")
        return False

def cleanup_build_files():
    """Clean up temporary build files"""
    print("\nCleaning up build files...")
    
    cleanup_dirs = ['build']
    for directory in cleanup_dirs:
        if Path(directory).exists():
            shutil.rmtree(directory)
            print(f"✓ Removed {directory}")
    
    cleanup_files = ['version_info.txt', 'installer_script.iss']
    for file in cleanup_files:
        if Path(file).exists():
            Path(file).unlink()
            print(f"✓ Removed {file}")

def main():
    """Main deployment function"""
    print("FileMaker Sync - Deployment Script")
    print("=" * 40)
    
    # Check dependencies
    deps_ok, inno_available = check_dependencies()
    if not deps_ok:
        sys.exit(1)
    
    # Prepare build environment
    prepare_build_environment()
    
    # Build executable
    if not build_executable():
        print("\n✗ Deployment failed at executable build stage")
        sys.exit(1)
    
    # Create installer
    installer_created = create_installer(inno_available)
    
    # Create portable package
    portable_created = create_portable_package()
    
    # Summary
    print("\nDeployment Summary:")
    print("=" * 20)
    print("✓ Executable built: dist/FileMaker_Sync.exe")
    
    if installer_created:
        print("✓ Installer created: installer/FileMaker_Sync_Setup.exe")
    else:
        print("⚠ Installer not created")
    
    if portable_created:
        print("✓ Portable package: installer/FileMaker_Sync_Portable.zip")
    else:
        print("⚠ Portable package not created")
    
    print("\nRecommended distribution:")
    if installer_created:
        print("- Use installer/FileMaker_Sync_Setup.exe for standard installation")
    if portable_created:
        print("- Use installer/FileMaker_Sync_Portable.zip for portable deployment")
    
    # Cleanup
    cleanup_build_files()
    
    print("\n✓ Deployment complete!")

if __name__ == "__main__":
    main()
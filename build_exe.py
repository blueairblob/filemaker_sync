# build_exe.py
"""
PyInstaller build script for FileMaker Sync Application
Creates a standalone executable for Windows deployment
"""

import PyInstaller.__main__
import sys
import os
from pathlib import Path

def build_executable():
    """Build the standalone executable"""
    
    # Define the main script
    main_script = "filemaker_gui.py"
    
    # PyInstaller arguments
    args = [
        main_script,
        '--name=FileMaker_Sync',
        '--onefile',  # Single executable file
        '--windowed',  # No console window
        '--icon=icon.ico',  # App icon (if available)
        '--add-data=config.toml;.',  # Include config file
        '--add-data=filemaker_extract.py;.',  # Include core script
        '--hidden-import=PIL._tkinter_finder',  # For PIL
        '--hidden-import=pandas',
        '--hidden-import=sqlalchemy',
        '--hidden-import=pyodbc',
        '--clean',  # Clean PyInstaller cache
        '--noconfirm',  # Overwrite output without confirmation
        # Performance optimizations
        '--optimize=2',
        '--strip',  # Strip debug symbols (Linux/Mac)
        # Additional paths
        '--distpath=dist',
        '--workpath=build',
        '--specpath=build',
    ]
    
    # Add version info for Windows
    if sys.platform == 'win32':
        args.extend([
            '--version-file=version_info.txt'
        ])
    
    print("Building executable with PyInstaller...")
    print(f"Arguments: {' '.join(args)}")
    
    # Run PyInstaller
    PyInstaller.__main__.run(args)
    
    print("\nBuild complete!")
    print("Executable location: dist/FileMaker_Sync.exe")

if __name__ == "__main__":
    build_executable()

# setup_installer.py
"""
Create Windows installer using Inno Setup script
"""

INNO_SETUP_SCRIPT = """
; FileMaker Sync Installer Script for Inno Setup
[Setup]
AppName=FileMaker Sync
AppVersion=1.0.0
AppPublisher=Your Company Name
AppPublisherURL=https://yourcompany.com
DefaultDirName={autopf}\\FileMaker Sync
DefaultGroupName=FileMaker Sync
UninstallDisplayIcon={app}\\FileMaker_Sync.exe
Compression=lzma2
SolidCompression=yes
OutputDir=installer
OutputBaseFilename=FileMaker_Sync_Setup
SetupIconFile=icon.ico
WizardImageFile=installer_banner.bmp
WizardSmallImageFile=installer_icon.bmp

[Files]
Source: "dist\\FileMaker_Sync.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "config.toml"; DestDir: "{app}"; Flags: ignoreversion
Source: "README.txt"; DestDir: "{app}"; Flags: ignoreversion isreadme
Source: "logs\\*"; DestDir: "{app}\\logs"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\\FileMaker Sync"; Filename: "{app}\\FileMaker_Sync.exe"
Name: "{group}\\Uninstall FileMaker Sync"; Filename: "{uninstallexe}"
Name: "{autodesktop}\\FileMaker Sync"; Filename: "{app}\\FileMaker_Sync.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"

[Run]
Filename: "{app}\\FileMaker_Sync.exe"; Description: "Launch FileMaker Sync"; Flags: nowait postinstall skipifsilent

[Code]
// Custom installation logic
function GetUninstallString(): String;
var
  sUnInstPath: String;
  sUnInstallString: String;
begin
  sUnInstPath := ExpandConstant('Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{#emit SetupSetting("AppId")}_is1');
  sUnInstallString := '';
  if not RegQueryStringValue(HKLM, sUnInstPath, 'UninstallString', sUnInstallString) then
    RegQueryStringValue(HKCU, sUnInstPath, 'UninstallString', sUnInstallString);
  Result := sUnInstallString;
end;

function IsUpgrade(): Boolean;
begin
  Result := (GetUninstallString() <> '');
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  V: Integer;
  iResultCode: Integer;
  sUnInstallString: String;
begin
  Result := '';
  
  // Check if app is running
  if CheckForMutexes('FileMakerSync_Running') then begin
    Result := 'FileMaker Sync is currently running. Please close it and try again.';
    exit;
  end;
  
  // Uninstall previous version
  if (IsUpgrade()) then
  begin
    V := MsgBox(ExpandConstant('An existing installation was detected. Do you want to uninstall it first?'), mbInformation, MB_YESNO);
    if V = IDYES then
    begin
      sUnInstallString := GetUninstallString();
      sUnInstallString := RemoveQuotes(sUnInstallString);
      if Exec(sUnInstallString, '/SILENT /NORESTART /SUPPRESSMSGBOXES','', SW_HIDE, ewWaitUntilTerminated, iResultCode) then
        Result := ''
      else
        Result := 'Uninstall failed with code: ' + IntToStr(iResultCode) + '. Please uninstall manually.';
    end else
      Result := 'Installation cancelled.';
  end;
end;
"""

def create_installer_script():
    """Create the Inno Setup script file"""
    with open("installer_script.iss", "w") as f:
        f.write(INNO_SETUP_SCRIPT)
    print("Inno Setup script created: installer_script.iss")


# RAT - Railway Archive Trust

<!-- To add TOC: Ad Extention 'Markdown All In One' then F1 then 'create table of contents' -->
- [RAT - Railway Archive Trust](#rat---railway-archive-trust)
  - [1. Backend Scripts Development](#1-backend-scripts-development)
  - [2. Overview](#2-overview)
    - [2.1. Project Details](#21-project-details)
  - [3. Setup](#3-setup)
  - [3.1 Filemaker Pro ODBC](#31-filemaker-pro-odbc)
  - [3.2 Python](#32-python)
  - [3.3 Other Tools](#33-other-tools)
      - [3.3.1. Local:git](#331-localgit)
      - [Local:git](#localgit)
      - [3.3.2. PSQL](#332-psql)
      - [3.3.3. Docker Commands](#333-docker-commands)
  - [3.4. VSCode](#34-vscode)
    - [3.4.1. Configs](#341-configs)
    - [3.4.2. Extensions](#342-extensions)
  - [3.5 Supabase](#35-supabase)
    - [3.5.1. Supabase user account](#351-supabase-user-account)
    - [3.5.2. Database Details](#352-database-details)
    - [3.5.3. Install Local Supabase](#353-install-local-supabase)
      - [3.5.3.1. Test Installation](#3531-test-installation)
    - [3.5.4. Setup Supabase](#354-setup-supabase)
    - [3.5.4.1. Generate Schema Diagrams](#3541-generate-schema-diagrams)
  - [4. Migration](#4-migration)
    - [4.1. 1. Export metadata (DML Files) and images](#41-1-export-metadata-dml-files-and-images)
    - [4.2. 2. Load DML Files into Supabase](#42-2-load-dml-files-into-supabase)
  - [5. When you restart your PC!](#5-when-you-restart-your-pc)
- [6. Examples Run Commands](#6-examples-run-commands)
- [7. References](#7-references)
- [8. Target Db Migration](#8-target-db-migration)
- [8. Useful SQL](#8-useful-sql)
- [9. Issues](#9-issues)


## 1. <a name='BackendScriptsDevelopment'></a>Backend Scripts Development

```bash
 ___________          _________           _________
| Migration |________|   API   |_________|   GUI   |
| Scripts   |        | (Python)|         | (React) |
      ^
      |
  This bit.
```

## 2. <a name='Overview'></a>Overview

The migration scripts migrate FileMaker Pro data to the target Database.

Note: To view this Markdown either:

        Right click on this file tab and Select "Open Preview"

      Or

        `Ctrl + k, v to view markdown`

### 2.1. <a name='ProjectDetails'></a>Project Details

Work dirs:

  - `/c/dev/frontend/projects/Native_React_Examples/rat`
  - `/c/dev/RAT_Trains_Project\Migration`

Data Export:

- `/c/dev/RAT_Trains_Project/Migration/exports`


## 3. Setup

## 3.1 <a name='FilemakerProODBC'></a>Filemaker Pro ODBC

Ensure that the ODBC drivers are installed and working:

`C:\dev\RAT_Trains_Project\FileMaker Pro 20.1.2\Extras\xDBC\ODBC Client Driver Installer`

```
  dsn: rat
  User: train  / no password
  Ref.: C:\dev\RAT_Trains_Project\rat_dev.log
```

## 3.2 <a name='Python'></a>Python

Install Python for ALL users

1. Download Python 3.12 from <https://www.python.org/downloads/>
1. Right-click on the downloaded installer.
   - Select "Run as administrator".
1. Click on Advanced Options:
   - Install for all users
   - Add Python to environment
   - Install to C:\Python312
1. Install setuptools: For Python 3.12 and later, distutils is no longer included by default.

   - You can install setuptools which includes a copy of distutils:

     `python -m pip install --upgrade pip setuptools wheel`

## 3.3 Other Tools 

#### 3.3.1. <a name='Local:git'></a>Local:git

Just use local git repo for now (push to online GitHub later?):

#### Local:git

  ```bash
  git --version
  git init
  git config --global user.email "stuffdotstuff@gmail.com"
  git config --global user.name "stuffdotstuff"
  git status
  git add .
  git commit -m "init expo project"
  ```

  Do not convert LF <> CRLF (linux/windows)

  ```
  git config --global core.autocrlf false
  ```

#### 3.3.2. PSQL

1. Add psql to access local Db on command line:

- Install PostgreSQL for Windows to get hold of client (psql):

  `https://www.enterprisedb.com/downloads/postgres-postgresql-downloads`

  Note: Install only command line tools.

  ```powershell

  $Env:PATH = $Env:PATH + ";C:\Program Files\PostgreSQL\16\bin"

  psql 'postgresql://postgres:postgres@127.0.0.1:54322/postgres'

  postgres=> select * from rat.ratlabels;

  ```

#### 3.3.3. <a name='DockerCommands:'></a>Docker Commands

  ```
  docker ps -a
  docker stop $(docker ps -a -q)
  docker stop $(docker ps -aq) && docker rm $(docker ps -aq)

  ```

## 3.4. VSCode

### 3.4.1. Configs

For the "Git Bash" terminal: Add to settings.json (Ctrl+Shift+P):

  ```
  "terminal.integrated.defaultProfile.windows": "Git Bash",
  "terminal.integrated.profiles.windows": {
    "Git Bash": {
      "path": "C:\\Program Files\\Git\\bin\\bash.exe"
    }
  }
  ```

### 3.4.2. <a name='Extensions'></a>Extensions

- `Markdown All In One`
- `Project Manager`

  Add to projects.json:

  ```json
  [
    {
      "name": "backend API",
      "rootPath": "c:\\dev\\RAT_Trains_Project\\Migration\\GUI\\backend",
      "paths": [],
      "tags": [],
      "enabled": true
    },
    {
      "name": "backend GUI",
      "rootPath": "c:\\dev\\RAT_Trains_Project\\Migration\\GUI\\migration-tool-gui",
      "paths": [],
      "tags": [],
      "enabled": true
    },
    {
      "name": "backend Scripts",
      "rootPath": "c:\\dev\\RAT_Trains_Project\\Migration\\scripts",
      "paths": [],
      "tags": [],
      "enabled": true
    }
  ]
  ```


## 3.5 Supabase

[Local Development](https://supabase.com/docs/guides/cli/local-development)

[Guide](https://notjust.notion.site/React-Native-Supabase-Masterclass-47a69a60bc464c399b5a0df4d3c4a630?p=086fd8293ad240399a5043c45c6750d5&pm=s)

[Supabase Dashboard](https://supabase.com/dashboard)

### 3.5.1. Supabase user account

   Uses GitHub user account:

   `stuffdotstuff / C1`

### 3.5.2. Database Details

[All details](https://supabase.com/dashboard/project/kmoehqdowgdupzdxtbei/settings/general)

  ```
  Project : Rat
  Password: <Ref. ./.env or KeePass>!
  ```

  Project URL: `https://kmoehqdowgdupzdxtbei.supabase.co`

  API Keys:

  ```
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imttb2VocWRvd2dkdXB6ZHh0YmVpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjIwOTQ5MTIsImV4cCI6MjAzNzY3MDkxMn0.-i5FRsukow0szh72xK-O35JIRg13NWXJqUOuOy4l_bI
  ```

Project Ref ID: `kmoehqdowgdupzdxtbei`

### 3.5.3. Install Local Supabase

- Start Docker Desktop with Supabase container
- Install Supabase CLI:

  Open Powershell:

  NPM client. Prepend commands with "npx"

  ```powershell
    cd C:\dev\RAT_Trains_Project
    # Install Supabase locally
    npm install supabase
  ```

  or Windows CLI:

  https://supabase.com/docs/guides/cli/getting-started?queryGroups=platform&platform=windows

  ```
    iwr -useb get.scoop.sh | iex
    scoop bucket add supabase https://github.com/supabase/scoop-bucket.git
    scoop install supabase
    scoop update supabase
  ```

#### 3.5.3.1. Test Installation

```powershell
  # Prepend with npx if npm install
  supabase --version
  1.187.10
```

### 3.5.4. Setup Supabase

1. Initialise

  ```
    supabase init           #  Optional  --force
  ```

1. Pull Supabase database locally

There are 2 Supabase Clients:

`1) in C:\dev\RAT_Trains_Project\Migration`

  ```powershell
    # user: rat  pwd: <Ref. ./.env or KeePass>!
    supabase login
    supabase link --project-ref kmoehqdowgdupzdxtbei

    # OR just
    supabase link    # choose Db

    # Pull from hosted Supabase project
    supabase db pull --schema auth,storage

    # To apply the new migration to your local database:
    supabase migration up

  ```

`2) in C:\dev\RAT_Trains_Project\Migrations\scripts`

This one runs the same as above but with "npx" added. This Supabase CLI is THE one which launches the Supabase Db.
So just prefix the above with **npx**

1. Logical backup

Remember to save any local schema and data changes before stopping because the --no-backup flag will delete them.

  ```
    supabase db diff my_schema
    supabase db dump --local --data-only > supabase/seed.sql
  ```

1. Starts the Supabase local development stack.

   `supabase start`

If you can't start the db you may need to change the default api ports?

  ```
  In C:\dev\RAT_Trains_Project\supabase\config.toml

  # Change                        # To
    port = 54321                  # 55321
    port = 54322                  # 55322
    shadow_port = 54320           # 55320
    port = 54329                  # 55329
    port = 54323                  # 55323
    port = 54324                  # 55324
    inspector_port = 8083         # 8085
    port = 54327                  # 55327
  ```

1. Local Database Details

```bash
supabase status

        API URL: http://127.0.0.1:54321
    GraphQL URL: http://127.0.0.1:54321/graphql/v1

S3 Storage URL: http://127.0.0.1:54321/storage/v1/s3
DB URL: postgresql://postgres:postgres@127.0.0.1:54322/postgres
Studio URL: http://127.0.0.1:54323
Inbucket URL: http://127.0.0.1:54324
JWT secret: super-secret-jwt-token-with-at-least-32-characters-long
anon key: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0
service_role key: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImV4cCI6MTk4MzgxMjk5Nn0.EGIM96RAZx35lJzdJsyH-qQwv8Hdp7fsn3W0YpN81IU
S3 Access Key: 625729a08b95bf1b7ff351a663f3a23c
S3 Secret Key: 850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907
S3 Region: local
```

1. Loaded in DDL:

   `./schema/new_rat_schema.sql`


### 3.5.4.1. Generate Schema Diagrams

How to generate a custom schema diagrams:

1. Options 1: 'Schema Visualized'

   `https://supabase-schema.vercel.app/`

   Link:

   `./schema/schema_link.html`

   To print used snippet tool.

1. Option 2: Custom Schemas:

  `https://supabase.com/docs/guides/api/using-custom-schemas?queryGroups=language&language=curl#creating-custom-schemas`


## 4. Migration

Need to migrate to a Supabase Schema?

`
FileMakerPro -->> 1. **filemaker_extract.py** -->> DML Files -->> 2. **db_dml_loader.py** -->> Supabase!
`

In MySQL: tables:

    `images, prompts, ratbuilders, ratcatalogue, ratcollections, ratcopyright,ratlabels, ratroutes`

### 4.1. <a name='ExportmetadataDMLFilesandimages'></a>1. Export metadata (DML Files) and images

  ```
  /c/dev/RAT_Trains_Project/Migration/scripts
  . run_env
  pip install pyodbc pandas sqlalchemy tomli Pillow tqdm pymysql

  ./filemaker_extract.py --fn-exp True --images

  # With DDL
  ./filemaker_extract.py --fn-exp True --images
  
  ```

### 4.2. <a name='LoadDMLFilesintoSupabase'></a>2. Load DML Files into Supabase

  ```
  . /c/dev/RAT_Trains_Project/python/py312/Scripts/activate
  . run_env
  pip install --upgrade pip
  pip install pandas supabase, pandas, python-dotenv, tqdm

  EXPPTH=/c/dev/RAT_Trains_Project/Migration/scripts/exports
  ./db_dml_loader.py --export-path $EXPPTH --batch-size 5000 --user-id ratty
  ```

## 5. <a name='When you restart your PC!'></a>When you restart your PC!

1. When PC switched off had to restart supabase:

   - Launch Docker Desktop
   - Launch Terminal:

   ```
     cd C:\dev\RAT_Trains_Project\Migration\
     supabase login
     supabase link --project-ref kmoehqdowgdupzdxtbei
     <PWD> = <Ref. ./.env or KeePass>!
     supabase start
     supabase status
     ...
     Studio URL: http://127.0.0.1:54323
   ```

1. Update Supabase

  ```
  scoop update supabase
  ```

1. Restart Supabase / Link

  ```
  supabase link --project-ref kmoehqdowgdupzdxtbei
  ```

  Got this issue:

    ```
    PS C:\dev\RAT_Trains_Project\Migration> supabase link --project-ref kmoehqdowgdupzdxtbei
    Authorization failed for the access token and project ref pair: {"message":"Failed to fetch project service details"}
    Try rerunning the command with --debug to troubleshoot the error.

    ```

  Then restore RAT database online as it gets switched off if idle:

    ```
    https://supabase.com/dashboard/project/pmmyqsfrjplualdkxqyp

    ```

# 6. Examples Run Commands

```
filemaker_extract.py -t ratcatalogue --fn-exp False --db-exp True --check-ddl
```

# 7. <a name='References'></a>References

Back End - [notJust.dev: Module 2: Backend](https://youtu.be/rIYzLhkG9TA?t=12791)

# 8. Target Db Migration

  Run the following SQL on the Supabase database:
  
  ```powershell
    C:\dev\RAT_Trains_Project\Migration\schema\rat_target_schema.sql
    C:\dev\RAT_Trains_Project\Migration\schema\rat_migration_schema.sql
  ```

# 8. Useful SQL

1. List PK constraints
  ```bash
    SELECT tc.table_schema, tc.table_name, kc.column_name
    FROM 
        information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc 
          ON tc.constraint_name = kc.constraint_name
          AND tc.table_schema = kc.table_schema
    WHERE 
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'rat_migration'
        AND tc.table_name = 'ratcatalogue';
    ```

1. Close idle Supabase sessions

  ```bash
    SELECT pg_terminate_backend(pid) 
    FROM pg_stat_activity 
    WHERE datname = 'your_database_name'  -- replace with your database name
      AND pid <> pg_backend_pid()
      AND state = 'idle'
      AND state_change < current_timestamp - INTERVAL '10 minutes';
  ```

2. 

# 9. Issues

1. Commas in values:

    "Freudenstein 0-4-0WT Penlee plinthed at Penlee Quarries Ltd., Newlyn, formerly used on their 2' gauge system"

    If there is single quote the thats handled by "" around the value however commas in the value are not handled, unless directly by MySQL.
    _Resolved_

1. Brackets in description:

    "WD 2-10-0 number 600 Gordon, built by North British), heads away from Liss Forest Road for Longmoor with a visitors special during an open day"

    _Workaround_: Handle this by rejection to the .reject.sql file.

1. Having issues with filemaker_extract.py not doing upsert and saving conflict

1. PK not sticking in Supabase?
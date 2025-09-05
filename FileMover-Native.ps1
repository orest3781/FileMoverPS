#Requires -Version 5.1
<#
.SYNOPSIS
    PDF File Mover with date validation and comprehensive logging.

.DESCRIPTION
    Moves PDF files based on filename date patterns. Tags problematic files with prefixes.
    Logs successful moves to CSV for Power Query reporting.

.PARAMETER ConfigPath
    Path to configuration INI file.

.PARAMETER SourcePath
    Source directory override.

.PARAMETER DestinationPath
    Destination directory override.

.EXAMPLE
    .\FileMover-Native.ps1 -ConfigPath ".\configs\production.ini"

.EXAMPLE
    .\FileMover-Native.ps1 -SourcePath "C:\Source" -DestinationPath "C:\Destination"
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(HelpMessage = "Path to configuration file")]
    [ValidateScript({
        if ($_ -and -not (Test-Path $_)) {
            throw "Configuration file '$_' not found."
        }
        $true
    })]
    [string]$ConfigPath,
    
    [Parameter(HelpMessage = "Source directory override")]
    [ValidateScript({
        if ($_ -and -not (Test-Path $_ -PathType Container)) {
            throw "Source path '$_' is not a valid directory."
        }
        $true
    })]
    [string]$SourcePath,
    
    [Parameter(HelpMessage = "Destination directory override")]
    [string]$DestinationPath
)

Set-StrictMode -Version Latest

# Script variables
$Script:Config = @{}
$Script:Stats = @{
    ProcessedFiles = 0
    MovedFiles = 0
    SkippedFiles = 0
    ErrorFiles = 0
    StartTime = Get-Date
}
$Script:LogPath = $null
$Script:CsvLogPath = $null
$Script:iTextSharpLoaded = $false

class FileMoverConfig {
    [string]$FileExtension = '.pdf'
    [string]$LogFileNameBase = 'LOG'
    [string]$LogDateFormat = 'yyyyMMdd'
    [string[]]$LogHeaders = @('DATE-TIME', 'FILE-OWNER', 'FILE-NAME', 'ACTION', 'PDF-PAGE-COUNT', 'SOURCE-FOLDER', 'SOURCE-PATH', 'DESTINATION-PATH', 'ARCHIVE-PATH', 'PDF-FILE-SIZE', 'LAST-ACCESSED')
    [string]$ArchiveFolderPrefix = 'Archive'
    [string]$ArchiveFolderFormat = 'yyyyMMdd'
    [bool]$SearchSubfolders = $false
    [int]$LogMaxSizeMB = 10
    [int]$LogMaxFiles = 5
    [int]$DateWindowDays = 4
    [string]$SourceFolder
    [string]$DestinationFolder
    [string]$ArchiveFolderBase
    [string]$LogFolderPath
    
    # Default constructor
    FileMoverConfig() {}
    
    # Constructor that loads from hashtable (typically from INI file)
    FileMoverConfig([hashtable]$ConfigData) {
        $this.LoadFromHashtable($ConfigData)
    }
    
    # Load configuration values from hashtable
    # Used when parsing INI files or parameter overrides
    [void]LoadFromHashtable([hashtable]$ConfigData) {
        # Map INI keys to class properties
        $propertyMap = @{
            'file_extension' = 'FileExtension'
            'log_file_name_base' = 'LogFileNameBase'
            'log_date_format' = 'LogDateFormat'
            'log_headers' = 'LogHeaders'
            'archive_folder_prefix' = 'ArchiveFolderPrefix'
            'archive_folder_format' = 'ArchiveFolderFormat'
            'search_subfolders' = 'SearchSubfolders'
            'log_max_size_mb' = 'LogMaxSizeMB'
            'log_max_files' = 'LogMaxFiles'
            'date_window_days' = 'DateWindowDays'
            'source_folder' = 'SourceFolder'
            'destination_folder' = 'DestinationFolder'
            'archive_folder_base' = 'ArchiveFolderBase'
            'log_folder_path' = 'LogFolderPath'
        }
        
        foreach ($key in $ConfigData.Keys) {
            if ($propertyMap.ContainsKey($key)) {
                $propertyName = $propertyMap[$key]
                # Handle type conversions
                if ($key -eq 'search_subfolders') {
                    $this.$propertyName = [System.Convert]::ToBoolean($ConfigData[$key])
                } elseif ($key -eq 'log_max_size_mb' -or $key -eq 'log_max_files') {
                    $this.$propertyName = [int]$ConfigData[$key]
                } else {
                    $this.$propertyName = $ConfigData[$key]
                }
            }
        }
    }
    
    # Convert configuration to hashtable for serialization
    [hashtable]ToHashtable() {
        $result = @{}
        foreach ($property in $this.PSObject.Properties) {
            $result[$property.Name] = $property.Value
        }
        return $result
    }
}

<#
.SYNOPSIS
    Imports configuration from INI file format
    
.DESCRIPTION
    Parses INI configuration files and converts them to FileMoverConfig objects.
    Supports sections, comments, and comma-separated array values.
    
.PARAMETER Path
    Path to the INI configuration file
    
.EXAMPLE
    $config = Import-ConfigurationFile -Path ".\configs\production.ini"
#>
function Import-ConfigurationFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )
    
    Write-Verbose "Loading configuration from: $Path"
    
    try {
        # Read entire file content for parsing
        $configContent = Get-Content -Path $Path -Raw
        $config = @{}
        
        # Process each line of the configuration file
        foreach ($line in ($configContent -split "`n")) {
            $line = $line.Trim()
            
            # Skip empty lines and comments (# or ; prefixed)
            if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith('#') -or $line.StartsWith(';')) {
                continue
            }
            
            # Section headers [SectionName] - currently not used but parsed for future extensibility
            if ($line -match '^\[(.+)\]$') {
                # Section parsing could be enhanced in future versions
                continue
            }
            
            # Key-value pairs: key = value
            if ($line -match '^([^=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                
                # Handle comma-separated arrays (e.g., log_headers)
                if ($value.Contains(',')) {
                    $value = $value -split ',' | ForEach-Object { $_.Trim() }
                }
                
                # Store in configuration hashtable
                $config[$key] = $value
            }
        }
        
        # Convert hashtable to strongly-typed configuration object
        return [FileMoverConfig]::new($config)
    }
    catch {
        Write-Error "Failed to load configuration from '$Path': $_"
        throw
    }
}

function New-DefaultConfiguration {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$OutputPath
    )
    
    $templateContent = @'
[GeneralSettings]
file_extension = .pdf
log_file_name_base = LOG
log_date_format = yyyyMMdd
log_headers = DATE-TIME, FILE-OWNER, FILE-NAME, ACTION, PDF-PAGE-COUNT, SOURCE-FOLDER, SOURCE-PATH, DESTINATION-PATH, ARCHIVE-PATH, PDF-FILE-SIZE, LAST-ACCESSED
archive_folder_prefix = Archive
archive_folder_format = yyyyMMdd
search_subfolders = false
log_max_size_mb = 10
log_max_files = 5

[Paths]
source_folder = C:\Source
destination_folder = C:\Destination
archive_folder_base = C:\Archive
log_folder_path = C:\Logs
'@
    
    try {
        $outputDir = Split-Path -Parent $OutputPath
        if (-not (Test-Path $outputDir)) {
            New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
        }
        
        Set-Content -Path $OutputPath -Value $templateContent -Encoding UTF8
        Write-Information "Template configuration created: $OutputPath" -InformationAction Continue
        return $true
    }
    catch {
        Write-Error "Failed to create template configuration: $_"
        return $false
    }
}

function Find-ConfigurationFiles {
    [CmdletBinding()]
    param()
    
    $scriptDir = Split-Path -Parent $MyInvocation.PSCommandPath
    $configsDir = Join-Path $scriptDir "configs"
    
    if (Test-Path $configsDir) {
        $configFiles = Get-ChildItem -Path $configsDir -Filter "*.ini" | Sort-Object Name
        if ($configFiles) {
            Write-Verbose "Found configuration files: $($configFiles.Name -join ', ')"
            return $configFiles.FullName
        }
    }
    
    return @()
}
#endregion

#region Logging Functions
function Invoke-LogRotation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$LogPath,
        
        [Parameter(Mandatory)]
        [int]$MaxSizeMB,
        
        [Parameter(Mandatory)]
        [int]$MaxFiles
    )
    
    if (-not (Test-Path $LogPath)) {
        return
    }
    
    $logFile = Get-Item $LogPath
    $logSizeMB = [math]::Round($logFile.Length / 1MB, 2)
    
    if ($logSizeMB -ge $MaxSizeMB) {
        $logDir = Split-Path -Parent $LogPath
        $logBaseName = [System.IO.Path]::GetFileNameWithoutExtension($LogPath)
        $logExtension = [System.IO.Path]::GetExtension($LogPath)
        
        # Rotate existing log files
        for ($i = $MaxFiles - 1; $i -gt 0; $i--) {
            $oldLogPath = Join-Path $logDir "$logBaseName.$i$logExtension"
            $newLogPath = Join-Path $logDir "$logBaseName.$($i + 1)$logExtension"
            
            if (Test-Path $oldLogPath) {
                if ($i -eq ($MaxFiles - 1)) {
                    # Delete the oldest log
                    Remove-Item -Path $oldLogPath -Force -ErrorAction SilentlyContinue
                } else {
                    # Move to next number
                    Move-Item -Path $oldLogPath -Destination $newLogPath -Force -ErrorAction SilentlyContinue
                }
            }
        }
        
        # Move current log to .1
        $firstRotatedLog = Join-Path $logDir "$logBaseName.1$logExtension"
        Move-Item -Path $LogPath -Destination $firstRotatedLog -Force -ErrorAction SilentlyContinue
        
        Write-Verbose "Log rotated: $LogPath (${logSizeMB}MB) -> $firstRotatedLog"
    }
}

function Initialize-Logging {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [FileMoverConfig]$Config
    )
    
    # Always use script directory for .log file (errors/warnings only)
    $scriptDir = Split-Path -Parent $MyInvocation.PSCommandPath
    $logFileName = "file_mover.log"
    $Script:LogPath = Join-Path $scriptDir $logFileName
    
    # CSV files use configured log folder path from INI
    $csvLogDir = if ($Config.LogFolderPath -and (Test-Path $Config.LogFolderPath)) {
        $Config.LogFolderPath
    } else {
        $scriptDir
    }
    
    # CSV uses date-based naming from config
    $dateString = Get-Date -Format $Config.LogDateFormat
    $csvLogFileName = "$($Config.LogFileNameBase)_$dateString.csv"
    $Script:CsvLogPath = Join-Path $csvLogDir $csvLogFileName
    
    # Rotate .log file if it exists and is too large (errors/warnings only)
    Invoke-LogRotation -LogPath $Script:LogPath -MaxSizeMB $Config.LogMaxSizeMB -MaxFiles $Config.LogMaxFiles
    
    # Initialize CSV log with headers if it doesn't exist
    if (-not (Test-Path $Script:CsvLogPath)) {
        $Config.LogHeaders -join ',' | Out-File -FilePath $Script:CsvLogPath -Encoding UTF8
        Write-Verbose "CSV log initialized: $Script:CsvLogPath"
    }
    
    Write-Information "Logging initialized - Error Log: $Script:LogPath, CSV: $Script:CsvLogPath" -InformationAction Continue
}

function Write-LogEntry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, ValueFromPipeline)]
        [string]$Message,
        
        [Parameter()]
        [ValidateSet('Information', 'Warning', 'Error', 'Verbose')]
        [string]$Level = 'Information'
    )
    
    process {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $logEntry = "$timestamp [$Level] $Message"
        
        # Console output using appropriate Write-* cmdlets
        switch ($Level) {
            'Information' { Write-Information $logEntry -InformationAction Continue }
            'Warning' { Write-Warning $Message }
            'Error' { 
                # Use Write-Host for error output to avoid parameter binding issues
                Write-Host $logEntry -ForegroundColor Red
            }
            'Verbose' { Write-Verbose $Message }
        }
        
        # Only write errors and warnings to .log file
        if ($Script:LogPath -and $Script:LogPath -ne "" -and ($Level -eq 'Error' -or $Level -eq 'Warning')) {
            $retryCount = 0
            $maxRetries = 3
            
            do {
                try {
                    $logEntry | Out-File -FilePath $Script:LogPath -Append -Encoding UTF8 -ErrorAction Stop
                    break
                }
                catch [System.IO.IOException] {
                    $retryCount++
                    if ($retryCount -lt $maxRetries) {
                        Start-Sleep -Milliseconds (Get-Random -Minimum 10 -Maximum 50)
                    }
                }
            } while ($retryCount -lt $maxRetries)
        }
    }
}

function Write-CsvLogEntry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [hashtable]$LogData
    )
    
    if (-not $Script:CsvLogPath) { return }
    
    $csvLine = ($Script:Config.LogHeaders | ForEach-Object {
        $value = $LogData[$_]
        if ($value -match '[",\r\n]') {
            "`"$($value -replace '"', '""')`""
        } else {
            $value
        }
    }) -join ','
    
    $retryCount = 0
    $maxRetries = 3
    
    do {
        try {
            $csvLine | Out-File -FilePath $Script:CsvLogPath -Append -Encoding UTF8 -ErrorAction Stop
            break
        }
        catch [System.IO.IOException] {
            $retryCount++
            if ($retryCount -lt $maxRetries) {
                Start-Sleep -Milliseconds (Get-Random -Minimum 10 -Maximum 50)
            }
        }
    } while ($retryCount -lt $maxRetries)
}
#endregion

#region PDF Processing Functions
function Initialize-iTextSharp {
    if ($Script:iTextSharpLoaded) {
        return $true
    }
    
    $iTextSharpPath = Join-Path (Split-Path -Parent $MyInvocation.PSCommandPath) "itextsharp.dll"
    if (Test-Path $iTextSharpPath) {
        try {
            Add-Type -Path $iTextSharpPath -ErrorAction Stop
            $Script:iTextSharpLoaded = $true
            Write-Verbose "iTextSharp library loaded successfully"
            return $true
        }
        catch {
            Write-Verbose "Failed to load iTextSharp: $_"
            return $false
        }
    }
    return $false
}

function Get-PdfPageCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )
    
    Write-Verbose "Getting PDF page count for: $FilePath"
    
    # Try iTextSharp first if available
    if (Initialize-iTextSharp) {
        try {
            $reader = New-Object iTextSharp.text.pdf.PdfReader($FilePath)
            $pageCount = $reader.NumberOfPages
            $reader.Close()
            Write-Verbose "PDF page count (iTextSharp): $pageCount"
            return $pageCount
        }
        catch {
            Write-Verbose "iTextSharp failed, using fallback: $_"
        }
    }
    
    # Enhanced fallback parsing - read larger chunks for better accuracy
    try {
        # Read first 64KB for better /Encrypt detection and page count parsing
        $maxBytes = [Math]::Min(65536, (Get-Item $FilePath).Length)
        $pdfBytes = [System.IO.File]::ReadAllBytes($FilePath)
        
        # Try to read the entire file if it's reasonable size (< 10MB)
        if ($pdfBytes.Length -lt 10MB) {
            $pdfText = [System.Text.Encoding]::ASCII.GetString($pdfBytes)
        } else {
            # For large files, read first and last 32KB chunks
            $firstChunk = [System.Text.Encoding]::ASCII.GetString($pdfBytes[0..32767])
            $lastChunk = [System.Text.Encoding]::ASCII.GetString($pdfBytes[($pdfBytes.Length-32768)..($pdfBytes.Length-1)])
            $pdfText = $firstChunk + $lastChunk
        }
        
        # Enhanced parsing strategies with better regex patterns
        $strategies = @(
            { param($text) if ($text -match '/Type\s*/Pages[^>]*?/Count\s+(\d+)') { return [int]$matches[1] } },
            { param($text) if ($text -match '/Pages[^>]*?/Count\s+(\d+)') { return [int]$matches[1] } },
            { param($text) if ($text -match '/Count\s+(\d+)') { return [int]$matches[1] } },
            { param($text) return ([regex]::Matches($text, '/Type\s*/Page(?!\s*s)\b')).Count },
            { param($text) if ($text -match '/N\s+(\d+)') { return [int]$matches[1] } },
            { param($text) return ([regex]::Matches($text, '\bendobj\s')).Count / 2 }  # Rough estimate
        )
        
        foreach ($strategy in $strategies) {
            $result = & $strategy $pdfText
            if ($result -and $result -gt 0 -and $result -lt 10000) {  # Sanity check
                Write-Verbose "PDF page count (fallback): $result"
                return $result
            }
        }
        
        Write-Verbose "Could not determine PDF page count"
        return 1  # Default to 1 page instead of null
    }
    catch {
        Write-Verbose "Failed to get PDF page count: $_"
        return 1  # Default to 1 page instead of null
    }
}

function Test-PdfPasswordProtected {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )
    
    # Try iTextSharp first for more reliable detection
    if (Initialize-iTextSharp) {
        try {
            $reader = New-Object iTextSharp.text.pdf.PdfReader($FilePath)
            $isEncrypted = $reader.IsEncrypted()
            $reader.Close()
            Write-Verbose "PDF password protection (iTextSharp): $isEncrypted"
            return $isEncrypted
        }
        catch {
            Write-Verbose "iTextSharp password check failed, using fallback: $_"
        }
    }
    
    # Enhanced fallback - scan more bytes for /Encrypt
    try {
        $maxBytes = [Math]::Min(65536, (Get-Item $FilePath).Length)  # Read up to 64KB
        $headerBytes = Get-Content -Path $FilePath -Encoding Byte -TotalCount $maxBytes -ErrorAction Stop
        $header = [System.Text.Encoding]::ASCII.GetString($headerBytes)
        
        # Look for various encryption indicators
        $encryptPatterns = @(
            '/Encrypt\s',
            '/Encrypt\[',
            '/Encrypt\d',
            'UserPassword',
            'OwnerPassword'
        )
        
        foreach ($pattern in $encryptPatterns) {
            if ($header -match $pattern) {
                Write-Verbose "PDF password protection detected with pattern: $pattern"
                return $true
            }
        }
        
        return $false
    }
    catch {
        Write-Verbose "Failed to check PDF password protection: $_"
        return $false
    }
}
#endregion

#region File Operations
function Test-FileInUse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )
    
    try {
        $fileStream = [System.IO.File]::Open($FilePath, 'Open', 'ReadWrite', 'None')
        $fileStream.Close()
        return $false
    }
    catch [System.IO.IOException] {
        return $true
    }
    catch {
        return $true
    }
}

function Get-FileOwner {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FilePath
    )
    
    try {
        $acl = Get-Acl -Path $FilePath -ErrorAction Stop
        return $acl.Owner
    }
    catch {
        Write-Verbose "Failed to get file owner: $_"
        return "Unknown"
    }
}

function Get-UniqueFileName {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Directory,
        
        [Parameter(Mandatory)]
        [string]$FileName
    )
    
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $extension = [System.IO.Path]::GetExtension($FileName)
    $counter = 1
    $newFileName = $FileName
    
    while (Test-Path (Join-Path $Directory $newFileName)) {
        $newFileName = "${baseName}($counter)$extension"
        $counter++
    }
    
    return $newFileName
}

function Copy-FileWithSecurity {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string]$SourcePath,
        
        [Parameter(Mandatory)]
        [string]$DestinationPath,
        
        [Parameter()]
        [switch]$PreserveSecurity
    )
    
    if ($PSCmdlet.ShouldProcess($SourcePath, "Copy to $DestinationPath")) {
        try {
            # Get original security descriptor
            $originalAcl = if ($PreserveSecurity) { Get-Acl -Path $SourcePath } else { $null }
            
            # Ensure destination directory exists
            $destDir = Split-Path -Parent $DestinationPath
            if (-not (Test-Path $destDir)) {
                New-Item -ItemType Directory -Path $destDir -Force | Out-Null
            }
            
            # Copy operation
            Copy-Item -Path $SourcePath -Destination $DestinationPath -Force -ErrorAction Stop
            
            # Restore security if requested
            if ($originalAcl) {
                Set-Acl -Path $DestinationPath -AclObject $originalAcl -ErrorAction SilentlyContinue
            }
            
            return $true
        }
        catch {
            Write-Error "Failed to copy file '$SourcePath' to '$DestinationPath': $_"
            return $false
        }
    }
    
    return $true  # WhatIf scenario
}

function Move-FileWithSecurity {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string]$SourcePath,
        
        [Parameter(Mandatory)]
        [string]$DestinationPath,
        
        [Parameter()]
        [switch]$PreserveSecurity
    )
    
    if ($PSCmdlet.ShouldProcess($SourcePath, "Move to $DestinationPath")) {
        try {
            # Get original security descriptor
            $originalAcl = if ($PreserveSecurity) { Get-Acl -Path $SourcePath } else { $null }
            
            # Ensure destination directory exists
            $destDir = Split-Path -Parent $DestinationPath
            if (-not (Test-Path $destDir)) {
                New-Item -ItemType Directory -Path $destDir -Force | Out-Null
            }
            
            # Atomic move operation
            Move-Item -Path $SourcePath -Destination $DestinationPath -Force -ErrorAction Stop
            
            # Restore security if requested
            if ($originalAcl) {
                Set-Acl -Path $DestinationPath -AclObject $originalAcl -ErrorAction SilentlyContinue
            }
            
            return $true
        }
        catch {
            Write-Error "Failed to move file '$SourcePath' to '$DestinationPath': $_"
            return $false
        }
    }
    
    return $true  # WhatIf scenario
}
#endregion

#region Date Validation
function Test-DateInFilename {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FileName,
        
        [Parameter()]
        [int]$DateWindowDays = 4
    )
    
    # Only accept YYYYMMDD format at the beginning of filename
    $datePattern = '^(?<year>20\d{2})(?<month>0[1-9]|1[0-2])(?<day>0[1-9]|[12]\d|3[01])'
    
    if ($FileName -match $datePattern) {
        try {
            $dateString = "$($matches.year)-$($matches.month)-$($matches.day)"
            # Use ParseExact with try/catch to validate dates like Feb 30
            try {
                $parsedDate = [DateTime]::ParseExact($dateString, 'yyyy-MM-dd', $null)
            }
            catch {
                return @{ IsValid = $false; Reason = "Invalid date: $dateString" }
            }
            
            # Validate date is within configured window
            $now = Get-Date
            $minDate = $now.AddDays(-$DateWindowDays).Date
            $maxDate = $now.AddDays($DateWindowDays).Date
            $parsedDateOnly = $parsedDate.Date
            
            if ($parsedDateOnly -ge $minDate -and $parsedDateOnly -le $maxDate) {
                return @{
                    IsValid = $true
                    Date = $parsedDate
                    Pattern = $datePattern
                }
            } else {
                return @{ IsValid = $false; Reason = "Date $dateString is outside allowed range (+/-$DateWindowDays days from today)" }
            }
        }
        catch {
            return @{ IsValid = $false; Reason = "Date parsing failed: $_" }
        }
    }
    
    return @{ IsValid = $false; Reason = "Filename does not start with YYYYMMDD format" }
}
#endregion

#region Progress Reporting
function Write-ProgressStatus {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [int]$Current,
        
        [Parameter(Mandatory)]
        [int]$Total,
        
        [Parameter(Mandatory)]
        [string]$CurrentFile,
        
        [Parameter()]
        [string]$Status = "Processing"
    )
    
    $percentComplete = [math]::Round(($Current / $Total) * 100, 1)
    $elapsed = (Get-Date) - $Script:Stats.StartTime
    
    if ($Current -gt 0) {
        $avgTimePerFile = $elapsed.TotalSeconds / $Current
        $remainingFiles = $Total - $Current
        $estimatedTimeRemaining = [TimeSpan]::FromSeconds($avgTimePerFile * $remainingFiles)
        $filesPerMinute = [math]::Round(($Current / $elapsed.TotalMinutes), 1)
        
        Write-Progress -Activity "File Mover" -Status "$Status - $Current/$Total files" -PercentComplete $percentComplete -CurrentOperation $CurrentFile
        Write-Host "[PROGRESS] [$Current/$Total] $CurrentFile" -ForegroundColor Cyan
        Write-Host "[SPEED] $filesPerMinute files/min | [ETA] $($estimatedTimeRemaining.ToString('hh\:mm\:ss'))" -ForegroundColor Yellow
    }
}
#endregion

#region Main Processing Function
function Invoke-FileProcessing {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [FileMoverConfig]$Config
    )
    
    Write-LogEntry "Starting file processing" -Level Information
    
    # Initialize iTextSharp once at the beginning
    Initialize-iTextSharp | Out-Null
    
    # Get files to process (with optional recursive search)
    $getChildItemParams = @{
        Path = $Config.SourceFolder
        Filter = "*$($Config.FileExtension)"
        File = $true
    }
    
    if ($Config.SearchSubfolders) {
        $getChildItemParams.Recurse = $true
        Write-LogEntry "Searching for files recursively in subfolders" -Level Information
    } else {
        Write-LogEntry "Searching for files in root folder only" -Level Information
    }
    
    $sourceFiles = @(Get-ChildItem @getChildItemParams | Sort-Object Name)
    
    if (-not $sourceFiles -or $sourceFiles.Count -eq 0) {
        Write-LogEntry "No $($Config.FileExtension) files found in source directory" -Level Warning
        return
    }
    
    Write-LogEntry "Found $($sourceFiles.Count) files to process" -Level Information
    
    $fileIndex = 0
    foreach ($file in $sourceFiles) {
        $fileIndex++
        
        Write-ProgressStatus -Current $fileIndex -Total $sourceFiles.Count -CurrentFile $file.Name
        
        try {
            # Skip files that are already tagged
            if ($file.Name.StartsWith("__CHECK-DATE__") -or 
                $file.Name.StartsWith("__REMOVE-PASSWORD__") -or 
                $file.Name.StartsWith("__PROBLEMATIC__") -or 
                $file.Name.StartsWith("__ACCESS-DENIED__")) {
                Write-Host "   [SKIP] Already tagged file: $($file.Name)" -ForegroundColor Gray
                $Script:Stats.SkippedFiles++
                continue
            }
            
            # Check if file is in use
            Write-Host "   [VALIDATE] Checking: File access" -ForegroundColor Gray
            if (Test-FileInUse -FilePath $file.FullName) {
                Write-LogEntry "Skipped '$($file.Name)': File is in use" -Level Warning
                Write-Host "   [SKIP] Skipped: File in use" -ForegroundColor Red
                $Script:Stats.SkippedFiles++
                continue
            }
            
            # Validate date in filename
            Write-Host "   [VALIDATE] Validating: Date format" -ForegroundColor Gray
            $dateValidation = Test-DateInFilename -FileName $file.Name -DateWindowDays $Config.DateWindowDays
            
            if (-not $dateValidation.IsValid) {
                $reason = if ($dateValidation.Reason) { $dateValidation.Reason } else { "Invalid date format" }
                $newName = "__CHECK-DATE__$($file.Name)"
                $newName = Get-UniqueFileName -Directory $file.Directory -FileName $newName
                
                if ($PSCmdlet.ShouldProcess($file.FullName, "Rename to $newName")) {
                    Rename-Item -Path $file.FullName -NewName $newName
                    Write-LogEntry "Renamed invalid date file: $($file.Name) -> $newName ($reason)" -Level Warning
                    Write-Host "   [RENAME] Invalid date -> $newName" -ForegroundColor Yellow
                }
                $Script:Stats.SkippedFiles++
                continue
            }
            
            # Check for 0KB (empty) files
            if ($file.Length -eq 0) {
                $newName = "__PROBLEMATIC__$($file.Name)"
                $newName = Get-UniqueFileName -Directory $file.Directory -FileName $newName
                
                if ($PSCmdlet.ShouldProcess($file.FullName, "Rename to $newName")) {
                    Rename-Item -Path $file.FullName -NewName $newName
                    Write-LogEntry "Renamed 0KB file: $($file.Name) -> $newName" -Level Warning
                    Write-Host "   [RENAME] 0KB file -> $newName" -ForegroundColor Yellow
                }
                $Script:Stats.SkippedFiles++
                continue
            }
            
            # Check for password protection
            if (Test-PdfPasswordProtected -FilePath $file.FullName) {
                $newName = "__REMOVE-PASSWORD__$($file.Name)"
                $newName = Get-UniqueFileName -Directory $file.Directory -FileName $newName
                
                if ($PSCmdlet.ShouldProcess($file.FullName, "Rename to $newName")) {
                    Rename-Item -Path $file.FullName -NewName $newName
                    Write-LogEntry "Renamed password-protected file: $($file.Name) -> $newName" -Level Warning
                    Write-Host "   [RENAME] Password protected -> $newName" -ForegroundColor Yellow
                }
                $Script:Stats.SkippedFiles++
                continue
            }
            
            # Get file metadata for CSV logging
            $fileOwner = try { (Get-Acl $file.FullName).Owner } catch { "Unknown" }
            $fileSize = $file.Length
            $lastAccessed = $file.LastAccessTime.ToString("yyyy-MM-dd HH:mm:ss")
            $pageCount = Get-PdfPageCount -FilePath $file.FullName
            
            # Build destination and archive paths
            $destinationPath = Join-Path $Config.DestinationFolder $file.Name
            $archiveDate = $dateValidation.Date.ToString($Config.ArchiveFolderFormat)
            $archiveFolder = "$($Config.ArchiveFolderPrefix)$archiveDate"
            $archiveFolderPath = Join-Path $Config.ArchiveFolderBase $archiveFolder
            $archivePath = Join-Path $archiveFolderPath $file.Name
            
            # Ensure destination and archive directories exist
            if (-not (Test-Path $Config.DestinationFolder)) {
                New-Item -Path $Config.DestinationFolder -ItemType Directory -Force | Out-Null
                Write-Host "   [CREATE] Created destination directory" -ForegroundColor Cyan
            }
            if (-not (Test-Path $archiveFolderPath)) {
                New-Item -Path $archiveFolderPath -ItemType Directory -Force | Out-Null
                Write-Host "   [CREATE] Created archive directory: $archiveFolder" -ForegroundColor Cyan
            }
            
            # Step 1: Copy to destination
            Write-Host "   [COPY] Copying to destination..." -ForegroundColor Yellow
            if ($PSCmdlet.ShouldProcess($file.FullName, "Copy to $destinationPath")) {
                try {
                    Copy-Item -Path $file.FullName -Destination $destinationPath -Force -ErrorAction Stop
                    Write-Host "   [COPY] Destination copy completed" -ForegroundColor Green
                }
                catch {
                    Write-Host "   [ERROR] Failed to copy to destination: $_" -ForegroundColor Red
                    $Script:Stats.ErrorFiles++
                    continue
                }
            }
            
            # Step 2: Copy to archive
            Write-Host "   [COPY] Copying to archive..." -ForegroundColor Yellow
            if ($PSCmdlet.ShouldProcess($file.FullName, "Copy to $archivePath")) {
                try {
                    Copy-Item -Path $file.FullName -Destination $archivePath -Force -ErrorAction Stop
                    Write-Host "   [COPY] Archive copy completed" -ForegroundColor Green
                }
                catch {
                    Write-Host "   [ERROR] Failed to copy to archive: $_" -ForegroundColor Red
                    $Script:Stats.ErrorFiles++
                    continue
                }
            }
            
            # Step 3: Verify both copies exist and delete source
            if ((Test-Path $destinationPath) -and (Test-Path $archivePath)) {
                Write-Host "   [VERIFY] Both copies verified, deleting source..." -ForegroundColor Green
                
                if ($PSCmdlet.ShouldProcess($file.FullName, "Delete source file")) {
                    try {
                        Remove-Item -Path $file.FullName -Force -ErrorAction Stop
                        Write-Host "   [DELETE] Source file deleted successfully" -ForegroundColor Green
                        $moveSuccess = $true
                    }
                    catch {
                        Write-Host "   [ERROR] Failed to delete source file: $_" -ForegroundColor Red
                        $moveSuccess = $false
                    }
                } else {
                    $moveSuccess = $true  # WhatIf scenario
                }
            } else {
                Write-Host "   [ERROR] Verification failed - copies not found" -ForegroundColor Red
                $moveSuccess = $false
            }
            
            if ($moveSuccess) {
                $action = if ($WhatIfPreference) { "WOULD_MOVE" } else { "MOVED" }
                Write-Host "   [SUCCESS] File moved successfully" -ForegroundColor Green
                $Script:Stats.MovedFiles++
                
                # Log to CSV (only for non-tagged files)
                if (-not ($file.Name.StartsWith("__CHECK-DATE__") -or 
                         $file.Name.StartsWith("__REMOVE-PASSWORD__") -or 
                         $file.Name.StartsWith("__PROBLEMATIC__") -or 
                         $file.Name.StartsWith("__ACCESS-DENIED__"))) {
                    $logData = @{
                        'DATE-TIME' = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                        'FILE-OWNER' = $fileOwner
                        'FILE-NAME' = $file.Name
                        'ACTION' = $action
                        'PDF-PAGE-COUNT' = $pageCount
                        'SOURCE-FOLDER' = $(
                            if ($file.DirectoryName -eq $Config.SourceFolder) { 
                                Split-Path -Leaf $Config.SourceFolder 
                            } else { 
                                $relativePath = $file.DirectoryName.Replace($Config.SourceFolder, '').TrimStart('\')
                                "$(Split-Path -Leaf $Config.SourceFolder)\$relativePath"
                            }
                        )
                        'SOURCE-PATH' = $file.FullName
                        'DESTINATION-PATH' = $destinationPath
                        'ARCHIVE-PATH' = $archivePath
                        'PDF-FILE-SIZE' = $fileSize
                        'LAST-ACCESSED' = $lastAccessed
                    }
                    
                    Write-CsvLogEntry -LogData $logData
                }
                Write-LogEntry "Successfully processed: $($file.Name)" -Level Information
            }
            else {
                Write-Host "   [ERROR] Failed to move file" -ForegroundColor Red
                $Script:Stats.ErrorFiles++
            }
            
            $Script:Stats.ProcessedFiles++
        }
        catch {
            Write-LogEntry "Error processing '$($file.Name)': $_" -Level Error
            Write-Host "   [ERROR] $($_.Exception.Message)" -ForegroundColor Red
            $Script:Stats.ErrorFiles++
        }
    }
    
    Write-Progress -Activity "File Mover" -Completed
}
#endregion

function Invoke-SingleConfigProcessing {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [FileMoverConfig]$Config
    )
    
    # Validate required paths
    if (-not $Config.SourceFolder -or -not (Test-Path $Config.SourceFolder)) {
        throw "Source folder not configured or does not exist: $($Config.SourceFolder)"
    }
    
    if (-not $Config.DestinationFolder) {
        throw "Destination folder not configured"
    }
    
    # Initialize logging for this configuration
    Initialize-Logging -Config $Config
    
    # Process files
    Write-Information "=== File Mover Started ===" -InformationAction Continue
    Write-Information "Source: $($Config.SourceFolder)" -InformationAction Continue
    Write-Information "Destination: $($Config.DestinationFolder)" -InformationAction Continue
    Write-Information "Archive: $($Config.ArchiveFolderBase)" -InformationAction Continue
    
    if ($WhatIfPreference) {
        Write-Information "WhatIf mode: No files will be moved" -InformationAction Continue
    }
    
    # Store current stats for this config
    $configStartStats = @{
        ProcessedFiles = $Script:Stats.ProcessedFiles
        MovedFiles = $Script:Stats.MovedFiles
        SkippedFiles = $Script:Stats.SkippedFiles
        ErrorFiles = $Script:Stats.ErrorFiles
        StartTime = Get-Date
    }
    
    Invoke-FileProcessing -Config $Config
    
    # Calculate stats for this configuration only
    $configProcessed = $Script:Stats.ProcessedFiles - $configStartStats.ProcessedFiles
    $configMoved = $Script:Stats.MovedFiles - $configStartStats.MovedFiles
    $configSkipped = $Script:Stats.SkippedFiles - $configStartStats.SkippedFiles
    $configErrors = $Script:Stats.ErrorFiles - $configStartStats.ErrorFiles
    $configElapsed = (Get-Date) - $configStartStats.StartTime
    
    Write-Information "=== Configuration Processing Complete ===" -InformationAction Continue
    Write-Information "Processed: $configProcessed files" -InformationAction Continue
    Write-Information "Moved: $configMoved files" -InformationAction Continue
    Write-Information "Skipped: $configSkipped files" -InformationAction Continue
    Write-Information "Errors: $configErrors files" -InformationAction Continue
    Write-Information "Time: $($configElapsed.ToString('hh\:mm\:ss'))" -InformationAction Continue
    
    Write-LogEntry "Configuration processing completed successfully" -Level Information
}

#region Main Function
function Invoke-FileMover {
    [CmdletBinding(SupportsShouldProcess)]
    param()
    
    try {
        # Initialize variables
        $configFile = $null
        $configFiles = $null
        
        # Load configuration
        if ($ConfigPath) {
            $configFile = $ConfigPath
        } elseif ($PSBoundParameters.ContainsKey('SourcePath') -or $PSBoundParameters.ContainsKey('DestinationPath')) {
            # Create temporary config from parameters
            $Script:Config = [FileMoverConfig]::new()
            if ($SourcePath) { $Script:Config.SourceFolder = $SourcePath }
            if ($DestinationPath) { $Script:Config.DestinationFolder = $DestinationPath }
            
            # Process this single configuration
            Invoke-SingleConfigProcessing -Config $Script:Config
        } else {
            $configFiles = Find-ConfigurationFiles
            if (-not $configFiles -or $configFiles.Count -eq 0) {
                $defaultConfigPath = Join-Path (Split-Path -Parent $MyInvocation.PSCommandPath) "configs\template.ini"
                Write-Warning "No configuration files found. Creating template at: $defaultConfigPath"
                
                if (New-DefaultConfiguration -OutputPath $defaultConfigPath) {
                    Write-Information "Please configure the template file and run the script again." -InformationAction Continue
                    return
                }
                throw "Failed to create default configuration"
            }
        }
        
        # Process single config file if specified
        if ($configFile) {
            $Script:Config = Import-ConfigurationFile -Path $configFile
            Write-Information "Loaded configuration from: $configFile" -InformationAction Continue
            
            # Process this single configuration
            Invoke-SingleConfigProcessing -Config $Script:Config
        }
        # Process all config files if none specified
        elseif ($configFiles) {
            Write-Information "=== Processing Multiple Configuration Files ===" -InformationAction Continue
            Write-Information "Found $($configFiles.Count) configuration files to process" -InformationAction Continue
            
            $configIndex = 0
            foreach ($configFilePath in $configFiles) {
                $configIndex++
                
                try {
                    Write-Information "" -InformationAction Continue
                    Write-Information "=== Configuration $configIndex of $($configFiles.Count): $(Split-Path -Leaf $configFilePath) ===" -InformationAction Continue
                    
                    $Script:Config = Import-ConfigurationFile -Path $configFilePath
                    Write-Information "Loaded configuration from: $configFilePath" -InformationAction Continue
                    
                    # Process this configuration
                    Invoke-SingleConfigProcessing -Config $Script:Config
                }
                catch {
                    Write-LogEntry "Failed to process configuration '$configFilePath': $_" -Level Error
                    Write-Error "Failed to process configuration '$configFilePath': $_"
                }
            }
            
            # Final overall statistics
            $elapsed = (Get-Date) - $Script:Stats.StartTime
            Write-Information "" -InformationAction Continue
            Write-Information "=== All Configurations Processing Complete ===" -InformationAction Continue
            Write-Information "Total Processed: $($Script:Stats.ProcessedFiles) files" -InformationAction Continue
            Write-Information "Total Moved: $($Script:Stats.MovedFiles) files" -InformationAction Continue
            Write-Information "Total Skipped: $($Script:Stats.SkippedFiles) files" -InformationAction Continue
            Write-Information "Total Errors: $($Script:Stats.ErrorFiles) files" -InformationAction Continue
            Write-Information "Total time: $($elapsed.ToString('hh\:mm\:ss'))" -InformationAction Continue
            
            Write-LogEntry "All configurations processed successfully" -Level Information
        }
    }
    catch {
        Write-LogEntry "File mover failed: $_" -Level Error
        throw
    }
}
#endregion

# Execute main function if script is run directly
if ($MyInvocation.InvocationName -ne '.') {
    Write-Host "Starting FileMover-Native.ps1..." -ForegroundColor Green
    try {
        Invoke-FileMover
    }
    catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Line: $($_.InvocationInfo.ScriptLineNumber)" -ForegroundColor Yellow
        exit 1
    }
}

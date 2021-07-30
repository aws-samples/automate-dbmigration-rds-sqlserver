param ([Parameter(Mandatory)]$servername, [Parameter(Mandatory)]$database, [Parameter(Mandatory)]$S3LSBucket,[Parameter(Mandatory)]$BkDir  )


Function Get-SQLServerPSModule {
    $PPPresent = Get-PackageProvider -Name 'Nuget' -Force -ErrorAction SilentlyContinue
    If (-not $PPPresent) {
        Write-Output 'INFO: Installing the NuGet package provider'
        Try {
            $Null = Install-PackageProvider -Name 'NuGet' -MinimumVersion '2.8.5' -Force -ErrorAction Stop
        } Catch [System.Exception] {
            Write-Output " ERROR: Failed to install NuGet package provider $_"
            Exit 1
        }
    }

    $PsRepPresent = Get-PSRepository -Name 'PSGallery' | Select-Object -ExpandProperty 'InstallationPolicy' -ErrorAction SilentlyContinue
    If ($PsRepPresent -ne 'Trusted') {
        Write-Output 'INFO: Setting PSGallery respository to trusted'
        Try {
            Set-PSRepository -Name 'PSGallery' -InstallationPolicy 'Trusted' -ErrorAction Stop
        } Catch [System.Exception] {
            Write-Output "ERROR: Failed to set PSGallery respository to trusted $_"
            Exit 1
        }
    }

    Write-Output 'INFO: Downloading and installing the SQL Server PowerShell module'
    Try {
        Install-Module 'SqlServer' -AllowClobber -Force -ErrorAction Stop
    } Catch [System.Exception] {
        Write-Output "ERROR: Failed to download and install the SQL Server PowerShell module $_"
        Exit 1
    }
}

	
#Add-Type -AssemblyName "Microsoft.SqlServer.Smo,Version=11.0.0.0,Culture=neutral,PublicKeyToken=89845dcd8080cc91"
#Add-PSSnapin *SQL*

#load assemblies
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | Out-Null
#Need SmoExtended for backup
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoExtended") | Out-Null

$ModuleVersion = Get-Module -ListAvailable -Name 'SqlServer' -ErrorAction SilentlyContinue | Select-Object -ExpandProperty 'Version' | Select-Object -ExpandProperty 'Major'
#Write-Host $ModuleVersion
If ($ModuleVersion -lt '21' -and -not ($ModuleVersion -ge '21')) {
    Write-Output 'INFO: Installing the SQL Server PowerShell Module'
    Get-SQLServerPSModule
}

$srv = New-Object Microsoft.SqlServer.Management.Smo.Server $servername

setx AWS_PROFILE default
Get-AWSCredential -ProfileName default

#pull the current date
$date = Get-Date -UFormat "%m/%d/%Y %R"
$date= $date | ForEach-Object { $_ -replace "/", "" }| ForEach-Object { $_ -replace ":", "" }| ForEach-Object { $_ -replace " ", "_" }


#set location for the backup files
$directory = $BkDir

#s3 file loc
$s3_bucket = $S3LSBucket 
$dbname = $database

$bakfile = $directory + $dbname+ "_" + $date + ".trn"
$s3_file = "s3://"+$s3_bucket+"/"+$dbname+ "_" + $date + ".trn"
#Write-Host $s3_file

#Backup Log of source DB
$backup = New-Object Microsoft.SqlServer.Management.Smo.Backup -Property @{
   Action = [Microsoft.SqlServer.Management.Smo.BackupActionType]::Log
   Database = $dbname
   MediaDescription = "Disk"
}
 
$backup.Devices.AddDevice($bakfile, 'File')
$backup.SqlBackup($srv)


#Copy to S3 
aws s3 cp "$bakfile" $s3_file

##Delete files from local folder that are older than 26 hours
Get-ChildItem $BkDir | where {$_.Lastwritetime -lt (date).addhours(-26)} | remove-item
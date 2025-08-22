# Distributed NBA Endpoint Processing - PowerShell Script
# This script runs multiple nodes in parallel for distributed processing

param(
    [switch]$DryRun,
    [string]$DatabaseConfig = "config\database_config.json"
)

$ErrorActionPreference = "Continue"

# Node configurations
$nodes = @(
    @{
        config = "config\node1_config.json"
        description = "Node 1: Game-based endpoints"
    },
    @{
        config = "config\node2_config.json" 
        description = "Node 2: Player-based endpoints"
    },
    @{
        config = "config\node3_config.json"
        description = "Node 3: Team-based endpoints"
    }
)

Write-Host "===============================================" -ForegroundColor Green
Write-Host "NBA DISTRIBUTED ENDPOINT PROCESSING" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
Write-Host ""

if ($DryRun) {
    Write-Host "[DRY RUN MODE] - Showing what would be executed" -ForegroundColor Yellow
    Write-Host ""
}

$jobs = @()

foreach ($node in $nodes) {
    Write-Host "Starting: $($node.description)" -ForegroundColor Cyan
    
    $scriptBlock = {
        param($configFile, $dbConfigFile, $isDryRun)
        
        Set-Location $using:PWD
        
        $args = @("scripts\distributed_runner.py", "--config", $configFile)
        
        if ($dbConfigFile) {
            $args += "--db-config", $dbConfigFile
        }
        
        if ($isDryRun) {
            $args += "--dry-run"
        }
        
        & python @args
    }
    
    if ($DryRun) {
        # Run synchronously for dry run
        Write-Host "  DRY RUN: python scripts\distributed_runner.py --config $($node.config) --db-config $DatabaseConfig --dry-run"
    } else {
        # Start as background job for parallel execution
        $job = Start-Job -ScriptBlock $scriptBlock -ArgumentList $node.config, $DatabaseConfig, $false
        $jobs += @{
            Job = $job
            Config = $node.config
            Description = $node.description
        }
        Write-Host "  Started job ID: $($job.Id)" -ForegroundColor Green
    }
}

if (-not $DryRun -and $jobs.Count -gt 0) {
    Write-Host ""
    Write-Host "All nodes started. Monitoring progress..." -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop all jobs" -ForegroundColor Yellow
    Write-Host ""
    
    try {
        # Monitor jobs
        while ($true) {
            $runningJobs = $jobs | Where-Object { $_.Job.State -eq "Running" }
            $completedJobs = $jobs | Where-Object { $_.Job.State -eq "Completed" }
            $failedJobs = $jobs | Where-Object { $_.Job.State -eq "Failed" }
            
            if ($runningJobs.Count -eq 0) {
                break
            }
            
            Write-Host "Status: Running: $($runningJobs.Count), Completed: $($completedJobs.Count), Failed: $($failedJobs.Count)" -ForegroundColor Cyan
            Start-Sleep -Seconds 30
        }
        
        # Show final results
        Write-Host ""
        Write-Host "===============================================" -ForegroundColor Green
        Write-Host "FINAL RESULTS" -ForegroundColor Green
        Write-Host "===============================================" -ForegroundColor Green
        
        foreach ($jobInfo in $jobs) {
            $status = switch ($jobInfo.Job.State) {
                "Completed" { "[SUCCESS]"; break }
                "Failed" { "[FAILED]"; break }
                default { "[UNKNOWN]"; break }
            }
            
            Write-Host "$status $($jobInfo.Description)" -ForegroundColor $(if ($jobInfo.Job.State -eq "Completed") { "Green" } else { "Red" })
            
            # Show job output
            $output = Receive-Job -Job $jobInfo.Job
            if ($output) {
                Write-Host "  Output: $($output[-1])" -ForegroundColor Gray
            }
        }
        
    } catch {
        Write-Host "Interrupted by user. Stopping all jobs..." -ForegroundColor Yellow
        $jobs | ForEach-Object { Stop-Job -Job $_.Job; Remove-Job -Job $_.Job }
    }
    
    # Clean up jobs
    $jobs | ForEach-Object { Remove-Job -Job $_.Job }
}

Write-Host ""
Write-Host "Distributed processing complete!" -ForegroundColor Green

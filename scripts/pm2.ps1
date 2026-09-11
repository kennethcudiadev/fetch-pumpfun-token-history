#Requires -Version 5.1
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

New-Item -ItemType Directory -Force -Path logs, data/.state | Out-Null

if (-not (Get-Command pm2 -ErrorAction SilentlyContinue)) {
    Write-Error "PM2 not found. Install: npm install -g pm2"
}

switch ($args[0]) {
    "scan" {
        pm2 start ecosystem.config.cjs --only pump-scan
    }
    "fetch" {
        pm2 start ecosystem.config.cjs --only pump-fetch
    }
    "stop" {
        pm2 stop pump-scan, pump-fetch 2>$null
    }
    "logs" {
        if ($args[1]) { pm2 logs $args[1] } else { pm2 logs }
    }
    "status" {
        pm2 status
    }
    default {
        Write-Host "Usage: .\scripts\pm2.ps1 {scan|fetch|stop|logs|status}"
        Write-Host "Settings: config.json (target_wallet, lookback_hours, helius_keys)"
        exit 1
    }
}

# Optional Windows helper for environments where only PowerShell can reach HTTPS.
# Downloads the official Go proxy artifacts into Go's normal module cache.
# Go still checks the downloaded modules against the project's go.sum.
param([string]$ModuleFile = 'go.mod', [string[]]$Extra = @())
$ErrorActionPreference = 'Stop'
$cache = Join-Path (go env GOMODCACHE) 'cache/download'
$seen = @{}
function Cache-Module([string]$name, [string]$version) {
    $name = $name.Trim('"')
    $version = $version.Trim('"')
    $key = "$name@$version"
    if ($seen.ContainsKey($key)) { return }
    $seen[$key] = $true
    $escaped = [regex]::Replace($name, '[A-Z]', { param($m) '!' + $m.Value.ToLowerInvariant() })
    $folder = Join-Path $cache "$escaped/@v"
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    foreach ($ext in @('mod', 'info', 'zip')) {
        $dest = Join-Path $folder "$version.$ext"
        if (!(Test-Path $dest)) {
            Write-Host "Caching $key ($ext)"
            Invoke-WebRequest "https://proxy.golang.org/$escaped/@v/$version.$ext" -OutFile $dest
        }
    }
}
foreach ($line in (Get-Content $ModuleFile)) {
    if ($line -match '^\s*(?:require\s+)?([^\s]+)\s+(v[^\s]+)') {
        Cache-Module $Matches[1] $Matches[2]
    }
}
foreach ($entry in $Extra) {
    $parts = $entry.Split('@')
    Cache-Module $parts[0] $parts[1]
}

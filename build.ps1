param(
  [string]$OS = "windows",
  [string]$Arch = "amd64"
)
$ErrorActionPreference = "Stop"
$outName = if ($OS -eq "windows") { "bin/build-to-deploy.exe" } else { "bin/build-to-deploy-$OS-$Arch" }
$env:GOOS = $OS
$env:GOARCH = $Arch
$env:CGO_ENABLED = "0"

Write-Host "Building for $OS/$Arch -> $outName..."
go build -trimpath -ldflags "-s -w" -o $outName ./cmd/agent
Write-Host "Build finished successfully: $outName"

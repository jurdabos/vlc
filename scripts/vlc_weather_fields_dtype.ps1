# Base URLs
$BASE  = 'https://valencia.opendatasoft.com/api/explore/v2.1'
$BASE2 = 'https://valencia.opendatasoft.com/api/v2'

function Get-OdsMeta {
  param([Parameter(Mandatory)][string]$DatasetId)
  # Try v2.1; if it fails or has no dataset, try v2 (older but sturdy)
  try {
    $r = irm "$BASE/catalog/datasets/$DatasetId"
    if ($r.dataset) { return $r }
  } catch {}
  irm "$BASE2/catalog/datasets/$DatasetId"
}

function Get-OdsFields {
  param([Parameter(Mandatory)][string]$DatasetId)
  $meta = Get-OdsMeta $DatasetId
  $fields = $meta.dataset.fields
  if ($fields) { return $fields | Select-Object name,type }

  # Fallback: infer from one record’s keys (works on any dataset)
  $rec = (irm "$BASE/catalog/datasets/$DatasetId/records?limit=1").results | Select-Object -First 1
  if ($rec) {
    $rec.PSObject.Properties |
      Select-Object @{n='name';e={$_.Name}}, @{n='type';e={'(from sample)'}}
  }
}

function Get-OdsCount {
  param([Parameter(Mandatory)][string]$DatasetId)
  # Use count(*) and read the single aggregate row
  (irm "$BASE/catalog/datasets/$DatasetId/records?select=count(*)%20as%20n&limit=1").results[0].n
}

function Get-OdsLatest {
  param([Parameter(Mandatory)][string]$DatasetId)
  $fields = (Get-OdsMeta $DatasetId).dataset.fields
  $dateField = ($fields | Where-Object { $_.type -match 'date' }).name | Select-Object -First 1
  if ($dateField) {
    return (irm "$BASE/catalog/datasets/$DatasetId/records?select=max($dateField)%20as%20last_ts&limit=1").results[0].last_ts
  }
  # Fallback for ISO-ish text timestamps
  $candidates = @('update_jcd','timestamp','fechahora','fecha','updated_at','date','data','last_update')
  $avail = $fields.name
  $cand = ($candidates | Where-Object { $_ -in $avail } | Select-Object -First 1)
  if ($cand) {
    return (irm "$BASE/catalog/datasets/$DatasetId/records?order_by=-$cand&limit=1").results[0].$cand
  }
  $null
}

# Meteo stations — fields plus dtype
$id = 'estacions-atmosferiques-estaciones-atmosfericas'
Get-OdsFields $id | ft -a

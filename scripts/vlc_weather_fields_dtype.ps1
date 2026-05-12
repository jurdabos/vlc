# Lists the (name, type) pairs and a few useful aggregates for the weather
# layer on the Valencia geoportal ArcGIS REST endpoint (default: layer 157).
$BASE     = if ($env:VLC_ARCGIS_BASE) { $env:VLC_ARCGIS_BASE } else { 'https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer' }
$LAYER_ID = if ($env:VLC_LAYER_ID) { [int]$env:VLC_LAYER_ID } else { 157 }

function Get-ArcGisLayerMeta {
  param([Parameter(Mandatory)][string]$Base, [Parameter(Mandatory)][int]$LayerId)
  $url = "$($Base.TrimEnd('/'))/$LayerId`?f=json"
  irm $url
}

function Get-ArcGisFields {
  param([Parameter(Mandatory)][string]$Base, [Parameter(Mandatory)][int]$LayerId)
  $meta = Get-ArcGisLayerMeta -Base $Base -LayerId $LayerId
  if ($meta.fields) { return $meta.fields | Select-Object name,type,alias }
  $url = "$($Base.TrimEnd('/'))/$LayerId/query`?where=1=1&outFields=*&returnGeometry=false&resultRecordCount=1&f=json"
  $rec = (irm $url).features | Select-Object -First 1
  if ($rec -and $rec.attributes) {
    $rec.attributes.PSObject.Properties |
      Select-Object @{n='name';e={$_.Name}}, @{n='type';e={'(from sample)'}}, @{n='alias';e={''}}
  }
}

function Get-ArcGisCount {
  param([Parameter(Mandatory)][string]$Base, [Parameter(Mandatory)][int]$LayerId)
  $url = "$($Base.TrimEnd('/'))/$LayerId/query`?where=1=1&returnCountOnly=true&f=json"
  (irm $url).count
}

function Get-ArcGisLatest {
  param([Parameter(Mandatory)][string]$Base, [Parameter(Mandatory)][int]$LayerId)
  $fields = (Get-ArcGisLayerMeta -Base $Base -LayerId $LayerId).fields
  $dateField = ($fields | Where-Object { $_.type -eq 'esriFieldTypeDate' } | Select-Object -First 1).name
  if (-not $dateField) {
    $candidates = @('fecha_carg','update_jcd','timestamp','fechahora','fecha','updated_at','date','data','last_update')
    $dateField = ($candidates | Where-Object { $_ -in $fields.name } | Select-Object -First 1)
  }
  if ($dateField) {
    $url = "$($Base.TrimEnd('/'))/$LayerId/query`?where=1=1&outStatistics=" + `
           [uri]::EscapeDataString('[{"statisticType":"max","onStatisticField":"' + $dateField + '","outStatisticFieldName":"last_ts"}]') + `
           '&f=json'
    $resp = irm $url
    $epoch_ms = $resp.features[0].attributes.last_ts
    if ($epoch_ms) {
      [datetimeoffset]::FromUnixTimeMilliseconds([int64]$epoch_ms).ToString('yyyy-MM-ddTHH:mm:ssZ')
    }
  }
}

# Meteo stations -- list fields + dtype
Get-ArcGisFields -Base $BASE -LayerId $LAYER_ID | ft -a

import requests
import json
url = 'https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records'
params = {'limit': '100'}
response = requests.get(url, params=params)
data = response.json()
pollutants = ['so2', 'no2', 'o3', 'co', 'pm10', 'pm25']
print('\\nSUMMARY: Which stations report which pollutants')
print('=' * 80)
print(f'{'Station':<30} SO2  NO2  O3   CO   PM10 PM2.5')
print('-' * 80)
for record in sorted(data['results'], key=lambda x: x['objectid']):
    name = record['nombre'][:28]
    row = f'{name:<30}'
    for pollutant in pollutants:
        value = record.get(pollutant)
        if value is not None:
            row += ' ✓   '
        else:
            row += ' -   '
    print(row)
print('\\n' + '=' * 80)
print('\\nPollutant coverage:')
for pollutant in pollutants:
    count = sum(1 for r in data['results'] if r.get(pollutant) is not None)
    pct = (count / len(data['results'])) * 100
    print(f'  {pollutant.upper():<6}: {count:2}/11 stations ({pct:5.1f}%)')
print('\\nKey insights:')
print('• ALL 11 stations measure NO2 (nitrogen dioxide)')
print('• 8 stations measure PM10 and PM2.5 (particulate matter)')
print('• 6 stations measure O3 (ozone) and SO2 (sulfur dioxide)')
print('• Only 3 stations measure CO (carbon monoxide)')
print('• NO weather/meteorological data (temp, humidity, pressure, wind) available')
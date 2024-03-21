import re
import json

from mrjob.job import MRJob

class MRWeatherReport(MRJob):
    
    def mapper(self, _, line):
        # Extracting the three things - wind direction, temperature, and quality
        val = line.strip()
        wind_direction = val[60:63]
        temperature = float(val[87:92])
        temperature_quality = val[63:64]
        
        # Filtering out unknown values and invalid wind direction
        if (temperature != '+9999' and temperature_quality in ['0', '1', '4', '5', '9'] and wind_direction != '999'):
            yield wind_direction, (temperature, temperature_quality)
    
    def reducer(self, key, values):
        min_temp = float('inf')
        max_temp = float('-inf')
        count = 0
        
        for temp, _ in values:
            min_temp = min(min_temp, temp)
            max_temp = max(max_temp, temp)
            count += 1
        
        yield int(key), {'low': int(min_temp), 'high': int(max_temp), 'count': count}

if __name__ == '__main__':
    MRWeatherReport.run()

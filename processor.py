from database import (insert_tides, insert_water_quality, insert_meteorological, insert_waves,
                      insert_ctd, insert_air_quality, insert_currents)

class DataProcessor:
    """Processa e armazena dados de weather em banco de dados estruturado"""
    
    def __init__(self, db_path="weather.db"):
        self.db_path = db_path
        self.insert_functions = {
            "tides": insert_tides,
            "ctd": insert_ctd,
            "wq": insert_water_quality,
            "water_quality": insert_water_quality,
            "air_quality": insert_air_quality,
            "met": insert_meteorological,
            "meteorological": insert_meteorological,
            "waves": insert_waves,
            "currents": insert_currents,
        }

    def process_dataset(self, table_name, csv_chunks):
        """Processa múltiplos chunks de CSV
        
        Args:
            table_name: Nome da tabela
            csv_chunks: Lista de tuplas (csv_data, status_code, period_start)
        
        Returns:
            (total_inseridos, erros_count)
        """
        if table_name not in self.insert_functions:
            return 0, len(csv_chunks)
        
        insert_func = self.insert_functions[table_name]
        total = 0
        errors = 0
        
        for csv_data, status_code, period_start in csv_chunks:
            if not csv_data:
                if status_code != 204:
                    errors += 1
                continue
            
            try:
                inserted = insert_func(csv_data, self.db_path)
                total += inserted
            except Exception as e:
                errors += 1
        
        return total, errors
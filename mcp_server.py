from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime

app = Flask(__name__)
CSV_FILE = 'ejecuciones_pipelines.csv'

class McpServer:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def load_data(self):
        return pd.read_csv(self.csv_path, parse_dates=['start_time', 'end_time'])

    def get_execution_status(self, region, pipeline, dia):
        df = self.load_data()
        df['date'] = df['start_time'].dt.date
        dia_dt = datetime.strptime(dia, '%Y-%m-%d').date()
        return df[(df['region'] == region) & 
                  (df['pipeline_name'] == pipeline) & 
                  (df['date'] == dia_dt)]

    def get_statistics(self, region, pipeline, dia_inicio, dia_fin):
        df = self.load_data()
        df['date'] = df['start_time'].dt.date
        df['duration'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60
        return df[(df['region'] == region) &
                  (df['pipeline_name'] == pipeline) &
                  (df['date'] >= dia_inicio) &
                  (df['date'] <= dia_fin)]

    def get_update_level(self, region, pipeline, dia):
        df = self.load_data()
        df['date'] = df['start_time'].dt.date
        return df[(df['region'] == region) & (df['date'] == dia)]

mcp = McpServer(CSV_FILE)

# ---------------------
# Endpoint de descubrimiento de tools
# ---------------------
@app.route('/mcp-tools', methods=['GET'])
def mcp_tools():
    return jsonify([
        {
            "name": "estado_ejecucion",
            "description": "Obtiene el estado de ejecución de un pipeline",
            "method": "GET",
            "endpoint": "/estado-ejecucion",
            "inputs": [
                {"name": "region", "type": "string"},
                {"name": "pipeline", "type": "string"},
                {"name": "dia", "type": "string"}
            ],
            "outputs": [
                {"name": "Estado de ejecución", "type": "string"},
                {"name": "Log", "type": "string"}
            ]
        },
        {
            "name": "estadisticas_ejecucion",
            "description": "Obtiene estadísticas de ejecuciones de pipelines",
            "method": "GET",
            "endpoint": "/estadisticas-ejecucion",
            "inputs": [
                {"name": "region", "type": "string"},
                {"name": "pipeline", "type": "string"},
                {"name": "dia_inicio", "type": "string"},
                {"name": "dia_fin", "type": "string"}
            ],
            "outputs": [
                {"name": "Duración media", "type": "number"},
                {"name": "Duración máxima", "type": "number"},
                {"name": "Duración mínima", "type": "number"},
                {"name": "Por estado", "type": "object"}
            ]
        },
        {
            "name": "nivel_actualizacion",
            "description": "Devuelve el nivel de actualización de un pipeline según sus dependencias",
            "method": "GET",
            "endpoint": "/nivel-actualizacion",
            "inputs": [
                {"name": "region", "type": "string"},
                {"name": "pipeline", "type": "string"},
                {"name": "dia", "type": "string"},
                {"name": "dependencias_pipeline", "type": "array"}
            ],
            "outputs": [
                {"name": "Situación", "type": "array"}
            ]
        }
    ])

# ---------------------
# Endpoints reales
# ---------------------
@app.route('/estado-ejecucion', methods=['GET'])
def estado_ejecucion():
    region = request.args.get('region')
    pipeline = request.args.get('pipeline')
    dia = request.args.get('dia')
    result = mcp.get_execution_status(region, pipeline, dia)
    if result.empty:
        return jsonify({'error': 'No se encontró ejecución'}), 404
    row = result.iloc[0]
    return jsonify({
        'Region': row['region'],
        'Pipeline': row['pipeline_name'],
        'Dia': dia,
        'Estado de ejecución': str(row['run_status']),
        'Log': f"Inicio: {row['start_time']}, Fin: {row['end_time']}"
    })

@app.route('/estadisticas-ejecucion', methods=['GET'])
def estadisticas_ejecucion():
    region = request.args.get('region')
    pipeline = request.args.get('pipeline')
    dia_inicio = datetime.strptime(request.args.get('dia_inicio'), '%Y-%m-%d').date()
    dia_fin = datetime.strptime(request.args.get('dia_fin'), '%Y-%m-%d').date()
    df_filtered = mcp.get_statistics(region, pipeline, dia_inicio, dia_fin)
    if df_filtered.empty:
        return jsonify({'error': 'No se encontraron ejecuciones'}), 404
    stats = {
        'Region': region,
        'Pipeline': pipeline,
        'Duración media': df_filtered['duration'].mean(),
        'Duración máxima': df_filtered['duration'].max(),
        'Duración mínima': df_filtered['duration'].min(),
        'Mínima hora de inicio': df_filtered['start_time'].min().strftime('%H:%M'),
        'Máxima hora de inicio': df_filtered['start_time'].max().strftime('%H:%M'),
        'Mínima hora de fin': df_filtered['end_time'].min().strftime('%H:%M'),
        'Máxima hora de fin': df_filtered['end_time'].max().strftime('%H:%M'),
        'Por estado': {}
    }
    for status, group in df_filtered.groupby('run_status'):
        stats['Por estado'][status] = {
            'Duración media': group['duration'].mean(),
            'Duración máxima': group['duration'].max(),
            'Duración mínima': group['duration'].min(),
            'Mínima hora de inicio': group['start_time'].min().strftime('%H:%M'),
            'Máxima hora de inicio': group['start_time'].max().strftime('%H:%M'),
            'Mínima hora de fin': group['end_time'].min().strftime('%H:%M'),
            'Máxima hora de fin': group['end_time'].max().strftime('%H:%M')
        }
    return jsonify(stats)

@app.route('/nivel-actualizacion', methods=['GET'])
def nivel_actualizacion():
    region = request.args.get('region')
    pipeline = request.args.get('pipeline')
    dia = datetime.strptime(request.args.get('dia'), '%Y-%m-%d').date()
    dependencias = request.args.getlist('dependencias_pipeline')
    df = mcp.load_data()
    df['date'] = df['start_time'].dt.date

    df_filtered = df[(df['region'] == region) & (df['date'] == dia)]
    pipeline_row = df_filtered[df_filtered['pipeline_name'] == pipeline]
    if pipeline_row.empty:
        return jsonify({'error': 'No se encontró ejecución del pipeline'}), 404
    pipeline_start = pipeline_row.iloc[0]['start_time']

    resultados = []
    for dep in dependencias:
        dep_row = df_filtered[df_filtered['pipeline_name'] == dep]
        if dep_row.empty:
            situacion = 2
        else:
            dep_status = dep_row.iloc[0]['run_status']
            dep_end = dep_row.iloc[0]['end_time']
            if dep_status == 0:
                situacion = 0
            elif dep_status in [2, 3, 4]:
                situacion = 2
            elif dep_status == 1:
                if dep_end <= pipeline_start:
                    situacion = 1
                else:
                    situacion = 3
            else:
                situacion = 2
        resultados.append({
            'Region': region,
            'Pipeline': pipeline,
            'Pipeline del que depende': dep,
            'Situación': situacion
        })
    return jsonify(resultados)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
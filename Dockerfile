# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY mcp_server.py .
COPY ejecuciones_pipelines.csv .

# Expose port
EXPOSE 5000

# Run the server
CMD ["python", "mcp_server.py"]
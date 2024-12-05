FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y p7zip-full libmagic1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
# Create application directories
RUN mkdir -p /ToProcess /Processed /Failed
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

CMD ["python", "/app/monitor.py"]

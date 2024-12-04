FROM python:3.10-slim

RUN apt-get update && apt-get install -y p7zip-full && apt-get clean
# Create application directories
RUN mkdir -p /ToProcess /Processed /Failed
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

CMD ["python", "/app/monitor.py"]

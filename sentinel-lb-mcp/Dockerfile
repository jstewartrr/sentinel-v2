FROM python:3.11-slim

WORKDIR /app

# Install Azure CLI for container management
RUN apt-get update && apt-get install -y curl gnupg lsb-release && \
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY sentinel.py .

# Run sentinel
CMD ["python", "-u", "sentinel.py"]

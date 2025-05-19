
# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Load environment variables from .env file if needed
# (Only if you COPY it above and intend to run locally)
ENV PYTHONUNBUFFERED=1

# Entry point: run your main script
CMD ["python", "fins_all.py"]

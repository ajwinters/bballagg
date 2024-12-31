FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy the script and install dependencies
COPY games_process.py /app/
COPY allintwo.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the script
CMD ["python", "games_process.py"]

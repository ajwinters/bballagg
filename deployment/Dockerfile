FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy the script and install dependencies
COPY ../src/games_process.py /app/
COPY ../src/allintwo.py /app/
COPY ../config/requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the script
CMD ["python", "games_process.py"]

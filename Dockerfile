# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt to the container
COPY requirements.txt .

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Make port 80 available to the world outside this container
EXPOSE 29092

# Run app.py when the container launches
ENTRYPOINT [ "./entrypoint.sh" ]

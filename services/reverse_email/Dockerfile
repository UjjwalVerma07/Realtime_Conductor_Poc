# Use the official Python image as a parent image
FROM python:3.9-slim

#Set work directory
WORKDIR /app

#Copy the requirements.txt file into the container
COPY requirements.txt .

#Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt


#Copy the app code
COPY . .

#Expose the port the app runs on 
EXPOSE 9000

#Start the service with the unicorn 
CMD ["uvicorn", "service:service", "--host", "0.0.0.0", "--port", "9000"]
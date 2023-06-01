FROM python:3.9
RUN mkdir -p /home/app
COPY . /home/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer.py .
CMD ["python", "consumer.py"]
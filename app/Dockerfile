FROM python:3.8.10

WORKDIR /app

RUN pip install pandas

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY Odata2020File.csv /app/Odata2020File.csv
COPY Odata2021File.csv /app/Odata2021File.csv

COPY main.py /app/main.py

CMD ["python", "./main.py"]
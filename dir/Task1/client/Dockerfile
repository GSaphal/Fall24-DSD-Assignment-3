FROM python:3.10-slim

WORKDIR /app

COPY client.py /app/client.py
COPY saphal.txt /app/saphal.txt

EXPOSE 65432

ENTRYPOINT ["python3", "client.py"]



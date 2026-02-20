FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

EXPOSE 5001

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "5001", "src.main:app"]
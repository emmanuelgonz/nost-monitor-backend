FROM python:3
WORKDIR /var/tatc
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade -r requirements.txt
COPY src/app app

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000", "--proxy-headers"]
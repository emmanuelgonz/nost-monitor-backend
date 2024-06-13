FROM python:3
WORKDIR /var/tatc
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade -r requirements.txt
COPY src/app app

CMD ["fastapi", "run", "app/main.py", "--proxy-headers", "--port", "3000"]
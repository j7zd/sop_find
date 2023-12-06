FROM python:3.11

WORKDIR /py_db/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "main.py" ]
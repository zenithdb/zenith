FROM python:3.12@sha256:5893362478144406ee0771bd9c38081a185077fb317ba71d01b7567678a89708
WORKDIR /source

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD ["python3", "asyncpg_example.py"]

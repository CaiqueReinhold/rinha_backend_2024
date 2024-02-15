FROM python:3.10.13-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1

RUN useradd -m myuser
USER myuser
WORKDIR /home/myuser/app

COPY --chown=myuser:myuser ./src /home/myuser/app/

RUN pip install --no-cache-dir asyncpg starlette uvicorn[standard] orjson

ENV PATH="/home/myuser/.local/bin:${PATH}"
ENV PYTHONPATH="/home/myuser/.local/lib/python3.10/site-packages:${PYTHONPATH}"

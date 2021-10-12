#FROM python:3
FROM public.registry.u-hopper.com/python:3.8

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "./run.py"]
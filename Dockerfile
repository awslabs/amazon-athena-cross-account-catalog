FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt && rm requirements.txt

COPY heracles ./heracles

CMD [ "heracles.lambda.handler" ]

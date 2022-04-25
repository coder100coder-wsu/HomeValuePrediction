FROM python:3.10

ADD requirements.txt /

RUN pip install -r /requirements.txt
# model deployment workflow
# model deployment is productionized, so expected to be scaled. 
# So use of Docker warranted.
ADD mongodb_&_kafka_&_predictions_5.py /

# model development is NOT expected to be scaled.
# model development workflow
# ADD houses_3.py /

ENV PYTHONUNBUFFERED=1

CMD [ "python", "./mongodb_&_kafka_&_predictions_5.py" ]
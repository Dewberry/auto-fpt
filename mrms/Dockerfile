FROM public.ecr.aws/lambda/python:3.13@sha256:31814562fc6d222dd84bf659ae2f0a1bb5ac17b7d217e9c734c7612e326e7559

# Copy dependency file and install into the Lambda Python environment
COPY mrms/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy script and shared module into the Lambda task root
COPY mrms/mrms_latest.py ${LAMBDA_TASK_ROOT}/
COPY shared/ ${LAMBDA_TASK_ROOT}/shared/

# Set the Lambda handler
CMD ["mrms_latest.handler"]

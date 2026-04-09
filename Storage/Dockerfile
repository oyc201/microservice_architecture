FROM python:3.11
LABEL maintainer="joh76@my.bcit.ca"
RUN mkdir /app
# We copy just the requirements.txt first to leverage Docker cache
# on `pip install`
COPY ./requirements.txt /app/requirements.txt
# Set the working directory
WORKDIR /app
# Install dependencies
RUN pip3 install -r requirements.txt
# Copy the source code
COPY . /app
# Change permissions and become a non-privileged user
RUN chown -R nobody:nogroup /app
USER nobody
# Tells on which port the service listens in the container
EXPOSE 8090
# # Entrypoint = run Python
# ENTRYPOINT [ "python3" ]

COPY --chmod=755 wait.sh /wait.sh

# Default = run app.py
CMD [ "python3", "app.py" ]
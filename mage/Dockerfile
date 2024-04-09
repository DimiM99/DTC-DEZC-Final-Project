FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

# Note: this overwrites the requirements.txt file in your new project on first run. 
# You can delete this line for the second run :) 
COPY requirements.txt ${USER_CODE_PATH}requirements.txt 

RUN pip3 install -r ${USER_CODE_PATH}requirements.txt

RUN apt-get update \
    && apt-get install -y wget firefox-esr \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get purge -y --auto-remove wget

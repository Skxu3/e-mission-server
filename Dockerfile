# SCONE
FROM sconecuratedimages/apps:python-3.5-alpine as scone

# install general dependencies
RUN apk --update add --no-cache \ 
    lapack-dev \ 
    gcc \
    freetype-dev \
    build-base \
	bats \
	libbsd \
	openssl

RUN apk add python py-pip python-dev 

# install build dependencies
RUN apk add --no-cache --virtual .build-deps \
    gfortran \
    musl-dev \
    g++ \
    libffi-dev \
    libxml2-dev \
	libxslt-dev \
	openssl-dev \
	libpng-dev
RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

# create /test subdir
RUN mkdir /test

# clone from repo
COPY . /test

# set working directory
WORKDIR /test

# give permission to run script to install requirements
RUN chmod +x ./setup/requirements.sh
RUN ./setup/requirements.sh

# install seaborn, scikit-learn, statsmodels
RUN SCONE_HEAP=256M SCONE_VERSION=1 SCONE_MODE=AUTO && pip install seaborn==0.8
RUN SCONE_HEAP=256M SCONE_VERSION=1 SCONE_MODE=AUTO && pip install scikit-learn==0.19.0
RUN SCONE_HEAP=256M SCONE_VERSION=1 SCONE_MODE=AUTO && pip install statsmodels==0.8.0

# remove build dependencies
RUN apk del .build-deps

# expose port 8080
EXPOSE 8080

# set environment variables for simulation mode
CMD SCONE_HEAP=256M SCONE_VERSION=1 SCONE_MODE=AUTO

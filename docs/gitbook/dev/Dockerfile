FROM node:8

RUN npm install -g gitbook-cli

# init gitbook
RUN mkdir /tmp/test
RUN cd /tmp/test && gitbook init
RUN rm -r /tmp/test

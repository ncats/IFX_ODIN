FROM memgraph/memgraph-mage:latest

USER root

# Install pip if it's not already installed
RUN apt-get update && \
    apt-get install -y python3-pip

# Install pymongo with override for PEP 668
RUN pip3 install pymongo --break-system-packages

USER memgraph

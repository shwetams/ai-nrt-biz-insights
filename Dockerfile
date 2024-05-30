FROM hilopracr.azurecr.io/hadoop:3.3.1-0.1.2023070304
RUN useradd -m test
USER test
CMD ["/bin/bash"]


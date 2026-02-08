FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-postgres~=1.9.0

WORKDIR /dbt
COPY . /dbt/

# Installing dbt packages (dbt-utils) at build time.
RUN dbt deps --profiles-dir /dbt

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/dbt"]

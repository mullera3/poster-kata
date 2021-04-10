FROM postgres:latest

ENV POSTGRES_PASSWORD = postgres
ENV POSTGRES_USER: postgres
EXPOSE 5432

CMD [ "postgres" ]
# Read the environment variables
source .env


# 
# Create a network
docker network create pg-network


# 
# Run Postgres (change the path)
docker run -itd \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ${PWD}/data/db:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name postgres \
  postgres:latest


# 
# Run pgAdmin
docker run -itd \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v ${PWD}/data/pgadmin-data:/var/lib/pgadmin \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4


# 
# Create the pipeline image
docker build -t elt:v001 .

# 
# Run the ELT pipeline
docker run -it --network=pg-network elt:v001 -t=green -y=2019 -m=9

# OR

docker run -it \
  --network=pg-network \
  elt:v001 \
    --taxi_service=yellow \
    --year=2019 \
    --month=9


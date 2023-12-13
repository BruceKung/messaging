
echo "Running media driver, data stored in ${PWD}/data/aeron/"

mkdir -p data/
docker compose up -d aeron_media_driver
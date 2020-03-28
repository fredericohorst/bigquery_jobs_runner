
# eval $(docker-machine env default) # if mac
echo starting build...
docker build -t alcaline .
echo builded successfully!
echo starting services from composer...
docker-compose up
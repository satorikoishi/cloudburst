cd dockerfiles
docker build . -f cloudburst.dockerfile -t jwkaguya/cloudburst
cd ..
docker push jwkaguya/cloudburst
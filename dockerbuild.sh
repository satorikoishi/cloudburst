# WARNING: should not update image, just push code is ok.
cd dockerfiles
docker build --build-arg HTTP_PROXY=$HTTP_PROXY -f cloudburst.dockerfile -t jwkaguya/cloudburst .
cd ..
# docker push jwkaguya/cloudburst
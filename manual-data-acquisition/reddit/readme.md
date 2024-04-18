### Reddit Scraper, manual execution via docker container

build the image with
```bash
docker build . -t reddit_data_fetcher
```
and run it, sometimes it fails for no reason, just run it again 
```bash
docker run -d -v $PWD/../../data/:/usr/src/app/output/ reddit_data_fetcher
```

after execution the container will spit out the output into\
data-dumps folder, the file will be named `reddit_data.json`
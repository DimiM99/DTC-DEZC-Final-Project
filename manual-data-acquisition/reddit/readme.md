### Reddit Scraper, manual execution via docker container

build the image with
```bash
docker build . -t reddit-scraper
```
and run it, sometimes it fails for no reason, just run it again 
```bash
docker run -d -v "$(pwd)"/../data-dumps:/usr/src/app/output/ reddit-scraper
```

after execution the container will spit out the output into\
data-dumps folder, the file will be named `reddit_data.json`
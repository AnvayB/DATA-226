docker vespa:
	docker run -d --name vespa-container -v "/Users/anvaybhanap/Desktop/DATA 226/sjsu-data226/week11/app:/app" -v "/Users/anvaybhanap/Desktop/DATA 226/sjsu-data226/week11/clean_tmdb.jsonl:/clean_tmdb.jsonl" --publish 8080:8080 --publish 19071:19071 vespaengine/vespa

install:
	pip3 install --ignore-installed vespacli

config:
	vespa config set target local

deploy:
	vespa status deploy --wait 300

run:
	vespa deploy app

feed:
	vespa feed -t http://localhost:8080 clean_tmdb.jsonl


docker run -d --name vespa-container -v "/Users/anvaybhanap/Desktop/DATA 226/sjsu-data226/week11/app:/app" -v "/Users/anvaybhanap/Desktop/DATA 226/sjsu-data226/week11/clean_tmdb.jsonl:/clean_tmdb.jsonl" --publish 8080:8080 --publish 19071:19071 vespaengine/vespa

docker exec -it 2d682e6b86ef curl http://localhost:8080/status.html

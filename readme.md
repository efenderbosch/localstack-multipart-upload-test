start localstack w/ "docker-compose -f docker-compose.yml up"

run test w/ "./gradlew check"

localstack test will fail, S3 test will pass

kill docker-compose

change localstack image in docker-compose.yml from latest to 0.12.7

start localstack w/ "docker-compose -f docker-compose.yml up"

run test w/ "./gradlew check"

localstack test will pass, S3 test will pass

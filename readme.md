* create bucket in S3 named 'test-bucket'
* start localstack w/ "docker-compose -f docker-compose.yml up"
* run test w/ "./gradlew check"
* localstack test will fail due to signature validation, S3 test will pass
* kill docker-compose
* change localstack image in docker-compose.yml from latest to 0.12.7
* start localstack w/ "docker-compose -f docker-compose.yml up"
* run test w/ "./gradlew check"
* localstack test will fail due to missing tag, S3 test will pass

# Creating test_foxecast_inserter container
docker build --target tests -t test_foxecast_inserter:1.0.0 .

# Creating foxecast_inserter container
docker build --target runtime -t foxecast_inserter:1.0.0 .
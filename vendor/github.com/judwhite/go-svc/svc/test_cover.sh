if [ ! -f cover.out ]; then
  echo "Running tests..."
  go test -timeout 3m -coverprofile cover.out
  if [ $? -ne 0 ]; then
    exit 255
  fi
fi

go tool cover -html=cover.out

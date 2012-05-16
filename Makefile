
TESTS = test/*.test.coffee
REPORTER = dot

build:
	@cake build
test:
	@NODE_ENV=test ./node_modules/.bin/mocha  --compilers coffee:coffee-script \
		--ui exports \
		--reporter $(REPORTER) \
		--timeout 0 \
		$(TESTS)
.PHONY: test build

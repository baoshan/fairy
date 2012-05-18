TESTS = test/*.test.coffee
REPORTER = spec

build:
	@NODE_ENV=build mkdir -p lib/server
	@cp src/server/fairy.html lib/server/fairy.html
	@cp src/server/fairy.css  lib/server/fairy.css
	@cp src/server/fairy_active.js lib/server/fairy_active.js
	@coffee -c$(opt) -o lib src  
test:
	@NODE_ENV=test ./node_modules/.bin/mocha  --compilers coffee:coffee-script \
		--ui exports \
		--reporter $(REPORTER) \
		--timeout 0 \
		--slow 50000 \
		--ui bdd \
		$(TESTS)
.PHONY: test build

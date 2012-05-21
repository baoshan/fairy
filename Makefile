TESTS = test/*.test.coffee
REPORTER = spec

build:
	@NODE_ENV=build mkdir -p lib/web
	@cp src/web/fairy.html lib/web/fairy.html
	@cp src/web/fairy.css  lib/web/fairy.css
	@cp src/web/fairy_active.js lib/web/fairy_active.js
	@coffee -c$(opt) -o lib src  
test:
	@rm -f test/workers/*.dmp
	@NODE_ENV=test ./node_modules/.bin/mocha  --compilers coffee:coffee-script \
		--ui exports \
		--reporter $(REPORTER) \
		--timeout 0 \
		--slow 50000 \
		--ui bdd \
		$(TESTS)
.PHONY: test build

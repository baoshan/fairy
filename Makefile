TESTS = test/*.test.coffee
REPORTER = spec
NAME    := $(shell node -e "console.log(JSON.parse(require(\
  'fs').readFileSync('package.json', 'utf8')).name)")
VERSION := $(shell node -e "console.log(JSON.parse(require(\
  'fs').readFileSync('package.json', 'utf8')).version)")
TARBALL := $(NAME)-$(VERSION).tgz

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

publish:
	@rm -Rf package
	@mkdir package
	@cp -R lib package/lib
	@cp -R web package/web
	@cp package.json package
	@tar czf $(TARBALL) package
	@rm -r package

.PHONY: test build

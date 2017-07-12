# Colossus Documentation Site

### Branches

* `gh-pages` - contains only the compiled site
* `gh-pages-source` - contains only the source files

### Editing the docs

You must have ruby 2.0+ and bundler installed and then run bundler (`bundler` on the command line) to install the correct gems.

To start a localhost server:

`jekyll serve --watch --baseurl=''`

Then browse to localhost:4000

### Publishing

When publishing just run :

`rake site:publish`

This will build the site, merge it into the existing `gh-pages` branch, and push the commit



